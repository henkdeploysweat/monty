# Monty Dashboards — Technical Specification & Maintenance Guide

Version: 1.0 · Covers the pipeline timeline and anomaly detection dashboards
served by Flask over the Monty events table in Snowflake.

---

## 1. Purpose & scope

Monty writes one row per metric observation into a single Snowflake events
table. These two dashboards read that table and present it two ways:

- **Pipeline timeline** (`/`) — a Gantt-style view of pipeline runs over time,
  colour-coded by severity, with staleness detection and alert volume.
- **Anomaly detection** (`/anomalies`) — a robust statistical detector that
  flags metrics whose values deviate from their own recent baseline, using an
  STL seasonal forecast as the expected value when the metric is cyclical
  (auto-falls back to a flat robust median/MAD baseline otherwise).

This document specifies the SQL layer in detail and gives a maintenance runbook
for the whole system. It assumes familiarity with SQL and basic Flask.

---

## 2. Architecture

Data flows in one direction, and each layer has one job:

```
 Snowflake events table
        │   (SQL: sql/fetch_events.sql — one windowed pull)
        ▼
     db.py            fetch_events(lookback_days, env, now)
        │             returns list[dict] with native Python types
        ▼
   transform.py       build_timeline_context(...)         (trailing / day)
        │             build_timeline_range_context(...)   (range aggregate)
        │             build_anomaly_context(...)          (point / period-avg)
        │             — all aggregation, stats, bucketing, tz formatting
        ▼
      app.py          Flask routes: /  /anomalies  /api/*
        │             parses URL params, picks the builder, renders
        ▼
   templates/         timeline.html · anomaly.html
                      Jinja renders context; small JS for interactivity
```

**Design principle:** SQL stays trivial (a windowed `SELECT`), and *all*
aggregation/statistics happen in Python (`transform.py`). This means detector
logic, bucketing, thresholds, and timezone handling can be changed and tested
without touching or redeploying SQL. The one exception is `anomaly_scores.sql`,
an optional in-warehouse version of the detector for when the data outgrows the
pull-into-Python approach (see §4.3).

### 2.1 File map

| File | Responsibility |
|---|---|
| `sql/fetch_events.sql` | The single query that feeds both dashboards |
| `sql/anomaly_scores.sql` | Optional: the anomaly detector, computed in Snowflake |
| `db.py` | Snowflake connection + fetch; CSV fallback for local dev |
| `transform.py` | All aggregation, statistics, bucketing, timezone formatting |
| `app.py` | Flask routes, URL-param parsing, JSON endpoints |
| `templates/timeline.html` | Timeline UI (pickers, drawer, run bars) |
| `templates/anomaly.html` | Anomaly UI (chart, ranking table, click-to-swap) |

---

## 3. Data model — the events table

Every row is one metric observation. Column types are as produced by Monty and
seen in the sample export:

| Column | Type | Notes |
|---|---|---|
| `ID` | INTEGER | Surrogate key, unique per event |
| `PIPELINE_NAME` | STRING | Logical pipeline (`ai-ingest-postgressql-ai-ingest`, `plausible`, `mart_marketing_email_performance`, …) |
| `METRIC_NAME` | STRING | Dotted metric path (`ingest.postgresql.users.rows`, `dbt_model_run`, `unsub_rate_pct_24h`, `cloudwatch_alarm`) |
| `METRIC_VALUE` | FLOAT | Numeric value; **NULL** for non-numeric events (e.g. `cloudwatch_alarm`) |
| `SEVERITY` | STRING | One of `info`, `warning`, `error`, `critical` |
| `RUN_ID` | STRING | Populated for some events (~15% in sample); not relied upon |
| `PAYLOAD` | STRING | JSON blob: watermark info for ingest, full alarm object for alerts |
| `OCCURRED_AT` | TIMESTAMP_NTZ(9) | **Source-local wall-clock** (ingestion writer's zone, `America/Los_Angeles` by default) — *not* UTC. Normalised to UTC in `fetch_events.sql`. |
| `IS_ALERT` | BOOLEAN | True if this event raised an alert |
| `SENT_TO_SLACK` | BOOLEAN | In the sample, 1:1 with `IS_ALERT` |
| `SENT_AT` | TIMESTAMP | When the Slack message went out (nullable) |
| `ENVIRONMENT` | STRING | `prod` or `dev`; every query filters on this |

**Assumptions the code depends on:**

- `OCCURRED_AT` is `TIMESTAMP_NTZ` storing **source-local wall-clock** (the
  ingestion writer's session zone — Pacific / `America/Los_Angeles` in practice),
  because the writer inserts `CURRENT_TIMESTAMP()` from a non-UTC session into a
  zone-less column. `fetch_events.sql` **normalises it to UTC** on the way out
  (via `CONVERT_TIMEZONE(%(src_tz)s, 'UTC', OCCURRED_AT)`), so everything
  downstream *does* receive UTC. The source zone is configurable via
  `MONTY_SOURCE_TZ` (§5.1). All display-timezone conversion then happens at the
  Python layer (§6.4). **Fix at source if you can:** pin ingestion to UTC or
  write `SYSDATE()` instead of `CURRENT_TIMESTAMP()`, then set
  `MONTY_SOURCE_TZ='UTC'` and the conversion becomes a no-op.
- `SEVERITY` values are lowercase strings from the fixed set above. The severity
  ranking (`info=1, warning=2, error=3, critical=4`) is defined in
  `transform.SEV_RANK`.
- A metric is a *freshness/watermark* metric if its name ends in `.max_date` or
  contains `watermark`. These climb monotonically and are excluded from value
  anomaly scoring (they are covered by timeline staleness instead).

---

## 4. The SQL layer (detailed)

### 4.1 `fetch_events.sql` — the primary query

This is the only query that runs in normal operation. It is intentionally a
plain windowed projection:

```sql
SELECT ID, PIPELINE_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY, RUN_ID, PAYLOAD,
       CONVERT_TIMEZONE(%(src_tz)s, 'UTC', OCCURRED_AT) AS OCCURRED_AT,   -- -> UTC
       IS_ALERT, SENT_TO_SLACK,
       CONVERT_TIMEZONE(%(src_tz)s, 'UTC', SENT_AT)     AS SENT_AT,       -- -> UTC
       ENVIRONMENT
FROM {{table}}
WHERE ENVIRONMENT = %(env)s
  AND OCCURRED_AT >= CONVERT_TIMEZONE('UTC', %(src_tz)s,
                        DATEADD('day', -%(lookback_days)s, SYSDATE()))
  AND OCCURRED_AT <= CONVERT_TIMEZONE('UTC', %(src_tz)s, SYSDATE())
ORDER BY OCCURRED_AT;
```

**Placeholders and parameters**

- `{{table}}` — a literal string replaced by `db.py` *before* the query is sent
  (your `MONTY_TABLE`). String interpolation, not a bind parameter, because you
  cannot bind an identifier in Snowflake. Trusted config only (see §7.9).
- `%(env)s` — bind parameter, `'prod'` or `'dev'`.
- `%(lookback_days)s` — bind parameter, integer days of history.
- `%(src_tz)s` — bind parameter, the zone `OCCURRED_AT` is stored in
  (`MONTY_SOURCE_TZ`, default `America/Los_Angeles`).

**Timezone normalisation.** `OCCURRED_AT` is `TIMESTAMP_NTZ` holding
*source-local* wall-clock, so the SELECT converts it to UTC
(`CONVERT_TIMEZONE(src_tz, 'UTC', …)`). The range predicate does the inverse —
it converts the UTC bounds *into* the source zone and compares against the raw
column — so Snowflake can still prune micro-partitions on `OCCURRED_AT` (wrapping
the column in a function would defeat pruning). Once ingestion is fixed to write
UTC, set `MONTY_SOURCE_TZ='UTC'` and both conversions become no-ops.

**The `SYSDATE()` anchor + override.** `SYSDATE()` is always UTC (unlike
`CURRENT_TIMESTAMP()`, which is session-local — the original bug source). By
default the window ends at `SYSDATE()`. For a historical/reproducible window
(archive day, range, tests) `db.py` rewrites `SYSDATE()` to a bound UTC
`%(now)s`. Same query serves live, archive, and range — only the anchor and
lookback change.

**Why one query for both dashboards.** The timeline needs ~24 h of runs but also
~7 days of prior history to *infer each pipeline's cadence* for staleness. The
anomaly detector needs the full baseline window. Pulling 7 days once (the
default `lookback_days`) satisfies both; each Python builder then filters to the
sub-window it cares about. Fewer round-trips, simpler SQL.

**What comes back.** A cursor of rows; `db.py` zips them into `list[dict]` keyed
by column name, with native Python types (Snowflake's connector returns real
`datetime` for TIMESTAMP, `float` for FLOAT, `bool` for BOOLEAN — so no parsing
is needed in prod; the RFC-date parsing in `db.py` is only for the CSV dev path).

### 4.2 Row-volume and cost characteristics

The result set size is roughly `events_per_day × lookback_days`, filtered to one
environment. With the default 7-day window this is bounded and cheap. Practical
guidance:

- The query scans by `OCCURRED_AT` range and `ENVIRONMENT` equality. **Cluster
  or partition the table on `OCCURRED_AT`** (Snowflake auto-clustering or a
  clustering key) so the range predicate prunes micro-partitions. If queries get
  slow, this is the first lever.
- Add `ENVIRONMENT` to the clustering key if you run many environments and
  usually filter to one.
- Snowflake result caching means an identical live query within the caching
  window returns instantly; the `CURRENT_TIMESTAMP()` in the predicate changes
  each second, so live requests won't cache-hit. If you want caching for live,
  round the anchor to the minute in `db.py` before binding `%(now)s`.
- Every page load runs this query. For high traffic, add a short application
  cache (see §7.5).

### 4.3 `anomaly_scores.sql` — optional in-warehouse detector

This computes the **same point-mode ranking** as
`transform.build_anomaly_context`, but in Snowflake, for when the baseline
window is too large to pull into the app. It returns one scored row per numeric,
non-freshness metric. Structure (CTE by CTE):

1. `series` — numeric events in the baseline window, for the environment, with
   freshness metrics excluded via `NOT (name LIKE '%.max_date' OR name LIKE
   '%watermark%')`.
2. `latest` — the newest point per metric (`QUALIFY ROW_NUMBER() … = 1`); this
   is the value being scored.
3. `baseline` — every point *except* that latest one, per metric (so a metric
   isn't compared against itself).
4. `med` — stage 1 of MAD: baseline median and point count, gated by
   `HAVING COUNT(*) >= %(min_points)s`.
5. `mad` — stage 2: `MEDIAN(ABS(value − med))`, the median absolute deviation.
6. `stats` — robust sigma = `MAD / 0.6745` (the constant makes MAD a consistent
   estimator of standard deviation for normal data); drops constant metrics
   where `mad = 0`.
7. Final `SELECT` — z-score, percent change, `direction` (drop/spike), and
   `is_anomaly` = `|z| ≥ z_threshold AND |pct| ≥ min_pct`.

**Important SQL correctness note.** MAD is a *two-stage* aggregation
(`median(|x − median(x)|)`). You **cannot** nest a window `MEDIAN() OVER(...)`
inside an aggregate `MEDIAN()` in a single `GROUP BY` — Snowflake rejects it.
That is why `med` and `mad` are separate CTEs joined on `METRIC_NAME`. If you
ever inline them "to simplify," the query will fail to compile.

**Params:** `%(env)s`, `%(baseline_days)s`, `%(min_points)s`, `%(z_threshold)s`,
`%(min_pct)s` — the same knobs as the Python detector, so results match.

**Range/period-average mode is not implemented in SQL.** The dashboard's range
mode (average over a window vs. prior baseline) exists only in Python. If you
adopt the in-warehouse path and need range mode, replicate the `agg_start/agg_end`
logic from `build_anomaly_context` as an additional query.

### 4.4 Snowflake dialect notes

- `DATEADD('day', -N, ts)` for windowing; `DATEDIFF('second', a, b)` if you port
  cadence logic to SQL.
- `MEDIAN()` is a native aggregate. `QUALIFY` filters on window functions without
  a subquery.
- `%%` in the SQL files is an escaped `%` — the connector uses `%(name)s`
  binding, so literal `%` in `LIKE` patterns must be doubled.
- **Identifier casing:** unquoted names resolve as UPPERCASE. The column names
  here are unquoted uppercase, matching Monty's table. If your objects were
  created quoted-lowercase, you must quote them in `MONTY_TABLE`
  (e.g. `'"db"."schema"."monty_events"'`).

### 4.5 Required Snowflake grants

The connecting role needs:

- `USAGE` on the database and schema containing the events table.
- `SELECT` on the events table.
- `USAGE` on the warehouse named in `SNOWFLAKE_WAREHOUSE`.

A missing grant surfaces as `SQL compilation error: Database '…' does not exist
or not authorized` — the same message you get for a wrong table name, so check
both (see §8).

---

## 5. Configuration reference

### 5.1 Environment variables (read by `db.py`)

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `MONTY_TABLE` | prod | `MONITORING.PUBLIC.MONTY_EVENTS` | Fully-qualified events table — **the main thing to set** |
| `MONTY_SOURCE_TZ` | no | `America/Los_Angeles` | Zone `OCCURRED_AT` is stored in; SQL converts it to UTC. Set to `UTC` once ingestion is UTC |
| `MONTY_SOURCE` | no | `snowflake` | `csv` for local dev; `dynamo` (live warn/info), `s3` (pre-cutover history), `sqlite`, or `both` (union) — see `dash/README.md` |
| `MONTY_CSV` | csv mode | `sample_events.csv` | Path to sample CSV in dev |
| `MONTY_DDB_TABLE` | dynamo mode | `monty-{env}-metrics-ddb` | DynamoDB table pattern; `{env}` filled from the PROD/DEV toggle |
| `MONTY_DDB_REGION` | no | `us-east-1` | Region of the DynamoDB metrics tables |
| `SNOWFLAKE_ACCOUNT` | prod | — | Account identifier |
| `SNOWFLAKE_USER` | prod | — | Username |
| `SNOWFLAKE_PASSWORD` | prod* | — | Password (*or use authenticator) |
| `SNOWFLAKE_AUTHENTICATOR` | no | `snowflake` | e.g. `externalbrowser`, `oauth` |
| `SNOWFLAKE_WAREHOUSE` | no | `MONITORING_WH` | Warehouse to run the query |
| `SNOWFLAKE_ROLE` | no | account default | Role with the grants in §4.5 |

### 5.2 Detector / window knobs (`app.DEFAULTS`)

```python
DEFAULTS = dict(window_hours=24, baseline_days=7, z_threshold=3.5, min_pct=10.0)
```

| Knob | Meaning | Where it bites |
|---|---|---|
| `window_hours` | Timeline trailing window length | Axis span, run bucketing |
| `baseline_days` | Anomaly baseline length + timeline cadence history | Fetch lookback, detector |
| `z_threshold` | Min robust z to flag an anomaly | Anomaly gate |
| `min_pct` | Min percent change to flag an anomaly | Anomaly gate (kills tiny-move noise) |

### 5.3 URL parameters (all optional, all stackable)

| Param | Applies to | Values | Effect |
|---|---|---|---|
| `env` | both | `prod` \| `dev` | Environment filter |
| `tz` | both | `utc` \| `sydney` \| `adelaide` | Display timezone (default UTC) |
| `day` | both | `YYYY-MM-DD` | View a single archive day |
| `from` + `to` | both | `YYYY-MM-DD` | Range mode (aggregate / period-average) |
| `z` | anomalies | float | Override `z_threshold` per request |
| `min_pct` | anomalies | float | Override `min_pct` per request |
| `days` | anomalies | int | Override `baseline_days` per request |

Precedence: `from`+`to` (range) → `day` (archive) → neither (live). Views are
fully URL-driven, so any state is shareable/bookmarkable.

---

## 6. The transform layer (what the SQL feeds)

### 6.1 Timeline — runs, cadence, staleness

- A **run** = a cluster of events a pipeline emitted in the same minute (one
  Lambda/dbt invocation fans out many metrics). Each run is coloured by its worst
  severity and marked if any event alerted.
- **Cadence** is inferred per pipeline as the median gap between *distinct minute
  buckets* over the fetched history (this fixes the "many events share one
  timestamp" fan-out). No cadence is hard-coded.
- **Stale** = time since a pipeline's last event exceeds
  `max(cadence × 3, 2 hours)`. So a pipeline's staleness is judged against its
  own rhythm.

### 6.2 Timeline — range aggregation

For `from`+`to`, the span is tiled into adaptive buckets (`_pick_bucket`: hourly
up to weekly, targeting ≤ ~40 columns). Each pipeline shows one bar per bucket,
coloured by worst severity, with opacity scaled to event volume.

### 6.3 Anomaly detector

- **Point mode (default):** score each metric's latest value against its trailing
  baseline using a robust z (median + MAD, not mean + stddev, so one bad point
  doesn't poison the baseline). Flag when `|z| ≥ z_threshold AND |pct| ≥ min_pct`.
- **Range mode (`from`+`to`):** score each metric's *mean over the range* against
  the baseline formed from points *before* the range. Answers "which metrics ran
  abnormally on average over this window." Requires baseline history before the
  range start.
- **Freshness metrics** (`*.max_date`, `*watermark*`) are excluded from value
  scoring and left to timeline staleness.
- The `min_pct` gate is what prevents "far in sigma but only moved 0.4%" false
  alarms on stable, low-variance metrics.

### 6.4 Timezone handling

Display-only. `transform` localizes every shown timestamp with `zoneinfo`
(`Australia/Sydney`, `Australia/Adelaide`), DST-correct. The database and all
SQL stay in UTC. The active zone's abbreviation (AEST/ACST/…) is shown on the
axis and footer. **Date selection is still by UTC calendar day** — see §9.

### 6.5 Click-to-detail drawer

Each timeline block carries a compact, server-computed event list embedded as
JSON; clicking opens a drawer with the run's events (metric, value, time, and a
payload snippet for warnings/failures/alerts). Detail is capped (≤ 40 events per
run; range buckets keep ≤ 15 alert/fail + ≤ 25 other) to bound page weight.

---

## 7. Maintenance runbook

### 7.1 Point it at a different table
Set `MONTY_TABLE` (env var) or edit the default in `db.py`. Nothing else changes.
Verify grants (§4.5). If the table was created quoted-lowercase, quote it.

### 7.2 Rotate credentials
Update the `SNOWFLAKE_*` env vars. No code change. Prefer a key-pair or OAuth
authenticator over a static password in prod (`SNOWFLAKE_AUTHENTICATOR`).

### 7.3 Tune the anomaly detector
Change `app.DEFAULTS` for the global default, or pass `?z=…&min_pct=…&days=…`
per request. Raise `z_threshold` / `min_pct` to reduce noise; lower to catch
smaller deviations. `.rows`-style batch metrics are naturally volatile — expect
to run them at a higher `min_pct` than rate metrics.

### 7.4 Change the timeline window or add a timezone
Window length: `DEFAULTS['window_hours']`. New timezone: add an entry to
`transform.TZ_MAP` (any IANA zone name) and a toggle button in both templates'
`.tztoggle` blocks.

### 7.5 Performance — query cost and page weight
- **Query:** cluster the table on `OCCURRED_AT` (§4.2). For heavy traffic, wrap
  `db.fetch_events` in a short TTL cache (e.g. 30–60 s) keyed by
  `(env, lookback, anchor-rounded-to-minute)`.
- **Page weight:** the timeline embeds per-block event detail. In a very
  high-volume 24 h window this grows. If it becomes large, move detail to an
  on-demand `/api/run/<id>` endpoint that fetches a block's events on click
  instead of embedding them (the drawer UI stays identical). This is the main
  planned scaling path for the timeline.
- **Detector at scale:** if pulling the baseline window into Python gets heavy,
  switch to `sql/anomaly_scores.sql` (§4.3) and read scored rows directly.

### 7.6 Retention & how far back you can look
Archive day and range views can only reach as far back as the events table
retains `OCCURRED_AT`. Range anomaly mode additionally needs `baseline_days` of
history *before* the range start, so the oldest usable range start is roughly
`retention − baseline_days`.

### 7.7 Local development / testing
Run entirely offline against the sample CSV — no Snowflake needed:

```bash
MONTY_SOURCE=csv MONTY_CSV=sample_events.csv flask --app app run
```

Smoke-test all modes via the Flask test client:

```python
import app
c = app.app.test_client()
for p in ['/', '/?day=2026-07-05', '/?from=2026-07-04&to=2026-07-06',
          '/anomalies', '/anomalies?from=2026-07-06&to=2026-07-09']:
    assert c.get(p).status_code == 200
```

### 7.8 Adding a drop-only anomaly view
Each scored metric has a `direction` of `drop`/`spike`. Operationally, drops
(volume falling = upstream broke) usually matter most. To offer a drop-only view,
filter `anomalies`/`table` on `direction == 'drop'` in `build_anomaly_context`
(or add a `?direction=drop` URL param that filters before ranking).

### 7.9 Security notes
- `{{table}}` is interpolated, not bound. It must come only from config
  (`MONTY_TABLE`), never from a request. Do not wire any user input into it.
- All other values (`env`, dates, thresholds) are bound parameters —
  injection-safe.
- The dashboards are read-only; the Snowflake role should have `SELECT` only.

---

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Database '…' does not exist or not authorized` | Wrong `MONTY_TABLE`, or role lacks grants | Verify the fully-qualified name; grant `USAGE`+`SELECT` (§4.5) |
| Same error but table *does* exist | Objects created quoted-lowercase | Quote in `MONTY_TABLE`: `'"db"."schema"."tbl"'` |
| Timeline sparse / few lanes near "now" | Genuinely little recent data in the window | Expected; fills in with live data or pick a denser day |
| Anomaly range shows 0 tracked metrics | Range starts before any baseline history | Pick a range with prior days behind it (§7.6) |
| `anomaly_scores.sql` won't compile | MAD window nested in aggregate (old version) | Use the corrected two-stage `med`/`mad` CTEs (§4.3) |
| Times look wrong by hours | Reading the UTC labels as local | Use the tz toggle; note date *selection* is UTC-day (§9) |
| Live view never cache-hits | `CURRENT_TIMESTAMP()` changes each second | Round anchor to the minute in `db.py` if caching matters |

---

## 9. Known limitations & design choices

- **Date selection is UTC-day, display is local.** The tz toggle relabels the
  same UTC instants into local wall-clock, but `day`/`from`/`to` still select
  UTC calendar days. Picking "July 5" in Sydney view shows the UTC-July-5 window
  labelled in AEST. To make selection snap to local calendar days, offset the
  anchors in `app.py` by the zone's UTC offset before fetching.
- **Timeline detail is embedded.** Great for instant drawers, but page weight
  grows with event volume; `/api/run/<id>` is the escape hatch (§7.5).
- **Range anomaly needs prior baseline** (§7.6).
- **In-warehouse detector is point-mode only** (§4.3) — range/period-average
  lives only in Python.
- **`SENT_TO_SLACK` is treated as 1:1 with `IS_ALERT`** (true in the sample). If
  that ever diverges, adjust the alert counting in `build_timeline_context`.

---

## 10. Quick "where do I change X?" index

| To change… | Edit… |
|---|---|
| Which table is queried | `MONTY_TABLE` env / `db.py` |
| The query window shape | `sql/fetch_events.sql` |
| Detector thresholds | `app.DEFAULTS` or URL params |
| Detector algorithm | `transform.build_anomaly_context` |
| Freshness exclusion rule | `transform._is_freshness_metric` + SQL `LIKE` filter |
| Staleness rule | `stale_factor` / `stale_floor_s` in `build_timeline_context` |
| Run/bucket sizing | `build_timeline_range_context._pick_bucket` |
| Timezones offered | `transform.TZ_MAP` + template `.tztoggle` |
| Detail drawer contents | `transform._event_detail` + template drawer JS |
| In-warehouse scoring | `sql/anomaly_scores.sql` |
