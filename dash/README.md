# Monty dashboards — pipeline timeline + anomaly detection

Two Flask-served dashboards over the Monty events table in Snowflake, driven by
the real schema in your export (`PIPELINE_NAME`, `METRIC_NAME`, `METRIC_VALUE`,
`SEVERITY`, `OCCURRED_AT`, `IS_ALERT`, ...).

```
monty/
  app.py                 Flask routes:  /  and  /anomalies  (+ /api/*)
  db.py                  Snowflake fetch (CSV fallback for local dev)
  transform.py           all the logic: builds template context from raw rows
  sql/
    fetch_events.sql     the one query that feeds both dashboards
    anomaly_scores.sql   optional: same detector, computed in Snowflake
  templates/
    timeline.html        your 03_pipeline_timeline mockup, now data-driven
    anomaly.html         your anomaly mockup, now data-driven
  requirements.txt
```

## Quick start — the full dashboard (Snowflake + S3)

Everything is already configured in `dash/.env` (`MONTY_SOURCE=both`, Snowflake
creds, S3 profiles). `app.py` loads `.env` itself, so you only need three steps:

```bash
# 1. one-time: install deps
pip install -r requirements.txt

# 2. every day: refresh the SSO sessions for the two S3 accounts
#    (skip only if you already have a live session; expired -> S3 returns 0 rows
#     with "session has expired")
aws sso login --profile SWEATAnalytics    
aws sso login --profile audiences-dev      # dev  S3 bucket  (account 116981766237)
aws sso login --profile SWEATAnalytics     # prod S3 bucket  (account 534977985440)
aws sso login --profile audiences-dev      # dev  S3 bucket  (account 116981766237)

# 3. run it
cd dash && python3 app.py
python3 app.py
#   http://localhost:5000/            timeline
#   http://localhost:5000/anomalies   anomaly detection
```

**The first page load is slow (~1-3 min) and that is normal, not a hang.**
`both` unions Snowflake (`critical`/`error` + every dbt/auditor row) with S3
(lambda `warning`/`info`). Today's S3 partition is ~34,600 tiny Parquet files
(one per event), so the cold read of that many objects takes minutes. It's
cached afterwards (`MONTY_TTL_S3`, default 60s), so every refresh is ~1s. If you
don't need the S3 `warning`/`info` rows, set `MONTY_SOURCE=snowflake` in `.env`
for an instant (<1s) load.

To change source/behaviour, edit `dash/.env` — it is authoritative and overrides
your shell env on every launch. Restart `python3 app.py` after any `.env` change
(Flask's reloader re-reads `.py` files only, never env vars).

## Run it locally (against the CSV, no Snowflake needed)

```bash
pip install -r requirements.txt
MONTY_SOURCE=csv MONTY_CSV=sample_events.csv flask --app app run
# http://localhost:5000/          timeline
# http://localhost:5000/anomalies anomaly detection
```

## Point it at Snowflake

Set the table and connection, then unset the CSV source:

```bash
export MONTY_TABLE="MONITORING_DB.MONITORING.CUSTOM_METRICS"   # <- your real table
export SNOWFLAKE_ACCOUNT=...  SNOWFLAKE_USER=...  SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_ROLE=...
# SNOWFLAKE_WAREHOUSE is optional — omit it to use the user's DEFAULT_WAREHOUSE.
flask --app app run
```

### Or reuse your existing `snowsql` connection (no secrets in the environment)

If you already have a `[connections.<name>]` block in `~/.snowsql/config`, point
the dash at it instead of exporting credentials:

```bash
export MONTY_SNOWSQL_PROFILE=dev        # reads [connections.dev]
```

It maps `accountname/username/password/warehousename/rolename/dbname/schemaname`
onto the connector. Any `SNOWFLAKE_*` env var still overrides the matching field,
so you can keep the profile and just swap the warehouse or role.

`MONTY_TABLE` in `db.py` is the only thing you must change. To fold these into
your existing app instead of running `app.py`, copy the four routes from
`app.py` and the `templates/` + `sql/` folders across.

## Point the timeline/anomaly events at S3 (Parquet)

The core event source (`db.fetch_events`, feeding the timeline + anomaly
dashboards) can read Parquet from the per-environment metric buckets instead of
Snowflake. Requires `boto3` + `pyarrow` (already in `requirements.txt`) and AWS
credentials via the standard boto3 chain (env keys, `AWS_PROFILE`, or an
instance/task role).

```bash
export MONTY_SOURCE=s3
# bucket is a pattern; {env} is filled from the PROD/DEV toggle:
#   prod -> s3://monty-prod-metrics/run_date=YYYYMMDD/*.parquet
#   dev  -> s3://monty-dev-metrics/run_date=YYYYMMDD/*.parquet
export MONTY_S3_BUCKET="monty-{env}-metrics"   # default; override if named differently
# export MONTY_S3_PREFIX="some/prefix"          # optional, before run_date= (default: root)
# export MONTY_S3_TZ=UTC                         # default; the writer stamps UTC
flask --app app run
```

### Dev and prod live in different AWS accounts

`monty-dev-metrics` is in account `116981766237` and `monty-prod-metrics` is in
`534977985440`, so one credential can't read both. Give the reader a profile per
environment and the PROD/DEV toggle switches accounts with it:

```bash
export MONTY_S3_PROFILE_DEV=audiences-dev     # account 116981766237
export MONTY_S3_PROFILE_PROD=SWEATAnalytics   # account 534977985440
# or, if your profile names share a shape:  export MONTY_S3_PROFILE='monty-{env}'
aws sso login --profile audiences-dev
aws sso login --profile SWEATAnalytics
```

Resolution order: `MONTY_S3_PROFILE_<ENV>` → `MONTY_S3_PROFILE` (a `{env}`
pattern) → the default credential chain (single account / instance / task role,
which is what a deployed dashboard would use).

## The `/segment` page has its OWN source

Segment search introspects `SEGMENT_EVENTS.INFORMATION_SCHEMA`, which only
exists in Snowflake — so it is independent of `MONTY_SOURCE` (where the Monty
*event* rows come from). It has its own switch:

```bash
# force real Snowflake segment data even while events run off CSV/S3:
export SEGMENT_SOURCE=snowflake
```

`SEGMENT_SOURCE` defaults to **Snowflake**. It only uses the local
`segment_sample.csv` when you set `SEGMENT_SOURCE=csv`, OR (for offline laptop
dev) when the whole app is in `MONTY_SOURCE=csv` mode and `SEGMENT_SOURCE` is
unset. The header shows a **SNOWFLAKE** / **CSV SAMPLE** badge so you always
know which one you're looking at.

The reader enumerates every `run_date=YYYYMMDD/` partition overlapping the
lookback window, reads all `*.parquet` objects, and applies the exact
`[start, now]` + `ENVIRONMENT` filter in Python. Rows come back on the same
contract as the Snowflake/CSV paths (naive-UTC `OCCURRED_AT`, `float|None`
`METRIC_VALUE`, `bool` flags).

**Note on the producer split:** the routing is per *producer*, not purely per
severity.

| producer | where its rows land |
|---|---|
| Lambdas (`metric_writer`) | `critical`/`error` → Snowflake · `warning`/`info` → S3 |
| dbt hooks, auditor proc | **all severities → Snowflake** (they run inside Snowflake and cannot write S3) |

So an **S3-only source shows neither failures nor any dbt metric**. The credits
chart and `/segment` page always use Snowflake (they read Snowflake-internal
views with no S3 equivalent), so `SNOWFLAKE_*` creds are still needed for those.

## Complete picture: `MONTY_SOURCE=both`

To see everything, use `both` — the **union** of both stores: Snowflake (lambda
`critical`/`error` **plus every dbt/auditor row, `info` included**) and S3
(lambda `warning`/`info`). Needs both the `SNOWFLAKE_*` and `MONTY_S3_*` config
above:

```bash
export MONTY_SOURCE=both
# ... SNOWFLAKE_* and MONTY_S3_* as above ...
flask --app app run
```

The two stores are disjoint in practice (verified: zero overlapping
`(pipeline, metric, occurred_at)` keys), and rows are deduped on that natural
key anyway, so nothing is double-counted. It is resilient: if one store is
unreachable it's logged and the other still renders, so the dashboard never
fails because a single source hiccuped.

## Performance

A page load makes four Snowflake queries. Two things dominate, and both are
handled in `db.py`:

1. **Connection reuse.** Opening a connection costs ~1.6s of auth handshake, and
   we used to open one per query (~6.5s of pure overhead). There is now a single
   process-wide connection, lock-guarded and transparently reopened if dropped.
2. **Caching the slow, lagging views.** `ACCOUNT_USAGE` lags real time by 1-3h
   and is slow (the credits query measured 1.6-11.8s), so caching it costs no
   freshness. **Live event rows are never cached.**

3. **The credits chart is PAUSED.** Its two `ACCOUNT_USAGE` queries were the
   slowest thing in the app, so they are skipped entirely by default. Nothing
   was deleted — the SQL, the fetch code and the chart are all intact:

   ```bash
   export MONTY_ENABLE_CREDITS=1   # bring the credits chart straight back
   ```

4. **S3 events are the cold-load bottleneck, so they're cached + read in
   parallel.** Historical `run_date=` partitions are one consolidated file each
   (fast), but *today's* partition is thousands of tiny live-write files (~8s to
   read). In `both` mode Snowflake and S3 are fetched **concurrently** (S3 ~8s
   dwarfs Snowflake ~0.8s, so the merge waits on the slower one, not the sum),
   and the S3 read is cached for `MONTY_TTL_S3` so every refresh after the first
   is instant. `MONTY_S3_WORKERS` (default 48) tunes read concurrency.

| env var | default | caches / controls |
|---|---|---|
| `MONTY_ENABLE_CREDITS` | `0` (paused) | — turns the credits chart on/off |
| `MONTY_TTL_S3` | 60s | S3 warn/info events (biggest win) |
| `MONTY_S3_WORKERS` | 48 | parallel S3 object reads |
| `MONTY_TTL_CREDITS` | 300s | hourly warehouse credits |
| `MONTY_TTL_PEAKS` | 1800s | all-time credit peaks |
| `MONTY_TTL_LAST_SEEN` | 120s | pipeline last-seen (ghost lanes) |

Measured on dev `both` mode: **~10s → ~7.8s cold → ~0.6s warm.** The remaining
cold cost is purely the count of un-aggregated files in today's S3 partition —
the real fix is producer-side hourly aggregation, not the dashboard.

Set any to `0` to disable that cache. Measured on dev: **18.0s → 7.2s cold →
0.75s warm.** The Python `transform` layer is ~0.03s — it is never the bottleneck.

## How each dashboard is built

**Timeline.** One 7-day pull. `transform.build_timeline_context` groups events
into "runs" (events a pipeline emits in the same minute = one invocation),
colours each run by its worst severity, and marks alerts. Staleness is inferred
per pipeline: it computes each pipeline's own median gap between runs, and flags
it STALE when the time since its last event exceeds ~3× that cadence. So you
never hard-code "expected hourly" — it learns each pipeline's rhythm.

**Anomaly detection.** For every numeric metric, it scores the latest value
against that metric's own trailing baseline using a **robust z-score**
(median + MAD, not mean + stddev — so one bad point doesn't poison the
baseline). The baseline is a **smooth curve that hugs the data**, consistent
across every chart, in one of three flavours the header badges: **`seasonal ~Nh`**
when the metric is dense over enough full cycles to trust a period (STL
trend + seasonal, band width breathes per phase — z measures "off *for this point
in the cycle*"); **`smooth trend`** when there's no trustworthy cycle but enough
history (a robust rolling smoother that follows the rise/fall instead of a flat
slab); **`flat`** for very short series or if `statsmodels` is missing. The
seasonal gate is deliberately strict (needs raw density over several cycles) so a
thin series never over-fits a fake wave. A point is flagged only when BOTH
`|z| ≥ 3.5` AND the change is `≥ 10%` — the percent gate kills the "far in sigma
but only moved 2%" false alarms. Only the scored latest point is dotted (no
historical red-dot spam). Monotonic watermark metrics (`*.max_date`,
`*watermark*`) are excluded — *freshness* signals, covered by the timeline's
STALE status.

**Sensitivity / the threshold.** The knob is `z_threshold` (robust-σ, *not* a
literal percentile — but they map directly: 2σ ≈ 97.7th pctile → `z≈2.0`, 98th →
`z≈2.05`, default `3.5` ≈ 99.98th). Change the default in `app.py` (`DEFAULTS`),
or tune per request without redeploying: `/anomalies?z=2.05&min_pct=20&days=14`.
The seasonal detector needs `statsmodels>=0.14` (in `requirements.txt`); its
`SEASONAL_*` knobs live at the top of `transform.py`. Full detail in
`MAINTENANCE.md` §8.

## Formatting — pipeline name → family / kind (`formatting.py`)

Both dashboards group and tag pipelines by **name alone** (no config, no lookup
table) using two pure helpers in `dash/formatting.py`. They're called by
`transform.py` at render time, **once per distinct pipeline** as it builds each
timeline lane / anomaly group — `build_timeline_context`,
`build_timeline_range_context`, and `build_anomaly_context`. Pure string in →
string out, so they're cheap and unit-testable in isolation.

**`pipeline_family(name)`** collapses related pipelines into one family so the
dozens of one-off models don't each spawn a lane. First matching rule wins:

| Input pattern | Rule | Example → family |
|---|---|---|
| contains `-ai-` | split on `-ai-`, take the head (lambda ingest stages) | `ai-ingest-postgressql-ai-load-sn` → `ai-ingest-postgressql` |
| starts with a dbt-test prefix (`not_null_`, `unique_`, `accepted_values_`, `relationships_`, `dbt_utils_`, `expect_`) | all tests collapse to one family | `not_null_dim_x_id` → `dbt tests` |
| contains `__` (dbt source segment) | split on `__`, take the head | `stg_braze__email_click` → `stg_braze` |
| first `_`-segment is a dbt layer (`stg`/`int`/`mart`/`dim`/`fct`/`agg`/`base`) | use the layer as the family | `dim_dates` → `dim` |
| otherwise | the name is its own family | `plausible` → `plausible` |
| empty | `—` | |

**`kind(name)`** classifies a pipeline into a source category for its icon/tag:

| Returns | When | Used for |
|---|---|---|
| `aws` | name starts with `ai-ingest` / `ingest` | lambda-ingest icon |
| `dbt` | first segment is `stg`/`mart`/`fct`/`dim`/`int`, or starts with `dbt` | dbt model/test icon |
| `sf` | `plausible`, `iterate`, `auditor_heartbeat`, or name contains `alarm` | Snowflake-native icon |
| `task` | everything else | default tag |

Both are dashboard-only display logic — the observer/Slack alert path does **not**
use them. To change grouping, edit the rule lists (`DBT_LAYERS`,
`DBT_TEST_PREFIXES`) at the top of `formatting.py`; nothing else needs to change.

## Notes / next steps

- The detector currently flags spikes and drops equally. Operationally, **drops**
  usually matter more (a volume drop = something broke upstream). Filter with
  `direction == 'drop'` if you want a drop-only alerting view.
- For a full 24h timeline you need ~24h of live data; the sample export thins out
  near its end, so the trailing-24h view is sparse there. It fills in once live.
- `sql/anomaly_scores.sql` moves the whole detector into Snowflake for when the
  Python-side stats over a large window get heavy. Same output, same thresholds.
- Both templates keep your original CSS, so restyling stays in the HTML.
