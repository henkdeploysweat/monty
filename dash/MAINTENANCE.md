# Monty Dashboard — How It Works & How To Maintain It

A maintenance + architecture reference for the `dash/` Flask app. This is the
detailed companion to `README.md` (which is left as the quick-start). Read this
before changing data loading, environment routing, formatting, or the anomaly
detector.

Last updated: 2026-07-15.

---

## 1. What this is

A Flask dashboard over **Monty events** — one row per metric emitted by a
pipeline (ingest jobs, dbt models/tests, Snowflake tasks, Braze syncs, …). It
has two views:

- **Timeline** (`/`, `templates/timeline.html`) — pipelines grouped into
  families, each showing its runs over a 24h (or ranged) window, with status
  (OK / WARN / STALE / DEGRADED / DOWN).
- **Anomaly** (`templates/anomaly.html`) — per-metric statistical outlier
  detection over a trailing baseline.

Everything is driven by **raw event rows**. `db.py` fetches them, `transform.py`
turns them into template context, `formatting.py` decides display names /
families. The dashboard's *look* is fully decoupled from *where the data comes
from* — any source that returns the row contract renders identically.

### The event-row contract

Every row (dict, case-insensitive keys) has:

```
ID, PIPELINE_NAME, METRIC_NAME, METRIC_VALUE (float|None), SEVERITY,
RUN_ID, PAYLOAD (json str), OCCURRED_AT (naive-UTC datetime),
IS_ALERT (bool), SENT_TO_SLACK (bool), SENT_AT (datetime|None), ENVIRONMENT
```

`OCCURRED_AT` / `SENT_AT` are **naive UTC** everywhere downstream. Severities are
exactly `critical | error | warning | info`.

### File map

```
app.py            Flask routes + request-arg parsing; calls db + transform
db.py             data loading: sources, caching, env routing, Braze merge
transform.py      rows -> timeline/anomaly context (the detector lives here)
formatting.py     pipeline display name / family / kind / hide-list
sqlite_store.py   local SQLite cache (events + ingest_log) helpers
loadS3.py         S3 -> SQLite ingest + archive job (CLI)
braze_cdi.py      Braze CDI sync-status -> event rows, SQLite-cached (PROD)
scheduler/        launchd plist + wrapper for the 60s ingest job
sql/              Snowflake query templates (fetch_events, pipeline_last_seen, …)
templates/        timeline.html, anomaly.html, segment.html
test_ingest.py    pytest for sqlite_store + loadS3 (no AWS/Snowflake needed)
monty.db          the local SQLite cache (events, ingest_log, braze_cdi_syncs)
```

---

## 2. How data is loaded (`MONTY_SOURCE`)

`db.fetch_events(lookback_days, env, now)` is the single entry point. It dispatches
on `MONTY_SOURCE` (in `.env`):

| `MONTY_SOURCE` | Reads from | Notes |
|----------------|-----------|-------|
| `snowflake`    | `CUSTOM_METRICS` table | fast (<1s), no S3/SSO |
| `s3`           | live S3 Parquet | slow cold (~34k tiny files/day) |
| `sqlite`       | local `monty.db` events | instant; only as fresh as last ingest |
| `both` (default) | Snowflake **∪** SQLite **∪** live-S3 | see below |
| `csv`          | `sample_events.csv` | offline dev |

### `both` — the production setting

Two producers split by severity:

- **Snowflake** holds every `critical`/`error` row **plus all dbt + auditor
  metrics** (dbt hooks / the auditor proc write directly to the table, so *all*
  their severities including `info` land here).
- **S3** holds only the lambda `warning`/`info` rows (one tiny Parquet per event).

`both` takes the **union** and de-dups on `(PIPELINE_NAME, METRIC_NAME,
OCCURRED_AT, METRIC_VALUE)`. The warning/info side is itself a union of two legs
(controlled by `MONTY_BOTH_WARN_SOURCE`, default `union`):

- **SQLite** — the archived history (fast).
- **live S3** — the recent tail not yet ingested (kept small by the archiver).

Reading both and de-duping means the ingest job moving objects to `archive/` can
**never leave a gap**. If a leg errors (creds, network), it's logged and the
others still render (`db.LAST_SOURCE_ERRORS` drives a "partial data" banner).
Override with `MONTY_BOTH_WARN_SOURCE=s3` (live only) or `sqlite` (cache only).

**On top of any source, for `env='prod'`, Braze CDI sync rows are merged in**
(see §5).

### Caching (`db.CACHE_TTL`)

In-process memoization, keyed to the minute so live views hit it:

| bucket | default TTL | what |
|--------|-------------|------|
| `s3` | 60s | live S3 read |
| `last_seen` | 120s | ghost-lane last-seen query |
| `credits` | 300s | warehouse credits (paused by default) |
| `peaks` | 1800s | all-time credit peak |

The live Snowflake/SQLite event fetch is **not** cached — it's the data the
dashboard is for. `db.clear_cache()` drops everything.

---

## 3. The SQLite cache (`monty.db`)

Written by `loadS3.py --ingest`, read by the `sqlite`/`both` sources. Path =
`MONTY_SQLITE_PATH` (default `dash/monty.db`). Tables:

- **`events`** — raw event rows (PK on `ID`, `INSERT OR IGNORE` = idempotent).
- **`ingest_log`** — one row per consumed S3 key, with an `archived` flag
  (idempotency + archive-retry).
- **`braze_cdi_syncs`** — Braze CDI runs (see §5).

### The ingest + archive job (`loadS3.py`)

```
python3 loadS3.py --env prod --ingest --recent-days 2          # incremental (scheduler)
python3 loadS3.py --env prod --start 2026-07-01 --end 2026-07-14 --ingest   # backfill
python3 loadS3.py --env prod --ingest --recent-days 2 --dry-run             # preview moves
```

Flow: list `run_date=YYYYMMDD/` keys → parallel-read new ones → **serial SQLite
transaction (commit) → parallel copy-to-`archive/` → verify (head) → delete
original**. Ordering guarantees: **DB commits before any S3 delete** (a crash
re-ingests, never loses data); **copy+verify before delete** (a failed copy
never deletes the source). A cross-process **flock** stops overlapping runs.

**Consequence:** archiving *drains* the live `run_date=` partition. Afterward,
`MONTY_BOTH_WARN_SOURCE=s3` only sees the un-ingested tail — history lives in
SQLite + the `archive/` prefix. That's why `both` unions SQLite in.

The `daily_summary` table was removed — the store holds **raw rows only**; the
dashboard aggregates them itself (via `transform.py`), same as every other source.

---

## 4. Environment routing (`db.ENV_ROUTING`)

Maps a **dashboard env** (the `?env=` / PROD-DEV toggle) to the Snowflake
`CUSTOM_METRICS.ENVIRONMENT` value(s) that appear in it:

```python
ENV_ROUTING = {
    "prod": ["prod", "Prod", "default"],   # NULL deliberately excluded
    "dev":  ["dev"],
}
```

- `_snowflake_env_filter()` turns this into the SQL predicate injected as
  `{{env_filter}}` into `sql/fetch_events.sql` and `sql/pipeline_last_seen.sql`.
- Any value not listed anywhere is **dropped**. Rows returned for a dashboard env
  are **relabelled** to it (so folded `default` rows display as one `prod`).
- Use `None` in a list to include `ENVIRONMENT IS NULL`. **We removed `None` from
  prod** because dev pipelines (e.g. `audiences`) have stray NULL rows that then
  leaked into prod as stale ghost lanes.
- `'default'` is the dbt/auditor bucket (100+ dbt test/model node runs).

**Scope:** `ENV_ROUTING` governs the **Snowflake** leg only. The S3/SQLite leg
uses its own rule (`db.py`: `ENVIRONMENT` NULL matches *any* env). Keep that in
mind if NULL-env rows ever appear in S3.

> There are two `fetch_events.sql` files: `sql/fetch_events.sql` (the one `db.py`
> loads) and a **stale, unused** copy in the repo root. Edit the `sql/` one.

---

## 5. Braze CDI sync (`braze_cdi.py`) — PROD ONLY

Pulls Braze CDI sync-job status and shows it as the **`braze-cdisync`** family
with **`delete`** and **`attribute`** lanes.

### Config (env vars in `.env`)

```
BRAZE_REST_ENDPOINT   e.g. https://rest.iad-05.braze.com
BRAZE_API_KEY         REST key with CDI read scope (auth: Bearer)
BRAZE_CDI_BACKFILL_FROM   default 2026-06-01
BRAZE_CDI_TTL             default 60  (seconds to cache the live fetch)
```

The feature is a **no-op** unless endpoint + key are set (`braze_cdi.enabled()`).

### Tracked integrations (matched by exact ID)

```
d94b1797-b7a1-4fc6-be8b-30308ddfbe33  -> delete     (Snowflake Delete Ingestion Prod)
f6683b97-356a-49f5-8044-a510b743bd70  -> attribute  (Snowflake Attribution Ingestion Prod)
```

Matched by `integration_id` in `TRACKED_INTEGRATIONS` — the API name is
"Attribut**ion**", so a name substring match would miss it.

### Flow

1. **Backfill** once: on first run, every run since `BACKFILL_FROM` is stored.
2. **Live on every load**: `db.fetch_events(env='prod')` calls
   `braze_cdi.fetch_events()` → hits `/cdi/integrations/{id}/job_sync_status` for
   both IDs → **upserts** new runs into `braze_cdi_syncs` (dedup on
   `integration_id + sync_start_time`) → reads the window back from SQLite.
3. A **60s cache** (`BRAZE_CDI_TTL`) stops rapid reloads hammering Braze.
4. If the API call fails, it logs and **serves the stored SQLite rows** — a Braze
   outage never blanks the lanes.

### Severity mapping (`_severity_for`)

| sync outcome | severity | color |
|--------------|----------|-------|
| synced **with rows** (`rows_synced > 0`) | `info` | green / success |
| synced **0 rows** | `warning` | yellow |
| **didn't sync** (`job_status` not a success value) | `error` | red (alerts) |

`METRIC_VALUE = rows_synced`, `METRIC_NAME = cdi_sync`,
`PIPELINE_NAME = braze-cdi-<type>`, `ENVIRONMENT = prod`. Only `error` sets
`IS_ALERT`.

### Backfill caveat

The `job_sync_status` endpoint returns only the ~recent runs (no date-range
param). So the first run stores whatever it returns (all ≥ `BACKFILL_FROM`);
**older history can't be pulled retroactively** — it accumulates in SQLite as the
dashboard keeps upserting. If Braze exposes a paginated/date-ranged history
endpoint, wire it into `fetch_runs_from_api()` for a true backfill.

---

## 6. Formatting (`formatting.py`)

Pure functions: name string in → string out. Called per pipeline by
`transform.py`. `transform` passes a combined `"<payload_json>||<metric_name>"`
so the formatter can read the dbt model out of the payload.

- **`pipeline_name(name, payload)`** — the lane's display name (**level 3**).
- **`pipeline_family(name, payload)`** — the family the lane sits under (**level 2**).
- **`pipeline_group(family)`** — the group the family sits under (**level 1**, see below).
- **`kind(name)`** — `aws` / `dbt` / `sf` / `task` (drives the icon).
- **`remove_from_dashboard(name)`** — the hide-list.

### Three-level grouping (group → family → lane)

The timeline nests **group → family → lane**. Level 2 (family) is
`pipeline_family`; level 1 (group) is `pipeline_group(family) -> (label, named)`,
which collapses related families into one collapsible, colour-coded header:

| family matches | group label | `named` |
|---|---|---|
| starts `dbt_snowflake_transformation` | **Dbt Snowflake Transformations** | yes |
| starts `sweat_analytics_core_dbt` | **SweatAnalyticsCoreDBT** | yes |
| contains `postgres` | **Postgres** | yes |
| contains `plausible` | **Plausible** | yes |
| anything else | the family name, noise-stripped | no |

**Display names** — ONLY group labels get cleaned; **family and pipeline names
keep their exact original format**. `prettify` (the Jinja `pretty` filter in
`app.py`) strips only the `ai-ingest-` / `_load` noise — no case change, no
`_`/`-` removal: `ai-ingest-audiences` → `audiences`, but
`braze_cdi_attribute_sync` stays `braze_cdi_attribute_sync`. Group labels in
`NAMED_GROUPS` are hand-cased final strings shown verbatim. All three row levels
(group / family / lane) share **one 24px icon** (`.pipe-name/.fam-name/.grp-name
.ico`). `dim` and `int` (the bare stale dbt families) are on the hide-list.

The **same groups drive the anomaly page**: `build_anomaly_context` keys its
metric-table top level on `pipeline_group(family)` too, so Postgres / Dbt
Snowflake Transformations / … collapse there exactly as on the timeline.

`named=True` families with ≥2 members render a group header with the families
nested + collapsible under it (`transform._supergroups` rolls their counts +
merged track up). Everything else is a standalone passthrough whose label is the
family name run through `_simplify_family` — strip the `ai-ingest-` prefix and
`_load` suffix: `ai-ingest-iterate` → `iterate`, `appsflyer_load` → `appsflyer`.
**To add/retune a group:** edit `NAMED_GROUPS` (or `_simplify_family`) in
`formatting.py`. Groups + families both start collapsed; the header's
"Expand all / Collapse" drives both levels. All rows are flat grid siblings —
nesting is by `data-in-grp` / `data-in-fam`, not DOM — so collapse is class-based
(`gcollapsed` for group, `collapsed` for family).

### dbt resolution (`_dbt_identity`)

dbt rows carry the real model in the payload's `unique_id`
(`<resource>.<project>.<model>`):

- `PIPELINE_NAME == 'dbt_run_failures'` → `failures[0].unique_id`
- `METRIC_NAME == 'dbt_model_run'` → root `unique_id`

So a failure collector row displays as the **actual failed model** and groups
under `<project>_<layer>` (e.g. `dbt_snowflake_transformation_stg`).
`clean_and_parse_json` parses the raw JSON **first** (unescaping only as a
fallback) — unescaping first corrupts a `\"` inside an error message and breaks
the parse (that bug made failures show as raw `dbt_run_failures`).

### Special cases

- **appsflyer** (`PROJECT_PREFIXED`) — its bare model names aren't distinctive,
  so name = `appsflyer_load_<model>` and family = `appsflyer_load`. Add a
  project prefix to `PROJECT_PREFIXED` to give another project the same
  treatment.
- **Braze CDI** — `braze-cdi-*` → family `braze-cdisync`, name = the suffix
  (`delete`/`attribute`), kind `aws`.
- **`DBT_LAYERS` / `DBT_TEST_PREFIXES`** — control layer folding (`stg_*` →
  family layer `stg`) and the "dbt tests" bucket.

### Hide-list

```python
REMOVE_FROM_DASHBOARD = { "some-pipeline", "a family", "a_model" }
```

Matched **case-insensitively** against a pipeline's raw name, formatted name,
**and** family — so you can hide one pipeline, a whole family, or a model.
Applied in `transform.py` (both timeline builders + anomaly).

---

## 7. Timeline dashboard

- **Lanes** are keyed by raw `PIPELINE_NAME`; each gets a display name + family
  (via formatting) + kind + status.
- **Families** (`_lane_groups`) group lanes by their formatted family; each group
  carries a merged run track (so a collapsed family still shows activity) and a
  `kind` (dominant lane kind). **Families and lanes are sorted alphabetically**
  and **start collapsed** — click a header or "Expand all".
- **Status** per lane: `DOWN` (critical) › `DEGRADED` (error) › `STALE` (silent
  past its cadence) › `WARN` (warning) › `OK`.
- **Ghost / stale lanes** — a pipeline that emitted nothing in the window but ran
  within the retention horizon (`PIPELINE_RETENTION_WEEKS`, default **14 weeks**)
  is kept visible as an empty STALE row, so a dead pipeline is *seen*, not
  silently dropped. Their last-seen comes from `fetch_pipeline_last_seen`
  (Snowflake grouped-MAX, merged with the SQLite cache for `both`).

---

## 8. Anomaly dashboard — the detector in full

Lives in `transform.build_anomaly_context`. This is the part you asked about most.

### The series

Every **`(METRIC_NAME, PIPELINE_NAME)`** pair is one series of `(timestamp,
METRIC_VALUE)` points over the baseline window. Keying on the pipeline too is
essential — dbt emits `dbt_model_run` for every model, so metric-alone would
merge unrelated models into one meaningless series.

### What defines an anomaly

Point mode (default — scores each metric's **latest** value):

1. `baseline` = all points **before** the latest; `observed` = the latest value.
2. `center` = the **expected** value the observed point is compared against —
   the seasonal forecast when a cycle is found, else the flat `median(baseline)`
   (see "Cyclical baseline" below).
3. `robust_sd = MAD / 0.6745` of the residuals `(value − center)` (MAD → σ for a
   normal dist — so one bad point doesn't poison the spread).
4. `z = (observed − center) / robust_sd` and `pct = (observed − center) / |center| × 100`.
5. **Anomaly iff** `|z| ≥ z_threshold` **AND** `|pct| ≥ min_pct`.

Defaults (`app.DEFAULTS`, overridable via URL args; UI slider ranges/clamps in
`app.DETECTOR_LIMITS`):

| param | default | meaning |
|-------|---------|---------|
| `baseline_days` | 7 | how much history the baseline uses |
| `z_threshold` (`z`) | **3.5** | how many robust-σ the latest must be off |
| `min_pct` | **10.0** | and at least this % off the center (kills tiny-abs noise) |
| `min_points` | **8** | minimum observations before a series is scored |
| `window_hours` | 24 | timeline window (not the anomaly baseline) |

Range mode (`agg_start`/`agg_end` set): scores the **mean over the selected
range** against the baseline formed from points **before** the range — "which
metrics ran abnormally on average over this window".

### 🎚️ Where to change the threshold (the "98th percentile")

The sensitivity knob is **`z_threshold`**, expressed as robust-σ, **not** a
literal percentile. For a normal distribution the two map directly, so pick `z`
for the percentile you want to flag beyond (two-sided — it flags both spikes and
drops):

| Flag beyond… (two-sided) | one-tailed pctile | set `z` ≈ |
|---|---|---|
| 2σ (the old rule) | **97.7th** | **2.0** |
| **98th percentile** | 98th | **2.05** |
| 99th percentile | 99th | 2.33 |
| current default | 99.98th | **3.5** |

Change it in **one** of these places:

- **Default for everyone:** `z_threshold=3.5` in `DEFAULTS`, `dash/app.py:58`
  (restart Flask after editing). Bounds/slider step live in `DETECTOR_LIMITS`
  (`dash/app.py:64`).
- **One-off, no redeploy:** append `?z=2.05` to the URL, e.g.
  `/anomalies?z=2.05&min_pct=10&days=14`, or drag the **z slider** on the page.

Note the **`min_pct` gate still applies** — a point must be past `z` *and* at
least `min_pct` % off the center, so lowering `z` alone won't surface trivial
moves.

### Smooth baseline — trend, or seasonal (`transform._baseline_curve`)

The `center` in step 2 is a **smooth curve that hugs the data**, not a flat
line — and it's consistent across every chart. One of three flavours per metric:

1. **`seasonal ~24h`** — the metric is dense over enough full cycles to trust a
   period: STL `trend + seasonal`, with a band whose width **breathes** (per-phase
   σ). A value high *for 3am* trips; a normal daily peak doesn't.
2. **`smooth trend`** — no trustworthy cycle, but enough history: a robust
   rolling smoother (median → mean) gives a centre that follows the rise/fall,
   with a steady-width band. Replaced the old flat slab so trending metrics look
   like the cyclical ones (smooth centre + band), just without the waves.
3. **`flat`** — very short series (`< TREND_MIN_POINTS`) or `statsmodels` missing:
   the classic flat median/MAD band.

Pipeline (`_baseline_curve`): **resample** to an even grid (`_resample_even`,
step = median gap, capped by `SEASONAL_MAX_GRID`) → **auto-detect period**
(`_detect_period`, ACF peak ≥ `SEASONAL_ACF_MIN`) → **decide seasonal** only if
dense enough (see gate below) → STL or rolling smoother → **per-phase σ** with a
floor (`SEASONAL_SIGMA_FLOOR`, so no hairline bands) → interpolate the curve back
onto the real timestamps for scoring. The chart header badges the flavour.

**Why the seasonal gate is strict** (this fixed the "29-point metric drawn as a
confident 24h wave, covered in red dots but HEALTHY" over-fit): a cycle is
trusted only when ALL hold —

| gate | default | meaning |
|---|---|---|
| `TREND_MIN_POINTS` | 16 | below this → flat; at/above → at least a smooth trend |
| `SEASONAL_MIN_POINTS` | 42 | raw points before a cycle is even considered |
| `SEASONAL_MIN_PERIODS` | 3 | need ≥ this many full cycles in the data |
| `SEASONAL_MIN_PTS_PER_CYCLE` | 6 | and ≥ this many **raw** points per cycle (density) |
| `SEASONAL_ACF_MIN` | 0.35 | min autocorrelation to accept a detected period |
| `SEASONAL_SIGMA_FLOOR` | 0.5 | per-phase σ can't drop below this × overall σ |
| `SEASONAL_MAX_GRID` | 720 | cap on resampled points per metric (bounds STL cost) |
| `CURVE_RENDER_POINTS` | 120 | dense points kept for the smooth drawn band |

To make **more** metrics go seasonal: lower `SEASONAL_MIN_PTS_PER_CYCLE` /
`SEASONAL_ACF_MIN`, or widen `baseline_days` (a weekly cycle needs ≈ 28 days — 7
days has ~1 sample per weekday, so the default window only learns the *daily*
rhythm). **Dependency:** `statsmodels>=0.14` (`dash/requirements.txt`, pulls
scipy); absent → `flat` only.

**Red dots:** only the **scored latest point** is dotted, and only when it's the
anomaly — historical out-of-band pokes are no longer peppered across the chart
(that made healthy metrics look alarming).

### When a pipeline (metric) becomes available in the anomaly view

A series is **skipped** (won't appear at all) if any of:

- **Too few points** — `< min_points` (default 8) in the baseline window. A new
  pipeline, or one that runs rarely, simply hasn't got enough history yet.
- **Freshness/watermark metric** — `_is_freshness_metric()` (monotonic
  timestamps/watermarks that always climb) — skipped, they'd false-positive
  constantly.
- **Constant / near-constant** — `robust_sd == 0` (i.e. `MAD == 0`). If the
  baseline's median absolute deviation is 0, there's no variation to score.
  Because MAD is **robust** (median-based), a series that is mostly one value
  with a rare spike still has `MAD == 0` and is skipped.

**Practical consequences (this is why "not everything is there yet"):**

- It genuinely needs data — **≥ 8 runs of a numeric metric** within
  `baseline_days`, accumulating over time.
- Mostly-constant metrics never score. Example: **Braze CDI** — the `delete` sync
  is always 0 rows (constant → skipped); `attribute` is 0 except a rare spike
  (MAD 0 → skipped). The rows are persisted and fed to the detector, but robust
  stats won't flag near-constant counts by design. To make such metrics
  participate you'd detect on **failures / `rows_failed`**, or add a non-robust
  (std-based, or "0 when usually > 0") rule.
- Only numeric `METRIC_VALUE` metrics are scored; pure event/text metrics aren't.

### Chart confidence bands (the "fan")

The chart draws a **nested fan** of confidence bands around the expected value —
a tight inner band and wider outer ones — so you can eyeball how far off a point
is, plus the detection threshold as the outer **dashed ALERT** boundary.

When the metric is **cyclical**, the envelope is a **smooth "prediction
envelope"** (mockup-style), not a flat band: it is drawn as a Catmull-Rom spline
on the dense STL grid (`CURVE_RENDER_POINTS` points, default 120), and its
half-width **breathes with the cycle** — narrow where the metric is predictable
for that phase, wide where it's normally noisy. That width comes from a
**per-phase robust σ** (`_seasonal_expected` buckets STL residuals by position in
the cycle, MAD→σ per phase, circular-smoothed). The latest value is scored
against **its own phase's σ** (`sigma_latest`), so "is 3am unusually low"
uses 3am's normal spread, not the all-hours average. Non-cyclical metrics keep
the flat constant-width band. The observed line stays angular either way.

Edit the list to add/widen bands (`CONFIDENCE_BANDS` near the top of
`dash/transform.py`):

```python
CONFIDENCE_BANDS = [("90%", 1.645), ("95%", 1.960)]   # (label, σ-multiplier)
#  80% -> 1.282   90% -> 1.645   95% -> 1.960   99% -> 2.576
```

Add `("99%", 2.576)` for a wider band, drop an entry to simplify. Opacity deepens
inward automatically; the `z_threshold` alert band is always drawn on top. Restart
Flask after editing.

### Sensitivity + drift

The page also shows a **sensitivity curve** (how many metrics would trip at other
`z` thresholds — free, computed from the already-scored set) and a **drift
inspector** (top movers by `|pct|`). Loosening `z`/`min_pct` surfaces more; these
help you see the cost before applying it.

---

## 9. What needs to be scheduled

| Job | Cadence | How | Failure mode |
|-----|---------|-----|--------------|
| **S3 → SQLite ingest** | every 60s | `scheduler/com.monty.ingest.prod.plist` (launchd) → `run_ingest.sh prod` (`--ingest --recent-days 2`) | if it stops, the SQLite cache goes stale; `both` still fills the gap from live S3 (slower) |
| **AWS SSO login** | ~every 8–12h (token expiry) | `aws sso login --profile SWEATAnalytics` (prod) / `audiences-dev` (dev) | S3 legs return 0 rows / "session expired"; ingest ticks fail-and-log, dashboard serves last SQLite data |
| **Braze CDI** | none — **live on load** | automatic in `fetch_events` (60s cache) | API down → serves stored SQLite rows |
| **Snowflake auth** | token in `.env` has an `exp` | refresh `SNOWFLAKE_PASSWORD` when it expires | queries fail auth |

Install the ingest scheduler (one-time), then run a backfill so the timeline has
its 7-day baseline — full steps in **`scheduler/README.md`**. To ingest **dev**
too, copy the plist with the `dev` arg and log into `audiences-dev`.

There is **no separate anomaly job** — anomalies are computed on each page load
from whatever `fetch_events` returns. "More data over time" comes from the ingest
job + Braze upserts filling SQLite, plus Snowflake accumulating naturally.

---

## 10. How to maintain — common tasks

- **Hide a pipeline/family/model:** add its name to `REMOVE_FROM_DASHBOARD` in
  `formatting.py`.
- **Route a Snowflake env somewhere:** edit `db.ENV_ROUTING`.
- **Add a project that needs `project_model` naming:** add its prefix to
  `PROJECT_PREFIXED` in `formatting.py`.
- **Add/track another Braze CDI integration:** add its ID → `(type, name)` to
  `TRACKED_INTEGRATIONS` in `braze_cdi.py`.
- **Tune anomaly sensitivity (the "98th-percentile" threshold):** change
  `z_threshold` in `DEFAULTS`/`DETECTOR_LIMITS` in `app.py` (2σ≈`z=2.0`,
  98th≈`z=2.05`), or pass `?z=&min_pct=&min_points=&days=` on the URL. See §8
  "Where to change the threshold".
- **Tune the cyclical detector:** edit the `SEASONAL_*` constants in
  `transform.py` (§8 "Cyclical baseline"); remove `statsmodels` to force the flat
  baseline.
- **Add/widen chart confidence bands:** edit `CONFIDENCE_BANDS` in `transform.py`
  (§8 "Chart confidence bands"), e.g. add `("99%", 2.576)`.
- **Change ingest cadence:** edit `StartInterval` in the launchd plist.
- **Run the tests:** `python3 -m pytest test_ingest.py -q` (no AWS/Snowflake).

### ⚠️ You must restart the Flask app after changing `.py` files or `.env`

Module-level constants (`ENV_ROUTING`, formatting rules, env vars) are read **once
at import**. The running process does **not** reliably hot-reload them. Almost
every "I don't see my change" this project has hit was a stale process — restart
it.

---

## 11. Gotchas

1. **Stale app** — see above. Restart after edits.
2. **Timezone** — the laptop is Sydney (UTC+10); Python `logging` stamps **local**
   time while all data/anchors are **UTC**. A log saying `00:26` can be `14:26`
   UTC. When something looks "stale", check real UTC with `SELECT SYSDATE()`
   before assuming a gap. `datetime.utcnow()` matches Snowflake `SYSDATE()`
   (~0 skew) — the clock is fine, the *logs* are local.
3. **SSO expiry** silently stalls the S3 ingest — re-`aws sso login`.
4. **`%(name)s` in a `.sql` comment** breaks the Snowflake param binder (it scans
   comments). If you inject a value as a `{{placeholder}}`, don't also leave a
   `%(name)s` mention in the file's comments.
5. **Archiving is destructive on S3** (copy→verify→delete). Always `--dry-run`
   first on prod.
6. **`.env` holds plaintext secrets** (Snowflake token, Braze key). Never commit
   it; rotate anything that leaks.
7. **Two `fetch_events.sql`** — `db.py` loads `sql/fetch_events.sql`; the root
   copy is stale/unused.

---

## 12. Ingestion & scheduling — deep dive / runbook

This section is the operational detail behind §3, §5 and §9: exactly how each
kind of data gets **into** the system, what runs it, on what cadence, and how to
install / verify / recover it.

### 12.1 The three ingestion paths

The dashboard shows one merged stream, but data arrives three different ways:

| # | Source | Ingestion style | Where it lands | Trigger |
|---|--------|-----------------|----------------|---------|
| 1 | **Snowflake** `CUSTOM_METRICS` | none — queried live | (stays in Snowflake) | every page load |
| 2 | **S3** warning/info Parquet | **batch job** → SQLite, then **archive** | `monty.db` `events` | scheduled (launchd, 60s) + on-demand backfill |
| 3 | **Braze CDI** sync status | **live API pull** → upsert SQLite | `monty.db` `braze_cdi_syncs` | every prod page load (60s-cached) |

Only **#2** needs an external scheduler. #1 is pull-on-demand; #3 schedules
itself inside `fetch_events`.

### 12.2 Path 2 — S3 → SQLite ingest (the scheduled one)

**Why it exists.** The lambda writer emits **one tiny Parquet file per event**
(~34k/day). Reading them live is minutes-slow. The ingest job drains them into
SQLite once and **moves the consumed objects to `archive/`**, keeping the live
partition tiny and the dashboard fast.

**The command** (`loadS3.py`):

```bash
# incremental (what the scheduler runs) — last 2 UTC days, covers midnight rollover
python3 loadS3.py --env prod --ingest --recent-days 2

# one-time backfill of history (explicit range)
python3 loadS3.py --env prod --start 2026-07-01 --end 2026-07-14 --ingest

# preview — logs what WOULD move, touches nothing
python3 loadS3.py --env prod --ingest --recent-days 2 --dry-run
```

Relevant flags: `--env dev|prod` (required, picks the bucket + SSO profile),
`--ingest` (ingest mode), `--recent-days N` (window = last N UTC days ending
today; default 2), `--start/--end` (explicit backfill window), `--dry-run`
(no S3 writes), `--db` (override `MONTY_SQLITE_PATH`).

**What one run does, in order:**

1. **List** `run_date=YYYYMMDD/` keys for each day in the window (skips `archive/`).
2. **Read** new keys (not already in `ingest_log`) in parallel into memory.
3. **Persist** — a single serial SQLite transaction inserts the event rows +
   an `ingest_log` row per key, then **commits**.
4. **Archive** — in parallel, for each consumed key: **copy** to
   `<prefix>archive/run_date=…/`, **verify** with `head_object`, **delete** the
   original, mark `ingest_log.archived=1`.
5. Log a summary: `{listed, ingested_keys, rows_inserted, archived}`.

**Safety guarantees (why this ordering matters):**

- SQLite **commits before any S3 delete** → a crash re-ingests, never loses data.
- **Copy + verify before delete** → a failed copy never deletes the source.
- A key ingested but not archived (delete failed) is **retried** next run
  (`ingest_log.archived=0`).
- A **cross-process `flock`** (`<db>.lock`) means a slow run and the next 60s
  tick can't collide — the second one skips cleanly.
- Idempotent: `events` PK on `ID`, `INSERT OR IGNORE`; re-running never
  double-counts. `rows_inserted: 0` on a re-run is **correct**, not a bug.

**Idempotency example.** If you see `{listed: 364, ingested_keys: 0,
rows_inserted: 0, archived: 364}`, an earlier run already inserted those rows and
committed; this run just finished the pending archive. Check `monty.db`:
`SELECT COUNT(*) FROM events;` and `SELECT SUM(ROW_COUNT) FROM ingest_log;` should
match.

### 12.3 The scheduler (macOS launchd)

Two files under `scheduler/`:

- **`run_ingest.sh <env>`** — wrapper. Sets `PATH` (so `aws` resolves for
  boto3's SSO cache), `cd`s to `dash/`, runs
  `python3 loadS3.py --env <env> --ingest --recent-days 2`, appends to
  `dash/ingest.<env>.log`. Uses the pinned interpreter
  `/opt/homebrew/opt/python@3.11/bin/python3.11` (the one with `boto3`/`pyarrow`).
- **`com.monty.ingest.prod.plist`** — launchd agent. `StartInterval = 60`
  (every 60s), `RunAtLoad = true`, arg `prod`, launch errors →
  `dash/ingest.launchd.log`.

**Install (one-time):**

```bash
cp scheduler/com.monty.ingest.prod.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.monty.ingest.prod.plist   # RunAtLoad fires immediately
```

**Operate:**

```bash
launchctl list | grep com.monty.ingest        # is it registered?
tail -f ingest.prod.log                        # per-run output (the [i/total] logs)
tail -f ingest.launchd.log                     # launchd-level launch errors
launchctl unload ~/Library/LaunchAgents/com.monty.ingest.prod.plist   # stop
launchctl load   ~/Library/LaunchAgents/com.monty.ingest.prod.plist   # start
```

**cron alternative** (if you prefer): `* * * * *
/Users/henkduplooy/Documents/Berg/Monty/dash/scheduler/run_ingest.sh prod`.

**Add a dev job:** copy the plist to `com.monty.ingest.dev.plist`, change the
`Label` and the `prod` arg to `dev`, and keep a live `audiences-dev` SSO session.
Both jobs share `monty.db` (rows tagged by `ENVIRONMENT`) and the same run-lock.

**Why 60s?** CDI/ingest data is low-latency and the archive keeps each tick's
work tiny (only ~1 minute of new files, since the rest is already archived). 60s
gives near-real-time freshness at negligible cost. Change it via `StartInterval`.

### 12.4 The hard dependency — AWS SSO

The ingest reads S3 with per-account **SSO profiles** (dev and prod live in
**separate AWS accounts**): `SWEATAnalytics` (prod), `audiences-dev` (dev). SSO
tokens expire (~8–12h) and **cannot refresh non-interactively**. When expired:

```bash
aws sso login --profile SWEATAnalytics      # prod bucket
aws sso login --profile audiences-dev       # dev bucket
```

Until you re-login, ingest ticks **fail and log** (`session has expired` /
`0 objects`) and the dashboard serves the **last-ingested SQLite data** (goes
stale, never blank — `both` also falls back to whatever live S3 it can read).
This is the single most common reason the cache stops updating.

### 12.5 Path 3 — Braze CDI (self-scheduling, no launchd)

No external job. `db.fetch_events(env='prod')` calls `braze_cdi.fetch_events()`
on **every prod load**, which:

1. `refresh()` — if > `BRAZE_CDI_TTL` (60s) since the last fetch, pull both
   integrations' `job_sync_status`, **upsert** new runs into `braze_cdi_syncs`
   (dedup on `integration_id + sync_start_time`, only ≥ `BRAZE_CDI_BACKFILL_FROM`).
2. Read the window back from SQLite and map to event rows.
3. On API error: log, **serve stored SQLite rows** (never blanks lanes).

So Braze is "live on every load" but rate-limited to once per 60s, and persisted.
The **only** thing to keep valid is `BRAZE_API_KEY` in `.env` (and endpoint). No
cron needed. (If you *want* it decoupled from page loads, you could add a launchd
job calling `python3 -c "import braze_cdi; braze_cdi.refresh(force=True)"` — not
currently done.)

### 12.6 End-to-end data lifecycle

```
producers ──► Snowflake CUSTOM_METRICS ─────────────────────────┐
  (lambda crit/error, dbt, auditor)                             │  live query
                                                                ▼
lambda warn/info ──► S3 run_date=…/*.parquet                 db.fetch_events ──► transform ──► dashboard
                          │  loadS3 --ingest (60s launchd)       ▲       ▲
                          ├─► monty.db `events` ─────────────────┘       │
                          └─► S3 archive/run_date=…/ (consumed)          │
                                                                         │
Braze CDI API ──► braze_cdi.fetch_events (live, 60s cache) ──► monty.db `braze_cdi_syncs` ─┘
```

### 12.7 Freshness & retention summary

| Data | Freshness bound | Retained where |
|------|-----------------|----------------|
| Snowflake (crit/error + dbt) | live (~1s) | Snowflake (source of truth) |
| S3 warn/info | ≤ ~1 min behind (60s ingest) + `MONTY_TTL_S3` 60s cache | `monty.db` `events` + S3 `archive/` |
| Braze CDI | ≤ 60s (`BRAZE_CDI_TTL`) | `monty.db` `braze_cdi_syncs` |
| Ghost/stale lanes | last-seen up to `PIPELINE_RETENTION_WEEKS` (14w) | derived per load |

SQLite rows are **never auto-deleted** — `events` and `braze_cdi_syncs` only grow
(via `INSERT OR IGNORE` / upsert), which is what gives the anomaly detector its
baseline. If `monty.db` ever needs pruning, that's a manual `DELETE … WHERE
OCCURRED_AT < …` — there is no retention job today.

### 12.8 Scheduling troubleshooting checklist

1. **Cache not updating?** → `tail ingest.prod.log`. `session has expired` →
   `aws sso login --profile SWEATAnalytics`.
2. **Nothing in the log at all?** → `launchctl list | grep com.monty.ingest`; if
   missing, re-`load` the plist. Check `ingest.launchd.log` for a bad path /
   wrong Python.
3. **`rows_inserted: 0`** → usually correct (idempotent re-run). Verify with the
   `events` count vs `SUM(ingest_log.ROW_COUNT)`.
4. **Dashboard stale but log looks fine?** → the **app** is stale, not the data —
   restart Flask (§10). Or you misread a **local-time** log stamp as UTC (§11.2).
5. **Braze lanes not updating?** → check `BRAZE_API_KEY`/`BRAZE_REST_ENDPOINT` in
   `.env` and that the app was restarted after setting them; a failed pull logs
   `braze cdi: live fetch failed, serving SQLite`.
