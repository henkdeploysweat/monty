# Onboarding a new ingest pipeline to the dashboard

**Read this when a new `ai-ingest-*` (or any warn/info-emitting) pipeline starts
writing to DynamoDB and you want it on the timeline + anomaly pages.**

If you skip these steps the new pipeline is **silently missing** — no error, it
just never appears. That is the deliberate cost of the fast cold-start
(`MONTY_DDB_PIPELINES`, see §5).

> **You do NOT need this for new dbt models.** Those come from Snowflake and
> auto-appear. This guide is **only** for pipelines whose warn/info metrics land
> in DynamoDB (`monty-<env>-metrics-ddb`) via `metric_writer` / `dynamo_writer`.

---

## TL;DR

```bash
cd dash

# 1. find the new pipeline's exact name (see §2 if you don't know it)
# 2. add it to the MONTY_DDB_PIPELINES line in .env  (comma-separated, no spaces)
# 3. build its hourly rollup history
python3 rollup.py --env prod --days 14

# 4. restart the app
flask --app app run --port 5011
```

Then repeat step 1-3 for **dev** if the pipeline also runs there
(`--env dev`). Verify with §4.

---

## 1. Why this is needed

The dashboard reads DynamoDB with a **Query per pipeline**, so it must be told
which pipelines exist. Discovering them live costs a ~51s full-table Scan on
every cold start, so we hard-code the list in `MONTY_DDB_PIPELINES` instead. A
pipeline not on that list is never queried — and its hourly rollup is never
built — so it is invisible on both pages.

Two independent things must therefore know about the new pipeline:

| what | where | if missing |
|---|---|---|
| the **read list** | `MONTY_DDB_PIPELINES` in `dash/.env` | pipeline never queried → no lane, no metrics |
| the **rollup history** | `rollup.py` backfill | lane/metrics appear only for the live 2h tail; older hours blank |

---

## 2. Find the new pipeline's exact name

The name is exactly what the ingest job passes as `pipeline_name` to
`metric_writer` — i.e. the `<pipeline>` in the DynamoDB partition key
`pk = "<env>#<pipeline>"`.

If you're not sure, list every pipeline currently in the table (this runs the
discovery Scan directly, ignoring the hard-coded list):

```bash
cd dash
python3 - <<'PY'
import os
from pathlib import Path
for line in Path(".env").read_text().splitlines():
    s = line.strip()
    if s.startswith("export "): s = s[7:]
    if s and not s.startswith("#") and "=" in s:
        k, _, v = s.partition("="); os.environ[k.strip()] = v.strip().strip('"').strip("'")
os.environ.pop("MONTY_DDB_PIPELINES", None)   # force real discovery
import db
for pk in db._ddb_pipelines("prod"):
    if not pk.startswith("rollup#"):
        print(pk.split("#", 1)[1])
PY
```

The new pipeline will be in that list (a pipeline only exists in the table once
it has written at least one row). Copy its name verbatim.

---

## 3. The three edits

### 3a. Add it to the read list (`dash/.env`)

Find the `MONTY_DDB_PIPELINES=` line and append the new name to the
comma-separated list. **No spaces around commas.** Example — adding
`ai-ingest-newsource-ai-ingest`:

```bash
export MONTY_DDB_PIPELINES=ai-ingest-iterate-ai-ingest,...,ai-ingest-sweatforum-ai-reconcile,ai-ingest-newsource-ai-ingest
```

The same list serves both envs — `db._ddb_pipelines(env)` prefixes each name
with `<env>#` at read time. If dev and prod have *different* pipeline sets, add
the union (a name that doesn't exist in one env simply returns no rows there,
which is harmless).

### 3b. Build its hourly rollup history

```bash
python3 rollup.py --env prod --days 14      # dry-run first if you like: add --dry-run
```

This reads the pipeline's raw events and writes hourly rollup items. It is
**idempotent** — safe to re-run, it recomputes and overwrites. Running it for
all pipelines (as here) is fine; only the new one's items are new.

Repeat for dev if applicable: `python3 rollup.py --env dev --days 14`.

### 3c. Restart the app

The `.env` is read once at startup, so the new list only takes effect on
restart:

```bash
flask --app app run --port 5011
```

---

## 4. Verify it appears

```bash
cd dash
python3 - <<'PY'
import os, math
from datetime import datetime
from pathlib import Path
for line in Path(".env").read_text().splitlines():
    s = line.strip()
    if s.startswith("export "): s = s[7:]
    if s and not s.startswith("#") and "=" in s:
        k, _, v = s.partition("="); os.environ[k.strip()] = v.strip().strip('"').strip("'")
import db
from transform import build_timeline_context
NOW = datetime.utcnow().replace(second=0, microsecond=0)
rows = db.fetch_events(lookback_days=8, env="prod", now=NOW, detail_days=1,
                       grain="hour", collapse_metrics=True)
lanes = {l["name"] for l in build_timeline_context(rows, NOW, window_hours=24,
                                                   env="prod", bucket_to_hour=True)["lanes"]}
new = "ai-ingest-newsource-ai-ingest"   # <-- put the new name here
print("APPEARS" if new in lanes else "MISSING", "-", new)
print("total lanes:", len(lanes))
PY
```

`APPEARS` → done. `MISSING` → re-check the name matches §2 exactly, that the
rollup backfill ran without error, and that you restarted.

---

## 5. Background: the speed/maintenance trade-off

`MONTY_DDB_PIPELINES` exists purely to skip the ~51s discovery Scan on cold
start (cold ~90s → ~38s). The price is this manual onboarding. It's set because
ingest pipelines change **rarely**.

If onboarding is happening often enough to be a nuisance, the alternative is to
**delete the `MONTY_DDB_PIPELINES` line** entirely. Then `db._ddb_pipelines`
falls back to the discovery Scan, new pipelines auto-appear (within the 1h
discovery cache), and no manual list edit is ever needed — at the cost of the
~51s scan returning on cold start. You still must run the rollup backfill (3b)
for a new pipeline's history either way.

There is also a fully-automatic option (persist the discovered list to a file so
cold start stays fast *and* new pipelines are picked up on a periodic refresh) —
not built yet; ask if the manual step becomes painful.

---

## 6. Related

- `dash/rollup.py` — the hourly rollup populate (schema + idempotency in its header)
- `dash/README.md` — "How the reader is fast" (Query-not-Scan, the rollup lanes)
- `dash/db.py` — `_ddb_pipelines` (the list), `fetch_rollup_events` (the read)
- `MONTY_TIMELINE_GRAIN=hour` / `MONTY_ANOMALY_GRAIN=hour` — the flags that make
  each page read the rollup (in `dash/.env`)
