# Swap low-priority metrics storage: S3 Parquet → DynamoDB

> ✅ **IMPLEMENTED 2026-07-17** — all items below are done (writer, routing,
> infra, dashboard read path, tests, docs), with one deliberate deviation:
> the S3 bucket + write grant + `MONTY_METRICS_BUCKET` env var are RETAINED
> through cutover (zero-downtime option in §"Files to change" item 3) instead
> of removed. Pending: `make cdk-deploy ENV=dev`, end-to-end verification
> (§Verification), then prod. This file is kept as the design record.

## Context

`warning`/`info` metrics are diverted away from Snowflake (the cost driver was
the **XS Snowflake warehouse** staying permanently awake on high-frequency
single-row INSERTs). Today they land as one snappy-Parquet object per write on
`s3://monty-<env>-metrics/run_date=YYYYMMDD/<uuid>.parquet`.

We want to move that low-priority store to **DynamoDB** for **operational
simplicity** and **operational lookups** (point queries + "recent N by
pipeline"), replacing the S3 object store.

DynamoDB fits this intent: single item store, free TTL retention, no small-file
compaction, point/range queries via a key design. Accepted trade-offs: ~10× S3
storage cost per GB (small at this volume), and loss of cheap full-scan
analytics (not needed — reads are operational).

**Scope discovery:** the write path is one isolated choke point, BUT a read path
already exists (contrary to the stale CLAUDE.md "read path deferred" note):
- `dash/db.py` (~lines 83–89, 500–543) reads the S3 Parquet for the dashboard.
- `compact_s3.py` compacts the many single-row files per `run_date=` prefix.
Both must be migrated/retired, or the dashboard breaks after the cutover.

## Difficulty summary

- **Write path swap:** trivial — one new module + one changed line.
- **Infra:** small — swap Bucket construct for Table construct.
- **Real work:** DynamoDB key-schema design + migrating the dashboard read path.
- **Overall:** ~1 day incl. infra, dashboard read, tests. Low code risk (the
  write path has one caller); the dashboard read rewrite is the largest piece.

## DynamoDB table design (the one real design decision)

Table `monty-<env>-metrics-ddb` (new name to coexist with the retained S3 bucket
during cutover), **on-demand billing** (`PAY_PER_REQUEST`) — matches the
"no idle capacity to manage" spirit.

- **Partition key** `pk` (S): `f"{environment}#{pipeline_name}"` — groups a
  pipeline's metrics per env; supports "recent N for pipeline X in prod".
- **Sort key** `sk` (S): `f"{occurred_at_iso}#{uuid4}"` — time-sortable and
  unique (guards identical timestamps). "Recent N" = `Query(pk=..., Limit=N,
  ScanIndexForward=False)`.
- **Attributes:** `metric_name`, `metric_value` (N, omit when null),
  `severity`, `run_id` (omit when null), `payload` (S — keep the JSON string,
  same as S3 path), `is_alert` (BOOL), `environment`, `occurred_at` (S, ISO
  UTC), and `ttl` (N, epoch seconds) for auto-expiry retention.
- **GSI (defer):** only add a `severity`-partitioned GSI if a future "recent
  across all pipelines by severity" view is needed. Not required for launch.

## Files to change

1. **New `lambdas/shared/dynamo_writer.py`** — mirror `s3_writer.write` exactly:
   signature `write(metric: "Metric", occurred_at: datetime | None = None) -> int`,
   returns `1` on success (keeps the rowcount contract). Build the item from the
   `Metric` dataclass fields (all flat scalars; `payload` via
   `json.dumps(metric.payload) if not None`). Cache the client with
   `@lru_cache(maxsize=1)` on a `_get_table()` helper (same pattern as
   `s3_writer._get_client`,
   `boto3.resource("dynamodb").Table(os.environ["MONTY_METRICS_TABLE"])`).
   Compute `ttl` from `occurred_at`. Keep the same per-write `logger.info` line
   (pipeline/metric/severity/env/table/key) per the observability rules.

2. **`lambdas/shared/metric_writer.py:107`** — change the S3 branch:
   `return s3_writer.write(metric)` → `return dynamo_writer.write(metric)`
   (and swap the import at line 14). The routing sets (`S3_SEVERITIES` etc.) and
   everything else stay — optionally rename `S3_SEVERITIES` → `DDB_SEVERITIES`
   for clarity (touches the docstring at lines 21–35 and tests).

3. **`infra/monty_stack.py`** — replace the S3 bucket wiring:
   - Swap `s3.Bucket(... "MetricsBucket" ...)` (lines 114–122) for a
     `dynamodb.Table(self, "MetricsTable",
     table_name=f"monty-{env_name}-metrics-ddb", partition_key=..., sort_key=...,
     billing_mode=PAY_PER_REQUEST, time_to_live_attribute="ttl",
     removal_policy=RETAIN)`.
   - `metrics_bucket.grant_write(role)` (line 125) →
     `metrics_table.grant_write_data(role)`.
   - `common_env` (line 130): `MONTY_METRICS_BUCKET` → `MONTY_METRICS_TABLE`
     (table name). Keep the bucket + its env var **during cutover** if you want
     zero-downtime; otherwise remove.
   - `CfnOutput` (lines 274–276): output the table name.
   - Import `aws_dynamodb as dynamodb` (replaces / joins the `aws_s3` import).

4. **`requirements.txt`** (Lambda image) — drop `pyarrow>=15`; only `s3_writer`
   used it and DynamoDB uses `boto3` (already present). Smaller image. (Do NOT
   remove `boto3`.) Note: `dash/db.py` and `compact_s3.py` also use pyarrow but
   are NOT in the Lambda image — their deps live in `dash/requirements.txt`.

5. **`dash/db.py`** (~lines 83–89, 500–543) — rewrite the read path from
   "list + read Parquet under `run_date=` prefixes" to a DynamoDB `Query`
   (by `pk`, time-range on `sk`) or `Scan` for the dashboard's aggregate views.
   Largest single piece — the query shape depends on what the dashboard renders;
   inspect its current pandas usage and mirror the resulting DataFrame columns
   from DynamoDB items.

6. **Retire `compact_s3.py`** — DynamoDB has no small-file problem. Delete or
   archive once cutover is confirmed.

7. **Tests:**
   - New `tests/test_dynamo_writer.py` mirroring `tests/test_s3_writer.py`
     (assert item keys, TTL, null handling, return `1`, table from env).
   - `tests/conftest.py` — replace the `_StubS3Client` / pyarrow stub (lines
     ~25, 44–69) with a DynamoDB `Table` stub recording `put_item` calls, and
     set `MONTY_METRICS_TABLE` instead of `MONTY_METRICS_BUCKET`.
   - `tests/test_metric_writer.py` — update routing tests to assert
     warning/info → `dynamo_writer` (no Snowflake); disjoint/coverage asserts
     unchanged.
   - Retire `tests/test_s3_writer.py`.

## Cutover / operational notes

- **Existing S3 data:** if the dashboard must show pre-cutover warning/info
  history, backfill S3 → DynamoDB with a one-off script (read Parquet via
  `pyarrow`, `batch_writer().put_item` per row, TTL from `occurred_at`). Confirm
  whether history matters; if not, skip and let S3 age out.
- **Keep the S3 bucket `RETAIN`** through cutover so nothing is lost; decommission
  only after the dashboard reads DynamoDB cleanly.
- **Cold-start gotcha:** new env var `MONTY_METRICS_TABLE` + the `@lru_cache`d
  client means all four Lambdas need a cold start after deploy (same class of
  issue as the secret loader — CLAUDE.md gotcha #1). `make cdk-deploy` replaces
  the image so this happens naturally, but verify.
- **Region:** table is created in `us-east-1` (both stacks). No cross-region
  concern.

## Verification (end-to-end)

1. `make test` — new dynamo_writer + updated metric_writer/conftest tests pass
   (98-test suite stays green, no AWS needed).
2. `make cdk-synth ENV=dev` then `make cdk-diff ENV=dev` — confirm the Table
   replaces the Bucket grant/env and nothing else drifts.
3. `make cdk-deploy ENV=dev` — deploy to dev account `116981766237`.
4. Send a `warning` metric through a writer (e.g. a signed POST to
   `failure_proxy`, or invoke locally) and confirm:
   - Lambda log shows the `metric -> dynamodb` line with table + key.
   - Item appears in `monty-dev-metrics-ddb` (`aws dynamodb get-item` /
     `query` by `pk`).
   - A `critical` metric still lands in Snowflake `CUSTOM_METRICS` (routing
     unchanged) and reaches Slack via the observer.
5. Load the dashboard (`dash/`) and confirm the warning/info views render from
   DynamoDB.
6. Only after dev is verified: repeat for prod (`534977985440`) with the
   AWS-state-change confirmation rule.
