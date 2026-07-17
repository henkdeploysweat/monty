# Monty — instructions for Claude

This file is read automatically by Claude Code at session start. Keep it
short, factual, and current.

## What this repo is

**Monty** = a single opinionated monitoring platform for Berg's data stack.
One Snowflake table is the inbox for every metric (failures, KPIs, alarms,
trending counters); one Lambda delivers alerts to Slack. Four AWS Lambdas
+ three Snowflake tables + one EventBridge rule, all defined in
`infra/monty_stack.py`.

Primary docs to consult before doing anything non-trivial:

- [`README.md`](README.md) — full deploy/operate guide, troubleshooting,
  rollback. **Read sections 3 + 9 before any deploy work.**
- [`ADD_METRIC.md`](ADD_METRIC.md) — copy-paste templates for sending a
  metric or failure from any repo/language. **Read this in full before
  instrumenting a new pipeline.** The four paths (HTTP POST / stdout JSON /
  SNS / dbt meta) are mutually exclusive — use the decision tree in §1.

## Architecture in one paragraph

Four Lambdas share one Docker image (`docker/Dockerfile`); `CMD` differs
per handler. `failure_proxy` accepts HMAC-signed HTTP POSTs and writes a row.
`log_scanner` watches CloudWatch Logs for `MONITORING_METRIC` JSON and writes
a row. `sns_subscriber` fans CloudWatch alarms in and writes a row.
`observer` runs every minute, finds `is_alert AND NOT sent_to_slack`, posts
to Slack, flips the flag. All rows land in
`MONITORING_DB.MONITORING.CUSTOM_METRICS`. SQL-based threshold rules live in
`AUDIT_REGISTRY` and are evaluated hourly by a Snowflake task
(`sql/procedures/auditor_sp.sql`).

**Severity-based storage routing (`metric_writer.write` in
`lambdas/shared/metric_writer.py`):** replaced the old `PERSISTED_SEVERITIES`
drop-gate. Two disjoint sets decide where a row lands:
`SNOWFLAKE_SEVERITIES = ("critical", "error")` → the classic single-row INSERT
into `CUSTOM_METRICS`; `DDB_SEVERITIES = ("warning", "info")` → DynamoDB item
via `lambdas/shared/dynamo_writer.py` (table `monty-<env>-metrics-ddb`,
on-demand billing, `pk = "<env>#<pipeline>"`, `sk = "<occurred_at ISO UTC>#<uuid>"`,
90-day TTL via the `ttl` attribute, `MONTY_METRICS_TTL_DAYS` overrides). This
cuts the high-frequency low-priority INSERT load that kept the XS warehouse
permanently awake. Anything outside both sets is a defensive logged drop.
**Consequence:** `info` rows no longer reach Slack (the observer only ever read
from Snowflake; `warning` never reached Slack anyway). DynamoDB replaced the
earlier S3 Parquet store (2026-07-17): `s3_writer.py`, `compact_s3.py` and the
pyarrow image dep are gone.

**Migration COMPLETE (2026-07-17), and the backfill is now VERIFIED complete.**
Both envs deployed and verified writing live; the pre-cutover history was
replayed into DynamoDB with `dash/backfill_dynamo.py` (idempotent:
`sk = "<occurred_at>#<identity>"`, TTL from the ORIGINAL `occurred_at`, so
replayed rows expire on their real schedule).

**The first backfill silently lost 78,912 rows (07-15 → 07-17) and reported
success.** It deduped on `row["ID"]`, but S3 Parquet rows have NO `ID` column —
every row keyed to `None`, so the whole S3 leg collapsed to one row while the
SQLite leg (which has IDs) sailed through. The gap was invisible because the
`both` union was still reading S3 and covering for it. Fixed 2026-07-17
(`_row_identity` + natural-key dedup + a warning when a leg contributes <1% of
what it read), re-run, and verified by key rather than by count:
**78,913/78,913 S3 rows and 174,325/174,325 SQLite rows now match a DynamoDB row
on `(pipeline, metric, occurred_at, value)`.** DynamoDB is a strict superset of
both old stores. Tests: `tests/test_backfill_dynamo.py`.

Only NOW are the S3/SQLite legs genuinely redundant. The dashboard reads
`MONTY_SOURCE=both` + `MONTY_BOTH_WARN_SOURCE=dynamo` (Snowflake for
critical/error + dbt, DynamoDB for warn/info, S3 leg dropped — it was the
slowest at ~103.8s and paced the whole union). `MONTY_SOURCE=dynamo` ALONE is
NOT equivalent: it would drop every failure and dbt metric. The
`monty-<env>-metrics` bucket + write grant + `MONTY_METRICS_BUCKET` env var are
retained for image-rollback safety only — decommissionable now that the replay
is verified. **This routing applies only
to the Lambda write path.** The auditor proc and dbt hooks INSERT directly into
`CUSTOM_METRICS` (bypassing `metric_writer`), so all four severities still persist
to Snowflake from them — deliberately left alone: each emits ~1 bulk INSERT per run
on an already-running warehouse, so they never caused the idle-load problem. Do not
"fix" this for consistency; gating dbt drops metrics and saves nothing.

**Slack channel routing** (`lambdas/observer/slack.py`): per-metric
`payload.slack_webhook` wins; else non-prod (`ENVIRONMENT != 'prod'`) →
`SLACK_WEBHOOK_DEV` (`#data-alerts-dev`), or `#data-alerts` if that webhook is
unset — non-prod NEVER hits the prod incident channel, regardless of severity;
else (prod) severity routing. `CUSTOM_METRICS.ENVIRONMENT` is always populated —
producers may set it, otherwise `metric_writer` defaults it to the Lambda's
`MONTY_ENV` (so CloudWatch-alarm rows via `sns_subscriber` inherit the deploy
env). A newly added/rotated `SLACK_WEBHOOK_DEV` needs a cold start to reach the
dedicated dev channel (gotcha #1); until then non-prod alerts degrade to
`#data-alerts`, not prod incidents.

**Slack message format** (`lambdas/observer/slack.py`): block order is headline
→ `:alert:` callout → `*Error by ai*` → `*Payload*` table → button → footer.
Two things to know:
- **dbt-run failures headline the failed model, not the collector.** These rows
  have `PIPELINE_NAME = 'dbt_run_failures'`; the real model/test name lives in
  `payload.failures[].pipeline_name`. `_dbt_failed_models()` extracts it so the
  headline + callout name the actual model (`braze_cdi_attribute_sync IS DOWN`;
  many → `first +N more`). Non-dbt rows keep their own `PIPELINE_NAME`. dbt also
  nests the error inside `failures[]` (no top-level `error_message`), so the
  callout/snippet fall back to the first failure's message.
- **`*Error by ai*`** is an Anthropic-generated ≤120-char summary via
  `ai_summary()` (`claude-haiku-4-5`, stdlib `urllib` — no SDK in the image).
  Needs `ANTHROPIC_API_KEY` in `monty-<env>-secrets`; **empty/missing → falls
  back to the raw last-4-lines, no API call.** Adding the key needs a cold start
  (gotcha #1). No `anthropic` package required — do not add it to
  `requirements.txt`.

## Environments

- **dev account**: `116981766237`, region `us-east-1`
- **prod account**: `534977985440`, region `us-east-1`

Region moved from `ap-southeast-2` → `us-east-1` on 2026-05-30. Both stacks
now live in `us-east-1`. The Sydney stacks may still exist in
`ap-southeast-2` (potentially in `DELETE_FAILED`); do not touch them without
explicit instruction.

## Critical gotchas

1. **`@lru_cache` on the secret loader** (`lambdas/shared/snowflake_client.py:41`).
   After populating `monty-<env>-secrets`, force a cold start on all four
   Lambdas — otherwise they keep serving the empty-template HMAC and every
   POST returns 401. See README §3.5 warning for the description-bump command.
2. **Secrets Manager regional state**. Moving regions does NOT carry secrets.
   Re-populate the secret in the new region using the same HMAC + Slack URLs
   + Snowflake creds — rotating during a region move would force every
   upstream consumer to update in lockstep.
3. **CDK bootstrap is per account+region.** `cdk bootstrap aws://<account>/<region>`
   must run once for any new region before the first `cdk deploy`.
4. **dbt double-encoding trap**. If you pipe `aws secretsmanager get-secret-value
   --query SecretString --output text` from one region directly into
   `put-secret-value --secret-string file://-` of another, the value may end
   up double-JSON-encoded. Sanity check by parsing the file with
   `python3 -c 'import json; assert isinstance(json.load(open("secret.json")), dict)'`.
5. **Deprecated `logRetention=` kwarg** in `infra/monty_stack.py:215`.
   Emits a CDK warning on every synth; non-blocking but worth migrating to
   explicit `LogGroup` constructs before the next `aws-cdk-lib` major bump.
6. **`requirements.txt` is Lambda-runtime-only** — it is baked into the Docker
   image (`docker/Dockerfile`). The four Lambdas import just two third-party
   packages (`snowflake-connector-python`, `boto3`); everything else
   (Slack + Anthropic) uses the stdlib. Do NOT `pip freeze` your local env into
   it — that pulls in dashboard/ML/CDK deps and the build dies with
   `ResolutionImpossible`. Dashboard deps live in `dash/requirements.txt`, CDK
   deps in `infra/requirements.txt`.

## Common commands

```bash
make install                       # runtime + infra + dev deps
make test                          # 98 pytest tests, no AWS/Snowflake
make cdk-synth ENV=dev|prod        # synth (no deploy)
make cdk-diff ENV=dev|prod         # show pending changes
make cdk-deploy ENV=dev|prod       # build Docker image + deploy
make sql-apply                     # PRINTS snowsql commands (review before `| sh`)
```

Snowflake schema lives in `sql/ddl/`, `sql/procedures/`, `sql/tasks/`.
Tests live in `tests/`. Mockups in `mockups/` are visual-only HTML.

## Working style

- **One command at a time** when troubleshooting AWS. The user's zsh has
  quoting issues with backticks, JMESPath brackets, and multi-line `\`
  continuations. For anything non-trivial, write a script to `/tmp/<task>.py`
  and have them run `python3 /tmp/<task>.py` — that copy-pastes cleanly.
- **Confirm before any AWS-state-changing command** (`create`, `update`,
  `put-secret-value`, `deploy`, `destroy`, `bootstrap`). Read-only checks
  (`describe`, `list`, log tails) can proceed without asking.
- **Never insert directly into `CUSTOM_METRICS` from outside Monty**, never
  call Slack directly, never invent new severities. The four allowed
  severities are `critical`, `error`, `warning`, `info` — enforced server-side.
- **Never commit secrets.** `MONTY_HMAC_SECRET`, the Snowflake password, the
  Slack webhook URLs, and `ANTHROPIC_API_KEY` live in Secrets Manager; if any
  leaks into git history, rotate and force-cold-start all four Lambdas.

## Repo map

```
infra/                            CDK app (Python)
  app.py                          dev/prod account router; pinned to us-east-1
  monty_stack.py                  single Stack: 4 Lambdas + API GW + EB + Secret
lambdas/
  observer/                       EventBridge 1m → Slack
  failure_proxy/                  API Gateway POST /failure (HMAC)
  sns_subscriber/                 SNS → row write
  log_scanner/                    CloudWatch Logs subscription → row write
  shared/                         Snowflake client + metric writer
sql/
  ddl/                            4 DDL files (database, 3 tables)
  procedures/auditor_sp.sql       Snowpark Python proc
  tasks/auditor_task.sql          hourly cron task
  seed/audit_registry_examples.sql
docker/Dockerfile                 one image, CMD overridden per Lambda
tests/                            98 pytest tests, no AWS/Snowflake required
docs/getting-started.md           older integration guide (superseded by ADD_METRIC.md)
README.md                         deploy + operate + troubleshoot
ADD_METRIC.md                     copy-paste templates for instrumenting a new pipeline
Makefile                          install/test/lint/cdk-*/sql-apply
```
