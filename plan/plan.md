# Monty — Unified Data Platform Monitoring (Master Plan)

> Cross-references the diagram in `../unified_monitoring_alert_architecture.svg` and the
> design narrative in `../architecture.md`.
>
> Sibling specs in this folder:
> - `dbt_changes_spec.md` — PR plan for `sweat_analytics_coredbt`
> - `aws_ingest_changes_spec.md` — PR plan for `analytics-ingest-iterate-repo`

## Context

Today's alerting is fragmented across the data platform:

- **AWS ingestion** (`analytics-ingest-iterate-repo`) — 50+ Lambdas pull 3rd-party data into S3/Snowflake. Each stack creates its own `ai-ingest-*-alerts` SNS topic, but the Slack-publish code is commented out (`ai_ingest_stack_iterate.py` lines 389–402). Failures get logged to CloudWatch and quietly die.
- **dbt** (`sweat_analytics_coredbt`) — no post-hooks, no `on-run-end`, no metric tracking, no failure paging. `dbt_project.yml` has zero hooks today.
- **Slack** — the user-facing surface, but no central writer.

Monty implements the "Central Metrics Table" pattern from `architecture.md`:

1. **Metric Sink** — one Snowflake table (`MONITORING_DB.MONITORING.CUSTOM_METRICS`) is the inbox for every metric from every pipeline.
2. **Audit Registry** — config table where you add a row (no code) to start tracking a new SQL-based KPI/DQ check; a stored proc runs it on a schedule.
3. **Observer Lambda** — reads alert rows, fans out to Slack, marks them sent.
4. **Failure proxy** — HTTP endpoint (for app/dbt failures) + SNS subscriber (for AWS infra alarms) + Log scanner (for `MONITORING_METRIC` JSON lines).

## Decisions (locked in)

| Question | Decision |
| --- | --- |
| Failure path | **Both** API Gateway HTTP endpoint **and** SNS subscriber |
| Snowflake home | `MONITORING_DB.MONITORING.*` |
| Slack routing | Severity-based: `#data-incidents` (critical/error), `#data-alerts` (warning/info) |
| Repo shape | Standalone deployable repo (CDK + Lambdas + SQL), mirrors `analytics-ingest-iterate-repo` |

## Repo layout

```
Monty/
├── README.md
├── architecture.md                              (exists)
├── unified_monitoring_alert_architecture.svg    (exists)
├── plan/
│   ├── plan.md                                  this document
│   ├── dbt_changes_spec.md
│   └── aws_ingest_changes_spec.md
├── infra/                                       CDK app
│   ├── app.py
│   ├── monty_stack.py
│   ├── cdk.json
│   └── requirements.txt
├── lambdas/
│   ├── observer/                                EventBridge 1m → poll custom_metrics → Slack
│   ├── failure_proxy/                           API Gateway POST /failure → write row
│   ├── sns_subscriber/                          SNS topics → write row (severity=critical)
│   ├── log_scanner/                             CloudWatch Logs filter on MONITORING_METRIC
│   └── shared/                                  Snowflake client + metric writer
├── sql/
│   ├── ddl/                                     001..004 — DB, schema, tables
│   ├── procedures/auditor_sp.sql                Snowflake Python stored proc
│   ├── tasks/auditor_task.sql                   hourly schedule
│   └── seed/audit_registry_examples.sql         starter rules
├── docker/Dockerfile                            python:3.12 base
├── requirements.txt
├── tests/
├── Makefile
└── .gitignore
```

## What gets built (in this folder)

### 1. Snowflake DDL — `sql/ddl/`

- **`CUSTOM_METRICS`** — `(id IDENTITY, pipeline_name, metric_name, metric_value FLOAT, severity, run_id, payload VARIANT, occurred_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP, is_alert BOOLEAN, sent_to_slack BOOLEAN, sent_at)`. Extends `architecture.md` with `severity` (routing), `payload` (rich context), `sent_to_slack` (idempotency).
- **`AUDIT_REGISTRY`** — `(rule_id, pipeline_name, metric_name, sql_check, comparator, threshold_value, severity, slack_channel_override, enabled, created_at, updated_at)`. `comparator` ∈ {`>`,`<`,`>=`,`<=`,`==`,`!=`}.
- **`ALERT_OUTBOX`** — `(delivery_id, metric_id, channel, status, sent_at, error_message)`. Decouples "metric arrived" from "Slack accepted it" so Slack failures can retry without rewriting the metric row.

### 2. Auditor stored procedure — `sql/procedures/auditor_sp.sql`

Snowflake Python stored proc:
- Loops `AUDIT_REGISTRY WHERE enabled = TRUE`
- Executes each `sql_check` (admin-only writes to registry; role permissions sandbox the exec)
- Compares result via `comparator`/`threshold_value` → inserts to `CUSTOM_METRICS` with `is_alert` and `severity`
- Emits a heartbeat metric every run (missing heartbeat is itself an alert)

### 3. Auditor Task — `sql/tasks/auditor_task.sql`

`SCHEDULE = 'USING CRON 0 * * * * UTC'` calling the stored proc. Hourly.

### 4. Failure-proxy Lambda — `lambdas/failure_proxy/`

API Gateway HTTP API → POST `/failure`:
- HMAC signature check (`X-Monty-Signature` header, secret in Secrets Manager)
- JSON: `{pipeline_name, run_id, error_message, severity, metric_name?, metric_value?, payload?}`
- Writes row with `is_alert=TRUE`. Returns 202.

### 5. SNS-subscriber Lambda — `lambdas/sns_subscriber/`

Subscribed by ingest stacks to their `ai-ingest-*-alerts` topics. Parses CloudWatch alarm JSON → writes metric row, `severity='critical'`.

### 6. Log-scanner Lambda — `lambdas/log_scanner/`

CloudWatch Logs subscription filter pattern `{ $.MONITORING_METRIC = "*" }`. Implements the JSON-log path from `architecture.md` item 3.

### 7. Observer Lambda — `lambdas/observer/`

EventBridge `rate(1 minute)`:
- `SELECT ... FROM CUSTOM_METRICS WHERE is_alert=TRUE AND sent_to_slack=FALSE ORDER BY occurred_at LIMIT 100`
- Routes by severity; posts to Slack; writes `ALERT_OUTBOX` then flips `sent_to_slack=TRUE`.
- On Slack 4xx/5xx: outbox row marked `failed`, `sent_to_slack` stays FALSE, retried next minute.

### 8. Shared modules — `lambdas/shared/`

- `snowflake_client.py` — Secrets Manager → `snowflake.connector.connect(**secret)` (adapted from `IngestionCode/snowflake_load.py` line 85).
- `metric_writer.py` — single parameterized INSERT, used by all writer Lambdas.

### 9. CDK stack — `infra/monty_stack.py`

Mirrors `MinimalIngestStack`:
- 4× `DockerImageFunction` (observer, failure_proxy, sns_subscriber, log_scanner)
- API Gateway HTTP API → failure_proxy
- EventBridge `rate(1 minute)` → observer
- Secrets Manager `monty-secrets` (Snowflake creds, HMAC secret, two Slack webhook URLs)
- IAM role: `secretsmanager:GetSecretValue`, `logs:*`, resource-policies for cross-stack subscription
- Outputs: API URL, log_scanner ARN, sns_subscriber ARN — referenced by sibling specs.

### 10. Tests — `tests/`

pytest with mocked boto3 + Snowflake. HMAC validation, severity routing, idempotency, Slack-5xx-retry, comparator logic.

## Spec docs (handed off — see sibling files)

11. `dbt_changes_spec.md` — exact PR plan: post-hook macro, on-run-end failure macro, dbt_project.yml diffs, Snowflake GRANTs, worked example on `mart_marketing_email_performance.sql`.
12. `aws_ingest_changes_spec.md` — exact PR plan: `monty_metric.py` helper, exception-handler `report_failure()` calls, CDK changes (subscription filter + SNS subscription), IAM/secret extensions.

## Verification (end-to-end)

1. **DDL applied** — `snowsql -f sql/ddl/...sql`; `DESC TABLE` confirms shape.
2. **Stack deploys to dev** — `make cdk-deploy ENV=dev`.
3. **Failure-proxy smoke test** — `curl -H "X-Monty-Signature: <hmac>" -d '{...}' $URL` → 202; row in `CUSTOM_METRICS`; ≤60s message in `#data-incidents`.
4. **Auditor smoke test** — insert deliberately-failing rule into `AUDIT_REGISTRY`, `EXECUTE TASK auditor_task`, see Slack.
5. **Custom-metric smoke test** — after dbt PR merges, `dbt run --select <model>` → row in `CUSTOM_METRICS`.
6. **Log-scanner smoke test** — after AWS PR merges, `print(json.dumps({"MONITORING_METRIC": ...}))` → row in `CUSTOM_METRICS`.
7. **Idempotency** — `UPDATE CUSTOM_METRICS SET sent_to_slack=FALSE` → resends; flip back, no duplicate.

## Out of scope (v1)

- Backfill of historical failures
- Web UI / dashboard (Sigma can do ad-hoc on `CUSTOM_METRICS`)
- PagerDuty (severity column is forward-compatible)
- Per-pipeline Slack channel routing — `slack_channel_override` column stubs the future capability
- Cross-region or multi-account fan-in

---

## Progress Log

> Append one line per completed task: `- [YYYY-MM-DD] <task> — <one-line summary>`

- [2026-05-09] Directory tree + master plan — created `infra/`, `lambdas/{observer,failure_proxy,sns_subscriber,log_scanner,shared}/`, `sql/{ddl,procedures,tasks,seed}/`, `docker/`, `tests/`; wrote `plan/plan.md`.
- [2026-05-09] Snowflake DDL — `sql/ddl/001_database_and_schema.sql` (DB + MONITORING schema + MONTY_SVC_ROLE/MONTY_WRITER_ROLE + MONTY_WH warehouse), `002_custom_metrics.sql` (sink table + index + grants), `003_audit_registry.sql` (rules table with comparator/severity check constraints), `004_alert_outbox.sql` (per-delivery audit trail).
- [2026-05-09] Auditor proc + task + seed — `sql/procedures/auditor_sp.sql` (Snowpark Python proc with per-rule try/except, heartbeat metrics, scalar-result enforcement), `sql/tasks/auditor_task.sql` (hourly cron, suspend-after-3-failures), `sql/seed/audit_registry_examples.sql` (heartbeat self-monitor + 2 example rules).
- [2026-05-09] Shared Lambda modules — `lambdas/shared/snowflake_client.py` (`@lru_cache`'d Secrets Manager read, `get_connection()`, `get_secret_value()` for HMAC/Slack URLs), `lambdas/shared/metric_writer.py` (frozen `Metric` dataclass with severity validation + parameterized INSERT helper).
- [2026-05-09] Failure-proxy Lambda — `lambdas/failure_proxy/schema.py` (PayloadError + validate() returning normalized dict, error_message merged into payload), `lambdas/failure_proxy/handler.py` (HMAC-SHA256 signature check via stdlib `hmac.compare_digest`, JSON parse, 401/400/202 responses).
- [2026-05-09] SNS-subscriber Lambda — `lambdas/sns_subscriber/handler.py` (per-record try/except for batch isolation, parses CloudWatch alarm JSON, derives pipeline name from `ai-ingest-<svc>-alerts` ARN, severity=critical).
- [2026-05-09] Log-scanner Lambda — `lambdas/log_scanner/handler.py` (gzip-decode of CloudWatch Logs subscription event, parses MONITORING_METRIC JSON lines, falls back to `/aws/lambda/<fn>` for pipeline name, defaults is_alert=FALSE for trending-only metrics).
- [2026-05-09] Observer Lambda + Slack — `lambdas/observer/slack.py` (severity-based webhook routing, Slack block-kit formatter with severity emoji, urllib HTTP POST returning `SlackResult`), `lambdas/observer/handler.py` (selects unsent alerts, posts to Slack, writes ALERT_OUTBOX, flips sent_to_slack only on success).
- [2026-05-09] CDK app + stack — `infra/cdk.json`, `infra/requirements.txt` (aws-cdk-lib pinned), `infra/app.py` (env-name router for dev/prod accounts), `infra/monty_stack.py` (4× DockerImageFunction sharing one IAM role and Secret, API Gateway HTTP API for failure_proxy, EventBridge `rate(1 minute)` for observer, resource policies on log_scanner+sns_subscriber for cross-stack invoke, CfnOutputs for FailureProxyUrl / SnsSubscriberArn / LogScannerArn / SecretName, ONE_MONTH log retention).
- [2026-05-09] Build + ops plumbing — `docker/Dockerfile` (`public.ecr.aws/lambda/python:3.12` base, deps layer cached separately from code), root `requirements.txt` (boto3, snowflake-connector-python), `Makefile` (`install`/`test`/`lint`/`cdk-synth`/`cdk-deploy`/`cdk-diff`/`sql-apply`), `.gitignore`, `README.md` (deploy walkthrough + secret JSON shape + "adding a metric" matrix).
- [2026-05-09] pytest suite — `tests/conftest.py` (forced sys.modules stubs for boto3 + snowflake.connector so tests run with no network deps), `tests/test_failure_proxy.py` (10 tests: schema validation, HMAC signing, 401/400/202/5xx paths), `tests/test_slack_formatter.py` (15 tests: severity routing, block-kit shape, urllib path with HTTPError/URLError), `tests/test_observer_dedup.py` (4 tests: idempotency contract — sent_to_slack flips only on Slack 2xx, outbox written either way, VARIANT JSON parse). All 36 tests pass under python3.11.
- [2026-05-09] dbt PR spec — `plan/dbt_changes_spec.md` (vars + +post-hook + on-run-end edits to `dbt_project.yml`; new macros `monty_post_hook.sql` and `monty_failure_hook.sql`; config-key contract for model authors; worked example on `mart_marketing_email_performance.sql`; Snowflake GRANTs for the dbt role; CI env-var matrix; 3-step validation plan; rationale for INSERT-direct over HTTP from Jinja).
- [2026-05-09] AWS ingest PR spec — `plan/aws_ingest_changes_spec.md` (new stdlib-only `IngestionCode/monty_metric.py` with `emit_metric()` + `report_failure()`; exception-handler edits at lines 377/422/462/491 of `ai_ingest_iterate.py`; CDK additions: 2 context inputs, secret keys `MONTY_FAILURE_URL`/`MONTY_HMAC_SECRET`, `logs.SubscriptionFilter` + `sns_subscriptions.LambdaSubscription` per Lambda; rollout order across ~50 ingest stacks; HMAC rotation risk noted).
- [2026-05-10] dbt spec applied — pushed branch `feat/monty-integration` to `henkdeploysweat/sweat_analytics_coredbt` (commit 53b744d9). 5 files: `dbt_project.yml` (vars + +post-hook + on-run-end), `macros/monty_post_hook.sql` + `macros/monty_failure_hook.sql` (new), `models/marts/marketing/mart_marketing_email_performance.sql` (worked example, `meta` block to dodge dbt's CustomKeyInConfigDeprecation), `README.md` (Monty integration section). `dbt parse` clean; PR ready at https://github.com/henkdeploysweat/sweat_analytics_coredbt/pull/new/feat/monty-integration.
- [2026-05-10] AWS ingest spec applied — pushed branch `feat/monty-integration` to `devteam-sweat/analytics-ingest-iterate-repo` (commit 259b35b). 4 files: `IngestionCode/monty_metric.py` (new, stdlib-only), `IngestionCode/ai_ingest_iterate.py` (top-level lambda_handler try/report_failure/raise + per-call report_failure on the 3 swallowed exception sites + 3 trending `emit_metric` calls before return), `CloudformationStack/ai_ingest_stack_iterate.py` (2 new context inputs, MONTY_* secret keys, per-Lambda `logs.SubscriptionFilter`, SNS `LambdaSubscription` on alarm topic, `secret_value_from_json` for CFN dynamic refs), `README.md` (Monty integration section). `python3 -m ast` parses both modules clean; PR ready at https://github.com/devteam-sweat/analytics-ingest-iterate-repo/pull/new/feat/monty-integration.
