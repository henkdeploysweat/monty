# ADD_METRIC.md — Sending a Metric or Failure to Monty

> **For Claude sessions in other repos:** read this file end-to-end, then use
> the decision tree in §1 to pick a path, copy the matching template, and adapt
> it to the calling code. Do not invent fields. Do not skip the verification
> step. The required-field list is enforced by the failure_proxy Lambda's JSON
> schema validator — sending the wrong shape returns HTTP 400 with no metric
> recorded.
>
> **For humans:** this is the only document you need to add Monty instrumentation
> to a new pipeline. Pick a path from the table below and copy the corresponding
> section. The four paths are mutually exclusive — pick one per integration point.

---

## 1. Decision tree — pick exactly one path

Answer these questions in order and stop at the first match.

| Question | If yes → use |
| --- | --- |
| Is this a **dbt model** that should emit a metric every run? | **Path D** — dbt `meta` block on the model |
| Is this a **dbt run failure** alert? | **Already automatic** — `monty_failure_hook()` is wired into `on-run-end`. No code required. Confirm `dbt_project.yml` has the hook. |
| Is this an **AWS Lambda you can edit**, and the metric is just "something happened, here's a number"? | **Path B** — print structured JSON to stdout |
| Is this an existing **CloudWatch Alarm** already wired to an SNS topic? | **Path C** — subscribe Monty's `sns_subscriber` to the SNS topic |
| Is this **any other code** (script, ECS task, Airflow DAG, GitHub Action, vendor webhook, Lambda where you want a specific numeric value)? | **Path A** — HMAC-signed HTTP POST to `/failure` |

> If two paths could work, prefer **B** over **A** for AWS Lambdas (no credentials
> needed, no extra dependencies), and **C** over **A** for CloudWatch alarms (the
> alarm state machine handles dedupe and recovery for you).

---

## 2. Severity — pick before you write any code

Every metric has a severity. This decides which Slack channel it routes to,
and whether it routes at all. Be honest about it — "critical" wakes people
up out of bed.

| Severity | Slack channel | When to use |
| --- | --- | --- |
| `critical` | `#data-incidents` | Data is down, corrupted, or actively wrong **right now**. Wake on-call. |
| `error` | `#data-incidents` | A pipeline failed and needs human attention today. The default for any uncaught exception. |
| `warning` | _(not sent to Slack)_ | Something looks off but data is still flowing. Investigate within a day. |
| `info` | _(not sent to Slack)_ | Trending metrics — row counts, durations, KPIs. |

> ℹ️ **Storage routing by severity — Lambda paths only.** `metric_writer.write`
> routes rows by severity: `critical`/`error` (`SNOWFLAKE_SEVERITIES`) are INSERTed
> into `CUSTOM_METRICS`; `warning`/`info` (`DDB_SEVERITIES`) are written as items
> to DynamoDB (`monty-<env>-metrics-ddb`, `pk = "<env>#<pipeline>"`,
> `sk = "<occurred_at ISO UTC>#<uuid>"`, 90-day TTL) by `dynamo_writer.py`, to keep
> the high-frequency low-priority single-row INSERTs off the Snowflake warehouse.
> **Consequences for Lambda-sourced rows:** `warning`/`info` no longer land in
> `CUSTOM_METRICS` (so they can't be thresholded by `AUDIT_REGISTRY` rules or read
> by SQL dashboards), and `info` no longer reaches Slack. They are queryable in
> DynamoDB (per-pipeline `Query`) and on the Monty dashboard (`dash/`).
>
> **This routing does NOT apply to dbt or the auditor.** Both run inside Snowflake
> and INSERT directly into `CUSTOM_METRICS`, bypassing `metric_writer` — so all
> four severities still persist to Snowflake from those producers. That is
> deliberate: they emit ~1 bulk INSERT per run on an already-running warehouse, so
> they never caused the idle-load problem the routing solves.

Default rule of thumb when unsure: pick **`error`** for failures, **`info`**
for trending values. Never default to `critical`.

### Routing an alert to a specific Slack channel (optional)

Each metric can override the severity-based default by setting **`slack_webhook`**
in its payload. The value is the **full Slack incoming-webhook URL** for the
target channel — not a channel name, not a secret reference, not a key
suffix. The observer posts directly to that URL as-is.

```jsonc
{
  // ...other fields...
  // full incoming-webhook URL, i.e. https://hooks.slack.com/services/<team>/<channel>/<token>
  "slack_webhook": "<your channel's full Slack incoming-webhook URL>"
}
```

How the observer decides where to send each alert:

1. If `slack_webhook` is present **and** starts with `https://hooks.slack.com/`
   → POST directly to that URL. ALERT_OUTBOX label = `custom-webhook`.
2. Else if the metric's `environment` is **not** `prod` → `#data-alerts-dev`
   (secret key `SLACK_WEBHOOK_DEV`), **regardless of severity**. This keeps
   dev/staging noise out of the prod channels. Set `environment` in your
   payload (HTTP body or `MONITORING_METRIC` JSON); if you omit it, the metric
   inherits the Monty Lambda's deploy env (`MONTY_ENV`).
3. Otherwise → severity-based default:
   - `critical` / `error` → `#data-incidents` (secret key `SLACK_WEBHOOK_INCIDENTS`)
   - `warning` / `info`   → `#data-alerts`    (secret key `SLACK_WEBHOOK_ALERTS`)

URLs that don't start with `https://hooks.slack.com/` are rejected as routing
targets (SSRF guard for log_scanner-fed metrics) — the alert falls back to
severity routing in that case rather than posting to an arbitrary endpoint.

Producers get the webhook URL from whoever owns the destination Slack channel
(Slack admin → "Incoming Webhooks" app → copy URL for the channel). Treat
each webhook URL as a credential: anyone who has it can post to that channel
as the bot. Store in env vars or your repo's secret manager, never commit to
git, rotate if leaked.

---

## 3. Required credentials and endpoints

You need these from the Monty CDK outputs and Secrets Manager. Get them from
the platform team or directly with:

```bash
# Per environment. Replace `dev` with `prod` for the prod versions.
aws cloudformation describe-stacks --stack-name monty-dev --region us-east-1 \
  --query 'Stacks[0].Outputs' --output table

aws secretsmanager get-secret-value --region us-east-1 --secret-id monty-dev-secrets \
  --query SecretString --output text | python3 -c 'import json,sys;print(json.load(sys.stdin)["MONTY_HMAC_SECRET"])'
```

| Variable | Used by | Notes |
| --- | --- | --- |
| `MONTY_FAILURE_URL` | Path A only | The full URL ending in `/failure`. From CDK output `FailureProxyUrl`. |
| `MONTY_HMAC_SECRET` | Path A only | 64-char hex string. **Never commit. Never log. Never print.** Store in the calling repo's own Secrets Manager / CI secret store. |
| `MONTY_LOG_SCANNER_ARN` | Path B (CDK only) | Pass as a CDK context input when adding the subscription filter to your Lambda's log group. From CDK output `LogScannerArn`. |
| `MONTY_SNS_SUBSCRIBER_ARN` | Path C (CDK only) | Pass as a CDK context input when subscribing Monty to your SNS topic. From CDK output `SnsSubscriberArn`. |

---

## 4. Path A — HMAC-signed HTTP POST

Use for: anything that can make an HTTP request — Airflow, GitHub Actions,
vendor webhooks, EC2/ECS scripts, Lambdas where you want a specific numeric
value rather than just "something broke".

### 4.1 Required and optional fields

| Field | Required | Type | Notes |
| --- | --- | --- | --- |
| `pipeline_name` | **yes** | string | Short identifier. Stable across runs. Example: `ingest.iterate`, `mart_marketing_email_performance`, `github-ci-myrepo`. |
| `run_id` | **yes** | string | Per-invocation identifier so humans can correlate the Slack alert back to the source — a Lambda request ID, a CI run number, a UUID, a timestamp. Any string works as long as it's unique enough. |
| `error_message` | **yes** | string | Human-readable description of what went wrong. Truncated to 4000 chars by the server. For non-failure metrics, use a short positive description like `"trending metric"`. |
| `severity` | **yes** | string | One of `critical`, `error`, `warning`, `info`. See §2. |
| `metric_name` | no | string | Defaults to `pipeline_failure`. Use a stable distinct name when you'll have multiple metrics per `pipeline_name` (`rows_loaded`, `latency_p95`, etc.). |
| `metric_value` | no | number | A scalar number. Required if you want this row to be thresholdable by `AUDIT_REGISTRY` rules later. |
| `payload` | no | object | Extra context dict. Shows up in Slack and is queryable in Snowflake via `PARSE_JSON`. |
| `environment` | no | string | `dev` or `prod`. Optional but recommended. |
| `slack_webhook` | no | string (URL) | Full Slack incoming-webhook URL (must start with `https://hooks.slack.com/`). Posts directly there instead of the severity default. See §2. |

> **Do not add `is_alert: false` here expecting it to suppress paging.** The
> failure_proxy handler hard-codes `is_alert=True` for everything that comes
> through this path (`lambdas/failure_proxy/schema.py:72`). The HTTP path is
> for **alerts**. For trending-only metrics, use Path B (log_scanner) or
> Path D (dbt meta block) — both default to `is_alert=False`.

### 4.2 Signing rule (CRITICAL)

The signature is **HMAC-SHA256(secret_key, raw_request_body) → hex**.

The body must be signed *before* being sent over the wire. The sender and
Monty must use the **exact same byte sequence** — if your HTTP library
re-serializes the JSON or adds whitespace between signing and sending, the
HMAC will mismatch and you'll get HTTP 401.

Always serialize the body to bytes **once**, sign those bytes, then send those
same bytes. Do not call `json.dumps()` twice with different defaults.

### 4.3 Python template (stdlib only)

```python
"""Send a single metric/failure to Monty via the HMAC-signed failure_proxy."""
import hashlib
import hmac
import json
import os
import urllib.request


def report_failure(
    pipeline_name: str,
    run_id: str,
    error_message: str,
    severity: str = "error",
    *,
    metric_name: str | None = None,
    metric_value: float | None = None,
    payload: dict | None = None,
    environment: str | None = None,
) -> None:
    """Post one row to Monty's CUSTOM_METRICS via failure_proxy.

    Silent no-op if MONTY_FAILURE_URL or MONTY_HMAC_SECRET is unset, so local
    dev does not page. Raises on HTTP 4xx/5xx in deployed environments —
    callers should *not* swallow this exception unless they have a reason;
    a lost alert is worse than a noisy traceback.
    """
    url = os.environ.get("MONTY_FAILURE_URL")
    secret = os.environ.get("MONTY_HMAC_SECRET")
    if not url or not secret:
        return  # local dev / not yet configured — silent

    body = {
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "error_message": error_message,
        "severity": severity,
    }
    if metric_name is not None:
        body["metric_name"] = metric_name
    if metric_value is not None:
        body["metric_value"] = metric_value
    if payload is not None:
        body["payload"] = payload
    if environment is not None:
        body["environment"] = environment

    raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()

    req = urllib.request.Request(
        url,
        data=raw,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Monty-Signature": sig,
        },
    )
    with urllib.request.urlopen(req, timeout=4) as resp:
        if resp.status >= 300:
            raise RuntimeError(f"monty rejected: {resp.status} {resp.read()!r}")
```

### 4.4 Shell template

```bash
# Required env: MONTY_FAILURE_URL, MONTY_HMAC_SECRET
BODY='{"pipeline_name":"my-pipeline","run_id":"'"$BUILD_ID"'","error_message":"x","severity":"error"}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$MONTY_HMAC_SECRET" -hex | awk '{print $NF}')
curl -sf -X POST "$MONTY_FAILURE_URL" \
  -H "Content-Type: application/json" \
  -H "X-Monty-Signature: $SIG" \
  -d "$BODY"
```

> Use `printf '%s'` not `echo -n`. `echo -n` appends a newline on some shells,
> breaking the signature. The `-sf` on curl makes it silent on success and
> fail on HTTP ≥ 400.

### 4.5 Node.js template

```javascript
const crypto = require('crypto');

async function reportFailure({pipelineName, runId, errorMessage, severity = 'error', metricName, metricValue, payload, environment}) {
  const url = process.env.MONTY_FAILURE_URL;
  const secret = process.env.MONTY_HMAC_SECRET;
  if (!url || !secret) return;  // local dev / not configured

  const body = {pipeline_name: pipelineName, run_id: runId, error_message: errorMessage, severity};
  if (metricName !== undefined) body.metric_name = metricName;
  if (metricValue !== undefined) body.metric_value = metricValue;
  if (payload !== undefined) body.payload = payload;
  if (environment !== undefined) body.environment = environment;

  const raw = JSON.stringify(body);
  const sig = crypto.createHmac('sha256', secret).update(raw).digest('hex');

  const resp = await fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json', 'X-Monty-Signature': sig},
    body: raw,
  });
  if (!resp.ok) throw new Error(`monty rejected: ${resp.status} ${await resp.text()}`);
}
```

### 4.6 Airflow on-failure callback

```python
def monty_failure_callback(context):
    from your_repo.monty_metric import report_failure
    report_failure(
        pipeline_name=context["dag"].dag_id,
        run_id=context["dag_run"].run_id,
        error_message=str(context.get("exception", "task failed")),
        severity="error",
        environment=os.environ.get("ENV", "prod"),
    )

with DAG(dag_id="my_pipeline", on_failure_callback=monty_failure_callback, ...) as dag:
    ...
```

### 4.7 GitHub Actions

```yaml
- name: Notify Monty on failure
  if: failure()
  env:
    MONTY_FAILURE_URL: ${{ secrets.MONTY_FAILURE_URL }}
    MONTY_HMAC_SECRET: ${{ secrets.MONTY_HMAC_SECRET }}
  run: |
    BODY='{"pipeline_name":"${{ github.repository }}","run_id":"${{ github.run_id }}","error_message":"workflow failed at ${{ github.job }}","severity":"error"}'
    SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$MONTY_HMAC_SECRET" -hex | awk '{print $NF}')
    curl -sf -X POST "$MONTY_FAILURE_URL" -H "Content-Type: application/json" -H "X-Monty-Signature: $SIG" -d "$BODY"
```

---

## 5. Path B — Structured stdout JSON (AWS Lambda only)

Use for: AWS Lambdas where you can add a `print()` call but don't want to add
credentials or extra dependencies. Monty's `log_scanner` is subscribed to the
log group and reads any line containing the key `MONITORING_METRIC`.

### 5.1 Required fields

| JSON key | Required | Notes |
| --- | --- | --- |
| `MONITORING_METRIC` | **yes** | The metric name. Presence of this key is what `log_scanner` matches on. |
| `pipeline` | **yes** | Short identifier. |
| `severity` | **yes** | `critical` / `error` / `warning` / `info`. |
| `value` | no | Numeric value. |
| `is_alert` | no | Defaults to `False`. Set `True` only when you want this to page Slack. |
| `environment` | no | `dev` or `prod`. |
| `payload` | no | Extra dict. |
| `slack_webhook` | no | Full Slack incoming-webhook URL (must start with `https://hooks.slack.com/`). Posts directly there instead of the severity default. See §2. |

### 5.2 Code template

```python
import json

# Anywhere inside your Lambda handler:
print(json.dumps({
    "MONITORING_METRIC": "rows_ingested",  # the metric name
    "value": rows_processed,                # number
    "pipeline": "ingest.shopify.orders",    # stable identifier
    "severity": "info",                     # trending — won't page
    "is_alert": False,                      # explicit even though it's the default
    "environment": "prod",
}))
```

For a failure-path alert from the same Lambda:

```python
print(json.dumps({
    "MONITORING_METRIC": "shopify_api_error",
    "pipeline": "ingest.shopify.orders",
    "severity": "error",
    "is_alert": True,                       # required to page
    "payload": {"http_status": resp.status_code, "endpoint": "/orders"},
}))
```

### 5.3 Wiring the subscription filter (one-time, per Lambda)

The log_scanner needs a CloudWatch Logs subscription filter on the producing
Lambda's log group. In the producer's CDK stack:

```python
from aws_cdk import aws_logs as logs
from aws_cdk import aws_logs_destinations as destinations
from aws_cdk import aws_lambda as _lambda

monty_log_scanner_arn = self.node.try_get_context("monty_log_scanner_arn")
if monty_log_scanner_arn:
    monty_fn = _lambda.Function.from_function_arn(self, "MontyLogScanner", monty_log_scanner_arn)
    logs.SubscriptionFilter(self, "MontySubscription",
        log_group=your_lambda.log_group,
        destination=destinations.LambdaDestination(monty_fn),
        filter_pattern=logs.FilterPattern.exists("$.MONITORING_METRIC"),
    )
```

Then deploy with the ARN as context:

```bash
cdk deploy your-stack \
  --context monty_log_scanner_arn=arn:aws:lambda:us-east-1:116981766237:function:monty-dev-logscanner
```

> **WARNING — the ARN MUST end in `-logscanner`.** Monty has four Lambdas
> per env (`-observer`, `-failureproxy`, `-snssubscriber`, `-logscanner`)
> and only `-logscanner` parses CloudWatch Logs events. The other three
> accept the subscription delivery without error but silently discard
> the payload — no row in `CUSTOM_METRICS`, no error in any log, and
> the wrong Lambda's invocation count quietly spikes at your emission
> rate. Verify the destination immediately after deploy:
>
> ```bash
> aws logs describe-subscription-filters \
>   --log-group-name /aws/lambda/<your-fn> --region us-east-1 \
>   --query 'subscriptionFilters[].[filterName,destinationArn]' --output table
> ```
>
> The ARN in the output must contain `:function:monty-<env>-logscanner`.
> Pull the correct value from the Monty stack's `LogScannerArn` CfnOutput
> rather than typing it by hand.

Omit the context flag to skip the wiring (useful for local-dev or
pre-Monty-deployment-in-account deploys).

---

## 6. Path C — SNS subscription (CloudWatch alarms)

Use for: any existing CloudWatch Alarm that already publishes to an SNS topic
when it fires. Every alarm message becomes a `critical` row in CUSTOM_METRICS
— Monty treats any alarm fire as an incident.

### 6.1 No code in the producer

You're already producing the alarm. All that's needed is to subscribe Monty
to the SNS topic.

```python
from aws_cdk import aws_sns_subscriptions as subs
from aws_cdk import aws_lambda as _lambda

monty_sns_subscriber_arn = self.node.try_get_context("monty_sns_subscriber_arn")
if monty_sns_subscriber_arn:
    monty_fn = _lambda.Function.from_function_arn(self, "MontySnsSubscriber", monty_sns_subscriber_arn)
    your_alarm_topic.add_subscription(subs.LambdaSubscription(monty_fn))
```

Deploy:

```bash
cdk deploy your-stack \
  --context monty_sns_subscriber_arn=arn:aws:lambda:us-east-1:116981766237:function:monty-dev-snssubscriber
```

### 6.2 Caveats

- All SNS-driven alerts are written with `severity=critical`. There is no way
  to demote them — by design, alarms shouldn't be ignored. If a particular
  alarm is too noisy, fix the alarm threshold; don't try to lower the Monty
  severity.
- Alarm OK (recovery) messages are also written as rows, with a recovery
  marker in the payload. Use this for a "service is back" Slack message if
  you want; ignore otherwise.
- To target a non-default Slack channel, set the SNS `MessageAttributes` key
  `slack_webhook` to the full webhook URL string (must start with
  `https://hooks.slack.com/`). CloudWatch Alarms don't set this, so this
  only matters for custom SNS publishers.

---

## 7. Path D — dbt `meta` block

Use for: any dbt model where you want a metric emitted every time the model
finishes. The `monty_post_hook()` macro reads the model's `config(meta=...)`
and writes one row to CUSTOM_METRICS.

> **Already set up?** If your `dbt_project.yml` already has the `+post-hook` /
> `on-run-end` wiring (§7.3), skip to §7.1. If your project has **no Monty
> macros yet**, do the one-time setup in §7.0 first.

### 7.0 First-time setup — a dbt project with no Monty macros yet

Monty's dbt path writes **directly** to `CUSTOM_METRICS` over the existing
Snowflake connection (no HTTP, no HMAC, no `requests` dependency). One-time setup:

**Step 1 — Snowflake grant.** The role dbt connects as must be able to INSERT
into the metrics table. Ask the Monty/platform team to run (once):

```sql
-- simplest: hand the dbt role Monty's writer role (INSERT-only)
GRANT ROLE MONTY_WRITER_ROLE TO ROLE <your_dbt_role>;
-- or grant the single privilege directly:
GRANT INSERT ON TABLE MONITORING_DB.MONITORING.CUSTOM_METRICS TO ROLE <your_dbt_role>;
```

**Step 2 — add two macros.** Create these files under `macros/`:

`macros/monty_post_hook.sql` — per-model metric emit (no-op unless a model
declares `monty_metric_name` + `monty_metric_sql`):

```jinja
{% macro monty_post_hook() %}
  {%- set meta          = config.get('meta', {}) or {} -%}
  {%- set metric_name   = meta.get('monty_metric_name') -%}
  {%- set metric_sql    = meta.get('monty_metric_sql') -%}
  {%- set severity      = (meta.get('monty_severity', 'info')) | lower -%}
  {%- set is_alert      = meta.get('monty_is_alert', false) -%}
  {%- set slack_webhook = meta.get('monty_slack_webhook', none) -%}
  {%- set environment   = target.name -%}   {# <-- routing key; see §7.0 caveat #}
  {#- Persist gate. dbt INSERTs straight into CUSTOM_METRICS (it never goes
      through metric_writer), so the Lambda-side severity routing does not apply
      to dbt rows — all four severities persist to Snowflake by default. Narrow
      `monty_persisted_severities` to re-block low-priority rows during a load
      spike. dbt's write volume is ~1 INSERT per run on an already-hot warehouse,
      so this is not a cost concern. -#}
  {%- set persisted = var('monty_persisted_severities', ['critical', 'error', 'warning', 'info']) | map('lower') | list -%}

  {%- if metric_name is none or metric_sql is none -%}
    select 1 as monty_skipped
  {%- elif severity not in persisted -%}
    select 1 as monty_skipped_severity
  {%- else -%}
    {#- `this` in the meta string renders at parse time (wrong schema); rewrite to hook-time path -#}
    {%- set parse_time_path = this.database ~ '.' ~ target.schema ~ '.' ~ this.identifier -%}
    {%- set metric_sql = metric_sql | replace(parse_time_path, this | string) -%}

    insert into {{ var('monty_database') }}.{{ var('monty_schema') }}.CUSTOM_METRICS
      (PIPELINE_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY, RUN_ID, PAYLOAD, IS_ALERT, ENVIRONMENT)
    select
      '{{ this.name }}',
      '{{ metric_name }}',
      ({{ metric_sql }})::float,
      '{{ severity }}',
      '{{ invocation_id }}',
      to_variant(object_construct(
        'model',  '{{ this }}',
        'schema', '{{ this.schema }}'
        {% if slack_webhook %}, 'slack_webhook', '{{ slack_webhook }}'{% endif %}
      )),
      {{ 'true' if is_alert else 'false' }},
      '{{ environment }}'
  {%- endif -%}
{% endmacro %}
```

`macros/monty_failure_hook.sql` — pages on any model that ends `dbt run` in
`error`/`fail` (one multi-row INSERT, run from `on-run-end`):

```jinja
{% macro monty_failure_hook() %}
  {%- if execute and results -%}
    {%- set select_clauses = [] -%}
    {%- for r in results if r.status in ('error', 'fail') -%}
      {%- set node    = r.node -%}
      {%- set message = ((r.message or '') | replace("'", "''"))[:4000] -%}
      {%- set _ = select_clauses.append(
            "select '" ~ node.name ~ "','" ~ target.name ~ "','pipeline_failure','error','"
            ~ invocation_id ~ "',to_variant(object_construct('error_message','" ~ message
            ~ "','unique_id','" ~ node.unique_id ~ "')),true") -%}
    {%- endfor -%}
    {%- if select_clauses | length > 0 -%}
      {%- set sql -%}
        insert into {{ var('monty_database') }}.{{ var('monty_schema') }}.CUSTOM_METRICS
          (PIPELINE_NAME, ENVIRONMENT, METRIC_NAME, SEVERITY, RUN_ID, PAYLOAD, IS_ALERT)
        {{ select_clauses | join('\n  union all\n  ') }}
      {%- endset -%}
      {%- do run_query(sql) -%}
    {%- endif -%}
  {%- endif -%}
{% endmacro %}
```

**Step 3 — wire `dbt_project.yml`.** Add the vars + project-wide hooks:

```yaml
vars:
  monty_database: "{{ env_var('MONTY_DATABASE', 'MONITORING_DB') }}"
  monty_schema:   "MONITORING"

models:
  <your_project_name>:
    +post-hook:
      - "{{ monty_post_hook() }}"   # no-op unless a model declares monty_metric_name + monty_metric_sql

on-run-end:
  - "{{ monty_failure_hook() }}"
```

**Step 4 — verify** with `dbt compile --select <one_model>` (inspect the
compiled post-hook), then a real `dbt run` and the queries in §8.

> **CAVEAT — `environment` = `target.name` (this drives channel routing).**
> Both macros stamp `ENVIRONMENT` with your **dbt target name**. The observer
> routes any row whose `environment` is not exactly `prod` (case-insensitive)
> to `#data-alerts-dev` (§2). So your **production dbt target must be named
> `prod`** — if it's `production`, `default`, etc., every prod metric silently
> lands in the dev channel. If you can't rename the target, set
> `environment` explicitly instead, e.g. via a var:
> `{%- set environment = var('monty_environment', target.name) -%}`.

### 7.1 Model template

```sql
-- models/marts/<your_model>.sql
{{
    config(
        materialized = 'table',
        meta = {
          'monty_metric_name': 'unsub_rate_pct_24h',
          'monty_metric_sql':  "select 100.0 * count_if(unsubscribed) / nullif(count(*), 0) from " ~ this ~ " where send_time >= dateadd(hour, -24, current_timestamp())",
          'monty_severity':    'warning',     -- optional, default 'info'
          'monty_is_alert':    false,         -- optional, default false (let AUDIT_REGISTRY decide)
        }
    )
}}

with sends as (
    -- existing model body
    ...
)
```

### 7.2 Required keys inside `meta`

| Key | Required | Notes |
| --- | --- | --- |
| `monty_metric_name` | **yes** | Distinct per model. Stable across runs. |
| `monty_metric_sql` | **yes** | A scalar `SELECT` expression. Wrapped as `(<sql>)::float` server-side. May reference `this`. |
| `monty_severity` | no | `critical` / `error` / `warning` / `info`. Default `info`. **All four persist from dbt** — the post-hook INSERTs directly into `CUSTOM_METRICS` and does not go through `metric_writer`, so the Lambda-side DynamoDB routing does not apply to dbt rows. Controlled by the `monty_persisted_severities` var (`dbt_project.yml`, default all four); narrow it to `['critical','error']` only to re-block during a Snowflake load spike. |
| `monty_is_alert` | no | `true` to always page on every run. Default `false` — prefer `AUDIT_REGISTRY` rules for thresholding. |
| `monty_slack_webhook` | no | Full Slack incoming-webhook URL (`https://hooks.slack.com/...`). The §7.0 `monty_post_hook` writes it into the row payload as `slack_webhook`; the observer then posts directly there (ALERT_OUTBOX label `custom-webhook`). Non-`hooks.slack.com` URLs are rejected (SSRF guard) and fall back to env/severity routing. See §2. |

> **Legacy `monty_channel_override` is a no-op.** Older copies of these macros
> wrote a `channel_override` into the payload, but the observer ignores it
> entirely — it changes nothing. Use `monty_slack_webhook` for a specific
> channel, or rely on `environment`/severity routing (§2).

> **Both `monty_metric_name` and `monty_metric_sql` must be present together.**
> If only one is set, the post-hook silently no-ops — by design, so half-written
> instrumentation doesn't fail dbt runs.

### 7.3 dbt run failures are already wired

You do **not** need to add anything for the failure path. The `on-run-end`
hook in `dbt_project.yml` already runs `monty_failure_hook()` which inserts
an alert row for any model that finished with status `error` or `fail`.
Pipeline name = model name, severity = `error`, `is_alert=TRUE`.

---

## 8. Verification — always do this after wiring up a new metric

### 8.1 Confirm rows are landing in Snowflake

```sql
select OCCURRED_AT, PIPELINE_NAME, METRIC_NAME, SEVERITY, IS_ALERT, SENT_TO_SLACK, PAYLOAD
from MONITORING_DB.MONITORING.CUSTOM_METRICS
where PIPELINE_NAME = '<your-pipeline-name>'
order by OCCURRED_AT desc
limit 20;
```

You should see at least one row from the past few minutes.

### 8.2 For alerts (is_alert=TRUE), confirm Slack delivery

```sql
select ID, METRIC_NAME, SEVERITY, IS_ALERT, SENT_TO_SLACK, SENT_AT
from MONITORING_DB.MONITORING.CUSTOM_METRICS
where PIPELINE_NAME = '<your-pipeline-name>'
  and IS_ALERT = TRUE
order by OCCURRED_AT desc
limit 5;
```

Within ~60 seconds of inserting a row with `IS_ALERT=TRUE`:
- `SENT_TO_SLACK` flips to `TRUE`
- `SENT_AT` is populated
- A message appears in `#data-incidents` (critical/error) or `#data-alerts` (warning) — or `#data-alerts-dev` if the row's `environment` is not `prod` (see §2)

If the row exists but `SENT_TO_SLACK` stays `FALSE` after 2 minutes, check:

```sql
select * from MONITORING_DB.MONITORING.ALERT_OUTBOX
where METRIC_ID = <id from query above>
order by SENT_AT desc;
```

`status='failed'` rows have the Slack response error in `ERROR_MESSAGE`.

### 8.3 For Path A specifically: confirm the proxy accepts it

If the row never lands in CUSTOM_METRICS, the request was rejected before
reaching Snowflake. Trace the HTTP response:

- **HTTP 202** with `{"status":"accepted"}` — success
- **HTTP 400** with `{"error":"missing or empty required field: …"}` — fix the payload shape
- **HTTP 401** with `{"error":"invalid signature"}` — HMAC mismatch; see [README §9 troubleshooting](README.md#9-troubleshooting)
- **HTTP 5xx** — Snowflake write failed; check
  `aws logs tail /aws/lambda/monty-<env>-failureproxy --since 5m`

---

## 9. Common pitfalls

| Symptom | Cause | Fix |
| --- | --- | --- |
| HTTP 401 on Path A right after first deploy | Lambda has cached the empty-template HMAC in `lru_cache`. | Bump the Lambda's config to force a cold start: `aws lambda update-function-configuration --function-name monty-<env>-failureproxy --region us-east-1 --description "$(date +%s)"`. |
| HTTP 401 on Path A from CI but works locally | The CI shell stripped quotes from the body or re-encoded JSON between signing and sending. | Sign the literal bytes you send. Use `printf '%s'` not `echo`. Don't pretty-print the body. |
| HTTP 400 "missing required field: run_id" | You omitted `run_id`. | Always include `run_id` for Path A — there is no default. Generate one if you don't have a natural identifier (UUID, timestamp, build ID). |
| Path B row never appears in CUSTOM_METRICS | The log group has no subscription filter to log_scanner, or the filter pattern doesn't match. | Confirm the filter exists: `aws logs describe-subscription-filters --log-group-name /aws/lambda/<your-fn> --region us-east-1`. The pattern must be `{ $.MONITORING_METRIC = "*" }` or `$.MONITORING_METRIC` exists. |
| Path B row never appears, filter exists, pattern matches, log_scanner has zero invocations | The subscription filter's `destinationArn` points at a Monty Lambda other than `-logscanner` (typically `-observer`). The wrong Lambda accepts the delivery and silently discards the payload — no error anywhere. Usually caused by passing the wrong ARN to `--context monty_log_scanner_arn=...` at deploy time. | `aws logs describe-subscription-filters --log-group-name /aws/lambda/<your-fn> --region us-east-1 --query 'subscriptionFilters[].destinationArn'` — value must contain `:function:monty-<env>-logscanner`. Fix the producer stack's context value, redeploy, then verify. Confirmed-seen failure mode (2026-06-04, `ai-ingest-postgressql`). |
| Path C row appears but severity is wrong | All SNS rows are forced to `critical`. | This is by design. Don't try to override it from the publisher. |
| Path D model runs but no metric appears | `monty_metric_name` or `monty_metric_sql` is missing, or one of them is null in the compiled `meta` dict. | Both keys must be set together. Run `dbt compile --select your_model` and inspect the compiled SQL for the post-hook. |
| Local `dbt run` errors with "MONTY_FAILURE_URL not set" | Production env var leaked into a local profile. | Set `MONTY_FAILURE_URL=""` and `MONTY_HMAC_SECRET=""` in your local profile — the macros short-circuit on empty values. |
| Secrets visible in your shell history after testing | You ran `echo $MONTY_HMAC_SECRET` or pasted it into a command. | Treat as a rotation event. Generate a new HMAC via `openssl rand -hex 32`, update `monty-<env>-secrets.MONTY_HMAC_SECRET`, force a cold start on all four Lambdas, and update every upstream consumer's secret in lockstep. |

---

## 10. What NOT to do

- **Do not insert directly into `CUSTOM_METRICS` from outside Monty.** The only
  exception is the dbt `monty_failure_hook()` macro, which is part of Monty's
  own code. If you find yourself reaching for `INSERT INTO CUSTOM_METRICS`,
  you're on the wrong path — go back to §1 and pick A/B/C/D.
- **Do not call Slack directly.** Monty's `observer` is the only thing
  allowed to post to Slack. The contract is: you write a row, observer
  delivers it. Bypassing the observer means lost retries, duplicate
  deliveries on failure, and no audit trail in `ALERT_OUTBOX`.
- **Do not invent new severities.** The four are enforced server-side and
  any other value returns HTTP 400 on Path A or is silently dropped on the
  others.
- **Do not put `is_alert: true` on `info` severity metrics.** It will page
  to `#data-incidents` regardless of the severity-to-channel mapping
  expectation. If you want a Slack message, use `warning`/`error`/`critical`
  with `is_alert=true`.
- **Do not commit the HMAC secret, Snowflake password, or Slack webhooks.**
  They live in the calling repo's Secrets Manager / CI secret store. If they
  leak into git history, rotate immediately and force a cold start on all
  four Monty Lambdas.

---

## 11. Quick reference card

```
                              ┌─────────────────────────────────────────┐
  Path A — HTTP POST          │ POST <FailureProxyUrl>                  │
   any language, any infra    │ Headers: X-Monty-Signature: <HMAC hex>  │
                              │ Body: pipeline_name, run_id,            │
                              │       error_message, severity (req)     │
                              └─────────────────────────────────────────┘
  Path B — stdout JSON       print(json.dumps({"MONITORING_METRIC": "...",
   AWS Lambda only                              "pipeline": "...", "severity": "..."}))
                              + CDK SubscriptionFilter → LogScannerArn

  Path C — SNS               your_alarm_topic.add_subscription(
   CloudWatch alarms            subs.LambdaSubscription(monty_sns_subscriber))

  Path D — dbt meta          config(meta={'monty_metric_name': '...',
   dbt models                              'monty_metric_sql':  '...'})
```

When in doubt: **Path A**. It works from anywhere, the schema is strict and
self-documenting, and the failure modes are all visible in the HTTP response.
