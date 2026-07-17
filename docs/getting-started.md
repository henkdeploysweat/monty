# Monty: Beginner's Integration Guide

Monty is the central monitoring brain for our data platform. Every pipeline failure, data quality check, and KPI metric flows into **one Snowflake table**, and **one Lambda** decides what hits Slack. This guide walks you through how to plug each part of the stack into Monty, starting from scratch.

---

## How Monty Works (the 30-second version)

```
Your pipeline         →   Monty ingest layer   →   CUSTOM_METRICS table   →   Observer Lambda   →   Slack
(AWS / dbt / webhook)     (4 different paths)       (Snowflake)                (every 1 minute)
```

Every metric ends up as a row in `MONITORING_DB.MONITORING.CUSTOM_METRICS`. The Observer Lambda wakes up every minute, finds unsent alerts, and posts them to Slack. You never talk to Slack directly — Monty does that for you.

---

## Part 1: Before You Start — What You Need

Before integrating anything, you need credentials from the platform team:

| What you need | Where it comes from |
|---|---|
| `MONTY_FAILURE_PROXY_URL` | CDK output after Monty is deployed |
| `MONTY_HMAC_SECRET` | AWS Secrets Manager (`monty-<env>-secrets`) |
| Snowflake user + role | `MONTY_WRITER_ROLE` (INSERT-only on `CUSTOM_METRICS`) |

> **Note:** If you are on the platform team setting up Monty for the first time, start with the [infrastructure setup](#appendix-infrastructure-setup) at the bottom of this guide before the sections below.

---

## Part 2: Integrating an AWS Pipeline (Lambda / CloudWatch)

There are two ways an AWS Lambda can send metrics to Monty. Use structured logging if you just want to capture failures cheaply. Use the HTTP proxy if you want to send custom metrics with specific values.

### Option A — Structured Logging (zero-config, easiest)

Monty's `log_scanner` Lambda subscribes to CloudWatch Logs from your ingest Lambda. Any log line that contains the key `MONITORING_METRIC` is automatically picked up.

**Step 1:** In your Lambda, print a JSON log line with this exact shape:

```python
import json

# inside your handler, whenever something is worth monitoring:
print(json.dumps({
    "MONITORING_METRIC": "rows_ingested",   # the metric name
    "value": 1500,                          # a number
    "pipeline": "my-pipeline-name",         # matches your pipeline name in Snowflake
    "severity": "info"                      # critical | error | warning | info
}))
```

That's it. No extra dependencies, no credentials in your Lambda.

**Step 2:** Ask the platform team to add a CloudWatch Logs **subscription filter** from your Lambda's log group to Monty's `log_scanner` Lambda. This is a one-time CDK/console change on the Monty side — you do not need to touch your own infrastructure.

**When a metric becomes an alert:** If `severity` is `error` or `critical`, the Observer will post it to `#data-incidents`. `warning` goes to `#data-alerts`. `info` is recorded but never sent to Slack. **Exception:** any metric whose `environment` is not `prod` is routed to `#data-alerts-dev` regardless of severity (set `environment` in your payload; defaults to the Lambda's `MONTY_ENV`).

---

### Option B — SNS Alarm (for CloudWatch metric alarms)

If you already have a CloudWatch Alarm wired to an SNS topic, Monty can subscribe to it directly. Every alarm that fires becomes a `critical` severity alert in `CUSTOM_METRICS`.

**Step 1:** In your CDK stack, subscribe Monty's `sns_subscriber` Lambda ARN to your alarm's SNS topic:

```python
# In your ingest stack's CDK code:
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs

your_sns_topic.add_subscription(
    subs.LambdaSubscription(monty_sns_subscriber_fn)
)
```

> The `monty_sns_subscriber_fn` ARN comes from the Monty CDK stack outputs. Ask the platform team.

All severity from SNS alarms is automatically set to `critical` — Monty treats any CloudWatch alarm firing as an incident.

---

### Option C — HTTP POST (for custom metrics with real values)

Use this when you want to send a specific numeric value (e.g., rows processed, API latency, error count) rather than just "something went wrong".

**Step 1:** Add the Monty URL and secret to your Lambda's environment (via your own Secrets Manager secret or CDK environment variables):

```
MONTY_FAILURE_URL = <the API Gateway URL from CDK outputs>/failure
MONTY_HMAC_SECRET = <same value as in monty-<env>-secrets>
```

**Step 2:** In your Lambda code, POST to Monty:

```python
import json
import hmac
import hashlib
import urllib.request

def send_to_monty(pipeline: str, metric_name: str, value: float, severity: str, env: str):
    """Send a metric to Monty's HTTP failure proxy."""
    url = os.environ["MONTY_FAILURE_URL"]
    secret = os.environ["MONTY_HMAC_SECRET"].encode()

    payload = json.dumps({
        "pipeline_name": pipeline,
        "metric_name": metric_name,
        "metric_value": value,
        "severity": severity,       # critical | error | warning | info
        "environment": env,
        "is_alert": True            # set False for trending-only metrics
    }).encode()

    # HMAC signature — Monty rejects requests without this
    signature = hmac.new(secret, payload, hashlib.sha256).hexdigest()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-Monty-Signature": signature,
        },
        method="POST",
    )
    urllib.request.urlopen(req, timeout=5)
```

**Payload fields:**

| Field | Required | Description |
|---|---|---|
| `pipeline_name` | yes | Short identifier, e.g. `garmin-ingest` |
| `metric_name` | yes | What you are measuring, e.g. `rows_loaded` |
| `metric_value` | no | A number. Omit if this is a pure failure alert |
| `severity` | yes | `critical`, `error`, `warning`, or `info` |
| `environment` | yes | `dev` or `prod` |
| `is_alert` | no | `true` = post to Slack when Observer picks it up. Default `true` |

If the signature is wrong, Monty returns `403`. If required fields are missing, it returns `400`.

---

## Part 3: Integrating dbt

Monty's dbt integration lives in two macros in `sweat_analytics_coredbt`. The setup has already been done — you just need to configure your models to use it.

### Environment variables your dbt runner needs

Add these to your `profiles.yml` environment or CI secret store:

```bash
MONTY_DATABASE=MONITORING_DB      # or leave unset — defaults to MONITORING_DB
MONTY_FAILURE_PROXY_URL=<url>     # only needed if you use the HTTP path (not required for direct Snowflake inserts)
MONTY_HMAC_SECRET=<secret>        # same
```

For local development, you can set `MONTY_FAILURE_PROXY_URL=""` and `MONTY_HMAC_SECRET=""` — the macros silently no-op when these are empty, so your local `dbt run` still works without Monty being deployed.

---

### How dbt failure alerts work (automatic — no config needed)

The `monty_failure_hook()` macro already runs at `on-run-end`. If any model in your run finishes with status `error` or `fail`, Monty automatically inserts an alert row into `CUSTOM_METRICS` with:

- `metric_name = 'pipeline_failure'`
- `is_alert = TRUE`
- `severity = 'error'`

You do not need to add anything to your model files. Just make sure the dbt project has `on-run-end: ["{{ monty_failure_hook() }}"]` in `dbt_project.yml` (already configured in the shared project).

---

### How to send a custom KPI metric from a dbt model

If you want a model to emit a metric every time it runs (e.g., "how many active users did this model produce?"), add a `meta` block to the model's config:

```sql
-- models/marts/fct_active_users.sql

{{
    config(
        materialized='table',
        meta={
            'monty_metric_name': 'active_users',
            'monty_metric_sql': 'select count(*) from {{ this }}'
        }
    )
}}

select ...
```

After the model finishes, the `monty_post_hook()` macro runs `monty_metric_sql`, takes the scalar result, and inserts it into `CUSTOM_METRICS` with `is_alert = FALSE` (trending only — it will not page Slack unless you add an `AUDIT_REGISTRY` rule for it).

**Both keys are required** if you use either one. If only one is set, the hook silently skips.

---

### Adding a data quality rule via AUDIT_REGISTRY

The Auditor runs every hour and evaluates SQL rules you define in `MONITORING_DB.MONITORING.AUDIT_REGISTRY`. No code changes needed — just insert a row.

```sql
INSERT INTO MONITORING_DB.MONITORING.AUDIT_REGISTRY (
    PIPELINE_NAME,
    METRIC_NAME,
    SQL_CHECK,
    COMPARATOR,
    THRESHOLD,
    SEVERITY,
    IS_ACTIVE
) VALUES (
    'fct_active_users',           -- your pipeline/model name
    'active_users_minimum',       -- a name for this rule
    'select count(*) from ANALYTICS.MARTS.FCT_ACTIVE_USERS where date = current_date()',
    '<',                          -- alert when count IS LESS THAN threshold
    1000,                         -- threshold value
    'warning',                    -- critical | error | warning | info
    TRUE
);
```

**Comparators:** `>`, `<`, `>=`, `<=`, `==`, `!=`

The rule fires if the SQL result compares to the threshold as specified. In the example above: if fewer than 1000 active users exist for today, Monty creates a `warning` alert.

The SQL can be anything that returns a single number. It can reference any table Monty's service role can read — ask the platform team to grant `SELECT` on new schemas if needed.

---

## Part 4: Integrating Anything That Can Send a Webhook

If a tool can make an HTTP POST request, it can send alerts to Monty. This covers tools like:

- Airflow / Prefect (pipeline orchestrators)
- Fivetran / Airbyte (data connectors)
- GitHub Actions / CI pipelines
- Custom scripts on EC2 or ECS
- Any SaaS product with a "webhook on failure" option

All of these use the same **HTTP failure proxy** endpoint described in Part 2, Option C.

### Step 1: Get your credentials

You need two things:
- `MONTY_FAILURE_URL` — the full URL ending in `/failure`
- `MONTY_HMAC_SECRET` — the shared signing secret

Store these securely (AWS Secrets Manager, your CI/CD secret store, etc.). Never hardcode them.

### Step 2: Sign your request

Every POST must include the HMAC-SHA256 signature header. Here is how to generate it in common languages:

**Python:**
```python
import hmac, hashlib, json
body = json.dumps(payload).encode()
sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
headers["X-Monty-Signature"] = sig
```

**Node.js:**
```javascript
const crypto = require('crypto');
const body = JSON.stringify(payload);
const sig = crypto.createHmac('sha256', secret).update(body).digest('hex');
headers['X-Monty-Signature'] = sig;
```

**Shell (curl):**
```bash
BODY='{"pipeline_name":"my-script","metric_name":"pipeline_failure","severity":"error","environment":"prod","is_alert":true}'
SIG=$(echo -n "$BODY" | openssl dgst -sha256 -hmac "$MONTY_HMAC_SECRET" | awk '{print $2}')
curl -X POST "$MONTY_FAILURE_URL" \
  -H "Content-Type: application/json" \
  -H "X-Monty-Signature: $SIG" \
  -d "$BODY"
```

### Step 3: Send the payload

```json
{
    "pipeline_name": "fivetran-salesforce",
    "metric_name": "sync_failure",
    "severity": "error",
    "environment": "prod",
    "is_alert": true
}
```

That's it. Monty's Observer picks it up within 60 seconds and posts to `#data-incidents`.

### Airflow example (on-failure callback)

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests, hmac, hashlib, json, os

def monty_failure_callback(context):
    """Send an alert to Monty when any task in this DAG fails."""
    url = os.environ["MONTY_FAILURE_URL"]
    secret = os.environ["MONTY_HMAC_SECRET"].encode()
    payload = json.dumps({
        "pipeline_name": context["dag"].dag_id,
        "metric_name": "task_failure",
        "severity": "error",
        "environment": os.environ.get("ENV", "prod"),
        "is_alert": True,
    }).encode()
    sig = hmac.new(secret, payload, hashlib.sha256).hexdigest()
    requests.post(url, data=payload, headers={
        "Content-Type": "application/json",
        "X-Monty-Signature": sig,
    }, timeout=5)

with DAG(
    dag_id="my_pipeline",
    on_failure_callback=monty_failure_callback,   # fires on any task failure
    ...
) as dag:
    ...
```

### GitHub Actions example

```yaml
# .github/workflows/my-pipeline.yml
- name: Notify Monty on failure
  if: failure()
  env:
    MONTY_FAILURE_URL: ${{ secrets.MONTY_FAILURE_URL }}
    MONTY_HMAC_SECRET: ${{ secrets.MONTY_HMAC_SECRET }}
  run: |
    BODY='{"pipeline_name":"my-repo-ci","metric_name":"build_failure","severity":"error","environment":"prod","is_alert":true}'
    SIG=$(echo -n "$BODY" | openssl dgst -sha256 -hmac "$MONTY_HMAC_SECRET" | awk '{print $2}')
    curl -sf -X POST "$MONTY_FAILURE_URL" \
      -H "Content-Type: application/json" \
      -H "X-Monty-Signature: $SIG" \
      -d "$BODY"
```

---

## Part 5: Understanding Severity Levels

Every metric row has a severity. This controls which Slack channel it goes to (and whether it goes at all).

| Severity | Slack channel | When to use |
|---|---|---|
| `critical` | `#data-incidents` | Data is down or corrupted right now. Wake someone up. |
| `error` | `#data-incidents` | A pipeline failed. Needs attention today. |
| `warning` | `#data-alerts` | Something looks off but data is still flowing. |
| `info` | _(not sent to Slack)_ | Trending metrics — row counts, durations, KPIs. |

Rule of thumb: if a human needs to act within the hour, use `error` or `critical`. If it can wait, use `warning`. If you just want historical data, use `info`.

> ℹ️ **All four severities persist** — the temporary `critical`/`error`-only
> gate was reverted 2026-07-10. The metric writer still drops (logs, doesn't
> error) any severity not in `PERSISTED_SEVERITIES`
> (`lambdas/shared/metric_writer.py`), so it can be re-narrowed to
> `("critical", "error")` in one line if Snowflake load spikes again. See
> `ADD_METRIC.md` §2.

---

## Part 6: Verifying Your Integration

After sending a metric (via any path), check that it arrived:

```sql
-- Run in Snowflake
select *
from MONITORING_DB.MONITORING.CUSTOM_METRICS
where PIPELINE_NAME = 'your-pipeline-name'
order by OCCURRED_AT desc
limit 10;
```

For alerts specifically, check whether the Observer sent them to Slack:

```sql
select ID, METRIC_NAME, SEVERITY, IS_ALERT, SENT_TO_SLACK, SENT_AT
from MONITORING_DB.MONITORING.CUSTOM_METRICS
where PIPELINE_NAME = 'your-pipeline-name'
  and IS_ALERT = TRUE
order by OCCURRED_AT desc
limit 10;
```

If `IS_ALERT = TRUE` but `SENT_TO_SLACK = FALSE` and `SENT_AT IS NULL`, the Observer has not processed it yet (wait up to 60 seconds) or Slack delivery is failing (check CloudWatch Logs for the Observer Lambda).

---

## Appendix: Infrastructure Setup (Platform Team Only)

This section is for first-time deployment of Monty itself. Skip this if Monty is already running.

### 1. Deploy Snowflake objects

```bash
cd /path/to/monty
make sql-apply | sh    # applies all SQL files in order
```

This creates the database, schema, roles, warehouse, tables, stored proc, and task.

### 2. Populate Secrets Manager

Create a secret named `monty-<env>-secrets` in AWS Secrets Manager with this JSON structure:

```json
{
    "user": "MONTY_SVC",
    "password": "<snowflake password>",
    "account": "<account>.<region>",
    "warehouse": "MONTY_WH",
    "database": "MONITORING_DB",
    "schema": "MONITORING",
    "role": "MONTY_SVC_ROLE",
    "MONTY_HMAC_SECRET": "<64-char random hex string>",
    "SLACK_WEBHOOK_INCIDENTS": "https://hooks.slack.com/services/...",
    "SLACK_WEBHOOK_ALERTS": "https://hooks.slack.com/services/...",
    "SLACK_WEBHOOK_DEV": "https://hooks.slack.com/services/...",
    "ANTHROPIC_API_KEY": "<optional: sk-ant-... — enables the AI error summary in Slack>"
}
```

Generate the HMAC secret with: `python3 -c "import secrets; print(secrets.token_hex(32))"`

### 3. Deploy the CDK stack

```bash
make cdk-deploy env=dev    # or env=prod
```

This creates the four Lambda functions, API Gateway, EventBridge rule, and wires everything together.

### 4. Note the CDK outputs

After deploy, CDK prints `FailureProxyUrl`. Share this (and `MONTY_HMAC_SECRET`) with every team that needs to integrate.

### 5. Verify the Observer is running

In EventBridge, confirm the rule `monty-<env>-observer-schedule` is enabled and triggering every minute. After a few minutes, run:

```sql
select * from MONITORING_DB.MONITORING.CUSTOM_METRICS
where METRIC_NAME = 'auditor_heartbeat'
order by OCCURRED_AT desc limit 5;
```

If rows appear, Monty is alive.

---

## Quick Reference

| I want to... | Use this |
|---|---|
| Alert when my Lambda fails | Structured log with `MONITORING_METRIC` key |
| Alert when a CloudWatch Alarm fires | SNS subscription → Monty `sns_subscriber` |
| Send a specific metric value from a Lambda | HTTP POST to `failure_proxy` |
| Alert when a dbt model fails | Already automatic via `monty_failure_hook()` |
| Send a row count from a dbt model | `meta.monty_metric_name` + `meta.monty_metric_sql` in model config |
| Define a SQL-based data quality rule | INSERT into `AUDIT_REGISTRY` |
| Alert from Airflow / GitHub Actions / any HTTP client | HTTP POST to `failure_proxy` with HMAC signature |
| Check if an alert was sent to Slack | Query `CUSTOM_METRICS` where `IS_ALERT = TRUE` |
