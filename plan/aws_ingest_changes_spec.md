# AWS Ingest Changes Spec — `analytics-ingest-iterate-repo`

> Companion to `plan.md`. PR plan to wire `analytics-ingest-iterate-repo` (and the
> 50+ sibling ingest stacks built from the same template) into Monty.
> Repo path on disk: `/Users/henkduplooy/Documents/Sweatran/analytics-ingest-iterate-repo/`.
> Handed off — not executed by the Monty build itself.

## Goal

After deployment, every ingest stack:

1. **POSTs structured failures to Monty's failure-proxy** when an ingest Lambda
   raises (within the same Lambda invocation, before the exception bubbles up to
   Step Functions / EventBridge).
2. **Forwards CloudWatch alarm fires** from its existing
   `ai-ingest-${ServiceName}-alerts` SNS topic to Monty's `sns_subscriber` Lambda.
3. **Emits free-form custom metrics** by `print(json.dumps({"MONITORING_METRIC": ...}))`,
   captured by Monty's `log_scanner` via a CloudWatch Logs SubscriptionFilter on
   each Lambda's log group.

No ingest Lambda calls Slack directly. No new IAM principal — Monty's three
receivers (failure-proxy HTTP, sns_subscriber, log_scanner) own the cross-account
permissions.

## Repo touched

```
analytics-ingest-iterate-repo/
├── IngestionCode/
│   ├── monty_metric.py                  (new — emit_metric + report_failure)
│   └── ai_ingest_iterate.py             (edit — wrap exception handlers)
├── CloudformationStack/
│   └── ai_ingest_stack_iterate.py       (edit — SubscriptionFilter + SNS sub + secret keys)
├── requirements.txt                     (edit — no new deps if we stick to stdlib urllib)
└── README.md                            (edit — add "Monty integration" section)
```

The same edits land in every ingest stack repo by templating, not just `iterate`.
List of repos to mirror lives in the `analytics-ingest-iterate-repo` Makefile.

## 1. New helper — `IngestionCode/monty_metric.py`

```python
"""Monty integration helpers — emit custom metrics and report failures.

Two paths:

  * `emit_metric(name, value, severity, payload)` — stdout JSON line with the
    `MONITORING_METRIC` key. Picked up by Monty's log_scanner via a CloudWatch
    Logs SubscriptionFilter, written to CUSTOM_METRICS without an HTTP call.
    Best for high-rate trending metrics (row counts, timings, etc.).

  * `report_failure(pipeline_name, run_id, error_message, severity)` — HMAC-signed
    HTTP POST to Monty's failure-proxy. Use this from exception handlers when
    the failure must page within seconds even if the log stream is delayed.

Stdlib only — `urllib.request`, `hmac`, `json`. No `requests` dependency, no
extra Lambda layer.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import urllib.request
import urllib.error
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# These two come from the per-stack secret (ServiceName-secrets) — surfaced to the
# Lambda env in CDK via secret.grant_read + a top-level fetch. See section 3.
_MONTY_FAILURE_URL = os.environ.get("MONTY_FAILURE_URL", "")
_MONTY_HMAC_SECRET = os.environ.get("MONTY_HMAC_SECRET", "")

_LOG_KEY = "MONITORING_METRIC"


def emit_metric(
    name: str,
    value: float | int | None = None,
    severity: str = "info",
    *,
    pipeline: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    is_alert: bool = False,
) -> None:
    """Emit one JSON line on stdout in the shape Monty's log_scanner expects.

    Cheap (no network), at-most-once (Lambda log delivery isn't guaranteed; use
    `report_failure` instead for must-deliver paths).
    """
    body = {
        _LOG_KEY: name,
        "value": value,
        "severity": severity,
        "pipeline": pipeline or os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown"),
        "is_alert": is_alert,
        "payload": payload or {},
    }
    # plain print so it lands in CloudWatch Logs as a single-line event
    print(json.dumps(body, default=str))


def report_failure(
    pipeline_name: str,
    run_id: str,
    error_message: str,
    severity: str = "error",
    *,
    payload: Optional[Dict[str, Any]] = None,
    timeout: float = 4.0,
) -> bool:
    """POST a signed failure record to Monty's failure-proxy.

    Returns True on 2xx, False otherwise. **Never raises** — alerting must not
    mask the original ingest exception. The caller still re-raises after this.
    """
    if not _MONTY_FAILURE_URL or not _MONTY_HMAC_SECRET:
        logger.warning("MONTY_FAILURE_URL/SECRET not set — skipping failure report.")
        return False

    body_dict: Dict[str, Any] = {
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "error_message": error_message[:4000],
        "severity": severity,
        "payload": payload or {},
    }
    body_bytes = json.dumps(body_dict).encode("utf-8")
    sig = hmac.new(
        _MONTY_HMAC_SECRET.encode("utf-8"), body_bytes, hashlib.sha256
    ).hexdigest()

    req = urllib.request.Request(
        _MONTY_FAILURE_URL,
        data=body_bytes,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Monty-Signature": sig,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ok = 200 <= resp.status < 300
            if not ok:
                logger.error("Monty failure-proxy returned %s", resp.status)
            return ok
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        logger.error("Monty failure-proxy unreachable: %s", exc)
        return False
```

**Why stdlib urllib?** The ingest images already pull `requests` for API calls,
but the failure path needs a 4-second timeout that doesn't compete with `requests`
session handling — and we want this helper to be droppable into any Lambda
(including ones that don't ship `requests`).

## 2. Wrapping exception handlers — `IngestionCode/ai_ingest_iterate.py`

Three sites today swallow exceptions with a `print(...)` — convert each to also
call `report_failure` then re-raise (top-level handler) or just call (per-survey
loops, where swallowing is intentional):

### 2a. Top-level `lambda_handler` (line 491 in current file)

Wrap the *whole body* of `lambda_handler` so any unexpected error pages, then
re-raises so Step Functions still flips to `ExecutionFailed`:

```python
import uuid
from IngestionCode.monty_metric import report_failure

def lambda_handler(event, context):
    run_id = (event.get("run_id")
              or (context.aws_request_id if context else f"local-{uuid.uuid4()}"))
    pipeline = "ingest.iterate"  # one constant per stack; templatable
    try:
        # ----- existing body unchanged from line 471 to line 564 -----
        target_date = resolve_target_date(event)
        ...
        return [
            ...   # the three-record list that build_all_* returned
        ]
    except Exception as exc:
        report_failure(
            pipeline_name=pipeline,
            run_id=run_id,
            error_message=f"{type(exc).__name__}: {exc}",
            severity="critical",
            payload={
                "event": event,
                "function_name": getattr(context, "function_name", None),
            },
        )
        raise
```

### 2b. `build_all_surveys_compiled` (line 377)

`Failed fetching /survey list` is currently swallowed. Add a non-raising
`report_failure` so we can see how often it happens *without* changing the
swallow semantics:

```python
except Exception as e:
    print(f"Failed fetching /survey list: {e}")
    report_failure(
        pipeline_name="ingest.iterate.surveys",
        run_id=os.environ.get("AWS_LAMBDA_REQUEST_ID", "unknown"),
        error_message=f"Failed fetching /survey list: {e}",
        severity="error",
    )
```

### 2c. `build_all_stats` and `build_all_response_groups_latest`
(lines 422 and 462 in current file)

Same pattern — keep the swallow, add `report_failure` with a per-survey severity
of `warning` (one survey failing isn't critical — pipeline can complete with
partial data):

```python
except Exception as e:
    print(f"Failed stats for survey {sid}: {e}")
    report_failure(
        pipeline_name="ingest.iterate.stats",
        run_id=os.environ.get("AWS_LAMBDA_REQUEST_ID", "unknown"),
        error_message=f"Failed stats for survey {sid}: {e}",
        severity="warning",
        payload={"survey_id": sid},
    )
```

**Why per-call instead of one outer wrap:** the inner failures don't propagate;
one outer wrap would never see them.

### 2d. Optional — `emit_metric` for trending

At the end of `lambda_handler` (just before `return`), emit success counters so
trending dashboards work without a custom CloudWatch metric:

```python
emit_metric("survey_count", len(all_surveys_compiled), pipeline="ingest.iterate")
emit_metric("stats_count",  len(all_stats),            pipeline="ingest.iterate")
emit_metric("response_groups_count",
            len(all_response_groups), pipeline="ingest.iterate")
```

Cheap — three `print()` calls — and seeds the `AUDIT_REGISTRY` for future
threshold rules without code changes.

## 3. CDK changes — `CloudformationStack/ai_ingest_stack_iterate.py`

### 3a. Imports — append:

```python
from aws_cdk import (
    aws_logs_destinations as logs_destinations,
    aws_sns_subscriptions as sns_subscriptions,
)
```

### 3b. Two new CDK context params (passed in via `cdk deploy --context ...` or
`cdk.json`):

```python
monty_log_scanner_arn = self.node.try_get_context("monty_log_scanner_arn")
monty_sns_subscriber_arn = self.node.try_get_context("monty_sns_subscriber_arn")
```

(Both come from Monty's CfnOutputs `LogScannerArn` and `SnsSubscriberArn` —
documented in `Monty/README.md`.)

### 3c. Extend the per-stack secret (around line 99–123) with two new keys:

```python
ingest_secret = secretsmanager.Secret(
    self,
    "IngestSecret",
    secret_name=Fn.sub("${ServiceName}-secrets", {"ServiceName": service_name.value_as_string}),
    generate_secret_string=secretsmanager.SecretStringGenerator(
        secret_string_template=json.dumps({
            "API_KEY": "TBD",
            "SNOWFLAKE_USER":     "TBD",
            "SNOWFLAKE_PASSWORD": "TBD",
            "SNOWFLAKE_ACCOUNT":  "TBD",
            "MONTY_FAILURE_URL":  "TBD",   # NEW — paste from Monty CfnOutput FailureProxyUrl
            "MONTY_HMAC_SECRET":  "TBD",   # NEW — copy from Monty's monty-secrets
        }),
        generate_string_key="placeholder",
    ),
)
```

After deploy, run once per env:

```bash
aws secretsmanager update-secret \
  --secret-id "$SERVICE-secrets" \
  --secret-string "$(aws secretsmanager get-secret-value \
                       --secret-id "$SERVICE-secrets" \
                       --query SecretString --output text \
                     | jq --arg url "$FAILURE_URL" --arg sec "$HMAC" \
                          '.MONTY_FAILURE_URL=$url | .MONTY_HMAC_SECRET=$sec')"
```

### 3d. Pass the two values into every ingest Lambda's environment.

In the `_lambda.DockerImageFunction(...)` definition (around line 222), the
existing `environment` dict already includes `SECRET_NAME`. Add two
**deferred** lookups so the values come from the secret, not from the CDK
template (avoids CFN parameter drift on rotation):

```python
environment={
    "SECRET_NAME":       ingest_secret.secret_name,
    "DATA_BUCKET":       data_bucket.bucket_name,
    "MONTY_FAILURE_URL": ingest_secret.secret_value_from_json("MONTY_FAILURE_URL").unsafe_unwrap(),
    "MONTY_HMAC_SECRET": ingest_secret.secret_value_from_json("MONTY_HMAC_SECRET").unsafe_unwrap(),
},
```

> `unsafe_unwrap()` here is fine because both values are already provisioned in
> the secret the Lambda's role can read; CDK uses dynamic references in the
> CloudFormation template, so the cleartext never appears in CDK output.

### 3e. CloudWatch Logs SubscriptionFilter — fan log lines to Monty.

After each `_lambda.DockerImageFunction(...)` is defined, add a subscription on
its log group:

```python
for logical_id, fn in functions.items():
    if not monty_log_scanner_arn:
        continue   # local/dev: skip cross-account wiring
    logs.SubscriptionFilter(
        self, f"{logical_id}MontySubscription",
        log_group=fn.log_group,
        destination=logs_destinations.LambdaDestination(
            _lambda.Function.from_function_arn(
                self, f"{logical_id}MontyLogScanner", monty_log_scanner_arn
            )
        ),
        filter_pattern=logs.FilterPattern.exists("$.MONITORING_METRIC"),
        filter_name=f"{logical_id}MontyMetric",
    )
```

**Filter pattern note:** `exists($.MONITORING_METRIC)` only matches lines where
the JSON has that top-level key — the rest of the Lambda's stdout (debug prints,
boto3 chatter) is never delivered to Monty.

### 3f. SNS subscription — fan alarms to Monty.

Right after the existing `alarm.add_alarm_action(cw_actions.SnsAction(ingest_alerts_topic))`
(line 387), add:

```python
if monty_sns_subscriber_arn:
    ingest_alerts_topic.add_subscription(
        sns_subscriptions.LambdaSubscription(
            _lambda.Function.from_function_arn(
                self, "MontySnsSubscriber", monty_sns_subscriber_arn
            )
        )
    )
```

You can also remove the commented-out `cfn_chatbot.CfnSlackChannelConfiguration`
block at lines 389–402 — Monty replaces it.

## 4. IAM — Monty side

These belong in the Monty CDK stack (`infra/monty_stack.py`) and are already
provisioned, but listed here so the AWS-ingest reviewer can verify:

- **`log_scanner`** has a resource-based policy permitting
  `logs.${region}.amazonaws.com` to invoke it from any account that owns ingest
  log groups (already added — see `monty_stack.py` `_grant_log_subscription` —
  using `lambda.Function.add_permission`).
- **`sns_subscriber`** allows `sns.amazonaws.com` to invoke it (same pattern).
- **`failure_proxy`** is reached over the public API Gateway URL — no IAM
  needed; HMAC signature is the auth.

## 5. Validation

Deploy to **dev** first:

1. `cdk deploy ai-ingest-iterate-stack-dev --context monty_log_scanner_arn=<arn> \
   --context monty_sns_subscriber_arn=<arn>` — synth shows the SubscriptionFilter
   and SNS subscription as new resources only.
2. **Force a failure**: temporarily flip `ITERATE_API_TOKEN` to a bad value;
   trigger the Step Function. Confirm:
   - Lambda logs show the exception message.
   - Within 60 s, `#data-incidents` Slack receives a Monty message.
   - `SELECT * FROM MONITORING_DB.MONITORING.CUSTOM_METRICS WHERE PIPELINE_NAME='ingest.iterate' ORDER BY OCCURRED_AT DESC LIMIT 1;` returns the row.
3. **Force the alarm**: manually push a metric value of `2` to the
   `${ServiceName} ingest WorkflowHealth` metric (`aws cloudwatch put-metric-data`).
   The CloudWatch alarm transitions to ALARM, fires SNS, Monty's sns_subscriber
   inserts a row, Observer pages Slack.
4. **Custom metric trace**: run any ingest Lambda manually; verify the
   `survey_count`/`stats_count` lines from §2d emit `MONITORING_METRIC` JSON,
   appear in CUSTOM_METRICS within ~60 s.

## 6. Rollout order

1. Merge & deploy **Monty** to dev. Capture `FailureProxyUrl`, `LogScannerArn`,
   `SnsSubscriberArn`, `MONTY_HMAC_SECRET` (random hex stored in `monty-secrets`).
2. Update each ingest stack's secret with `MONTY_FAILURE_URL` + `MONTY_HMAC_SECRET`.
3. Merge this PR; deploy first to one ingest stack (`iterate`) on dev.
4. Validate (§5).
5. Replicate to remaining ingest stacks via the existing template-deploy script.
6. Promote to prod.

## 7. Risks / call-outs

- **HMAC secret rotation**: the same `MONTY_HMAC_SECRET` value is mirrored into
  ~50 ingest stack secrets. Rotation requires updating Monty's secret first
  (failure-proxy will accept either old-or-new for a grace window — TODO in
  Monty `failure_proxy/handler.py`), then the ingest secrets. Until the dual-key
  feature lands, schedule rotation during a quiet window.
- **CloudWatch SubscriptionFilter quota** is one filter per log group; the new
  `<logical>MontyMetric` filter is the *only* filter we add, but a future debug
  filter would conflict — gate any local debug filter behind a context flag.
- **SNS cross-account invoke** depends on the ingest topics living in the same
  AWS partition; multi-region ingest stacks aren't covered (out-of-scope per
  master plan).
- **`unsafe_unwrap()`** appears in template renders; if your security review
  forbids it, switch to a custom resource that fetches the secret at deploy
  time and pushes the values into the Lambda env via `addEnvironment` — adds a
  custom-resource Lambda but avoids the keyword `unsafe_unwrap` showing up in
  audit reports.
