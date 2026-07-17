---
name: monty-stepfunction-metrics
description: >-
  Emit Monty metrics and failure alerts from AWS Lambdas and AWS Step Functions
  (CDK Python). Use when adding observability to a Lambda handler or a state
  machine — trending metrics, exception alerts, and catching state/task failures
  the Lambda itself can't report (timeouts, OOM, States.Runtime). Covers the two
  emission paths (stdout JSON, HMAC HTTP POST) and the state-machine Catch → reporter
  pattern. Scope: Lambda + Step Functions only.
---

# Emitting Monty metrics & failures from Lambda + Step Functions

Monty = Berg's central observability platform. You **write a row**; Monty's
`observer` decides Slack delivery. Never call Slack directly, never `INSERT INTO
CUSTOM_METRICS` yourself, never invent a severity.

## 0. Pick a path

| Need | Path | Mechanism | Creds? |
|---|---|---|---|
| Trending metric (row count, duration, KPI) | **B — stdout** | `print(json)` → `log_scanner` | none |
| Must-deliver failure alert (page within seconds) | **A — HMAC POST** | signed POST → `failure_proxy` | HMAC secret |

Inside a Step Function, use **both**: Path B in-Lambda for metrics, and a
state-machine **`Catch`** wired to a reporter Lambda (Path A) so failures that
kill the Lambda before its `except` runs still page. **One path per event** — do
not double-report the same failure from both the Lambda's `except` and the Catch.

## 1. Severities (enforced server-side — only these four)

| Severity | Slack | Use |
|---|---|---|
| `critical` | `#data-incidents` | Down/corrupt right now. Wake on-call. |
| `error` | `#data-incidents` | Pipeline failed, needs attention today. **Default for exceptions.** |
| `warning` | _(not sent to Slack)_ | Looks off, data still flowing. |
| `info` | _(not sent to Slack)_ | Trending values. **Default for metrics.** |

Default `error` for failures, `info` for trending. **Never default to `critical`.**
Never set `is_alert: true` on `info`.

> **Storage is Monty's job, not yours.** Your ingest Lambda only *emits* — a
> stdout JSON line (Path B) or a signed POST (Path A). Monty's own `log_scanner`
> (reads your logs) / `failure_proxy` (receives the POST) then call
> `metric_writer.write()`, which routes by severity: `critical`/`error` → Snowflake
> `CUSTOM_METRICS`, `warning`/`info` → S3 Parquet (`monty-<env>-metrics`). You never
> touch S3 or Snowflake. The only consequence for the emitter: `warning`/`info`
> never page Slack — so pick severity by urgency, not by destination.

## 2. Emit helper (stdlib only — no `requests`, no layer)

Copy this into the Lambda package (e.g. `monty_metric.py`). It reads config from
env vars set by the CDK stack (§4).

```python
"""Emit Monty metrics (stdout) and report failures (HMAC POST). Stdlib only."""
import hashlib
import hmac
import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any, Optional

logger = logging.getLogger(__name__)

_FAILURE_URL = os.environ.get("MONTY_FAILURE_URL", "")
_HMAC_SECRET = os.environ.get("MONTY_HMAC_SECRET", "")
_LOG_KEY = "MONITORING_METRIC"
_ALLOWED = {"critical", "error", "warning", "info"}

# "snbox" in the data bucket name => dev, else prod. Fail loud if unset: a Lambda
# deployed without DATA_BUCKET should surface immediately, not default to prod.
_ENV = "dev" if "snbox" in os.environ["DATA_BUCKET"] else "prod"
# Set by the Lambda runtime; used as the pipeline name in CUSTOM_METRICS.
_PIPELINE = os.environ["AWS_LAMBDA_FUNCTION_NAME"]


def _check_severity(severity: str) -> None:
    """Reject early — bad severities are dropped by log_scanner / 400'd by the proxy."""
    if severity not in _ALLOWED:
        raise ValueError(f"invalid severity {severity!r}; must be one of {sorted(_ALLOWED)}")


def emit_metric(
    name: str,
    value: "float | int | None" = None,
    severity: str = "info",
    *,
    payload: Optional[dict[str, Any]] = None,
    is_alert: bool = False,
) -> None:
    """Path B: one stdout JSON line log_scanner ingests. Cheap, at-most-once.

    Use for trending metrics. Not guaranteed (Lambda log delivery can drop) —
    use report_failure() for anything that must page.
    """
    _check_severity(severity)
    body = {
        _LOG_KEY: name,
        "value": value,
        "severity": severity,
        "pipeline": _PIPELINE,
        "is_alert": is_alert,
        "payload": payload or {},
        "environment": _ENV,
    }
    # Plain print so it lands in CloudWatch Logs as a single-line event. This is
    # the ingestion mechanism — do NOT convert to logger here.
    print(json.dumps(body, default=str))


def report_failure(
    run_id: str,
    error_message: str,
    severity: str = "error",
    *,
    metric_name: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
    timeout: float = 4.0,
) -> bool:
    """Path A: signed POST to the failure-proxy. Returns True on 2xx, never raises.

    Alerting must never mask the original exception — the caller re-raises after.
    """
    _check_severity(severity)
    if not _FAILURE_URL or not _HMAC_SECRET:
        logger.warning("MONTY_FAILURE_URL/SECRET unset — skipping failure report.")
        return False

    body_dict: dict[str, Any] = {
        "pipeline_name": _PIPELINE,
        "run_id": run_id,
        "error_message": error_message[:4000],
        "severity": severity,
        "payload": payload or {},
        "environment": _ENV,
    }
    if metric_name is not None:
        body_dict["metric_name"] = metric_name
    body_bytes = json.dumps(body_dict).encode("utf-8")
    signature = hmac.new(_HMAC_SECRET.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()

    request = urllib.request.Request(
        _FAILURE_URL,
        data=body_bytes,
        method="POST",
        headers={"Content-Type": "application/json", "X-Monty-Signature": signature},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            ok = 200 <= response.status < 300
            if not ok:
                logger.error("Monty failure-proxy returned %s", response.status)
            return ok
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        logger.error("Monty failure-proxy unreachable: %s", exc)
        return False
```

## 3. In a Lambda handler

```python
from monty_metric import emit_metric, report_failure

def handler(event, context):
    # run_id: prefer the Step Functions execution name (passed in via the task
    # payload, §5) so every metric/failure in one execution correlates. Fall
    # back to the Lambda request id for standalone invokes.
    run_id = event.get("run_id") or context.aws_request_id
    try:
        rows = do_work(event)
        emit_metric("rows_ingested", rows, "info")          # trending, no page
        return {"rows": rows, "run_id": run_id}
    except Exception as exc:                                 # report, then re-raise
        report_failure(run_id, f"ingest failed: {exc}", "error",
                       metric_name="ingest_failure")
        raise                                               # let the state fail
```

## 4. CDK Python — Lambda env wiring

The helper needs three env vars on every Lambda that emits. Mirror the Monty
HMAC values from your stack's secret; never hard-code them.

```python
fn = _lambda.Function(
    self, "IngestFn",
    # ...runtime, handler, code...
    environment={
        "MONTY_FAILURE_URL": monty_failure_url,        # from Monty CfnOutput FailureProxyUrl
        "MONTY_HMAC_SECRET": secret.secret_value_from_json("MONTY_HMAC_SECRET").unsafe_unwrap(),
        "DATA_BUCKET": data_bucket.bucket_name,         # drives dev/prod label
        # AWS_LAMBDA_FUNCTION_NAME is injected by the runtime — do not set it.
    },
)
```

Path B (`emit_metric`) needs **no** env beyond `DATA_BUCKET` — the log subscription
filter that feeds `log_scanner` is owned by Monty's stack. Path A needs the URL +
HMAC secret.

## 5. CDK Python — Step Functions Catch pattern

The state-machine `Catch` is the Step-Functions-specific win: it fires for
`Lambda.Timeout`, `States.Runtime`, out-of-memory, and any uncaught error — cases
where the Lambda died before its own `except` could POST.

```python
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks

# Pass the execution name in as run_id so the reporter and the work Lambda share it.
ingest = tasks.LambdaInvoke(
    self, "Ingest",
    lambda_function=ingest_fn,
    payload=sfn.TaskInput.from_object({
        "run_id": sfn.JsonPath.string_at("$$.Execution.Name"),
        "input": sfn.JsonPath.entire_payload,
    }),
    result_path="$.result",
)

# Dedicated reporter Lambda: reads the caught error + run_id, POSTs via report_failure.
report = tasks.LambdaInvoke(
    self, "ReportFailure",
    lambda_function=failure_reporter_fn,
    payload=sfn.TaskInput.from_object({
        "run_id": sfn.JsonPath.string_at("$$.Execution.Name"),
        "error": sfn.JsonPath.string_at("$.error.Error"),
        "cause": sfn.JsonPath.string_at("$.error.Cause"),
    }),
)

# States.ALL catches everything incl. timeouts/OOM the Lambda couldn't report.
# result_path keeps the original input intact under $, error lands at $.error.
ingest.add_catch(report, errors=["States.ALL"], result_path="$.error")

definition = ingest.next(sfn.Succeed(self, "Done"))
sfn.StateMachine(self, "IngestSfn", definition_body=sfn.DefinitionBody.from_chainable(definition))
```

Reporter Lambda handler:

```python
from monty_metric import report_failure

def handler(event, context):
    """Report a Step Functions state failure to Monty. run_id = execution name."""
    report_failure(
        run_id=event["run_id"],
        error_message=f"{event.get('error')}: {event.get('cause')}"[:4000],
        severity="error",
        metric_name="sfn_state_failed",
        payload={"error": event.get("error"), "cause": event.get("cause")},
    )
    return {"reported": True}
```

**Don't double-report:** if the work Lambda already `report_failure`s in its own
`except` and re-raises, don't also Catch→report the same error. Choose one — Catch
alone is usually enough and also covers crashes the `except` can't.

## 6. Rules

- Four severities only: `critical`, `error`, `warning`, `info`.
- No direct `INSERT INTO CUSTOM_METRICS`; no direct Slack calls.
- `is_alert: true` never on `info`.
- `report_failure` never raises — always re-raise the original exception after it.
- `run_id` = the Step Functions **execution name** so one execution's rows correlate.
- Never commit `MONTY_HMAC_SECRET` — source it from Secrets Manager in CDK.

## 7. Verify

1. Trigger the state machine (or invoke the Lambda) once.
2. Metrics (Path B): confirm the `MONITORING_METRIC` JSON line in the Lambda's
   CloudWatch log stream, then the row in `MONITORING_DB.MONITORING.CUSTOM_METRICS`
   (`critical`/`error`) or S3 `monty-<env>-metrics` (`warning`/`info`).
3. Failures (Path A): force a failure (raise, or a state timeout) and confirm a
   `critical`/`error` row lands and `SENT_TO_SLACK` flips to TRUE within ~2 min
   (`#data-incidents`, or `#data-alerts-dev` when `environment != prod`).
4. Catch path: shorten the task timeout to force `Lambda.Timeout` and confirm the
   reporter still writes a row (proves the state-level Catch works when the Lambda
   couldn't self-report).
