"""Log-scanner Lambda: receives gzipped CloudWatch Logs subscription events
and forwards `MONITORING_METRIC` JSON lines to CUSTOM_METRICS.

Implements the structured-logging path from `architecture.md` item 3:

    print(json.dumps({
        "MONITORING_METRIC": "order_value_sum",
        "VALUE": 54000.50,
        "PIPELINE": "daily_ingest",
        "SEVERITY": "info"     # optional, defaults to 'info'
    }))

The CloudWatch Logs subscription filter pattern (set up by each ingest stack)
matches `{ $.MONITORING_METRIC = "*" }`, so we only get the lines we care
about — no need to scan every log message.
"""

import base64
import gzip
import json
import logging
from typing import Any

from lambdas.shared import metric_writer

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """CloudWatch Logs subscription event handler."""
    payload = _decode(event)
    log_group = payload.get("logGroup", "unknown")
    log_events = payload.get("logEvents") or []

    written = 0
    skipped = 0

    for log_event in log_events:
        try:
            message = log_event.get("message", "")
            metric = _build_metric(message, log_group)
            if metric is None:
                skipped += 1
                continue
            metric_writer.write(metric)
            written += 1
        except Exception:
            # Per-event isolation. CloudWatch retries the whole batch on
            # uncaught exception; we'd rather drop one bad line than
            # duplicate the good ones.
            logger.exception(
                "log_scanner: failed to write metric, log_event_id=%s",
                log_event.get("id"),
            )
            skipped += 1

    return {"events_total": len(log_events), "written": written, "skipped": skipped}


def _decode(event: dict[str, Any]) -> dict[str, Any]:
    """CloudWatch Logs delivers events base64-encoded gzipped JSON in `awslogs.data`."""
    encoded = event["awslogs"]["data"]
    decompressed = gzip.decompress(base64.b64decode(encoded))
    return json.loads(decompressed)


def _build_metric(message: str, log_group: str) -> metric_writer.Metric | None:
    """Parse a log line; return None if it isn't a MONITORING_METRIC line.

    Subscription filter pattern guarantees these are JSON-shaped, but defensive
    parsing keeps us robust if the filter is misconfigured.

    emit_metric() uses lowercase keys; uppercase variants are supported for
    pipelines that emit JSON manually.
    """
    try:
        parsed = json.loads(message)
    except json.JSONDecodeError:
        return None

    name = parsed.get("MONITORING_METRIC")
    if not name:
        return None

    # Lowercase first (emit_metric output), uppercase as fallback.
    pipeline = (parsed.get("pipeline") or parsed.get("PIPELINE")
                or _pipeline_from_log_group(log_group))
    severity = (parsed.get("severity") or parsed.get("SEVERITY") or "info").lower()
    value = parsed.get("value") if "value" in parsed else parsed.get("VALUE")
    run_id = parsed.get("run_id") or parsed.get("RUN_ID")
    is_alert = bool(parsed.get("is_alert", parsed.get("IS_ALERT", False)))
    environment = parsed.get("environment") or parsed.get("ENVIRONMENT")
    slack_webhook = parsed.get("slack_webhook") or parsed.get("SLACK_WEBHOOK")

    _KNOWN_KEYS = frozenset({
        "MONITORING_METRIC",
        "value", "VALUE",
        "pipeline", "PIPELINE",
        "severity", "SEVERITY",
        "run_id", "RUN_ID",
        "is_alert", "IS_ALERT",
        "payload", "PAYLOAD",
        "environment", "ENVIRONMENT",
        "slack_webhook", "SLACK_WEBHOOK",
    })

    # Merge producer-supplied payload dict (emit_metric's canonical shape) with
    # any stray top-level non-reserved keys, so nothing the caller sent is lost.
    raw = {k: v for k, v in parsed.items() if k not in _KNOWN_KEYS}
    producer_payload = parsed.get("payload") or parsed.get("PAYLOAD")
    if isinstance(producer_payload, dict):
        raw.update(producer_payload)

    payload_out: dict[str, Any] = {"log_group": log_group, "raw": raw}
    if slack_webhook:
        payload_out["slack_webhook"] = slack_webhook

    return metric_writer.Metric.from_dict(
        {
            "pipeline_name": pipeline,
            "metric_name": name,
            "metric_value": value,
            "severity": severity,
            "run_id": run_id,
            "payload": payload_out,
            "is_alert": is_alert,
            "environment": environment,
        }
    )


def _pipeline_from_log_group(log_group: str) -> str:
    """Best-effort pipeline name from the log group.

    Lambda log groups look like `/aws/lambda/<function-name>`. Strip the
    prefix; if it doesn't match, fall back to the whole string.
    """
    prefix = "/aws/lambda/"
    if log_group.startswith(prefix):
        return log_group[len(prefix):]
    return log_group
