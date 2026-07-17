"""JSON schema and validation for the failure-proxy POST body.

Kept separate from handler.py so it can be imported by tests without pulling
in boto3/Snowflake. Handler validates first, then writes — invalid payloads
return 400 and never touch Snowflake.
"""

from typing import Any

# Mirrors metric_writer.ALLOWED_SEVERITIES but kept independent so changes to
# the writer don't silently expand what the public HTTP API will accept.
ALLOWED_SEVERITIES = ("critical", "error", "warning", "info")


class PayloadError(ValueError):
    """Raised when the incoming JSON fails validation."""


def validate(body: dict[str, Any]) -> dict[str, Any]:
    """Return a normalized payload dict; raise PayloadError on bad input.

    Required: pipeline_name, run_id, error_message, severity.
    Optional: metric_name (defaults to 'pipeline_failure'),
              metric_value, payload (extra context dict).

    Why we require run_id even for HTTP failures: lets users grep CloudWatch
    or correlate Slack alerts back to the source invocation.
    """
    if not isinstance(body, dict):
        raise PayloadError("body must be a JSON object")

    for required in ("pipeline_name", "run_id", "error_message", "severity"):
        value = body.get(required)
        if not isinstance(value, str) or not value.strip():
            raise PayloadError(f"missing or empty required field: {required}")

    severity = body["severity"].lower()
    if severity not in ALLOWED_SEVERITIES:
        raise PayloadError(
            f"severity must be one of {ALLOWED_SEVERITIES}, got {severity!r}"
        )

    metric_value = body.get("metric_value")
    if metric_value is not None:
        try:
            metric_value = float(metric_value)
        except (TypeError, ValueError) as exc:
            raise PayloadError("metric_value must be numeric") from exc

    extra_payload = body.get("payload")
    if extra_payload is not None and not isinstance(extra_payload, dict):
        raise PayloadError("payload must be an object")

    slack_webhook = body.get("slack_webhook")
    if slack_webhook is not None and not isinstance(slack_webhook, str):
        raise PayloadError("slack_webhook must be a string")

    # Bundle the error_message into payload so the Slack formatter can show
    # it without a separate column on CUSTOM_METRICS.
    merged_payload = dict(extra_payload or {})
    merged_payload["error_message"] = body["error_message"][:4000]
    if slack_webhook:
        merged_payload["slack_webhook"] = slack_webhook.strip()

    return {
        "pipeline_name": body["pipeline_name"],
        "metric_name": body.get("metric_name") or "pipeline_failure",
        "metric_value": metric_value,
        "severity": severity,
        "run_id": body["run_id"],
        "payload": merged_payload,
        "is_alert": True,
        "environment": body.get("environment"),
    }
