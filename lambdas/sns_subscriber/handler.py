"""SNS-subscriber Lambda: turns CloudWatch alarm SNS notifications into
CUSTOM_METRICS rows.

Subscribed by each ingest stack to its existing `ai-ingest-*-alerts` SNS
topic (see `plan/aws_ingest_changes_spec.md`). When a CloudWatch alarm
fires, AWS publishes a JSON message; we parse, write a metric row with
severity='critical', and let the Observer pick it up.

Why severity='critical' for everything here: by the time something has
tripped a CloudWatch alarm severe enough to publish to the alerts topic,
it deserves the incident channel. Pipelines that want lower-severity SNS
events can either use the failure-proxy HTTP path or extend this handler
to read severity from a SNS message attribute.
"""

import json
import logging
from typing import Any

from lambdas.shared import metric_writer

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """SNS event has a `Records` list; each record carries one Sns payload."""
    records = event.get("Records") or []
    written = 0

    for record in records:
        try:
            sns = record["Sns"]
            metric = _parse_sns(sns)
            metric_writer.write(metric)
            written += 1
        except Exception:
            # Per-record isolation: log and continue. SNS retries the whole
            # batch on uncaught exception, which can cause duplicate metrics
            # for records earlier in the batch that DID succeed.
            logger.exception(
                "failed to process SNS record message_id=%s",
                record.get("Sns", {}).get("MessageId"),
            )

    return {"records_total": len(records), "records_written": written}


def _parse_sns(sns: dict[str, Any]) -> metric_writer.Metric:
    """Build a Metric from a CloudWatch alarm SNS payload.

    CloudWatch alarm message shape (relevant subset):
        {"AlarmName": "ai-ingest-iterate-failure-alarm",
         "NewStateValue": "ALARM",
         "NewStateReason": "...",
         "StateChangeTime": "2026-05-09T...",
         "AlarmDescription": "...",
         "Trigger": {"MetricName": "...", ...}}
    """
    raw_message = sns.get("Message") or "{}"
    topic_arn = sns.get("TopicArn", "")
    message_id = sns.get("MessageId")

    try:
        alarm = json.loads(raw_message)
    except json.JSONDecodeError:
        # Some publishers send plain text; treat the whole string as the
        # error_message rather than dropping it.
        alarm = {"raw_message": raw_message}

    pipeline_name = _pipeline_from_topic_arn(topic_arn) or alarm.get(
        "AlarmName", "unknown"
    )

    payload: dict[str, Any] = {
        "alarm_name": alarm.get("AlarmName"),
        "state": alarm.get("NewStateValue"),
        "reason": alarm.get("NewStateReason"),
        "changed_at": alarm.get("StateChangeTime"),
        "description": alarm.get("AlarmDescription"),
        "topic_arn": topic_arn,
        "raw": alarm,
    }

    # Custom publishers can set slack_webhook via SNS MessageAttributes; the
    # CloudWatch alarms we subscribe to today never do, so this is a no-op for
    # the common case.
    slack_webhook = (
        sns.get("MessageAttributes", {}).get("slack_webhook", {}).get("Value")
    )
    if slack_webhook:
        payload["slack_webhook"] = slack_webhook

    return metric_writer.Metric.from_dict(
        {
            "pipeline_name": pipeline_name,
            "metric_name": "cloudwatch_alarm",
            "metric_value": None,
            "severity": "critical",
            "run_id": message_id,
            "payload": payload,
            "is_alert": True,
        }
    )


def _pipeline_from_topic_arn(topic_arn: str) -> str | None:
    """Extract a friendly pipeline name from the SNS topic ARN.

    ARN shape: `arn:aws:sns:<region>:<acct>:ai-ingest-<service>-alerts`.
    Returns the `<service>` segment, falling back to None if the ARN doesn't
    match the convention.
    """
    if not topic_arn:
        return None
    name = topic_arn.rsplit(":", 1)[-1]
    if name.startswith("ai-ingest-") and name.endswith("-alerts"):
        return name[len("ai-ingest-"): -len("-alerts")]
    return name or None
