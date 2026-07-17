"""Observer Lambda: every minute, scan CUSTOM_METRICS for unsent alerts and
post them to Slack.

Scheduled by EventBridge `rate(1 minute)` in `infra/monty_stack.py`.

Idempotency contract:
    - Read rows where is_alert=TRUE AND sent_to_slack=FALSE.
    - For each: post Slack -> on success, INSERT into ALERT_OUTBOX (status='sent')
      and UPDATE CUSTOM_METRICS.sent_to_slack=TRUE in the same transaction.
    - On Slack failure: INSERT outbox row (status='failed') but leave
      sent_to_slack=FALSE so next minute retries.

Slack routing:
    - Severity drives the default: critical/error -> SLACK_WEBHOOK_INCIDENTS,
      warning/info -> SLACK_WEBHOOK_ALERTS (both held in the secret).
    - Per-metric override: a payload with `slack_webhook` containing a full
      `https://hooks.slack.com/...` URL bypasses the severity routing and
      posts directly to that URL. Non-Slack URLs are rejected (SSRF guard).
"""

import logging
from typing import Any

from lambdas.observer import slack
from lambdas.shared.snowflake_client import get_connection, get_secret

logger = logging.getLogger()
logger.setLevel(logging.INFO)

POLL_LIMIT = 100  # max rows per Lambda invocation

# A single EventBridge rule now invokes this Lambda every 5 minutes with NO
# `lane` field, so the "all" fallback below runs (no severity clause) and picks
# up every unsent alert. Rationale: once warning/info moved to S3 Parquet,
# CUSTOM_METRICS holds only critical/error, so the old fast/batch split had
# nothing left to separate — see infra/monty_stack.py.
#
# The per-lane clauses are RETAINED for manual/legacy invokes: passing
# {"lane": "fast"} or {"lane": "batch"} still filters by these disjoint severity
# sets (they can never double-post the same row). Missing/unknown lane = all.
_LANE_SEVERITY_CLAUSE = {
    "fast": "AND SEVERITY IN ('critical', 'error')",
    "batch": "AND SEVERITY NOT IN ('critical', 'error')",
}

# `warning` rows are still written to CUSTOM_METRICS by metric_writer, but the
# observer never posts them to Slack — this exclusion is applied to every lane
# (fast, batch, and the manual/all fallback) so warnings can never be delivered.
_SELECT_UNSENT_TEMPLATE = """
    SELECT ID, PIPELINE_NAME, ENVIRONMENT, METRIC_NAME, METRIC_VALUE, SEVERITY,
           RUN_ID, PAYLOAD, OCCURRED_AT
    FROM MONITORING_DB.MONITORING.CUSTOM_METRICS
    WHERE IS_ALERT = TRUE AND SENT_TO_SLACK = FALSE
      AND SEVERITY <> 'warning'
    {severity_clause}
    ORDER BY OCCURRED_AT
    LIMIT %s
"""


def _build_select(lane: str | None) -> str:
    """Return the unsent-alerts query for the given lane.

    The severity clause is a fixed constant string (never user input), so there
    is no injection surface here — the only bound parameter remains the LIMIT.
    """
    severity_clause = _LANE_SEVERITY_CLAUSE.get(lane or "", "")
    return _SELECT_UNSENT_TEMPLATE.format(severity_clause=severity_clause)

_INSERT_OUTBOX = """
    INSERT INTO MONITORING_DB.MONITORING.ALERT_OUTBOX
        (METRIC_ID, CHANNEL, STATUS, ERROR_MESSAGE)
    VALUES (%s, %s, %s, %s)
"""

_MARK_SENT = """
    UPDATE MONITORING_DB.MONITORING.CUSTOM_METRICS
    SET SENT_TO_SLACK = TRUE, SENT_AT = CURRENT_TIMESTAMP()
    WHERE ID = %s
"""


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """EventBridge handler. Returns a small summary for CloudWatch logs."""
    lane = (event or {}).get("lane")
    logger.info(
        "observer invoked lane=%s severity_filter=%r",
        lane or "all",
        _LANE_SEVERITY_CLAUSE.get(lane or "", "(none: all severities)"),
    )
    select_sql = _build_select(lane)
    secret = get_secret()
    api_key = secret.get("ANTHROPIC_API_KEY", "")
    severity_webhooks = {
        "SLACK_WEBHOOK_INCIDENTS": secret.get("SLACK_WEBHOOK_INCIDENTS", ""),
        "SLACK_WEBHOOK_ALERTS": secret.get("SLACK_WEBHOOK_ALERTS", ""),
        # Non-prod metrics route here regardless of severity. Empty/missing
        # means resolve_destination() falls back to severity routing.
        slack.DEV_WEBHOOK_KEY: secret.get(slack.DEV_WEBHOOK_KEY, ""),
    }




    sent = 0
    failed = 0
    with get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(select_sql, (POLL_LIMIT,))
            rows = cursor.fetchall()
            columns = [c[0] for c in cursor.description]
            metrics = [dict(zip(columns, row, strict=True)) for row in rows]
        finally:
            cursor.close()

        for metric in metrics:
            metric = _normalize_payload(metric)
            payload = metric.get("PAYLOAD") or metric.get("payload") or {}
            slack_webhook = payload.get("slack_webhook")
            channel_label, webhook = slack.resolve_destination(
                metric.get("SEVERITY") or metric.get("severity"),
                slack_webhook,
                severity_webhooks,
                environment=metric.get("ENVIRONMENT") or metric.get("environment"),
            )

            body = slack.format_message(_lowercase_keys(metric), api_key)
            result = slack.post(webhook, body)

            cursor = conn.cursor()
            try:
                if result.delivered:
                    cursor.execute(
                        _INSERT_OUTBOX,
                        (metric["ID"], channel_label, "sent", None),
                    )
                    cursor.execute(_MARK_SENT, (metric["ID"],))
                    conn.commit()
                    sent += 1
                else:
                    cursor.execute(
                        _INSERT_OUTBOX,
                        (
                            metric["ID"],
                            channel_label,
                            "failed",
                            (result.error_message or "")[:4000],
                        ),
                    )
                    conn.commit()
                    failed += 1
                    logger.warning(
                        "slack delivery failed metric_id=%s reason=%s",
                        metric["ID"],
                        result.error_message,
                    )
            finally:
                cursor.close()

    polled = len(metrics) if 'metrics' in locals() else 0
    logger.info(
        "observer done lane=%s polled=%s sent=%s failed=%s",
        lane or "all", polled, sent, failed,
    )
    return {"lane": lane or "all", "polled": polled, "sent": sent, "failed": failed}


def _normalize_payload(metric: dict[str, Any]) -> dict[str, Any]:
    """Snowflake VARIANT comes back as a JSON string; parse it for the formatter."""
    payload = metric.get("PAYLOAD")
    if isinstance(payload, str):
        import json as _json
        try:
            metric["PAYLOAD"] = _json.loads(payload)
        except _json.JSONDecodeError:
            metric["PAYLOAD"] = {"raw": payload}
    return metric


def _lowercase_keys(metric: dict[str, Any]) -> dict[str, Any]:
    """Snowflake returns column names uppercase; the Slack formatter expects
    lowercase keys. One-shot conversion keeps the formatter free of Snowflake
    coupling so unit tests can pass plain dicts."""
    return {k.lower(): v for k, v in metric.items()}
