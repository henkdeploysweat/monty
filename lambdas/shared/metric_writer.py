"""Single INSERT helper for MONITORING_DB.MONITORING.CUSTOM_METRICS.

Used by failure_proxy, sns_subscriber, and log_scanner Lambdas. Keeping the
SQL in one place means a schema change to CUSTOM_METRICS only touches this
file.
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

from . import dynamo_writer
from .snowflake_client import get_connection

logger = logging.getLogger(__name__)

ALLOWED_SEVERITIES = ("critical", "error", "warning", "info")

# Severity-based storage routing. The ingestion contract (ALLOWED_SEVERITIES)
# is unchanged — every producer still sends the same four severities — but where
# a row lands now depends on its urgency:
#
#   - SNOWFLAKE_SEVERITIES: rare, high-value rows that must stay queryable in
#     CUSTOM_METRICS and reachable by the observer for Slack delivery.
#   - DDB_SEVERITIES: high-frequency, low-priority rows. A single-row INSERT per
#     write kept the XS warehouse permanently awake; these go to DynamoDB
#     (dynamo_writer) instead — formerly S3 Parquet (s3_writer). NOTE: this means
#     `info` rows no longer reach Slack via the observer (warning never did) —
#     an intentional behaviour change.
#
# The two sets together MUST cover ALLOWED_SEVERITIES; anything outside both is
# treated as a defensive drop (logged, not errored) below.
SNOWFLAKE_SEVERITIES = ("critical", "error")
DDB_SEVERITIES = ("warning", "info")

# Fully-qualified to avoid surprises from session-level USE statements.
_INSERT_SQL = """
    INSERT INTO MONITORING_DB.MONITORING.CUSTOM_METRICS
        (PIPELINE_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY,
         RUN_ID, PAYLOAD, IS_ALERT,ENVIRONMENT)
    SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s
"""


@dataclass(frozen=True)
class Metric:
    """Validated metric row, ready to insert.

    Build via `Metric.from_dict(...)` so validation rules live in one place.
    """

    pipeline_name: str
    metric_name: str
    metric_value: float | None
    severity: str
    run_id: str | None
    payload: dict[str, Any] | None
    is_alert: bool
    environment: str

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Metric":
        """Validate a raw payload and return a Metric.

        Raises ValueError on any missing required field or bad severity. We
        deliberately do NOT default the severity — every writer should be
        explicit about the urgency of what it's writing.
        """
        for required in ("pipeline_name", "metric_name", "severity"):
            if not raw.get(required):
                raise ValueError(f"missing required field: {required}")

        severity = raw["severity"].lower()
        if severity not in ALLOWED_SEVERITIES:
            raise ValueError(
                f"severity must be one of {ALLOWED_SEVERITIES}, got {severity!r}"
            )

        value = raw.get("metric_value")
        return cls(
            pipeline_name=raw["pipeline_name"],
            metric_name=raw["metric_name"],
            metric_value=float(value) if value is not None else None,
            severity=severity,
            run_id=raw.get("run_id"),
            payload=raw.get("payload"),
            # Default to TRUE for failure-style writers; callers can pass
            # is_alert=False explicitly for trending-only metrics.
            is_alert=bool(raw.get("is_alert", True)),
            # A producer-supplied environment wins; otherwise fall back to the
            # Lambda's deployment env (MONTY_ENV, set on all four functions in
            # infra/monty_stack.py). This guarantees the NOT NULL column is
            # always populated even for writers that have no natural
            # environment field — e.g. CloudWatch-alarm rows via sns_subscriber.
            environment=raw.get("environment") or os.environ.get("MONTY_ENV"),
        )


def write(metric: Metric) -> int:
    logger.debug("metric_writer.write called with %s", metric)

    # Route by severity. warning/info are diverted to DynamoDB (cheap, keeps
    # the Snowflake warehouse from staying awake on high-frequency low-priority
    # writes); critical/error fall through to the Snowflake INSERT below.
    if metric.severity in DDB_SEVERITIES:
        return dynamo_writer.write(metric)

    # Defensive: anything that is neither a DynamoDB nor a Snowflake severity is
    # dropped and logged explicitly (with the pipeline/metric so it can be
    # traced) rather than silently returned — a silent skip would look identical
    # to a successful write in the logs. This cannot happen while the two sets
    # cover ALLOWED_SEVERITIES, but guards against a future severity being added
    # to the ingestion contract without a routing decision.
    if metric.severity not in SNOWFLAKE_SEVERITIES:
        logger.info(
            "metric dropped (severity has no storage route) pipeline=%s metric=%s severity=%s environment=%s",
            metric.pipeline_name,
            metric.metric_name,
            metric.severity,
            metric.environment,
        )
        return 0

    payload_json = json.dumps(metric.payload) if metric.payload is not None else None

    with get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                _INSERT_SQL,
                (
                    metric.pipeline_name,
                    metric.metric_name,
                    metric.metric_value,
                    metric.severity,
                    metric.run_id,
                    payload_json,
                    metric.is_alert,
                    metric.environment,
                ),
            )
            # `LAST_QUERY_ID()` + RESULT_SCAN would let us return the IDENTITY
            # value, but the Snowflake connector exposes it cheaper via
            # `cursor.sfqid` + a follow-up scan. For Monty we don't strictly
            # need the ID returned to the caller — they hand off and forget —
            # so we just confirm the row was written.
            rows = cursor.rowcount
            logger.info(
                "metric written pipeline=%s metric=%s severity=%s is_alert=%s environment=%s rows=%s",
                metric.pipeline_name,
                metric.metric_name,
                metric.severity,
                metric.is_alert,
                metric.environment,
                rows,
            )
            return rows
        finally:
            cursor.close()
