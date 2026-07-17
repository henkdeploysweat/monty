"""DynamoDB writer for low-priority metrics.

`warning` and `info` metrics are diverted here instead of being INSERTed into
MONITORING_DB.MONITORING.CUSTOM_METRICS. High-frequency low-priority writes kept
the XS Snowflake warehouse permanently awake (single-row INSERT per write, one
connection each). This replaced the earlier S3 Parquet store (s3_writer): one
item per metric, free TTL retention, no small-file compaction, and operational
point/range queries via the key design.

Routing lives in `metric_writer.write()`; this module only knows how to turn one
`Metric` into one DynamoDB item.

Key design (table monty-<env>-metrics-ddb, on-demand billing):
    pk = "<environment>#<pipeline_name>"   groups a pipeline's metrics per env
    sk = "<occurred_at UTC ISO>#<uuid4>"   time-sortable, uuid guards identical
                                            timestamps
"Recent N for pipeline X" = Query(pk=..., Limit=N, ScanIndexForward=False).

Timestamps are UTC, deliberately. The Snowflake OCCURRED_AT default is
America/Los_Angeles (a known gotcha that makes live data look stale); this is a
fresh surface so we standardise on UTC.

Env vars expected at Lambda runtime:
    MONTY_METRICS_TABLE       Target table name. Provided by the CDK stack.
    MONTY_METRICS_TTL_DAYS    Optional retention override (default 90 days);
                              items expire via the table's `ttl` attribute.
    AWS_REGION                Standard Lambda env var; falls back to us-east-1.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from functools import lru_cache
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from .metric_writer import Metric

logger = logging.getLogger(__name__)

DEFAULT_REGION = "us-east-1"
DEFAULT_TTL_DAYS = 90


@lru_cache(maxsize=1)
def _get_table():
    """Return a cached DynamoDB Table handle for the Lambda's warm lifetime.

    Mirrors the caching rationale in snowflake_client._get_secret: Lambda freezes
    globals between invocations, so building the resource once on cold start and
    reusing it avoids per-invocation setup cost. Cold-start gotcha applies: a
    changed MONTY_METRICS_TABLE needs a cold start to be picked up.
    """
    region = os.environ.get("AWS_REGION", DEFAULT_REGION)
    table_name = os.environ["MONTY_METRICS_TABLE"]
    return boto3.resource("dynamodb", region_name=region).Table(table_name)


def _ttl_epoch(occurred_at: datetime) -> int:
    """Expiry epoch-seconds for the table's TTL attribute (occurred_at + N days)."""
    retention_days = int(os.environ.get("MONTY_METRICS_TTL_DAYS", DEFAULT_TTL_DAYS))
    return int(occurred_at.timestamp()) + retention_days * 86400


def write(metric: "Metric", occurred_at: "datetime | None" = None) -> int:
    """Write one metric as a single DynamoDB item.

    Returns 1 on success so the return contract matches metric_writer's
    Snowflake path (rowcount), keeping caller logging/handoff unchanged.

    `occurred_at` defaults to now (UTC) for the live write path. A backfill/replay
    passes the ORIGINAL event time so the item sorts (and TTL-expires) by when it
    happened, not when it was replayed. Must be a timezone-aware UTC datetime
    when supplied.
    """
    table = _get_table()
    occurred_at = occurred_at or datetime.now(timezone.utc)
    occurred_at_iso = occurred_at.isoformat()
    sort_key = f"{occurred_at_iso}#{uuid.uuid4()}"
    partition_key = f"{metric.environment}#{metric.pipeline_name}"

    item = {
        "pk": partition_key,
        "sk": sort_key,
        "pipeline_name": metric.pipeline_name,
        "metric_name": metric.metric_name,
        "severity": metric.severity,
        "is_alert": metric.is_alert,
        "environment": metric.environment,
        "occurred_at": occurred_at_iso,
        "ttl": _ttl_epoch(occurred_at),
    }
    # Optional fields are OMITTED when null rather than stored as DynamoDB NULL —
    # absent attributes cost nothing and read back as None either way.
    if metric.metric_value is not None:
        # DynamoDB rejects float; numbers must be Decimal (same as sweatai's
        # prompt-log writer). Stringify first to avoid float-repr noise.
        item["metric_value"] = Decimal(str(metric.metric_value))
    if metric.run_id is not None:
        item["run_id"] = metric.run_id
    if metric.payload is not None:
        # Keep the JSON string form (same as the S3/Snowflake paths) instead of
        # a DynamoDB Map — avoids guessing a struct shape from a free-form dict.
        item["payload"] = json.dumps(metric.payload)

    logger.info(
        "metric -> dynamodb pipeline=%s metric=%s severity=%s environment=%s table=%s pk=%s sk=%s",
        metric.pipeline_name,
        metric.metric_name,
        metric.severity,
        metric.environment,
        table.name,
        partition_key,
        sort_key,
    )
    table.put_item(Item=item)
    return 1
