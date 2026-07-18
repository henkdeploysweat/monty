"""Materialised HOURLY rollup of the DynamoDB warn/info events.

WHY
---
The dashboard fetches ~300k+ raw rows to render a view. Aggregating them per
hour first cuts that dramatically — measured on prod (7d):

    timeline grain (pipeline, hour)          308,797 -> 12,833   (24x)
    anomaly  grain (pipeline, metric, hour)  308,797 -> 77,602   (4x)

DynamoDB has no server-side aggregation, so the rollup must be pre-computed and
stored. It lives in the SAME table `monty-<env>-metrics-ddb` under a separate pk
namespace, read by the dashboard, written by this module.

SCHEMA (one item per (env, pipeline, metric, hour))
    pk = "rollup#<env>#<pipeline>"     # per-pipeline pk keeps Query parallelism,
                                       # no hot day-partition; own namespace so
                                       # raw reads never collide (db.ROLLUP_PK_PREFIX)
    sk = "<hour ISO UTC>#<metric>"     # hour-major: one sk range = all metrics
                                       # over a span of hours for one pipeline
    pipeline_name, metric_name, hour
    n            # raw event count in the metric-hour (timeline count + detector n)
    worst_sev    # highest-rank severity in the hour   (timeline colour)
    any_alert    # OR of is_alert over the hour         (timeline alert flag)
    mean_val     # mean(metric_value) -> the per-hour series value the detector scores
    last_val, last_ts   # last-in-hour value + its real event time (escape hatch:
                        # flip observed=last without a re-backfill)
    sum_val      # sum(metric_value) — cheap, lets a volume view reconstruct totals
    ttl          # hour_epoch + ROLLUP_TTL_DAYS*86400 (120d > raw's 90d, so a
                 # settled hour's rollup never expires before raw in a read window)

IDEMPOTENCY (the load-bearing property)
---------------------------------------
The item key is derived PURELY from (env, pipeline, metric, hour) and every item
is RECOMPUTED from whatever raw rows exist — never incremented, never keyed on an
event ID. This makes the "dedup on row['ID'] collapsed 78,912 rows to 1" class of
bug (see backfill_dynamo.py) impossible here: re-running produces byte-identical
items and overwrites in place. Source of truth is RAW DynamoDB only.

Usage:
    python3 rollup.py --env prod --days 28 --dry-run   # read + build, no writes
    python3 rollup.py --env prod --days 28             # apply
    python3 rollup.py --env prod --hours 3             # refresh recent settled hours
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("rollup")

ROLLUP_TTL_DAYS = int(os.environ.get("MONTY_ROLLUP_TTL_DAYS", "120"))
# Severity rank for "worst in the hour". Raw warn/info is all this leg holds, but
# rank all four so the rollup is correct if a higher severity ever lands here.
_SEV_RANK = {"info": 1, "warning": 2, "error": 3, "critical": 4}
_RANK_SEV = {v: k for k, v in _SEV_RANK.items()}


def _hour_floor(dt: datetime) -> datetime:
    """Truncate to the top of the hour (naive UTC, as db normalises timestamps)."""
    return dt.replace(minute=0, second=0, microsecond=0)


def _hour_iso(hour_dt: datetime) -> str:
    """Canonical hour key, matching db._iso (tz-aware UTC ISO)."""
    return hour_dt.replace(tzinfo=timezone.utc).isoformat()


def recompute_pipeline_span(db, table, env, pipeline, start, end) -> list[dict]:
    """Recompute every (metric, hour) rollup item for one pipeline over [start, end].

    Reads RAW via the dashboard's own per-pipeline sk-range Query (db._ddb_query_pk)
    and normaliser, so the aggregate sees exactly the rows the dashboard would.
    Whole hours only: `start` is floored to the hour so the first hour is complete.
    """
    start_hour = _hour_floor(start)
    raw_pk = "%s#%s" % (env, pipeline)
    sk_lo, sk_hi = db._sk_bounds(start_hour, end, include_end=True)
    # Lean projection — the rollup needs only these five fields.
    attrs = ("occurred_at", "metric_name", "metric_value", "severity", "is_alert")
    items = db._ddb_query_pk(table, raw_pk, sk_lo, sk_hi, attrs)

    # bucket[(hour_iso, metric)] -> running aggregate
    buckets: dict[tuple, dict] = {}
    for item in items:
        row = db._normalise_ddb_row(item)
        ts = row.get("OCCURRED_AT")
        if ts is None:
            continue
        metric = row.get("METRIC_NAME")
        hour_iso = _hour_iso(_hour_floor(ts))
        agg = buckets.get((hour_iso, metric))
        if agg is None:
            agg = {"n": 0, "sum": 0.0, "have_val": False,
                   "last_ts": None, "last_val": None,
                   "rank": 0, "any_alert": False}
            buckets[(hour_iso, metric)] = agg
        agg["n"] += 1
        val = row.get("METRIC_VALUE")
        if isinstance(val, (int, float)):
            agg["sum"] += val
            agg["have_val"] = True
        # last-by-event-time (raw items arrive sk-sorted asc, but don't rely on it)
        if agg["last_ts"] is None or ts > agg["last_ts"]:
            agg["last_ts"] = ts
            agg["last_val"] = val if isinstance(val, (int, float)) else agg["last_val"]
        sev = (row.get("SEVERITY") or "info").lower()
        agg["rank"] = max(agg["rank"], _SEV_RANK.get(sev, 1))
        agg["any_alert"] = agg["any_alert"] or bool(row.get("IS_ALERT"))

    out = []
    for (hour_iso, metric), agg in buckets.items():
        hour_dt = datetime.fromisoformat(hour_iso)
        item = {
            "pk": "rollup#%s#%s" % (env, pipeline),
            "sk": "%s#%s" % (hour_iso, metric),
            "pipeline_name": pipeline,
            "metric_name": metric,
            "hour": hour_iso,
            "n": agg["n"],
            "worst_sev": _RANK_SEV.get(agg["rank"], "info"),
            "any_alert": agg["any_alert"],
            "ttl": int(hour_dt.timestamp()) + ROLLUP_TTL_DAYS * 86400,
        }
        # Numeric aggregates only when the metric-hour had numeric values —
        # OMITTED otherwise, same discipline as the live writer.
        if agg["have_val"]:
            item["sum_val"] = Decimal(str(agg["sum"]))
            item["mean_val"] = Decimal(str(agg["sum"] / agg["n"]))
        if isinstance(agg["last_val"], (int, float)):
            item["last_val"] = Decimal(str(agg["last_val"]))
        if agg["last_ts"] is not None:
            # the REAL last-event time (full resolution), not the hour boundary —
            # so the anomaly page's "latest_at" stays a true event time.
            item["last_ts"] = agg["last_ts"].replace(tzinfo=timezone.utc).isoformat()
        out.append(item)
    return out


def backfill(env, start, end, table_name, profile, region, dry_run) -> int:
    """Recompute + upsert rollup items for every pipeline over [start, end]."""
    import boto3
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import db

    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    table = session.resource("dynamodb", region_name=region).Table(table_name)

    # Raw pks only (db._ddb_pipelines already skips rollup# — belt-and-braces here).
    pks = [pk for pk in db._ddb_pipelines(env)
           if not pk.startswith(db.ROLLUP_PK_PREFIX)]
    pipelines = [pk.split("#", 1)[1] for pk in pks]
    logger.info("rollup %s  window=[%s .. %s]  %d pipeline(s)%s",
                env, start, end, len(pipelines), "  [DRY RUN]" if dry_run else "")

    def work(pipeline):
        items = recompute_pipeline_span(db, table, env, pipeline, start, end)
        logger.info("  %-44s %6d item(s)", pipeline, len(items))
        return items

    with ThreadPoolExecutor(max_workers=max(1, db.MONTY_DDB_WORKERS)) as pool:
        per_pipe = list(pool.map(work, pipelines))
    items = [it for chunk in per_pipe for it in chunk]
    total = len(items)
    logger.info("built %d rollup item(s) across %d pipeline(s)", total, len(pipelines))

    if dry_run:
        if items:
            logger.info("DRY RUN — sample item:")
            for k, v in sorted(items[0].items()):
                logger.info("    %-14s %s", k, v)
        logger.info("DRY RUN — no writes. Re-run without --dry-run to apply.")
        return 0

    written = 0
    with table.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as batch:
        for i, item in enumerate(items, 1):
            batch.put_item(Item=item)
            written += 1
            if i % 5000 == 0 or i == total:
                logger.info("  [%d/%d] written (%.0f%%)", i, total, i / total * 100)
    logger.info("done — %d rollup item(s) into %s", written, table_name)
    return written


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, choices=("prod", "dev"))
    ap.add_argument("--days", type=int, help="lookback window in days")
    ap.add_argument("--hours", type=int, help="lookback window in hours (refresh)")
    ap.add_argument("--start", help="YYYY-MM-DD (overrides --days/--hours)")
    ap.add_argument("--end", help="YYYY-MM-DD (inclusive; default now)")
    ap.add_argument("--table", help="default: monty-<env>-metrics-ddb")
    ap.add_argument("--profile", help="default: MONTY_S3_PROFILE_<ENV> from .env")
    ap.add_argument("--region", default=os.environ.get("MONTY_DDB_REGION", "us-east-1"))
    ap.add_argument("--dry-run", action="store_true", help="read + build only")
    args = ap.parse_args()

    end = (datetime.strptime(args.end, "%Y-%m-%d") + timedelta(days=1)
           if args.end else datetime.utcnow())
    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d")
    elif args.hours:
        start = end - timedelta(hours=args.hours)
    else:
        start = end - timedelta(days=args.days or 28)

    # Load dash/.env FIRST (authoritative, same as backfill_dynamo/app).
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from app import _load_dotenv        # noqa: E402  (runs on import too)
    _load_dotenv()
    import db
    table_name = args.table or "monty-%s-metrics-ddb" % args.env
    profile = args.profile or db._s3_profile_for(args.env)

    backfill(args.env, start, end, table_name, profile, args.region, args.dry_run)


if __name__ == "__main__":
    main()
