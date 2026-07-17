"""Backfill the DynamoDB metrics table from the pre-cutover warning/info history.

Since the 2026-07-17 cutover the lambdas write `warning`/`info` to DynamoDB
(`monty-<env>-metrics-ddb`). Everything BEFORE that lives in the old stores:

    S3 live run_date=  2026-06-10 -> 07-06   (never ingested)
    S3 archive/        07-07 -> 07-14        (ingested, then moved)
    SQLite events      07-07 -> 07-15        (the ingested copy)

This replays that history into DynamoDB so the dashboard's dynamo leg alone can
serve the window (and the S3 bucket can eventually be decommissioned).

WHY IT REUSES db.py:  rows are fetched through the dashboard's own readers, so
they arrive already normalised to the canonical row contract (upper-case keys,
float METRIC_VALUE, naive-UTC OCCURRED_AT, PAYLOAD as a JSON string). One
normaliser = the backfilled items are byte-for-byte the shape the live writer
produces. Only `critical`/`error` are excluded — those belong in Snowflake and
are NOT part of the DynamoDB contract.

IDEMPOTENCY:  the live writer builds `sk` as "<occurred_at>#<uuid4>", which would
duplicate on every re-run. Here the sk is "<occurred_at>#<source ID>" — derived
from the event's own ID — so a re-run overwrites the same item instead of
duplicating. That makes this safe to resume after a failure.

TTL:  `occurred_at + MONTY_METRICS_TTL_DAYS` (default 90d), matching
dynamo_writer._ttl_epoch. Replayed rows therefore expire on their ORIGINAL
schedule — a 3-week-old row lands with ~69 days left, not a fresh 90.

Usage:
    # what would be written (no AWS writes at all)
    python3 backfill_dynamo.py --env prod --days 28 --dry-run

    # do it
    python3 backfill_dynamo.py --env prod --days 28

    # narrow it
    python3 backfill_dynamo.py --env dev --start 2026-06-19 --end 2026-07-15
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("backfill")

# Only these land in DynamoDB — mirrors metric_writer.DDB_SEVERITIES.
DDB_SEVERITIES = ("warning", "info")
DEFAULT_TTL_DAYS = int(os.environ.get("MONTY_METRICS_TTL_DAYS", "90"))
BATCH = 25          # DynamoDB batch_write_item hard limit


def _fetch_rows(env, start, end, source):
    """Pull normalised rows from the SQLite cache and/or the live S3 Parquet.

    Both legs are read and unioned because neither covers the full window on its
    own (SQLite only has what the ingest job archived). De-dup is on the event
    ID, which is unique per row in every store.
    """
    import db

    lookback = max(1, (datetime.utcnow() - start).days + 1)
    rows, seen = [], set()

    legs = []
    if source in ("both", "sqlite"):
        legs.append(("sqlite", db._fetch_sqlite))
    if source in ("both", "s3"):
        legs.append(("s3", db._fetch_s3))

    for name, fn in legs:
        try:
            got = fn(lookback, env, datetime.utcnow())
            logger.info("  leg %-7s -> %d row(s)", name, len(got))
        except Exception as exc:                      # a dead leg must not abort
            logger.warning("  leg %-7s FAILED, skipping: %s", name, exc)
            continue
        for r in got:
            rid = r.get("ID")
            if rid in seen:
                continue
            seen.add(rid)
            rows.append(r)

    kept = [r for r in rows
            if str(r.get("SEVERITY") or "").lower() in DDB_SEVERITIES
            and r.get("OCCURRED_AT") is not None
            and start <= r["OCCURRED_AT"] <= end]
    logger.info("  union=%d  after severity+window filter=%d", len(rows), len(kept))
    return kept


def _to_item(row):
    """One normalised row -> one DynamoDB item, matching dynamo_writer.write()."""
    occurred = row["OCCURRED_AT"]
    if occurred.tzinfo is None:                   # store stamps naive UTC
        occurred = occurred.replace(tzinfo=timezone.utc)
    occurred_iso = occurred.isoformat()

    env = row.get("ENVIRONMENT")
    pipeline = row.get("PIPELINE_NAME")
    item = {
        "pk": f"{env}#{pipeline}",
        # deterministic sk (source ID, not uuid4) -> re-runs overwrite, not duplicate
        "sk": f"{occurred_iso}#{row.get('ID')}",
        "pipeline_name": pipeline,
        "metric_name": row.get("METRIC_NAME"),
        "severity": row.get("SEVERITY"),
        "is_alert": bool(row.get("IS_ALERT")),
        "environment": env,
        "occurred_at": occurred_iso,
        "ttl": int(occurred.timestamp()) + DEFAULT_TTL_DAYS * 86400,
    }
    # Optional fields are OMITTED when null (same as the live writer).
    if row.get("METRIC_VALUE") is not None:
        item["metric_value"] = Decimal(str(row["METRIC_VALUE"]))
    if row.get("RUN_ID") is not None:
        item["run_id"] = row["RUN_ID"]
    if row.get("PAYLOAD") is not None:
        item["payload"] = row["PAYLOAD"]           # already a JSON string
    return item


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, choices=("prod", "dev"))
    ap.add_argument("--days", type=int, default=28, help="lookback window (default 28)")
    ap.add_argument("--start", help="YYYY-MM-DD (overrides --days)")
    ap.add_argument("--end", help="YYYY-MM-DD (inclusive; default now)")
    ap.add_argument("--source", default="both", choices=("both", "sqlite", "s3"))
    ap.add_argument("--table", help="default: monty-<env>-metrics-ddb")
    ap.add_argument("--profile", help="default: MONTY_S3_PROFILE_<ENV> from .env")
    ap.add_argument("--region", default=os.environ.get("MONTY_DDB_REGION", "us-east-1"))
    ap.add_argument("--dry-run", action="store_true", help="read + build only, no writes")
    args = ap.parse_args()

    end = (datetime.strptime(args.end, "%Y-%m-%d") + timedelta(days=1)
           if args.end else datetime.utcnow())
    start = (datetime.strptime(args.start, "%Y-%m-%d") if args.start
             else end - timedelta(days=args.days))

    # Load dash/.env FIRST (same contract as app.py: it is authoritative) so the
    # per-env AWS profiles / table / region resolve exactly like the dashboard.
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from app import _load_dotenv        # noqa: E402  (also runs it on import)
    _load_dotenv()
    import db
    table_name = args.table or f"monty-{args.env}-metrics-ddb"
    profile = args.profile or db._s3_profile_for(args.env)

    logger.info("backfill %s  window=[%s .. %s]  table=%s  profile=%s%s",
                args.env, start.date(), end.date(), table_name, profile or "(default chain)",
                "  [DRY RUN]" if args.dry_run else "")

    logger.info("reading history (%s legs)...", args.source)
    rows = _fetch_rows(args.env, start, end, args.source)
    if not rows:
        logger.info("nothing to backfill — done.")
        return

    items = [_to_item(r) for r in rows]
    total = len(items)
    oldest = min(i["occurred_at"] for i in items)
    newest = max(i["occurred_at"] for i in items)
    logger.info("%d item(s) to write  (%s .. %s)", total, oldest[:19], newest[:19])

    if args.dry_run:
        logger.info("DRY RUN — sample item:")
        for k, v in sorted(items[0].items()):
            logger.info("    %-14s %s", k, v)
        logger.info("DRY RUN — no writes issued. Re-run without --dry-run to apply.")
        return

    import boto3
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    table = session.resource("dynamodb", region_name=args.region).Table(table_name)

    written = 0
    with table.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as batch:
        for i, item in enumerate(items, 1):
            batch.put_item(Item=item)
            written += 1
            if i % 5000 == 0 or i == total:        # progress: never go silent
                logger.info("  [%d/%d] written (%.0f%%)", i, total, i / total * 100)
    logger.info("done — %d item(s) into %s", written, table_name)


if __name__ == "__main__":
    main()
