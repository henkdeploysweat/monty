#!/usr/bin/env python3
"""
loadS3.py
---------
Standalone script to pull Monty events from S3 (Parquet) into a DataFrame.

Usage:
    python loadS3.py --env dev --start 2026-07-01 --end 2026-07-10 --output data.parquet
    python loadS3.py --env prod --start 2026-07-01 --end 2026-07-10 --output data.csv

Requires AWS SSO login for the target environment profile.
"""
from __future__ import annotations
import argparse
import contextlib
import fcntl
import io
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timedelta
from pathlib import Path

import boto3
from botocore.config import Config
import pyarrow.parquet as pq
import pandas as pd

import sqlite_store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("loadS3")

# Load .env config (mirrors app.py/_load_dotenv)
def _load_dotenv():
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        logger.warning(".env not found, using environment variables only")
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ[key] = value


_load_dotenv()

# Config from .env
MONTY_S3_BUCKET = os.environ.get("MONTY_S3_BUCKET", "monty-{env}-metrics")
MONTY_S3_PREFIX = os.environ.get("MONTY_S3_PREFIX", "").strip("/")
MONTY_S3_TZ = os.environ.get("MONTY_S3_TZ", "UTC")
MONTY_S3_WORKERS = int(os.environ.get("MONTY_S3_WORKERS", "48"))


def _s3_profile_for(env: str) -> str | None:
    """AWS profile to use for this environment's bucket."""
    explicit = os.environ.get(f"MONTY_S3_PROFILE_{env.upper()}", "").strip()
    if explicit:
        return explicit
    return None


def _to_utc_naive(value):
    """Coerce a Parquet timestamp to a naive-UTC datetime."""
    if value is None:
        return None
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is not None:
        from datetime import timezone
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if MONTY_S3_TZ.upper() == "UTC":
        return value
    from zoneinfo import ZoneInfo
    return (value.replace(tzinfo=ZoneInfo(MONTY_S3_TZ))
                 .astimezone(timezone.utc).replace(tzinfo=None))


def _normalise_s3_row(raw: dict) -> dict:
    """Upper-case keys and coerce types."""
    row = {str(k).upper(): v for k, v in raw.items()}
    mv = row.get("METRIC_VALUE")
    row["METRIC_VALUE"] = float(mv) if mv is not None else None
    row["IS_ALERT"] = bool(row.get("IS_ALERT")) if row.get("IS_ALERT") is not None else False
    row["SENT_TO_SLACK"] = bool(row.get("SENT_TO_SLACK")) if row.get("SENT_TO_SLACK") is not None else False
    row["OCCURRED_AT"] = _to_utc_naive(row.get("OCCURRED_AT"))
    row["SENT_AT"] = _to_utc_naive(row.get("SENT_AT"))
    payload = row.get("PAYLOAD")
    if payload is not None and not isinstance(payload, str):
        import json
        try:
            row["PAYLOAD"] = json.dumps(payload)
        except (TypeError, ValueError):
            row["PAYLOAD"] = str(payload)
    return row


def fetch_s3_date_range(env: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Fetch events from S3 for a date range into a pandas DataFrame."""
    bucket = MONTY_S3_BUCKET.format(env=env)
    base = (MONTY_S3_PREFIX + "/") if MONTY_S3_PREFIX else ""

    # List all partitions and collect keys
    profile = _s3_profile_for(env)
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3", config=Config(max_pool_connections=MONTY_S3_WORKERS,
                                            retries={"max_attempts": 3}))

    keys: list[str] = []
    days = []
    d = start_date
    while d <= end_date:
        days.append(d)
        d += timedelta(days=1)

    total = len(days)
    for i, day in enumerate(days, 1):
        prefix = f"{base}run_date={day.strftime('%Y%m%d')}/"
        try:
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].lower().endswith(".parquet"):
                        keys.append(obj["Key"])
        except Exception as exc:
            logger.warning(f"[{i}/{total}] {prefix} — list failed: {exc}")
            continue
        logger.info(f"[{i}/{total}] {prefix} — found {len(keys)} total object(s)")

    if not keys:
        logger.info(f"No parquet objects in range {start_date} to {end_date}")
        return pd.DataFrame()

    logger.info(f"Reading {len(keys)} objects from {bucket}")

    # Fetch and parse in parallel
    def _read_object(key):
        try:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            table = pq.read_table(io.BytesIO(body))
        except Exception as exc:
            logger.error(f"Read failed for {key}: {exc}")
            return []
        rows = []
        for raw in table.to_pylist():
            row = _normalise_s3_row(raw)
            ts = row.get("OCCURRED_AT")
            if ts is None:
                continue
            # Filter by date range (inclusive)
            if isinstance(ts, datetime):
                ts_date = ts.date()
            else:
                ts_date = ts
            if start_date <= ts_date <= end_date:
                rows.append(row)
        return rows

    workers = min(MONTY_S3_WORKERS, max(4, len(keys)))
    rows: list[dict] = []
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for chunk in pool.map(_read_object, keys):
            rows.extend(chunk)
            done += 1
            if done % 250 == 0:
                logger.info(f"Read {done}/{len(keys)} objects")

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[["ID", "PIPELINE_NAME", "METRIC_NAME", "METRIC_VALUE", "SEVERITY",
                 "RUN_ID", "PAYLOAD", "OCCURRED_AT", "IS_ALERT", "SENT_TO_SLACK",
                 "SENT_AT", "ENVIRONMENT"]]
        df["OCCURRED_AT"] = pd.to_datetime(df["OCCURRED_AT"], utc=True)
        df["SENT_AT"] = pd.to_datetime(df["SENT_AT"], utc=True)
        df = df.sort_values("OCCURRED_AT").reset_index(drop=True)
        logger.info(f"Fetched {len(df)} events from {len(keys)} files")
    else:
        logger.info("No events matched the date range")

    return df


# ---------------------------------------------------------------------------
# Ingest mode: drain S3 -> local SQLite, then archive consumed objects.
# ---------------------------------------------------------------------------
# The dashboard's live S3 read is the slow path (~9s cold on a week of tiny
# per-event files). `--ingest` loads those objects into a local SQLite cache
# (sqlite_store) ONCE and then MOVES each consumed object to an archive/ prefix
# in the same bucket, so the source partition stays small and the dashboard can
# read the fast local DB (MONTY_SOURCE=sqlite).
#
# Safety / ordering guarantees:
#   * SQLite is committed BEFORE any S3 delete — a crash at worst re-ingests
#     (idempotent via the events PK), never loses data.
#   * Each object is COPIED to archive/ and the copy VERIFIED (head_object)
#     before the original is deleted — a failed copy never deletes the source.
#   * Already-ingested keys are skipped via ingest_log; keys ingested but whose
#     archive/delete failed on a prior run are retried on the next run.


def _s3_client(env: str):
    """A boto3 S3 client for this environment's account (via its SSO profile)."""
    profile = _s3_profile_for(env)
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return session.client("s3", config=Config(
        max_pool_connections=MONTY_S3_WORKERS, retries={"max_attempts": 3}))


def _list_source_keys(s3, bucket: str, base: str, days: list) -> list[str]:
    """List *.parquet keys under run_date=YYYYMMDD/ for each day (never archive/).

    The per-day prefix `{base}run_date=...` structurally excludes archived
    objects, which live under `{base}archive/run_date=...`."""
    keys: list[str] = []
    total = len(days)
    for i, day in enumerate(days, 1):
        prefix = f"{base}run_date={day.strftime('%Y%m%d')}/"
        before = len(keys)
        try:
            for page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    # defensive: never treat an already-archived object as source
                    if key.lower().endswith(".parquet") and "/archive/" not in key \
                            and not key.startswith(base + "archive/"):
                        keys.append(key)
        except Exception as exc:
            logger.warning(f"[{i}/{total}] {prefix} — list failed, skipping: {exc}")
            continue
        logger.info(f"[{i}/{total}] {prefix} — {len(keys) - before} object(s)")
    return keys


def archive_key_for(key: str, base: str) -> str:
    """Map a source key to its archive location, preserving folder structure.

    e.g. base=""            run_date=20260714/x.parquet
         -> archive/run_date=20260714/x.parquet
    e.g. base="metrics/"    metrics/run_date=20260714/x.parquet
         -> metrics/archive/run_date=20260714/x.parquet
    Pure function (no I/O) so it is unit-testable without AWS."""
    if base and key.startswith(base):
        rest = key[len(base):]
    else:
        rest = key
    return f"{base}archive/{rest}"


def _read_object_rows(s3, bucket: str, key: str) -> list[dict]:
    """Read one Parquet object and return normalised event rows (no filtering)."""
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    return [_normalise_s3_row(raw) for raw in table.to_pylist()]


def _archive_object(s3, bucket: str, key: str, base: str, dry_run: bool) -> bool:
    """Copy `key` to its archive location, verify, then delete the original.

    Returns True if the object now lives in the archive (or would, in dry-run).
    Copy + verify happen before the delete so a failed copy never loses data."""
    dest = archive_key_for(key, base)
    if dry_run:
        logger.info("    [dry-run] would move %s -> %s", key, dest)
        return True
    s3.copy_object(Bucket=bucket, Key=dest,
                   CopySource={"Bucket": bucket, "Key": key})
    # verify the copy exists before deleting the source
    s3.head_object(Bucket=bucket, Key=dest)
    s3.delete_object(Bucket=bucket, Key=key)
    return True


def ingest_to_sqlite(env: str, start_date: date, end_date: date,
                     dry_run: bool = False, db_path: str | None = None) -> dict:
    """Drain S3 events into the local SQLite cache and archive consumed objects.

    Steps:
      1. list source keys in [start_date, end_date]
      2. skip keys already in ingest_log; keep archive-only retries separately
      3. parallel-read new objects into memory
      4. serial SQLite transaction: insert events + ingest_log(archived=0), commit
      5. parallel copy->verify->delete of consumed keys, then mark archived=1

    Stores the ORIGINAL raw event rows only (no summaries). Returns a dict of
    counts."""
    bucket = MONTY_S3_BUCKET.format(env=env)
    base = (MONTY_S3_PREFIX + "/") if MONTY_S3_PREFIX else ""
    s3 = _s3_client(env)

    days = []
    d = start_date
    while d <= end_date:
        days.append(d)
        d += timedelta(days=1)

    logger.info("ingest: env=%s bucket=%s window=%s..%s dry_run=%s db=%s",
                env, bucket, start_date, end_date, dry_run,
                db_path or sqlite_store.SQLITE_PATH)

    # 1) list
    all_keys = _list_source_keys(s3, bucket, base, days)
    if not all_keys:
        logger.info("ingest: no source objects in window; nothing to do")
        return {"listed": 0, "ingested_keys": 0, "rows_inserted": 0, "archived": 0}

    # 2) partition into new (ingest) vs already-ingested-but-not-archived (retry)
    conn = sqlite_store.connect(db_path)
    try:
        known = sqlite_store.known_keys(conn)
        unarchived = sqlite_store.unarchived_keys(conn)
    finally:
        conn.close()
    new_keys = [k for k in all_keys if k not in known]
    retry_keys = [k for k in all_keys if k in unarchived]
    logger.info("ingest: %d listed, %d new to ingest, %d archive-retry",
                len(all_keys), len(new_keys), len(retry_keys))

    # 3) parallel read of new objects
    key_rows: dict[str, list[dict]] = {}
    if new_keys:
        workers = min(MONTY_S3_WORKERS, max(4, len(new_keys)))
        logger.info("ingest: reading %d new object(s) with %d workers",
                    len(new_keys), workers)

        def _read(key):
            try:
                return key, _read_object_rows(s3, bucket, key)
            except Exception as exc:
                logger.error("ingest: read failed, skipping %s: %s", key, exc)
                return key, None

        done = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for key, rows in pool.map(_read, new_keys):
                done += 1
                if rows is None:
                    logger.warning("[%d/%d] %s — read failed, will retry next run",
                                   done, len(new_keys), key)
                    continue
                key_rows[key] = rows
                if done % 250 == 0:
                    logger.info("ingest: read %d/%d objects", done, len(new_keys))

    # 4) serial SQLite write: insert rows + ingest_log for every successfully-read key
    rows_inserted = 0
    conn = sqlite_store.connect(db_path)
    try:
        ingested_at = datetime.utcnow()
        with sqlite_store._WRITE_LOCK:
            for key, rows in key_rows.items():
                inserted = sqlite_store.insert_events(conn, rows)
                rows_inserted += inserted
                sqlite_store.record_ingest(conn, key, len(rows), ingested_at,
                                           archived=False)
            conn.commit()
        logger.info("ingest: committed %d new row(s) from %d object(s) to SQLite",
                    rows_inserted, len(key_rows))
    finally:
        conn.close()

    # 5) archive consumed keys (freshly ingested + prior-run retries), then flag
    to_archive = list(key_rows.keys()) + [k for k in retry_keys if k not in key_rows]
    archived = 0
    if to_archive:
        workers = min(MONTY_S3_WORKERS, max(4, len(to_archive)))
        logger.info("ingest: archiving %d object(s)%s", len(to_archive),
                    " (dry-run)" if dry_run else "")

        def _do_archive(key):
            try:
                _archive_object(s3, bucket, key, base, dry_run)
                return key, True
            except Exception as exc:
                logger.error("ingest: archive failed for %s: %s", key, exc)
                return key, False

        conn = sqlite_store.connect(db_path)
        try:
            done = 0
            total_arch = len(to_archive)
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for key, ok in pool.map(_do_archive, to_archive):
                    done += 1
                    if ok and not dry_run:
                        with sqlite_store._WRITE_LOCK:
                            sqlite_store.mark_archived(conn, key)
                            conn.commit()
                        archived += 1
                        logger.info("[%d/%d] archived %s", done, total_arch, key)
                    elif ok and dry_run:
                        archived += 1
                    else:
                        logger.warning("[%d/%d] %s — archive failed, will retry "
                                       "next run", done, total_arch, key)
        finally:
            conn.close()

    result = {"listed": len(all_keys), "ingested_keys": len(key_rows),
              "rows_inserted": rows_inserted, "archived": archived}
    logger.info("ingest: done — %s", result)
    return result


@contextlib.contextmanager
def _single_run_lock(db_path: str | None):
    """Prevent overlapping ingests (e.g. a 1-minute cron firing while the
    previous run is still draining a busy partition). Takes a non-blocking
    exclusive flock on <db>.lock; if another run holds it, yields False so the
    caller exits cleanly instead of racing on the same S3 keys + SQLite writes."""
    lock_path = str((Path(db_path) if db_path else sqlite_store.SQLITE_PATH)) + ".lock"
    fh = open(lock_path, "w")
    try:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            yield False
            return
        yield True
    finally:
        fh.close()


def main():
    parser = argparse.ArgumentParser(description="Fetch/ingest Monty events from S3")
    parser.add_argument("--env", required=True, choices=["dev", "prod"],
                        help="AWS environment (dev or prod)")
    parser.add_argument("--start", type=str,
                        help="Start date (YYYY-MM-DD). Required for export; for "
                             "--ingest, omit to use --recent-days instead")
    parser.add_argument("--end", type=str,
                        help="End date (YYYY-MM-DD). Required for export; for "
                             "--ingest, omit to use --recent-days instead")
    parser.add_argument("--ingest", action="store_true",
                        help="Ingest S3 objects into local SQLite and archive them "
                             "(instead of exporting to a file)")
    parser.add_argument("--recent-days", type=int, default=None,
                        help="With --ingest and no --start/--end: ingest the last N "
                             "UTC days ending today (default 2 — covers the UTC "
                             "midnight rollover). Ideal for the scheduled job")
    parser.add_argument("--db", type=str, default=None,
                        help="SQLite path for --ingest (default: MONTY_SQLITE_PATH)")
    parser.add_argument("--dry-run", action="store_true",
                        help="With --ingest: log what would be archived without "
                             "copying or deleting any S3 objects")
    parser.add_argument("--output", type=str,
                        help="Output file path (.csv or .parquet) for export mode")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable debug logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.ingest:
        # window: explicit --start/--end (backfill) OR the last N days (scheduler)
        if args.start or args.end:
            if not (args.start and args.end):
                parser.error("--ingest with an explicit window needs BOTH --start and --end")
            start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
            end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
        else:
            days = args.recent_days if args.recent_days is not None else 2
            if days < 1:
                parser.error("--recent-days must be >= 1")
            end_date = datetime.utcnow().date()
            start_date = end_date - timedelta(days=days - 1)
            logger.info("ingest: recent window = last %d UTC day(s): %s..%s",
                        days, start_date, end_date)
        # only one ingest at a time (skip if another run is already draining)
        with _single_run_lock(args.db) as acquired:
            if not acquired:
                logger.info("ingest: another run holds the lock, skipping this tick")
                return
            ingest_to_sqlite(args.env, start_date, end_date,
                             dry_run=args.dry_run, db_path=args.db)
        return

    # --- export mode (unchanged) ---
    if not (args.start and args.end):
        parser.error("--start and --end are required for export mode")
    if not args.output:
        parser.error("--output is required unless --ingest is set")
    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    logger.info(f"Fetching {args.env} events from {start_date} to {end_date}")
    df = fetch_s3_date_range(args.env, start_date, end_date)

    if df.empty:
        logger.warning("No data fetched")
        return

    # Output
    output_path = Path(args.output)
    if output_path.suffix.lower() == ".parquet":
        df.to_parquet(output_path, index=False)
    elif output_path.suffix.lower() == ".csv":
        df.to_csv(output_path, index=False)
    else:
        raise ValueError("Output must be .csv or .parquet")

    # Summary
    logger.info(f"Saved {len(df)} rows to {args.output}")
    if not df.empty:
        logger.info(f"Date range: {df['OCCURRED_AT'].min()} to {df['OCCURRED_AT'].max()}")
        if "SEVERITY" in df.columns:
            logger.info("Severity breakdown:")
            logger.info(df["SEVERITY"].value_counts().to_string())


if __name__ == "__main__":
    main()
