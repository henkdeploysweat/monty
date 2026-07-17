"""
sqlite_store.py
---------------
Local SQLite cache for Monty S3 events.

The dashboard's S3 leg used to read thousands of tiny live-write Parquet objects
on every page load (~9s cold). This module lets a separate ingest job
(loadS3.py --ingest) drain those objects into a local SQLite database once, so
the dashboard reads the local DB instead (near-instant).

Two tables:
  * events        - one row per event, matching the Snowflake/S3 row contract
                    (see db._EVENT_COLS). PK on ID -> INSERT OR IGNORE makes
                    re-ingest idempotent (never double-counts).
  * ingest_log    - one row per consumed S3 key, with an `archived` flag. Drives
                    idempotency (skip already-ingested keys) and archive-retry.

The store holds the ORIGINAL raw event rows only — no pre-computed summaries.
The dashboard's transform.py does all aggregation from these rows, exactly as it
does for the Snowflake and live-S3 sources.

Timestamps are stored as ISO-8601 text in naive UTC — the same contract the rest
of the app uses (see db._to_utc_naive). fetch_events() parses them back into
naive datetimes so the dashboard's transform.py sees no difference from the
Snowflake/S3 paths.
"""
from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import threading
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("monty.sqlite_store")

# Local database file. Defaults to monty.db next to this module; override with
# MONTY_SQLITE_PATH (e.g. a path on a persistent volume in a container).
SQLITE_PATH = Path(os.environ.get(
    "MONTY_SQLITE_PATH", str(Path(__file__).parent / "monty.db")))

# The row contract shared with db.py. Kept in this order for stable inserts.
EVENT_COLS = ("ID", "PIPELINE_NAME", "METRIC_NAME", "METRIC_VALUE", "SEVERITY",
              "RUN_ID", "PAYLOAD", "OCCURRED_AT", "IS_ALERT", "SENT_TO_SLACK",
              "SENT_AT", "ENVIRONMENT")

# SQLite is single-writer. All write paths (ingest + summary rebuild) serialise
# through this lock so parallel S3 network work above never races the DB.
_WRITE_LOCK = threading.Lock()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    ID            TEXT PRIMARY KEY,
    PIPELINE_NAME TEXT,
    METRIC_NAME   TEXT,
    METRIC_VALUE  REAL,
    SEVERITY      TEXT,
    RUN_ID        TEXT,
    PAYLOAD       TEXT,
    OCCURRED_AT   TEXT,   -- ISO-8601, naive UTC
    IS_ALERT      INTEGER,-- 0/1
    SENT_TO_SLACK INTEGER,-- 0/1
    SENT_AT       TEXT,   -- ISO-8601, naive UTC (nullable)
    ENVIRONMENT   TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_occurred ON events (OCCURRED_AT);
CREATE INDEX IF NOT EXISTS idx_events_env_occurred ON events (ENVIRONMENT, OCCURRED_AT);

CREATE TABLE IF NOT EXISTS ingest_log (
    S3_KEY      TEXT PRIMARY KEY,
    INGESTED_AT TEXT,   -- ISO-8601 UTC of when this key was ingested
    ROW_COUNT   INTEGER,
    ARCHIVED    INTEGER DEFAULT 0  -- 1 once the object has been moved to archive/
);
"""


def connect(path: Path | str | None = None) -> sqlite3.Connection:
    """Open a SQLite connection with WAL enabled and the schema ensured.

    WAL lets the dashboard read while an ingest is writing without blocking.
    The connection is returned with row factory set so callers get dict-like
    access via column name.
    """
    db_path = Path(path) if path is not None else SQLITE_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    # WAL: concurrent readers during writes; NORMAL sync is durable enough for a
    # cache that can always be rebuilt from S3/Snowflake.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def _iso(value) -> str | None:
    """Serialise a naive-UTC datetime (or ISO string) to ISO-8601 text."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value)


def _parse_ts(value):
    """Parse ISO-8601 text back into a naive datetime (or None)."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None
    # Stored naive; strip any tzinfo defensively so the contract stays naive UTC.
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


def _row_id(row: dict) -> str:
    """Stable primary key for an event row.

    Prefer the producer's ID. If absent (older writers, malformed rows), derive
    a deterministic hash of the natural key so the same event always maps to the
    same PK and dedups on re-ingest instead of duplicating."""
    rid = row.get("ID")
    if rid:
        return str(rid)
    natural = "|".join(str(row.get(k)) for k in
                       ("PIPELINE_NAME", "METRIC_NAME", "OCCURRED_AT",
                        "METRIC_VALUE", "SEVERITY"))
    return "derived-" + hashlib.sha1(natural.encode("utf-8")).hexdigest()


def insert_events(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """Insert event rows, ignoring any whose ID already exists.

    Returns the number of rows actually inserted (existing IDs are skipped, so a
    re-ingest of the same object contributes 0). Caller is responsible for the
    surrounding transaction/commit."""
    if not rows:
        return 0
    before = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO events "
        "(ID, PIPELINE_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY, RUN_ID, "
        " PAYLOAD, OCCURRED_AT, IS_ALERT, SENT_TO_SLACK, SENT_AT, ENVIRONMENT) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [(
            _row_id(r),
            r.get("PIPELINE_NAME"),
            r.get("METRIC_NAME"),
            r.get("METRIC_VALUE"),
            r.get("SEVERITY"),
            r.get("RUN_ID"),
            r.get("PAYLOAD"),
            _iso(r.get("OCCURRED_AT")),
            1 if r.get("IS_ALERT") else 0,
            1 if r.get("SENT_TO_SLACK") else 0,
            _iso(r.get("SENT_AT")),
            r.get("ENVIRONMENT"),
        ) for r in rows],
    )
    return conn.total_changes - before


def record_ingest(conn: sqlite3.Connection, s3_key: str, row_count: int,
                  ingested_at: datetime, archived: bool = False) -> None:
    """Upsert an ingest_log entry for an S3 key. Caller commits."""
    conn.execute(
        "INSERT INTO ingest_log (S3_KEY, INGESTED_AT, ROW_COUNT, ARCHIVED) "
        "VALUES (?,?,?,?) "
        "ON CONFLICT(S3_KEY) DO UPDATE SET "
        "  INGESTED_AT=excluded.INGESTED_AT, ROW_COUNT=excluded.ROW_COUNT",
        (s3_key, _iso(ingested_at), row_count, 1 if archived else 0),
    )


def mark_archived(conn: sqlite3.Connection, s3_key: str) -> None:
    """Flag an already-ingested key as moved to the archive prefix. Caller commits."""
    conn.execute("UPDATE ingest_log SET ARCHIVED=1 WHERE S3_KEY=?", (s3_key,))


def known_keys(conn: sqlite3.Connection) -> set[str]:
    """All S3 keys already recorded in ingest_log (any archive state)."""
    return {r[0] for r in conn.execute("SELECT S3_KEY FROM ingest_log")}


def is_populated(path: Path | str | None = None) -> bool:
    """True if the cache holds at least one event row.

    Used by the `both` source to decide whether the SQLite leg is ready: before
    the first ingest the DB is empty, and reading it would silently drop every
    warning/info row — so callers fall back to live S3 until it's populated."""
    conn = connect(path)
    try:
        return conn.execute("SELECT EXISTS(SELECT 1 FROM events)").fetchone()[0] == 1
    finally:
        conn.close()


def unarchived_keys(conn: sqlite3.Connection) -> set[str]:
    """Keys ingested but not yet archived (archive/delete failed on a prior run)."""
    return {r[0] for r in
            conn.execute("SELECT S3_KEY FROM ingest_log WHERE ARCHIVED=0")}


def _row_to_event(row: sqlite3.Row) -> dict:
    """Convert a stored events row back into the dashboard's dict contract."""
    return {
        "ID": row["ID"],
        "PIPELINE_NAME": row["PIPELINE_NAME"],
        "METRIC_NAME": row["METRIC_NAME"],
        "METRIC_VALUE": row["METRIC_VALUE"],
        "SEVERITY": row["SEVERITY"],
        "RUN_ID": row["RUN_ID"],
        "PAYLOAD": row["PAYLOAD"],
        "OCCURRED_AT": _parse_ts(row["OCCURRED_AT"]),
        "IS_ALERT": bool(row["IS_ALERT"]),
        "SENT_TO_SLACK": bool(row["SENT_TO_SLACK"]),
        "SENT_AT": _parse_ts(row["SENT_AT"]),
        "ENVIRONMENT": row["ENVIRONMENT"],
    }


def fetch_events(start: datetime, now: datetime, env: str | None,
                 path: Path | str | None = None) -> list[dict]:
    """Return events in [start, now] for `env`, ordered by OCCURRED_AT.

    Mirrors db._fetch_s3's return contract exactly so the dashboard is agnostic
    to whether the warning/info rows came from S3 live or the local cache. A row
    with a NULL ENVIRONMENT matches any env (same as the S3 path)."""
    conn = connect(path)
    try:
        sql = ("SELECT * FROM events "
               "WHERE OCCURRED_AT IS NOT NULL "
               "  AND OCCURRED_AT BETWEEN ? AND ?")
        params: list = [_iso(start), _iso(now)]
        if env is not None:
            sql += " AND (ENVIRONMENT IS NULL OR ENVIRONMENT = ?)"
            params.append(env)
        sql += " ORDER BY OCCURRED_AT"
        rows = [_row_to_event(r) for r in conn.execute(sql, params)]
        logger.info("sqlite: %d event row(s) in window [%s, %s] env=%s",
                    len(rows), start, now, env)
        return rows
    finally:
        conn.close()


def fetch_pipeline_last_seen(env: str | None, start: datetime,
                             path: Path | str | None = None) -> dict:
    """{pipeline_name: last_seen_naive_utc} for pipelines active since `start`.

    Unlike the live-S3 path (which can't answer this cheaply), the local DB makes
    it a trivial grouped MAX — so ghost/stale lanes work again on the sqlite
    source. Returns naive-UTC datetimes."""
    conn = connect(path)
    try:
        sql = ("SELECT PIPELINE_NAME, MAX(OCCURRED_AT) FROM events "
               "WHERE OCCURRED_AT IS NOT NULL AND OCCURRED_AT >= ?")
        params: list = [_iso(start)]
        if env is not None:
            sql += " AND (ENVIRONMENT IS NULL OR ENVIRONMENT = ?)"
            params.append(env)
        sql += " GROUP BY PIPELINE_NAME"
        out = {name: _parse_ts(ts) for name, ts in conn.execute(sql, params)
               if name is not None}
        logger.info("sqlite: last_seen for %d pipeline(s) since %s", len(out), start)
        return out
    finally:
        conn.close()
