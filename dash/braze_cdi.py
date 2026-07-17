"""
braze_cdi.py
------------
Braze CDI (Cloud Data Ingestion) sync-job status -> Monty event rows, cached in
SQLite. PROD ONLY.

Flow (per the requirements):
  * First run backfills every sync run since BRAZE_CDI_BACKFILL_FROM (2026-06-01)
    into SQLite.
  * Every dashboard load then live-fetches the current status from the Braze REST
    API and UPSERTs new runs into SQLite ("fill it up"), so the view is live but
    persisted. If the API call fails, we fall back to the stored SQLite rows so a
    Braze hiccup never blanks the lanes.

Each sync run is mapped to the standard Monty event-row contract so it renders on
the timeline like any other pipeline. The two tracked integrations (delete +
attribute) become lanes under the `braze-cdisync` family (see formatting.py).

Config (env vars, e.g. in dash/.env — the feature is a no-op until both are set):
  BRAZE_REST_ENDPOINT   e.g. https://rest.iad-01.braze.com
  BRAZE_API_KEY         a REST API key with CDI read scope
  BRAZE_CDI_BACKFILL_FROM   optional, default 2026-06-01
  BRAZE_CDI_TTL             optional seconds to cache the live fetch, default 60
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

import sqlite_store

logger = logging.getLogger("monty.braze_cdi")

BRAZE_REST_ENDPOINT = os.environ.get("BRAZE_REST_ENDPOINT", "").rstrip("/")
BRAZE_API_KEY = os.environ.get("BRAZE_API_KEY", "").strip()
BACKFILL_FROM = os.environ.get("BRAZE_CDI_BACKFILL_FROM", "2026-06-01")
CACHE_TTL = int(os.environ.get("BRAZE_CDI_TTL", "60"))
HTTP_TIMEOUT = int(os.environ.get("BRAZE_CDI_TIMEOUT", "30"))

# The exact CDI integrations to track: integration_id -> (lane sync-type, name).
# Matched by ID (reliable) rather than by name — note the API name is
# "Attribution", not "Attribute".
TRACKED_INTEGRATIONS = {
    "d94b1797-b7a1-4fc6-be8b-30308ddfbe33": ("delete", "Snowflake Delete Ingestion Prod"),
    "f6683b97-356a-49f5-8044-a510b743bd70": ("attribute", "Snowflake Attribution Ingestion Prod"),
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS braze_cdi_syncs (
    INTEGRATION_ID   TEXT,
    INTEGRATION_NAME TEXT,
    SYNC_TYPE        TEXT,     -- delete | attribute
    JOB_STATUS       TEXT,
    SYNC_START_TIME  TEXT,     -- ISO-8601 naive UTC
    ROWS_SYNCED      INTEGER,
    ROWS_FAILED      INTEGER,
    PRIMARY KEY (INTEGRATION_ID, SYNC_START_TIME)
);
"""

# guard the live fetch so rapid page reloads don't hammer Braze
_LAST_REFRESH = 0.0
_REFRESH_LOCK = threading.Lock()


def enabled() -> bool:
    """True only when the endpoint AND key are configured."""
    return bool(BRAZE_REST_ENDPOINT and BRAZE_API_KEY)


def _connect() -> sqlite3.Connection:
    conn = sqlite_store.connect()          # same monty.db as the rest of the cache
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def _headers() -> dict:
    # Braze REST APIs authenticate with a Bearer token.
    return {"Authorization": "Bearer %s" % BRAZE_API_KEY,
            "Content-Type": "application/json"}


def _parse_ts(value):
    """Braze timestamps -> naive-UTC datetime (the downstream contract)."""
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _get(url: str) -> dict:
    resp = requests.get(url, headers=_headers(), timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_runs_from_api() -> list[dict]:
    """Pull each tracked integration's recent sync runs from the Braze REST API
    (matched by explicit integration_id). Returns a flat list of run dicts. A
    single integration failing is logged and skipped; a transport-level failure
    of the whole call propagates so the caller can fall back to SQLite."""
    runs: list[dict] = []
    for iid, (sync_type, name) in TRACKED_INTEGRATIONS.items():
        try:
            status = _get("%s/cdi/integrations/%s/job_sync_status"
                          % (BRAZE_REST_ENDPOINT, iid))
        except requests.exceptions.RequestException as exc:
            logger.error("braze cdi: job_sync_status failed for %s (%s): %s",
                         name, iid, exc)
            continue
        page = status.get("results", []) or []
        logger.info("braze cdi: %s (%s) — %d run(s)", name, sync_type, len(page))
        for run in page:
            runs.append({
                "integration_id": iid,
                "integration_name": name,
                "sync_type": sync_type,
                "job_status": run.get("job_status"),
                "sync_start_time": run.get("sync_start_time"),
                "rows_synced": run.get("rows_synced") or 0,
                "rows_failed": run.get("rows_failed_with_errors") or 0,
            })
    logger.info("braze cdi: %d run(s) total across %d integration(s)",
                len(runs), len(TRACKED_INTEGRATIONS))
    return runs


def _upsert(conn: sqlite3.Connection, runs: list[dict]) -> int:
    """Insert runs on/after the backfill date, deduped on
    (integration_id, sync_start_time). Returns rows inserted or updated.

    Uses UPSERT (not INSERT OR IGNORE) so a sync first seen while still
    `running` (0 rows) gets its JOB_STATUS / ROW counts corrected once it
    reaches a terminal status on a later fetch — the PK is the sync's START
    time, which is stable across its lifetime. The WHERE guard means a row whose
    values are unchanged (already terminal) is left untouched, so stable rows
    never churn and the returned count stays honest."""
    floor = BACKFILL_FROM
    before = conn.total_changes
    with sqlite_store._WRITE_LOCK:
        for r in runs:
            ts = _parse_ts(r["sync_start_time"])
            if ts is None or ts.isoformat(sep=" ")[:10] < floor:
                continue
            conn.execute(
                "INSERT INTO braze_cdi_syncs "
                "(INTEGRATION_ID, INTEGRATION_NAME, SYNC_TYPE, JOB_STATUS, "
                " SYNC_START_TIME, ROWS_SYNCED, ROWS_FAILED) "
                "VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(INTEGRATION_ID, SYNC_START_TIME) DO UPDATE SET "
                "  JOB_STATUS       = excluded.JOB_STATUS, "
                "  ROWS_SYNCED      = excluded.ROWS_SYNCED, "
                "  ROWS_FAILED      = excluded.ROWS_FAILED, "
                "  INTEGRATION_NAME = excluded.INTEGRATION_NAME "
                "WHERE JOB_STATUS  <> excluded.JOB_STATUS "
                "   OR ROWS_SYNCED <> excluded.ROWS_SYNCED "
                "   OR ROWS_FAILED <> excluded.ROWS_FAILED",
                (r["integration_id"], r["integration_name"], r["sync_type"],
                 r["job_status"], ts.isoformat(sep=" "),
                 int(r["rows_synced"] or 0), int(r["rows_failed"] or 0)),
            )
        conn.commit()
    return conn.total_changes - before


def refresh(force: bool = False) -> None:
    """Live-fetch from Braze and upsert into SQLite. Cached for CACHE_TTL seconds
    so rapid reloads don't hammer the API. Never raises — a failed fetch just
    leaves the stored rows in place."""
    global _LAST_REFRESH
    if not enabled():
        return
    now = time.monotonic()
    with _REFRESH_LOCK:
        if not force and CACHE_TTL > 0 and (now - _LAST_REFRESH) < CACHE_TTL:
            return
        try:
            runs = fetch_runs_from_api()
        except requests.exceptions.RequestException as exc:
            logger.error("braze cdi: live fetch failed, serving SQLite: %s", exc)
            _LAST_REFRESH = now            # don't retry-storm on a hard outage
            return
        conn = _connect()
        try:
            inserted = _upsert(conn, runs)
            logger.info("braze cdi: upserted %d new run(s)", inserted)
        finally:
            conn.close()
        _LAST_REFRESH = now


# job_status values that mean the sync actually completed
_SYNCED_OK = ("success", "succeeded", "complete", "completed", "ok")
# job_status values that mean the sync is still in flight (not a failure)
_IN_PROGRESS = ("running", "pending", "queued", "in_progress", "in progress",
                "started", "starting", "syncing", "processing")


def _severity_for(status, rows_synced) -> str:
    """CDI sync severity (dashboard-internal states):
      * still running (not yet terminal)   -> 'running'   (blue, no alert)
      * failed / any terminal non-success  -> 'error'     (red, alerts)
      * synced WITH rows (rows_synced > 0) -> 'info'       (OK / green)
      * synced with ZERO rows              -> 'noload'     (light green)

    An in-flight sync ('running', 'pending', …) has NOT failed — it just hasn't
    finished, so `rows_synced` is still 0. It gets a neutral 'running' state and
    never alerts; only a TERMINAL non-success status is a real failure (red).

    A completed zero-row sync is a healthy no-op, not a warning: nothing failed,
    there was simply nothing to load. It gets its own light-green 'noload' state
    so it reads distinct from a real load (green) without looking like a problem
    (orange). Both 'running' and 'noload' are dashboard-only visual states
    understood by transform.py; neither reaches CUSTOM_METRICS.
    """
    status_l = str(status or "").lower()
    if status_l in _IN_PROGRESS:
        return "running"
    if status_l not in _SYNCED_OK:
        return "error"
    return "info" if (rows_synced or 0) > 0 else "noload"


def _row_to_event(row: sqlite3.Row) -> dict:
    """Map a stored sync run to the Monty event-row contract."""
    sev = _severity_for(row["JOB_STATUS"], row["ROWS_SYNCED"])
    return {
        "ID": "braze-cdi-%s-%s" % (row["INTEGRATION_ID"], row["SYNC_START_TIME"]),
        "PIPELINE_NAME": "braze-cdi-%s" % row["SYNC_TYPE"],   # -> family braze-cdisync
        "METRIC_NAME": "cdi_sync",
        "METRIC_VALUE": float(row["ROWS_SYNCED"] or 0),
        "SEVERITY": sev,
        "RUN_ID": row["INTEGRATION_ID"],
        "PAYLOAD": json.dumps({
            "job_status": row["JOB_STATUS"],
            "rows_synced": row["ROWS_SYNCED"],
            "rows_failed": row["ROWS_FAILED"],
            "integration_name": row["INTEGRATION_NAME"],
        }),
        "OCCURRED_AT": sqlite_store._parse_ts(row["SYNC_START_TIME"]),
        "IS_ALERT": sev == "error",         # only a real sync failure alerts
        "SENT_TO_SLACK": False,
        "SENT_AT": None,
        "ENVIRONMENT": "prod",
    }


def fetch_events(lookback_days: int, now: datetime | None = None) -> list[dict]:
    """Live-refresh, then return the CDI sync event rows within the window from
    SQLite. Empty list when the feature isn't configured. PROD only."""
    if not enabled():
        return []
    from datetime import timedelta
    refresh()
    ref = now or datetime.utcnow()
    start = ref - timedelta(days=lookback_days)
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT * FROM braze_cdi_syncs "
            "WHERE SYNC_START_TIME BETWEEN ? AND ? ORDER BY SYNC_START_TIME",
            (start.isoformat(sep=" "), ref.isoformat(sep=" ")),
        ).fetchall()
        out = [_row_to_event(r) for r in rows]
        logger.info("braze cdi: %d sync event row(s) in window", len(out))
        return out
    finally:
        conn.close()
