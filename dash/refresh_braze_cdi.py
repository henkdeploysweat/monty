"""Scheduled Braze CDI sync-log refresh.

WHY THIS EXISTS
---------------
`braze_cdi.refresh()` is normally only called on a dashboard page load. The Braze
`job_sync_status` endpoint only returns RECENT runs, so anything that happens
while nobody has the page open is never captured — and a run first seen while
still `running` can age out of the API window before its terminal status is ever
read, freezing that row at "running" forever (the exact bug that needed a manual
`UPDATE braze_cdi_syncs SET JOB_STATUS='success'` fix).

Polling on a schedule closes that window: every tick upserts the current status
of every visible run, so a run reaches its terminal state in SQLite regardless of
who is looking. The upsert is guarded (only writes when something actually
changed), so a quiet tick is a no-op.

PROD ONLY, and a no-op unless BRAZE_REST_ENDPOINT + BRAZE_API_KEY are set — the
runner sources dash/.env so they are present. Never raises on a Braze outage:
`refresh()` logs and leaves the stored rows in place.

Usage:
    python3 refresh_braze_cdi.py            # one refresh (what launchd runs)
    python3 refresh_braze_cdi.py --status   # show what's stored, refresh nothing
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("braze-cdi-refresh")


def _load_dotenv():
    """Load dash/.env into the environment BEFORE braze_cdi is imported.

    Mirrors app._load_dotenv rather than importing it: importing app.py drags in
    Flask + transform (statsmodels/scipy), which is absurd for a 5-minute cron.

    Deliberately NOT `source .env` in the runner either — .env is only
    *nearly* shell-syntax: it contains `BRAZE_REST_ENDPOINT =<value>` (space
    before the `=`), which zsh reads as a command and drops. This loader splits
    on the first `=` and strips the key, exactly like the dashboard's, so
    whatever works for the app works here.
    """
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
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
            os.environ[key] = value      # .env is the source of truth; it wins


def _stored_summary():
    """Row counts by JOB_STATUS + the newest sync we hold, straight from SQLite."""
    import braze_cdi
    conn = braze_cdi._connect()
    try:
        by_status = conn.execute(
            "SELECT JOB_STATUS, COUNT(*) FROM braze_cdi_syncs GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        newest = conn.execute(
            "SELECT MAX(SYNC_START_TIME) FROM braze_cdi_syncs"
        ).fetchone()[0]
        return {s: n for s, n in by_status}, newest
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--status", action="store_true",
                    help="print what's stored and exit (no API call)")
    args = ap.parse_args()

    # ORDER MATTERS: braze_cdi reads BRAZE_REST_ENDPOINT / BRAZE_API_KEY into
    # module constants at IMPORT time, so .env must be loaded first or
    # enabled() is False and every run is a silent no-op.
    _load_dotenv()
    import braze_cdi

    if args.status:
        counts, newest = _stored_summary()
        logger.info("stored runs: %s | newest sync: %s", counts or "(none)", newest or "-")
        return

    if not braze_cdi.enabled():
        logger.error("BRAZE_REST_ENDPOINT / BRAZE_API_KEY not set — "
                     "is dash/.env sourced? nothing to do")
        sys.exit(1)

    before, _ = _stored_summary()
    try:
        braze_cdi.refresh(force=True)      # force: bypass the 60s page-load cache
    except sqlite3.Error as exc:           # a locked/corrupt cache must be loud
        logger.error("sqlite write failed: %s", exc)
        sys.exit(1)
    after, newest = _stored_summary()

    delta = sum(after.values()) - sum(before.values())
    logger.info("refresh ok — %d new run(s), %d stored %s | newest sync: %s",
                delta, sum(after.values()), after, newest or "-")


if __name__ == "__main__":
    main()
