#!/bin/zsh
# ---------------------------------------------------------------------------
# Monty dashboard — scheduled incremental S3 -> SQLite ingest.
#
# Drains new warning/info Parquet objects into the local SQLite cache and
# archives them, so the dashboard's `both` source can read the fast local DB
# (MONTY_BOTH_WARN_SOURCE=sqlite) instead of scanning ~34k tiny S3 files live.
#
# Run by launchd every 60s (see com.monty.ingest.prod.plist). loadS3.py holds a
# single-run flock, so if one tick runs long the next one skips cleanly.
#
# Usage: run_ingest.sh [env]   (env defaults to prod)
# ---------------------------------------------------------------------------
set -u

ENV="${1:-prod}"
DASH_DIR="/Users/henkduplooy/Documents/Berg/Monty/dash"
PYTHON="/opt/homebrew/opt/python@3.11/bin/python3.11"   # the interpreter with boto3/pyarrow

# aws CLI must be resolvable so boto3 can use the SSO token cache in ~/.aws.
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

cd "$DASH_DIR" || exit 1

# --recent-days 2 covers the UTC midnight rollover; the archive step keeps the
# live partition tiny, so each run only reads the ~1 min of new files.
exec "$PYTHON" loadS3.py --env "$ENV" --ingest --recent-days 2 \
    >> "ingest.${ENV}.log" 2>&1
