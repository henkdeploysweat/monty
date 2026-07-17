#!/bin/zsh
# ---------------------------------------------------------------------------
# Monty dashboard — scheduled Braze CDI sync-log refresh.
#
# Polls the Braze job_sync_status API and upserts every visible run into
# SQLite (braze_cdi_syncs). Without this, the sync log is only refreshed when
# somebody opens the dashboard — and because the Braze endpoint only returns
# RECENT runs, a run seen while still `running` can age out before its terminal
# status is ever read, freezing that row at "running".
#
# Run by launchd every 5 min (see com.monty.brazecdi.plist). PROD only.
#
# Usage: run_braze_cdi.sh
# ---------------------------------------------------------------------------
set -u

DASH_DIR="/Users/henkduplooy/Documents/Berg/Monty/dash"
PYTHON="/opt/homebrew/opt/python@3.11/bin/python3.11"   # the interpreter with requests

# aws CLI on PATH for parity with run_ingest.sh (not needed by Braze itself).
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

cd "$DASH_DIR" || exit 1

# NOTE: do NOT `source .env` here — it is only *nearly* shell syntax (it has
# `BRAZE_REST_ENDPOINT =<value>`, space before the `=`, which zsh drops with
# "command not found"). refresh_braze_cdi.py loads it with the same tolerant
# parser the dashboard uses, before it imports braze_cdi.
exec "$PYTHON" refresh_braze_cdi.py >> "braze_cdi.log" 2>&1
