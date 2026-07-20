"""Scheduled refresh of the materialised hourly rollup (dash/rollup.py).

WHY THIS EXISTS
---------------
The dashboard's fast path reads `rollup#<env>#<pipeline>` items instead of
~300k raw events. Nothing was writing them on a schedule: the rollup was
built by hand and then decayed. A stale rollup does NOT fail loudly — the
dashboard's live lane silently widens to cover the missing hours from RAW,
so the page just gets slower every hour until it times out. Running this on
a schedule is what keeps that fast path fast.

The work itself lives in dash/rollup.py (shared with the CLI, so there is
one implementation of the aggregation). backfill() is idempotent: items are
keyed purely on (env, pipeline, metric, hour) and recomputed from raw, so
re-running over the same span overwrites in place and can never double-count.
That is why re-covering recent hours on every run is safe — and why we do it:
it repairs any hour whose events arrived late.
"""
import logging
import os
import sys
from datetime import datetime, timedelta

# dash/db.py + dash/rollup.py are COPYed into the image beside lambdas/.
# rollup.py itself does `import db` after putting its own directory on the
# path, so both modules must live in the SAME directory (they do).
sys.path.insert(
    0, os.path.join(os.environ.get("LAMBDA_TASK_ROOT", "/var/task"), "dash")
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# How many trailing hours to recompute each run. Larger than the 1h schedule
# on purpose: overlapping windows repair late-arriving events and mean a
# single skipped invocation cannot leave a permanent hole in the rollup.
ROLLUP_HOURS = int(os.environ.get("MONTY_ROLLUP_HOURS", "6"))


def lambda_handler(event, context):
    """Recompute the trailing rollup window for this stack's environment.

    `event` may carry {"hours": N} to widen the window for a manual backfill
    invocation; otherwise MONTY_ROLLUP_HOURS applies.
    """
    import rollup  # imported after sys.path is set up

    env_name = os.environ["MONTY_ENV"]
    table_name = os.environ["MONTY_METRICS_TABLE"]
    region = os.environ.get("AWS_REGION", "us-east-1")

    hours = ROLLUP_HOURS
    if isinstance(event, dict) and event.get("hours"):
        try:
            hours = int(event["hours"])
        except (TypeError, ValueError):
            logger.warning("ignoring non-numeric hours=%r in event", event.get("hours"))

    end = datetime.utcnow()
    start = end - timedelta(hours=hours)
    logger.info(
        "rollup: env=%s table=%s window=%dh [%s .. %s]",
        env_name, table_name, hours, start, end,
    )

    started = datetime.utcnow()
    try:
        # profile=None -> default credential chain -> this Lambda's execution
        # role. Same account as the table, so no AssumeRole hop.
        written = rollup.backfill(
            env_name, start, end, table_name, None, region, False
        )
    except Exception:
        # Log the traceback before re-raising so the failure is readable in
        # CloudWatch rather than only as an EventBridge invocation error.
        logger.exception("rollup: FAILED for env=%s window=%dh", env_name, hours)
        raise

    elapsed = (datetime.utcnow() - started).total_seconds()
    logger.info(
        "rollup: wrote %d item(s) for env=%s in %.1fs", written, env_name, elapsed
    )
    return {"env": env_name, "hours": hours, "written": written,
            "elapsed_s": round(elapsed, 1)}
