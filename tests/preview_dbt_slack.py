"""Manual preview of the Slack message for a dbt-run failure — no AWS, no deploy.

Not a pytest file (no `test_` prefix, so pytest skips it). Automated assertions
for this behaviour live in `test_slack_formatter.py`; this script is for
eyeballing the *rendered* message — headline, alert callout, payload table, and
(optionally) the real AI error summary.

Run from anywhere — it bootstraps the repo root onto sys.path itself:

    python3 tests/preview_dbt_slack.py
    ANTHROPIC_API_KEY=sk-ant-... python3 tests/preview_dbt_slack.py   # test the AI snippet
"""

import json
import os
import sys
from pathlib import Path

# Make `lambdas` importable when run as a standalone script (conftest only sets
# this up under pytest). slack.py imports only stdlib, so no boto3/snowflake stub
# is needed here.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lambdas.observer import slack  # noqa: E402 — after the sys.path bootstrap

# A real dbt-run failure row: pipeline_name is the generic collector name and
# the failed model lives in payload.failures[].pipeline_name.
METRIC = {
    "id": 1330664,
    "pipeline_name": "dbt_run_failures",
    "metric_name": "pipeline_failure",
    "metric_value": 1.0,
    "severity": "error",
    "run_id": "ec427732-fd0c-40ef-8ca8-4ae47de7816b",
    "environment": "default",
    "payload": {
        "failure_count": 1,
        "failures": [{
            "error_message": (
                "Database Error in model braze_cdi_attribute_sync "
                "(models/marts/application/braze_cdi_attribute_sync.sql)\n"
                "  001003 (42000): SQL compilation error:\n"
                "  syntax error line 27 at position 0 unexpected 'distinct_load_hours'."
            ),
            "pipeline_name": "braze_cdi_attribute_sync",
            "resource_type": "model",
            "unique_id": "model.dbt_snowflake_transformation.braze_cdi_attribute_sync",
        }],
        "summary": "1 model(s) failed: braze_cdi_attribute_sync",
    },
}


def main():
    """Render METRIC and print the section blocks (and full JSON) to stdout."""
    api_key = os.environ.get("ANTHROPIC_API_KEY") or None
    body = slack.format_message(METRIC, api_key)
    blocks = body["attachments"][0]["blocks"]

    print("=" * 70)
    for block in blocks:
        if block.get("type") == "section":
            print(block["text"]["text"])
            print("-" * 70)

    if api_key:
        print("\n(ANTHROPIC_API_KEY set — the *Error by ai* block above is a real API call)")
    else:
        print("\n(no ANTHROPIC_API_KEY — snippet fell back to raw error lines; "
              "set the env var to test the AI path)")

    print("\nFull JSON body:")
    print(json.dumps(body, indent=2)[:1500])


if __name__ == "__main__":
    main()
