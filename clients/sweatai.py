"""SweatAI client — copy this file into any repo and call `sweatai(...)`.

    from sweatai import sweatai
    answer = sweatai("my dbt run failed with X", service="dbt-ci")

Config comes from two env vars (set once, per environment):
    SWEATAI_URL          the POST /sweatai endpoint URL (CDK output SweatAiUrl)
    MONTY_HMAC_SECRET    the shared HMAC secret from monty-<env>-secrets

Stdlib only — nothing to pip install.
"""

import hashlib
import hmac
import json
import os
import urllib.request


def sweatai(prompt: str, service: str = "unknown", system_prompt: str | None = None) -> str:
    """Send a prompt to SweatAI and return Claude's reply as a string.

    prompt         the question / failure log to analyse (required)
    service        a label for who's calling (shows up in the logs)
    system_prompt  override the default DataOps persona (optional)
    """
    url = os.environ["SWEATAI_URL"]
    secret = os.environ["MONTY_HMAC_SECRET"]

    payload = {"prompt": prompt, "service": service}
    if system_prompt is not None:
        payload["system_prompt"] = system_prompt

    body = json.dumps(payload).encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()

    request = urllib.request.Request(
        url,
        data=body,
        headers={"content-type": "application/json", "X-Monty-Signature": signature},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))["reply"]


if __name__ == "__main__":
    # Quick manual test: SWEATAI_URL=... MONTY_HMAC_SECRET=... python sweatai.py
    print(sweatai("My dbt run failed with a Snowflake SQL compilation error. What now?",
                  service="manual-test"))
