"""Print what a Slack token actually is and which scopes it really carries.

The app config page shows what you *granted*; the token only gets those scopes
once the app is REINSTALLED. This reads the truth off the API response header
(`x-oauth-scopes`), so it tells you whether a `missing_scope` error is a config
problem or a stale-token problem.

Companion to scripts/slack_last_messages.py, which needs channels:history
(public) or groups:history (private).

Usage:
    export SLACK_TOKEN='xoxb-...'
    python3 scripts/slack_token_scopes.py

Stdlib only.
"""

import json
import os
import sys
import urllib.request


def main() -> None:
    token = os.environ.get("SLACK_TOKEN")
    if not token:
        sys.exit("no token: export SLACK_TOKEN='xoxb-...'")

    kind = "bot token OK" if token.startswith("xoxb-") else "NOT a bot token — needs xoxb-"
    print(f"token starts with: {token[:9]}...  ({kind})")

    request = urllib.request.Request(
        "https://slack.com/api/auth.test", headers={"Authorization": f"Bearer {token}"}
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        # Slack reports the token's real scope set in this header — the body of
        # auth.test doesn't include it.
        granted = response.headers.get("x-oauth-scopes", "")
        result = json.loads(response.read().decode("utf-8"))

    if not result.get("ok"):
        sys.exit(f"auth.test failed: {result.get('error')}")

    print(f"team:    {result.get('team')}")
    print(f"bot id:  {result.get('bot_id')}")
    print(f"user:    {result.get('user')}")

    scopes = sorted(scope for scope in granted.split(",") if scope)
    print(f"\nscopes actually on this token ({len(scopes)}):")
    for scope in scopes:
        print(f"  - {scope}")

    needed = {"channels:history", "groups:history"}
    have = needed & set(scopes)
    print()
    if have:
        print(f"OK — can read history via: {', '.join(sorted(have))}")
    else:
        print("MISSING channels:history / groups:history —> add them to BOT token scopes, "
              "then Reinstall to Workspace and copy the NEW token.")


if __name__ == "__main__":
    main()
