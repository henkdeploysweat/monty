"""Show the last N messages a bot posted to a Slack channel.

Useful for confirming Monty actually delivered an alert (the observer posts via
incoming webhooks, which show up as bot messages with no `user` field).

Setup:
    export SLACK_TOKEN='xoxb-...'          # bot token, or pass --token

Required OAuth scopes (bot token) — that's all, nothing else:
    channels:history   read messages in PUBLIC channels Monty was added to
    groups:history     read messages in PRIVATE channels Monty was added to

Takes a channel ID on purpose: looking a channel up by NAME would additionally
need channels:read / groups:read, so we skip that and keep the token minimal.

The bot must be a member of the channel — note the scope wording, "channels
that Monty has been added to". Invite it with `/invite @Monty`, or you'll get
`not_in_channel` even with the scopes granted.

Find the channel ID in Slack: open the channel -> click its name -> the ID is at
the bottom of the dialog (it's also the C... segment in the channel URL).

Usage:
    python3 scripts/slack_last_messages.py --channel C0123456789
    python3 scripts/slack_last_messages.py --channel C0123456789 --limit 7
    python3 scripts/slack_last_messages.py --channel C0123456789 --all

Stdlib only.
"""

import argparse
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime

SLACK_API = "https://slack.com/api"

# C = public channel, G = legacy private channel, D = DM.
_CHANNEL_ID_RE = re.compile(r"^[CGD][A-Z0-9]{6,}$")


def slack_get(method: str, token: str, params: dict) -> dict:
    """Call a Slack Web API GET method and return the parsed JSON.

    Exits with a readable message when Slack reports ok=false — its errors
    ("invalid_auth", "not_in_channel", "missing_scope") are far more useful than
    a raw traceback.
    """
    url = f"{SLACK_API}/{method}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=20) as response:
        result = json.loads(response.read().decode("utf-8"))

    if not result.get("ok"):
        error = result.get("error", "unknown_error")
        hint = {
            "invalid_auth": "the token is wrong or revoked",
            "not_in_channel": "invite the bot to the channel: /invite @Monty",
            "channel_not_found": "wrong channel ID, or the bot can't see that channel",
            "missing_scope": (
                f"token lacks a scope. needed: {result.get('needed')} "
                "(grant channels:history and/or groups:history, then REINSTALL the app)"
            ),
        }.get(error, "")
        sys.exit(f"Slack API error on {method}: {error}" + (f" — {hint}" if hint else ""))
    return result


def message_text(message: dict) -> str:
    """Best-effort readable text for a message.

    Monty's alerts carry no top-level `text` — the content lives in
    attachments[].blocks[].text.text — so fall back to walking the blocks.
    """
    if message.get("text", "").strip():
        return message["text"].strip()

    lines: list[str] = []
    for attachment in message.get("attachments", []):
        if attachment.get("text", "").strip():
            lines.append(attachment["text"].strip())
        for block in attachment.get("blocks", []):
            text = (block.get("text") or {}).get("text", "")
            if text.strip():
                lines.append(text.strip())
            for element in block.get("elements", []):
                element_text = element.get("text")
                value = element_text.get("text") if isinstance(element_text, dict) else None
                if value and value.strip():
                    lines.append(value.strip())
    return "\n".join(lines) if lines else "(no text)"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Show the last N bot messages in a Slack channel. "
                    "Needs only channels:history (public) / groups:history (private).",
    )
    parser.add_argument("--channel", required=True,
                        help="channel ID, e.g. C0123456789 (not a #name — see --help notes)")
    parser.add_argument("--limit", type=int, default=7, help="how many messages to show (default 7)")
    parser.add_argument("--token", default=os.environ.get("SLACK_TOKEN"),
                        help="Slack bot token; defaults to $SLACK_TOKEN")
    parser.add_argument("--all", action="store_true",
                        help="show every message, not just bot ones")
    args = parser.parse_args()

    if not args.token:
        sys.exit("no token: export SLACK_TOKEN='xoxb-...' or pass --token")

    channel_id = args.channel.strip()
    if not _CHANNEL_ID_RE.match(channel_id):
        sys.exit(
            f"'{args.channel}' is not a channel ID. Pass the ID (e.g. C0123456789), not a name — "
            "resolving a name would need the extra channels:read / groups:read scopes.\n"
            "Find it in Slack: open the channel -> click its name -> ID is at the bottom."
        )

    # Over-fetch so filtering down to bot messages still leaves `limit` of them;
    # Slack returns newest-first.
    fetch = args.limit if args.all else min(args.limit * 10, 200)
    result = slack_get("conversations.history", args.token,
                       {"channel": channel_id, "limit": fetch})

    messages = result.get("messages", [])
    if not args.all:
        messages = [m for m in messages if m.get("bot_id")]
    messages = messages[: args.limit]

    if not messages:
        print("no messages found (bot hasn't posted here, or wrong channel)")
        return

    print(f"last {len(messages)} {'' if args.all else 'bot '}message(s) in {channel_id}:\n")
    for message in messages:
        when = datetime.fromtimestamp(float(message["ts"])).strftime("%Y-%m-%d %H:%M:%S")
        who = message.get("username") or message.get("bot_id") or message.get("user") or "?"
        print("=" * 70)
        print(f"[{when}] {who}")
        print(message_text(message))
    print("=" * 70)


if __name__ == "__main__":
    main()
