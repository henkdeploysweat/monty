"""Slack message formatter and HTTP client.

Severity-based routing maps to Slack incoming webhook URLs stored in Secrets
Manager:
    critical, error  -> SLACK_WEBHOOK_INCIDENTS  (#data-incidents)
    warning, info    -> SLACK_WEBHOOK_ALERTS     (#data-alerts)

Kept independent of the handler so unit tests can exercise the formatter
without standing up Snowflake or boto3.
"""

import html
import json
import logging
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from lambdas.shared.sweatai_client import sweatai

try:
    from zoneinfo import ZoneInfo

    _ADELAIDE = ZoneInfo("Australia/Adelaide")
    # CUSTOM_METRICS timestamps are TIMESTAMP_NTZ populated by CURRENT_TIMESTAMP()
    # under the Snowflake account timezone (America/Los_Angeles), so a naive
    # datetime from the connector holds LA wall-clock time — NOT UTC.
    _SOURCE_TZ = ZoneInfo("America/Los_Angeles")
except Exception:  # noqa: BLE001 - missing tzdata must not break formatting
    _ADELAIDE = None
    _SOURCE_TZ = None

logger = logging.getLogger(__name__)

INCIDENTS_CHANNEL = "#data-incidents"
ALERTS_CHANNEL = "#data-alerts"
# Any metric whose ENVIRONMENT is not 'prod' is routed here regardless of
# severity, so dev/staging noise never reaches the prod incident channels.
DEV_CHANNEL = "#data-alerts-dev"
DEV_WEBHOOK_KEY = "SLACK_WEBHOOK_DEV"

# Map severity -> (channel name for outbox row, secret key for the webhook URL)
_SEVERITY_ROUTING = {
    "critical": (INCIDENTS_CHANNEL, "SLACK_WEBHOOK_INCIDENTS"),
    "error":    (INCIDENTS_CHANNEL, "SLACK_WEBHOOK_INCIDENTS"),
    "warning":  (ALERTS_CHANNEL,    "SLACK_WEBHOOK_ALERTS"),
    "info":     (ALERTS_CHANNEL,    "SLACK_WEBHOOK_ALERTS"),
}

# Severity -> left-border attachment colour (Slack hex string)
_COLORS = {
    "critical": "#E24B4A",
    "error":    "#E24B4A",
    "warning":  "#FAC775",
    "info":     "#378ADD",
}

# Personality: one header emoji + punchy title template per severity.
# {metric_name} and {pipeline_name} are interpolated at render time.
_TITLES = {
    "critical": ("🔥", "Workflow health just went red"),
    "error":    ("😬", "{pipeline_name} threw a tantrum on {metric_name}"),
    "warning":  ("⚠️",  "{metric_name} is looking sus on {pipeline_name}"),
    "info":     ("✅",  "{metric_name} updated on {pipeline_name}"),
}

# Severity -> the status word shown in the headline next to the pipeline name.
_STATUS = {
    "critical": "IS DOWN",
    "error":    "IS DOWN",
    "warning":  "NEEDS ATTENTION",
    "info":     "UPDATE",
}

# Severity -> emoji for the alert callout line. (Slack has no "alert" block
# type; the callout is a normal section with a leading emoji + bold pipeline.)
_ALERT_EMOJI = {
    "critical": ":alert:",
    "error":    ":alert:",
    "warning":  "⚠️",
    "info":     "ℹ️",
}

# Payload keys handled elsewhere (or internal plumbing) — excluded from the
# generic key/value table so they aren't rendered twice or leaked.
_TABLE_SKIP_KEYS = frozenset(
    {"error_message", "reason", "log_group", "slack_webhook", "raw"}
)


def _esc(text: Any) -> str:
    """Escape the three characters Slack treats specially in mrkdwn text.

    Slack requires `&`, `<`, `>` to be HTML-entity-escaped in message text;
    an unescaped `<...>` is otherwise parsed as a link and can swallow content.
    """
    return html.escape(str(text), quote=False)


def _adelaide_str(value: Any = None) -> str:
    """Format a timestamp in Adelaide local time, e.g. '2026-07-07 05:14 ACST'.

    Accepts a datetime, an ISO-8601 string, or None (falls back to now). A naive
    datetime is interpreted as America/Los_Angeles — that is how CUSTOM_METRICS
    stores OCCURRED_AT/SENT_AT (TIMESTAMP_NTZ under the Snowflake account tz),
    and the connector returns those columns as naive datetimes. Treating them as
    UTC (the old behaviour) landed the Slack timestamp ~7h early.
    """
    dt: datetime | None = None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str) and value.strip():
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            dt = None
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        # Naive value from Snowflake NTZ → LA wall-clock (fall back to UTC only
        # if tzdata is unavailable and _SOURCE_TZ couldn't be loaded).
        dt = dt.replace(tzinfo=_SOURCE_TZ or timezone.utc)
    if _ADELAIDE is not None:
        dt = dt.astimezone(_ADELAIDE)
    return dt.strftime("%Y-%m-%d %H:%M %Z")


def _cloudwatch_url(log_group: str | None) -> str:
    """Deep link to the CloudWatch Logs console, targeting `log_group` if known.

    The console encodes the log-group path by replacing each `/` with the
    literal `$252F` (a double URL-encoded slash).
    """
    region = os.environ.get("AWS_REGION") or "us-east-1"
    base = (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home"
        f"?region={region}#logsV2:log-groups"
    )
    if log_group:
        return f"{base}/log-group/{log_group.replace('/', '$252F')}"
    return base


def _dbt_failed_models(payload: dict[str, Any]) -> list[str]:
    """Extract the actual failed dbt model/test names from a dbt-run payload.

    dbt-run failures arrive with a generic pipeline_name of `dbt_run_failures`;
    the real object names live in `payload.failures[].pipeline_name`. Returns
    the de-duplicated list of those names, preserving first-seen order. Empty
    list means this is not a dbt-run payload (so callers fall back to the row's
    own pipeline_name).
    """
    failures = payload.get("failures")
    if not isinstance(failures, list):
        return []
    names: list[str] = []
    for failure in failures:
        if not isinstance(failure, dict):
            continue
        name = failure.get("pipeline_name") or failure.get("unique_id")
        if name and str(name) not in names:
            names.append(str(name))
    return names


def _dbt_heading(models: list[str]) -> str:
    """Collapse the failed-model list into a short headline string.

    One model  -> that model's name.
    Two+ models -> "first_model +N more" so the header stays scannable.
    """
    if not models:
        return "?"
    if len(models) == 1:
        return models[0]
    return f"{models[0]} +{len(models) - 1} more"


def _payload_table(metric: dict[str, Any], payload: dict[str, Any]) -> str | None:
    """Render the metric's core fields plus its payload as an aligned
    monospace key/value table (shown inside a Slack code block, so it renders
    identically everywhere — unlike Slack's flaky `markdown`-block tables)."""
    rows: list[tuple[str, str]] = []

    def add(key: str, value: Any) -> None:
        if value is None or (isinstance(value, str) and not value.strip()):
            return
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        rows.append((key, str(value)))

    add("metric", metric.get("metric_name"))
    add("value", metric.get("metric_value"))
    add("severity", metric.get("severity"))
    add("run_id", metric.get("run_id"))
    add("environment", metric.get("environment"))

    for key, value in payload.items():
        if key not in _TABLE_SKIP_KEYS:
            add(key, value)
    raw = payload.get("raw")
    if isinstance(raw, dict):
        for key, value in raw.items():
            if key not in _TABLE_SKIP_KEYS:
                add(key, value)

    if not rows:
        return None

    width = min(max(len(k) for k, _ in rows), 18)
    table = "\n".join(f"{key[:18].ljust(width)}  {value}" for key, value in rows)
    return table[:2800]  # keep the block comfortably under Slack's 3000 limit

# Severity -> GIF pool. CURRENTLY UNUSED: the image block was removed from
# format_message() because expiring giphy tokens made Slack reject the whole
# message (HTTP 400 invalid_attachments). Kept here so stable direct-image
# URLs can be re-enabled later behind a validity check.
_GIFS: dict[str, list[str]] = {
    "critical": [
            "https://media2.giphy.com/media/LleZnSzSeiGEtmnsMw/giphy.gif?cid=6104955e9xp0lj3yuraxycalqbygbeadir3jlj7cw857jzmb&ep=v1_gifs_translate&rid=giphy.gif&ct=g",
                     "https://media4.giphy.com/media/l2Je3TM1m4nBPz6Mg/giphy.gif?cid=6104955ert3ymx1qm98gvd6zqr8lqy7d4cppoz7r4g6o8jwn&ep=v1_gifs_translate&rid=giphy.gif&ct=g",
        "https://media2.giphy.com/media/Af4oCYIkwN3dzu7LxH/giphy.gif?cid=6104955ep9efbeklnptvy0nv7n8v1l21f5v8eylv8br387qv&ep=v1_gifs_translate&rid=giphy.gif&ct=g",
        "https://media4.giphy.com/media/lVBtp4SRW6rvDHf1b6/giphy-downsized.gif?cid=6104955ephebsjequ1y3j9vc69b749ww16vpa8a005hjpfk4&ep=v1_gifs_translate&rid=giphy-downsized.gif&ct=g",
        "https://media4.giphy.com/media/2ZwylZb5m3lUD3Zz6D/giphy-downsized.gif?cid=6104955ewbuy6teiauh34k1h41oxo7li1jpt8xfxmlpjfgzj&ep=v1_gifs_translate&rid=giphy-downsized.gif&ct=g",
        "https://media2.giphy.com/media/HOsHtiVdeypFxOhLAf/giphy.gif?cid=6104955eu6a87di9y9fwmidg10n0hj552xqagcimferd9hw9&ep=v1_gifs_translate&rid=giphy.gif&ct=g",
        "https://media1.giphy.com/media/kYCck3WtttVSmKwX7H/giphy-downsized.gif?cid=6104955e2sculv1w09srzre9z0awcq8et3jj4qr14i0t9zg2&ep=v1_gifs_translate&rid=giphy-downsized.gif&ct=g",
        "https://media4.giphy.com/media/26xBHklzttKHVB7bO/giphy-downsized.gif?cid=6104955e7c9kp7tgh8stnw8fbdubm2fmmltlb57jb08qb0w6&ep=v1_gifs_translate&rid=giphy-downsized.gif&ct=g",
        "https://media1.giphy.com/media/qaDbEDavgvKBs5jJc5/giphy.gif?cid=6104955e6kkt33lemdrrv475fbl2fwdg8ewiohzh06qzmmnq&ep=v1_gifs_translate&rid=giphy.gif&ct=g",
        "https://media3.giphy.com/media/26BRxDMJ6v6fAKrvy/giphy.gif?cid=6104955euopmuqa0vohsj1lav7xv9y81ul0jj8k50ytwkyj5&ep=v1_gifs_translate&rid=giphy.gif&ct=g",
        "https://media0.giphy.com/media/jPBPXnZS9i5Rm/giphy-downsized.gif?cid=6104955eme0phldu7jevmu45w4piyjn76k1wczc6d8q7aq0q&ep=v1_gifs_translate&rid=giphy-downsized.gif&ct=g",
        "https://media4.giphy.com/media/l0DAHFfmzkrc1IkrS/giphy.gif?cid=6104955e9jhfinc2d07zda17mtjyfofj37xfilrab3hmo9it&ep=v1_gifs_translate&rid=giphy.gif&ct=g"

        # add more critical GIF URLs here
    ],
    "error": [
                "https://media4.giphy.com/media/l2Je3TM1m4nBPz6Mg/giphy.gif?cid=6104955ert3ymx1qm98gvd6zqr8lqy7d4cppoz7r4g6o8jwn&ep=v1_gifs_translate&rid=giphy.gif&ct=g",
        "https://media2.giphy.com/media/Af4oCYIkwN3dzu7LxH/giphy.gif?cid=6104955ep9efbeklnptvy0nv7n8v1l21f5v8eylv8br387qv&ep=v1_gifs_translate&rid=giphy.gif&ct=g",
        "https://media4.giphy.com/media/lVBtp4SRW6rvDHf1b6/giphy-downsized.gif?cid=6104955ephebsjequ1y3j9vc69b749ww16vpa8a005hjpfk4&ep=v1_gifs_translate&rid=giphy-downsized.gif&ct=g",
        "https://media4.giphy.com/media/2ZwylZb5m3lUD3Zz6D/giphy-downsized.gif?cid=6104955ewbuy6teiauh34k1h41oxo7li1jpt8xfxmlpjfgzj&ep=v1_gifs_translate&rid=giphy-downsized.gif&ct=g",
        "https://media2.giphy.com/media/HOsHtiVdeypFxOhLAf/giphy.gif?cid=6104955eu6a87di9y9fwmidg10n0hj552xqagcimferd9hw9&ep=v1_gifs_translate&rid=giphy.gif&ct=g",
        "https://media1.giphy.com/media/kYCck3WtttVSmKwX7H/giphy-downsized.gif?cid=6104955e2sculv1w09srzre9z0awcq8et3jj4qr14i0t9zg2&ep=v1_gifs_translate&rid=giphy-downsized.gif&ct=g",
        "https://media4.giphy.com/media/26xBHklzttKHVB7bO/giphy-downsized.gif?cid=6104955e7c9kp7tgh8stnw8fbdubm2fmmltlb57jb08qb0w6&ep=v1_gifs_translate&rid=giphy-downsized.gif&ct=g",
        "https://media1.giphy.com/media/qaDbEDavgvKBs5jJc5/giphy.gif?cid=6104955e6kkt33lemdrrv475fbl2fwdg8ewiohzh06qzmmnq&ep=v1_gifs_translate&rid=giphy.gif&ct=g",
        "https://media3.giphy.com/media/26BRxDMJ6v6fAKrvy/giphy.gif?cid=6104955euopmuqa0vohsj1lav7xv9y81ul0jj8k50ytwkyj5&ep=v1_gifs_translate&rid=giphy.gif&ct=g",
        "https://media0.giphy.com/media/jPBPXnZS9i5Rm/giphy-downsized.gif?cid=6104955eme0phldu7jevmu45w4piyjn76k1wczc6d8q7aq0q&ep=v1_gifs_translate&rid=giphy-downsized.gif&ct=g",
        "https://media4.giphy.com/media/l0DAHFfmzkrc1IkrS/giphy.gif?cid=6104955e9jhfinc2d07zda17mtjyfofj37xfilrab3hmo9it&ep=v1_gifs_translate&rid=giphy.gif&ct=g"
        # add more error GIF URLs here
    ],
    "warning": [
        "https://media1.tenor.com/m/moHsN8DJz4EAAAAC/this-is-fine.gif",
        # add more warning GIF URLs here
    ],
    "info": [
        "https://media1.tenor.com/m/fYPFlABkJGEAAAAC/good-morning.gif",
        # add more info GIF URLs here
    ],
}

# Severity -> action buttons shown at the bottom of the attachment.
# Each tuple is (button label, optional URL template).
# URL templates may reference {pipeline_name} and {run_id}.
_BUTTONS = {
    "critical": [
        ("📊 View in CloudWatch", "https://console.aws.amazon.com/cloudwatch/home"),
        ("🔕 Silence alarm", None),
        ("📋 Runbook", None),
    ],
    "error": [
        ("📄 View logs", "https://console.aws.amazon.com/cloudwatch/home#logsV2:log-groups"),
        ("▶️ Re-run", None),
        ("🎫 Create ticket", None),
    ],
    "warning": [
        ("📄 View logs", "https://console.aws.amazon.com/cloudwatch/home#logsV2:log-groups"),
        ("🔍 Investigate", None),
    ],
    "info": [],
}


@dataclass(frozen=True)
class SlackResult:
    """Outcome of one Slack delivery attempt. Used to write ALERT_OUTBOX rows."""

    delivered: bool
    channel: str
    error_message: str | None


def route(severity: str, channel_override: str | None) -> tuple[str, str]:
    """Return (channel_label, secret_key_for_webhook) for a given severity.

    `channel_override` only changes the outbox label, not the webhook URL.
    """
    severity = (severity or "info").lower()
    if severity not in _SEVERITY_ROUTING:
        severity = "info"
    channel_label, secret_key = _SEVERITY_ROUTING[severity]
    if channel_override:
        channel_label = channel_override
    return channel_label, secret_key


# Slack incoming webhook URLs always start with this prefix. Anything else is
# rejected as a routing target so a malicious log emitter can't turn the
# observer into an SSRF gadget against internal endpoints.
_WEBHOOK_PREFIX = "https://hooks.slack.com/"


def resolve_destination(
    severity: str,
    slack_webhook: str | None,
    severity_webhooks: dict[str, str],
    environment: str | None = None,
) -> tuple[str, str]:
    """Pick (channel_label, webhook_url) for a metric.

        1. slack_webhook is a valid Slack hooks.slack.com URL -> use it,
           label = "custom-webhook" (avoid leaking URLs into ALERT_OUTBOX).
        2. environment is set and is not 'prod' -> dev channel, regardless of
           severity. Falls through to severity routing if the dev webhook is
           not configured, so a missing secret never silently drops an alert.
        3. otherwise -> severity-based routing.

    `severity_webhooks` maps the secret-key names used by `route()` to URLs,
    plus the optional DEV_WEBHOOK_KEY for non-prod routing.
    """
    if slack_webhook and isinstance(slack_webhook, str) and slack_webhook.startswith(_WEBHOOK_PREFIX):
        return "custom-webhook", slack_webhook

    # Non-prod metrics never touch the prod incident channel. Prefer the
    # dedicated dev webhook; if it is not configured (or still stale in a
    # warm Lambda before a cold start), fall back to the ALERTS channel —
    # never severity routing, which would escalate error/critical to
    # #data-incidents. This keeps the invariant simple: non-prod -> dev or
    # alerts, prod -> severity routing.
    if environment and environment.strip().lower() != "prod":
        dev_webhook = severity_webhooks.get(DEV_WEBHOOK_KEY)
        if dev_webhook:
            return DEV_CHANNEL, dev_webhook
        logger.warning(
            "environment=%s is non-prod but %s is not configured; "
            "routing to %s instead of the prod incident channel",
            environment,
            DEV_WEBHOOK_KEY,
            ALERTS_CHANNEL,
        )
        return ALERTS_CHANNEL, severity_webhooks["SLACK_WEBHOOK_ALERTS"]

    label_fallback, secret_key = route(severity, None)
    return label_fallback, severity_webhooks[secret_key]


def format_message(metric: dict[str, Any], api_key: str | None = None) -> dict[str, Any]:
    """Build the Slack incoming-webhook JSON body for one CUSTOM_METRICS row.

    Layout:
        {emoji} *PIPELINE {status}* · {Adelaide timestamp}!!!
        metric `...` · run `...` · env `...`
        *Payload*  -> monospace key/value table of the row + its payload
        *Error*    -> last few lines of error_message/reason, when present
        [ 📄 View CloudWatch logs ]  link button, deep-linked to the log group
                                     when the producer recorded one
        ID `...` · sent {Adelaide timestamp} · env `...`

    Rendered via the legacy `attachments` wrapper for the coloured left border.
    A monospace code-block table is used instead of Slack's `markdown`-block
    tables, which render inconsistently over incoming webhooks.
    """
    severity = (metric.get("severity") or "info").lower()
    if severity not in _SEVERITY_ROUTING:
        severity = "info"

    payload   = metric.get("payload") or {}
    pipeline  = metric.get("pipeline_name") or "?"
    metric_nm = metric.get("metric_name")   or "?"
    run_id    = metric.get("run_id")        or ""
    env       = metric.get("environment")   or ""
    when      = _adelaide_str(metric.get("occurred_at"))

    emoji = _TITLES[severity][0]
    status = _STATUS[severity]

    # For dbt-run failures the row's pipeline_name is the generic
    # `dbt_run_failures`; the real failed model/test names live in the payload.
    # Use those as the headline subject so the alert names the actual model.
    dbt_models = _dbt_failed_models(payload)
    heading_name = _dbt_heading(dbt_models) if dbt_models else pipeline

    # ---------------------------------------------------------------- headline
    # `<!channel>` is Slack's mrkdwn syntax for an @channel ping; a literal
    # "@channel" would render as inert grey text and notify nobody.
    headline = f"<!channel> {emoji} *{_esc(heading_name)} {status}* · {when}!!!"
    sub_parts = [f"metric `{_esc(metric_nm)}`"]
    if run_id:
        sub_parts.append(f"run `{_esc(run_id[:12])}`")
    if env:
        sub_parts.append(f"env `{_esc(env)}`")
    sub_line = " · ".join(sub_parts)

    att_blocks: list[dict[str, Any]] = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"{headline}\n{sub_line}"},
        },
    ]

    # ------------------------------------------------------------- alert callout
    # Slack has no "alert" block type, so this is a plain section styled as a
    # callout: severity emoji + the pipeline name in *bold* + the alert message.
    # The message is the first line of the metric's error/reason (dynamic per
    # alert); to pin a static string instead, replace `alert_msg` below.
    # dbt-run payloads nest the error text inside each failure rather than at
    # the top level, so fall back to the first failure's error_message.
    first_failure = next(
        (f for f in payload.get("failures", []) if isinstance(f, dict)), {}
    )
    error_source = (
        payload.get("error_message")
        or payload.get("reason")
        or first_failure.get("error_message")
        or ""
    )
    reason_lines = [
        line for line in str(error_source).splitlines() if line.strip()
    ]
    alert_msg = (reason_lines[0] if reason_lines else f"{metric_nm} tripped an alert")[:300]
    att_blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{_ALERT_EMOJI[severity]} *{_esc(heading_name)}* — {_esc(alert_msg)}",
            },
        }
    )

    # ------------------------------------------------------------ error snippet
    # Rendered ABOVE the payload table so the (AI-summarised) root cause is the
    # first thing on-call reads, before the raw key/value dump.
    error_msg = (
        payload.get("error_message")
        or payload.get("reason")
        or first_failure.get("error_message")
    )
    if error_msg and api_key:
        try:
            error_text = "\n".join([line for line in str(error_msg).splitlines() if line.strip()])[:2000]
            # Route through the SweatAI endpoint so this call is captured in the
            # prompt-logs DynamoDB table, not fired at Anthropic directly.
            snippet = sweatai(
                error_text,
                "Monty",
                system_prompt=(
                    "You are a data expert who always looks on the bright side of "
                    "life. You accept that the error happened, but respond with a "
                    "whimsical, upbeat take on it to make everyone feel better."
                ),
            )[:800]
        except Exception:
            lines = [line for line in str(error_msg).splitlines() if line.strip()]
            snippet = "\n".join(lines[-4:])[:800]
    elif error_msg:
        lines = [line for line in str(error_msg).splitlines() if line.strip()]
        snippet = "\n".join(lines[-4:])[:800]
    else:
        snippet = ""

    if snippet:
        att_blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Error by ai*\n```{snippet}```"},
            }
        )

    # ------------------------------------------------------------ payload table
    table = _payload_table(metric, payload)
    if table:
        att_blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Payload*\n```{table}```"},
            }
        )

    att_blocks.append({"type": "divider"})

    # ---------------------------------------------- CloudWatch logs link button
    # A link button (has `url`) works over incoming webhooks with no
    # interactivity backend. Deep-link to the metric's log group when the
    # producer recorded one (log_scanner does), else the Logs console home.
    log_group = payload.get("log_group") or (payload.get("raw") or {}).get("log_group")
    button: dict[str, Any] = {
        "type": "button",
        "text": {"type": "plain_text", "text": "📄 View CloudWatch logs", "emoji": True},
        "url": _cloudwatch_url(log_group),
    }
    if severity in ("critical", "error"):
        button["style"] = "danger"
    att_blocks.append({"type": "actions", "elements": [button]})
#call summary
    
    # ------------------------------------------------------------ context footer
    meta_parts: list[str] = []
    if metric.get("id"):
        meta_parts.append(f"ID `{metric['id']}`")
    meta_parts.append(f"sent {when}")
    if env:
        meta_parts.append(f"env `{_esc(env)}`")
    att_blocks.append(
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": " · ".join(meta_parts)}],
        }
    )

    return {"attachments": [{"color": _COLORS[severity], "blocks": att_blocks}]}


def post(webhook_url: str, body: dict[str, Any], timeout_seconds: float = 5.0) -> SlackResult:
    """POST `body` to `webhook_url`. Return SlackResult.

    Uses urllib (stdlib) instead of `requests` to keep the Lambda image
    smaller and avoid a third-party dependency for one HTTP call.
    """
    encoded = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=encoded,
        headers={"content-type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            status = resp.status
            text = resp.read().decode("utf-8", errors="replace")
        if 200 <= status < 300:
            return SlackResult(delivered=True, channel="", error_message=None)
        return SlackResult(
            delivered=False,
            channel="",
            error_message=f"slack returned {status}: {text[:500]}",
        )
    except urllib.error.HTTPError as exc:
        body_snippet = exc.read()[:500].decode("utf-8", errors="replace")
        return SlackResult(
            delivered=False,
            channel="",
            error_message=f"slack HTTP {exc.code}: {body_snippet}",
        )
    except urllib.error.URLError as exc:
        return SlackResult(
            delivered=False,
            channel="",
            error_message=f"slack URL error: {exc.reason}",
        )
    except Exception as exc:  # noqa: BLE001
        return SlackResult(
            delivered=False, channel="", error_message=f"slack post failed: {exc}"
        )

