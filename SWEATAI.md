# SweatAI

An HTTP endpoint that turns a failure log (or any prompt) into an AI analysis.
You POST a prompt, it asks Claude, you get the reply back — and every call is
logged to DynamoDB with full token accounting.

It lives inside Monty: same CDK stack, same Docker image, same HMAC auth as the
`/failure` proxy. One new Lambda, one new API route, one new table.

---

## 1. Call it from code (the only thing most people need)

Copy [`clients/sweatai.py`](clients/sweatai.py) into your repo and call the
function. HMAC signing is done for you.

```python
from sweatai import sweatai

answer = sweatai("my dbt run failed with a Snowflake SQL compilation error")
print(answer)                                   # Claude's analysis, as a string

# with a service label (shows up in the logs) and a custom persona:
answer = sweatai(prompt, service="dbt-ci")
answer = sweatai(prompt, "dbt-ci", system_prompt="You are a Snowflake DBA...")
```

**One-time setup** — set two env vars so nobody hardcodes secrets:

```bash
export SWEATAI_URL="https://<api-id>.execute-api.us-east-1.amazonaws.com/sweatai"
export MONTY_HMAC_SECRET="<value from monty-<env>-secrets>"
```

`SWEATAI_URL` is the `SweatAiUrl` output printed at the end of `make cdk-deploy`.
`MONTY_HMAC_SECRET` is the same shared secret every Monty caller uses.

That's it. Everything below is reference.

---

## 2. The API contract

`POST /sweatai` on the Monty HTTP API. JSON in, JSON out. HMAC-signed.

### Request

Header:

| Header | Value |
|---|---|
| `content-type` | `application/json` |
| `X-Monty-Signature` | `HMAC-SHA256(MONTY_HMAC_SECRET, raw_request_body)` as hex |

Body:

| Field | Required | Default | Meaning |
|---|---|---|---|
| `prompt` | **yes** | — | The question / failure log to analyse |
| `system_prompt` | no | built-in DataOps persona | Overrides the system prompt |
| `service` | no | `"unknown"` | A label for who's calling (logged, not sent to Claude) |

```json
{ "prompt": "dbt model braze_cdi_delete_sync failed: TIMESTAMP_TZ vs TIMESTAMP_NTZ", "service": "dbt-ci" }
```

> **Sign the exact bytes you send.** The signature is over the raw body string.
> If you serialize the JSON differently than what you signed, you get a 401.

### Response — `200 OK`

```json
{
  "id": "d1f...uuid",
  "reply": "🚨 Error Summary: ...",
  "model": "claude-haiku-4-5",
  "service": "dbt-ci",
  "usage": {
    "prompt_tokens": 210,
    "system_prompt_tokens": 512,
    "completion_tokens": 480
  },
  "duration_sec": 3.912
}
```

`reply` is the answer. `id` matches the row written to DynamoDB.

### Errors

| Status | Meaning | Fix |
|---|---|---|
| `400` | Missing `prompt`, or body isn't valid JSON | Send `{"prompt": "..."}` |
| `401` | Bad or missing `X-Monty-Signature` | Sign with the correct `MONTY_HMAC_SECRET`, over the exact body |
| `503` | `ANTHROPIC_API_KEY` not in the secret | Add the key, then cold-start the Lambda (see §6) |
| `5xx` | Anthropic call failed after retries | Transient — retry; the caller's own retry logic should handle it |

---

## 3. What the Lambda does

`lambdas/sweatai/handler.py`:

1. **Verify HMAC** over the raw body (same scheme as `failure_proxy`). Bad → 401.
2. **Parse** JSON, pull `prompt` / `system_prompt` / `service`. No prompt → 400.
3. **Load `ANTHROPIC_API_KEY`** from `monty-<env>-secrets`. Missing → 503.
4. **Call Claude** (`claude-haiku-4-5`, `max_tokens=2048`, `temperature=0.2`) via
   stdlib `urllib` — no `anthropic` SDK (the image ships only stdlib + a few
   packages; see Monty gotcha #6). Retries on HTTP 429 with backoff.
5. **Break down tokens** (see §4).
6. **Log the row** to DynamoDB (see §5) — best-effort, never fails the request.
7. **Return** the reply + usage.

Runtime facts: function `monty-<env>-sweatai`, 30 s timeout, 512 MB, on the
shared Monty Lambda role. Model config lives at the top of the handler
(`MODEL`, `MAX_TOKENS`, `TEMPERATURE`, `DEFAULT_SYSTEM_PROMPT`).

> **Timeout budget:** API Gateway HTTP APIs cap a request at 30 s, and the
> Lambda is also 30 s. `max_tokens=2048` on Haiku finishes comfortably. If you
> raise `max_tokens` or switch to a slower model, a long analysis could hit the
> 30 s wall — raise the Lambda timeout *and* request an API Gateway increase.

---

## 4. Token accounting

The Messages API only reports a **combined** input-token count, so the handler
splits it itself:

- `completion_tokens` = `usage.output_tokens` from the main call
- `prompt_tokens` = a separate `count_tokens` call on the **user prompt alone**
- `system_prompt_tokens` = `total input_tokens − prompt_tokens`

This is **best-effort**: if the `count_tokens` call fails, `prompt_tokens` and
`system_prompt_tokens` are simply omitted (from both the response and the log)
rather than failing the request. `completion_tokens` always comes back.

---

## 5. What gets logged (DynamoDB)

Every call writes one row to `monty-<env>-sweatai-prompt-logs`
(on-demand billing, `RETAIN` on stack teardown).

| Column | Type | Notes |
|---|---|---|
| `id` | string | **partition key** — the per-request UUID (also returned as `id`) |
| `created_at` | string | **sort key** — ISO-8601 UTC, when the row was written |
| `timestamp` | string | ISO-8601 UTC, when the request was received |
| `model` | string | `claude-haiku-4-5` |
| `service` | string | the caller's `service` label |
| `system_prompt` | string | the system prompt used |
| `user_prompt` | string | the caller's `prompt` |
| `response` | string | Claude's reply |
| `prompt_tokens` | number | user-prompt tokens (omitted if `count_tokens` failed) |
| `system_prompt_tokens` | number | system-prompt tokens (omitted if `count_tokens` failed) |
| `completion_tokens` | number | response tokens |
| `duration_sec` | number | wall-clock of the Claude call |

The write is best-effort — a DynamoDB failure is logged but never fails the
caller's request. There is no read/query path yet; query the table directly
(console, `aws dynamodb`, or a future dashboard).

---

## 6. Configuration & secrets

`monty-<env>-secrets` must contain:

| Key | Needed for | Missing → |
|---|---|---|
| `MONTY_HMAC_SECRET` | request auth | every call is 401 |
| `ANTHROPIC_API_KEY` | the Claude call | every call is 503 |

> **Cold-start gotcha (Monty #1):** the secret loader is `@lru_cache`d for the
> Lambda's warm lifetime. After you add or change a key in the secret, **force a
> cold start** of `monty-<env>-sweatai` (e.g. bump an env var / redeploy),
> otherwise it keeps serving the old cached value. A brand-new deploy cold-starts
> on first invoke, so this only bites you when you edit the secret *after* deploy.

Environments: dev account `116981766237`, prod `534977985440`, both `us-east-1`.

---

## 7. Deploy

Defined in `infra/monty_stack.py` alongside the rest of Monty. A deploy creates
the Lambda, the `/sweatai` route, and the DynamoDB table, and adds
`dynamodb:PutItem` to the shared Lambda role.

```bash
make cdk-diff  ENV=dev      # review the changeset
make cdk-deploy ENV=dev     # build image + push + deploy
```

The deploy prints the `SweatAiUrl` output — that's your `SWEATAI_URL`.

---

## 8. Testing without writing a caller

`tests/preview_sweatai.py` signs and sends for you.

```bash
# LOCAL — in-process, no AWS. Proves signing + routing (returns 503 offline,
# since there's no ANTHROPIC_API_KEY locally):
python3 tests/preview_sweatai.py --prompt "test wiring"
python3 tests/preview_sweatai.py --bad-signature      # prove the 401 path

# REMOTE — real POST to the deployed endpoint:
export MONTY_HMAC_SECRET="<dev secret>"
python3 tests/preview_sweatai.py \
  --url "$SWEATAI_URL" \
  --prompt "dbt model X failed with a Snowflake error" \
  --service dbt-ci
```

If remote returns 401, the secret you exported doesn't match the deployed one —
that's the #1 cause. Compare fingerprints without printing the secret:
`python3 scratchpad/monty_hmac_check.py`.

---

## Files

```
clients/sweatai.py          copy-paste client — `from sweatai import sweatai`
lambdas/sweatai/handler.py  the Lambda (HMAC → Claude → DynamoDB → reply)
infra/monty_stack.py        Lambda + /sweatai route + prompt-logs table
tests/preview_sweatai.py    local + remote tester (signs for you)
```
