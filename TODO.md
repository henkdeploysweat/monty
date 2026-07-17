# Monty Monitoring Platform - TODO

## Current Status

Last updated: 2026-07-17

### Completed Tasks

✅ Swapped low-priority metrics storage: S3 Parquet → DynamoDB (2026-07-17)
- `warning`/`info` → one item in `monty-<env>-metrics-ddb` via new
  `lambdas/shared/dynamo_writer.py` (`pk=env#pipeline`, `sk=isoUTC#uuid`,
  on-demand billing, 90-day TTL); `critical`/`error` → Snowflake, unchanged
- Dropped `pyarrow` from the Lambda image (now just
  `snowflake-connector-python` + `boto3`); retired `s3_writer.py`,
  `compact_s3.py`, `tests/test_s3_writer.py`
- Dashboard reads DynamoDB: `MONTY_SOURCE=dynamo`, and `both` unions
  dynamo + sqlite + live-S3 legs (no gap across the cutover)
- S3 bucket + write grant + `MONTY_METRICS_BUCKET` env var RETAINED until the
  DynamoDB path is verified in both envs — then decommission
- NOT yet deployed — ships on the next `make cdk-deploy ENV=dev|prod`

✅ Replaced OpenAI API with Claude API in `lambdas/observer/slack.py:87-147`
- Changed API endpoint: `https://api.openai.com/v1/chat/completions` → `https://api.anthropic.com/v1/messages`
- Updated model: `gpt-4o-mini` → `claude-sonnet-4-5` → `claude-haiku-4-5` (2026-07-15)
- Changed response parsing: `result["choices"][0]["message"]["content"]` → `result["content"][0]["text"]`
- Updated headers: `Authorization: Bearer` → `x-api-key`, added `anthropic-version: 2023-06-01`
- Improved retry logic for rate limiting (429 errors)
- Uses stdlib `urllib` — **no `anthropic` package in the Lambda image**

✅ Integrated Claude error summarization in `format_message()`
- Error messages passed to `ai_summary()` with the Claude API
- Falls back to raw last-4-lines snippet if API key missing or the call fails
- `*Error by ai*` block now rendered **above** the `*Payload*` table (root cause first)

✅ Updated `handler.py` to pass `api_key` parameter
- Secret lookup: `ANTHROPIC_API_KEY` from `monty-<env>-secrets`

✅ dbt-run failure formatting (`lambdas/observer/slack.py`)
- Headline names the actual failed model, not the generic `dbt_run_failures`
  collector (`_dbt_failed_models()` reads `payload.failures[].pipeline_name`;
  many → `first +N more`)
- Callout + AI snippet fall back to the first failure's nested `error_message`
  (dbt has no top-level `error_message`)
- 5 new tests in `tests/test_slack_formatter.py`; visual preview in
  `tests/preview_dbt_slack.py` (not pytest-collected)

✅ Trimmed `requirements.txt` back to Lambda-runtime deps only
- Was a full `pip freeze` (263 pkgs) → Docker build failed `ResolutionImpossible`
- Now: `snowflake-connector-python`, `pyarrow`, `boto3` (+ header warning)

### Pending Tasks

- [ ] Populate `ANTHROPIC_API_KEY` in Secrets Manager (`monty-dev-secrets`,
      `monty-prod-secrets`) — same key both envs. Until then the AI snippet
      silently falls back to raw error lines.
- [ ] Deploy (`make cdk-deploy ENV=dev|prod`) — ships the slack.py + trimmed
      requirements. Match the SSO account to `ENV` (dev `116981766237`,
      prod `534977985440`).
- [ ] Force-cold-start all four Lambdas after populating the key (gotcha #1) —
      or populate the key **before** deploying and the deploy's cold-start
      covers it.

### Notes

- Model is `claude-haiku-4-5` — cheapest/fastest tier, ample for a ≤120-char
  summary. It still accepts the `temperature`/`max_tokens` in the request (Haiku
  4.5 keeps sampling params, unlike Opus 4.7+/Sonnet 5).
- When `api_key` is empty/missing, errors fall back to the raw error snippet —
  no API call, no error.
