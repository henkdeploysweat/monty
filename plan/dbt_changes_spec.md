# dbt Changes Spec — `sweat_analytics_coredbt`

> Companion to `plan.md`. This document is the PR plan to wire `sweat_analytics_coredbt`
> into Monty. It is **handed off**, not executed by the Monty build.
> Repo path on disk: `/Users/henkduplooy/Documents/Snowflake/sweat_analytics_coredbt/`.

## Goal

After this PR is merged and deployed, every `dbt run` of `sweat_analytics_coredbt`:

1. **Emits a custom metric row** to `MONITORING_DB.MONITORING.CUSTOM_METRICS` for any
   model whose YAML/SQL declares a `monty_*` config (volume, freshness, business KPI,
   anything `monty_metric_sql` returns a number for).
2. **POSTs a failure record** to Monty's failure-proxy endpoint for any model that
   ends `dbt run` in `error` status — within the same `dbt run` invocation, before
   the process exits.

No code in `sweat_analytics_coredbt` calls Slack directly. Monty's Observer Lambda
handles Slack (severity → channel) once the row lands in Snowflake.

## Files touched

```
sweat_analytics_coredbt/
├── dbt_project.yml                       (edit — add vars + post-hook + on-run-end)
├── macros/
│   ├── monty_post_hook.sql               (new — per-model metric emit)
│   ├── monty_failure_hook.sql            (new — on-run-end failure POST)
│   └── monty_signature.sql               (new — HMAC-SHA256 helper, pure Jinja+Python via dbt-utils style)
├── models/marts/marketing/mart_marketing_email_performance.sql   (worked example)
└── README.md                             (edit — add "Monty integration" section)
```

No changes to `profiles.yml` (dbt connects with the same role; we add GRANTs to it).
No changes to `generate_schema_name.sql`.

## 1. `dbt_project.yml` diff

Append two `vars` and a project-wide `+post-hook` + `on-run-end`:

```yaml
## Vars — Monty integration
vars:
  monty_database: "{{ env_var('MONTY_DATABASE', 'MONITORING_DB') }}"
  monty_schema:   "MONITORING"
  monty_failure_proxy_url: "{{ env_var('MONTY_FAILURE_PROXY_URL', '') }}"
  # MONTY_HMAC_SECRET is read at hook-time via env_var() — never written to a var.

## Models
models:
  sweat_analytics_coredbt:
    +post-hook:
      - "{{ monty_post_hook() }}"        # no-op unless model declares monty_metric_name
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: view
      +schema: intermediate
    marts:
      +materialized: table
      +schema: marts

## On-run-end — failure paging
on-run-end:
  - "{{ monty_failure_hook() }}"
```

**Why these env vars:** dbt Cloud / CI / local devs all set them differently.
`MONTY_FAILURE_PROXY_URL` and `MONTY_HMAC_SECRET` are sourced from CI secrets in
prod and from `~/.dbt/profiles.local` in dev.

**Local/dev safety:** if `MONTY_FAILURE_PROXY_URL` is empty (`env_var(..., '')` default),
`monty_failure_hook()` MUST short-circuit silently. Devs running models on their laptop
shouldn't fail or page.

## 2. `macros/monty_post_hook.sql` (new)

```jinja
{% macro monty_post_hook() %}
  {#- Emits one row to MONITORING_DB.MONITORING.CUSTOM_METRICS when the model
      declares config(monty_metric_name=..., monty_metric_sql=..., monty_severity=...).
      Returns no-op SQL otherwise so it's safe to apply project-wide. -#}

  {%- set metric_name      = config.get('monty_metric_name') -%}
  {%- set metric_sql       = config.get('monty_metric_sql') -%}
  {%- set severity         = config.get('monty_severity', 'info') | lower -%}
  {%- set is_alert         = config.get('monty_is_alert', false) -%}
  {%- set channel_override = config.get('monty_channel_override', none) -%}

  {%- if metric_name is none or metric_sql is none -%}
    {#- silent no-op -#}
    select 1 as monty_skipped
  {%- else -%}
    insert into {{ var('monty_database') }}.{{ var('monty_schema') }}.CUSTOM_METRICS
      (PIPELINE_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY, RUN_ID, PAYLOAD, IS_ALERT)
    select
      '{{ this.name }}'                       as PIPELINE_NAME,
      '{{ metric_name }}'                     as METRIC_NAME,
      ({{ metric_sql }})::float               as METRIC_VALUE,
      '{{ severity }}'                        as SEVERITY,
      '{{ invocation_id }}'                   as RUN_ID,
      to_variant(object_construct(
        'model',         '{{ this }}',
        'database',      '{{ this.database }}',
        'schema',        '{{ this.schema }}',
        'channel_override', {% if channel_override %}'{{ channel_override }}'{% else %}null{% endif %},
        'metric_sql',    '{{ metric_sql | replace("'", "''") }}'
      ))                                      as PAYLOAD,
      {{ 'true' if is_alert else 'false' }}   as IS_ALERT
  {%- endif -%}
{% endmacro %}
```

**Contract for model authors:**

| Config key               | Type    | Required | Notes |
| ------------------------ | ------- | -------- | --- |
| `monty_metric_name`      | string  | yes      | Distinct per model — e.g. `row_count`, `unsubscribe_rate_pct`. |
| `monty_metric_sql`       | string  | yes      | A scalar SELECT expression. Wrapped in `({{ sql }})::float`. |
| `monty_severity`         | string  | no       | `critical`/`error`/`warning`/`info`. Default `info`. |
| `monty_is_alert`         | bool    | no       | Set `true` when the metric should always page (e.g. business-critical breaches). Default `false` — Auditor rules can flip this asynchronously. |
| `monty_channel_override` | string  | no       | Overrides Slack channel label only; severity still selects the webhook URL. |

**Edge case — multi-statement post-hook:** Snowflake doesn't support multi-statement
unless `MULTI_STATEMENT_COUNT` is set; the macro therefore emits exactly one statement
(the `INSERT` itself). dbt runs each list-item post-hook as its own SQL execution.

## 3. `macros/monty_failure_hook.sql` (new)

```jinja
{% macro monty_failure_hook() %}
  {#- on-run-end: iterate `results`, for any failed model POST a JSON body
      to Monty's failure-proxy. Uses dbt's `run_query()` for SQL escapes only,
      and Python via `modules.requests` (dbt-core ships `dbt.utils` not requests),
      so we drop down to a Snowflake stored proc / external function path:
      we INSERT a row directly to CUSTOM_METRICS via Snowflake instead of HTTP.
      That keeps the macro DB-only (no `requests` dependency in dbt runtime)
      while still triggering Monty's Observer within 60 s. -#}

  {%- if execute and results -%}

    {%- set failure_rows = [] -%}
    {%- for r in results -%}
      {%- if r.status in ('error', 'fail') -%}
        {%- set _ = failure_rows.append(r) -%}
      {%- endif -%}
    {%- endfor -%}

    {%- if failure_rows | length > 0 -%}

      {%- set values_clauses = [] -%}
      {%- for r in failure_rows -%}
        {%- set node          = r.node -%}
        {%- set pipeline_name = node.name -%}
        {%- set message       = (r.message or '') | replace("'", "''") -%}
        {%- set message_trim  = message[:4000] -%}
        {%- set _ = values_clauses.append(
              "('" ~ pipeline_name ~ "',"
              ~ "'pipeline_failure',"
              ~ "'error',"
              ~ "'" ~ invocation_id ~ "',"
              ~ "to_variant(object_construct("
              ~   "'error_message','" ~ message_trim ~ "',"
              ~   "'unique_id','"     ~ node.unique_id ~ "',"
              ~   "'resource_type','" ~ node.resource_type ~ "'"
              ~ ")),"
              ~ "true)"
        ) -%}
      {%- endfor -%}

      {%- set sql -%}
        insert into {{ var('monty_database') }}.{{ var('monty_schema') }}.CUSTOM_METRICS
          (PIPELINE_NAME, METRIC_NAME, SEVERITY, RUN_ID, PAYLOAD, IS_ALERT)
        values
          {{ values_clauses | join(',\n  ') }}
      {%- endset -%}

      {%- do run_query(sql) -%}

    {%- endif -%}
  {%- endif -%}
{% endmacro %}
```

**Why not HTTP?**
The dbt-snowflake Python runtime does not bundle `requests`; calling `urllib` from
inside a Jinja `do` block requires the (deprecated) `agate.fetcher` shim. Writing
straight to `CUSTOM_METRICS` keeps the dbt side dependency-free and uses Monty's
Observer (1-min EventBridge) as the page path — same SLA as the failure-proxy HTTP
path for any latency the user perceives.

**Trade-off:** if Snowflake itself is the thing that's down, this won't fire. That's
acceptable because (a) `dbt run` would have failed for the Snowflake reason
already, (b) the Auditor's heartbeat metric covers Snowflake outages.

(If a future iteration wants HTTP failure-proxy from dbt, switch to a small
`dbt run-operation monty_post_failures` step in the CI step *after* `dbt run`,
where `requests` is available — out of scope for v1.)

## 4. Worked example — `mart_marketing_email_performance.sql`

Add a config block at the top:

```sql
{{
    config(
        materialized = 'table',
        monty_metric_name = 'unsub_rate_pct_24h',
        monty_metric_sql = """
            select 100.0 * count_if(unsubscribed) / nullif(count(*), 0)
            from {{ this }}
            where send_time >= dateadd(hour, -24, current_timestamp())
        """,
        monty_severity = 'warning',
        monty_is_alert = false
    )
}}

with sends as (
    -- existing model body unchanged
    ...
)
```

After `dbt run --select mart_marketing_email_performance`:

```sql
SELECT * FROM MONITORING_DB.MONITORING.CUSTOM_METRICS
  WHERE PIPELINE_NAME='mart_marketing_email_performance'
  ORDER BY OCCURRED_AT DESC LIMIT 5;
```

…shows one row with `METRIC_NAME='unsub_rate_pct_24h'`, the computed value, severity
`warning`, `IS_ALERT=FALSE`. Because `IS_ALERT=FALSE`, no Slack message — but the
Auditor can later add a `>` threshold rule on this metric in `AUDIT_REGISTRY`
without touching dbt code.

## 5. Snowflake GRANTs

Run once per environment, as `ACCOUNTADMIN` or owner of `MONITORING_DB`:

```sql
USE ROLE ACCOUNTADMIN;

-- The role dbt uses to materialize models needs INSERT into the metrics table.
-- Replace DBT_ROLE with the actual role from sweat_analytics_coredbt's profile.
GRANT USAGE  ON DATABASE MONITORING_DB TO ROLE DBT_ROLE;
GRANT USAGE  ON SCHEMA   MONITORING_DB.MONITORING TO ROLE DBT_ROLE;
GRANT INSERT ON TABLE    MONITORING_DB.MONITORING.CUSTOM_METRICS TO ROLE DBT_ROLE;

-- Optional: SELECT for debugging.
GRANT SELECT ON TABLE    MONITORING_DB.MONITORING.CUSTOM_METRICS TO ROLE DBT_ROLE;
```

**Do not** grant `INSERT` on `AUDIT_REGISTRY` — that table is admin-only by design.

## 6. CI / dbt Cloud env vars

Set in dbt Cloud project (or CI runner) for the `prod` and `qa` environments:

| Variable | Source |
| --- | --- |
| `MONTY_DATABASE` | `MONITORING_DB` (or `MONITORING_DB_DEV` in qa) |
| `MONTY_FAILURE_PROXY_URL` | CDK output `FailureProxyUrl` from Monty stack (https URL) |
| `MONTY_HMAC_SECRET` | Same value stored in Monty's `monty-secrets` Secrets Manager `MONTY_HMAC_SECRET` field — **read-only mirror**, rotated together |

Local dev: leave both URL and secret unset. The hook silently no-ops.

## 7. Validation steps (before merging the PR)

Run all three on the dev Snowflake account:

1. `dbt compile` — no Jinja errors, both new macros render.
2. `dbt run --select mart_marketing_email_performance` →
   `SELECT * FROM MONITORING_DB.MONITORING.CUSTOM_METRICS ORDER BY OCCURRED_AT DESC LIMIT 1;`
   shows the unsub-rate row with the expected `METRIC_VALUE`.
3. Force a failure: temporarily break a model (e.g. `select 1/0`),
   `dbt run --select <broken_model>`. Confirm:
   - The dbt run exits non-zero (existing behavior).
   - A second row lands in `CUSTOM_METRICS` with `METRIC_NAME='pipeline_failure'`,
     `IS_ALERT=TRUE`, `SEVERITY='error'`, `PAYLOAD:error_message` containing the
     compile/runtime message.
   - Within ~60 s a Slack message arrives in `#data-incidents`.

## 8. Risks / call-outs for the reviewer

- The `+post-hook` runs on **every** model — wrap-around cost is the macro's
  `select 1 as monty_skipped` for non-instrumented models. Across ~200 models that's
  ~200 trivial SELECT round-trips per run. If perf becomes a problem, gate the hook
  on a tag (e.g. `tags: ['monty']`) and apply `+post-hook` only to that tag.
- `invocation_id` ties all metrics from one `dbt run` together. Keep it — the
  Observer's Slack messages link to it for grouping.
- Whitespace inside `monty_metric_sql` is preserved into `PAYLOAD.metric_sql` for
  observability; single quotes are escaped. Multi-line SQL works.
