# Audit — dbt Path D bypasses the temporary severity gate

| | |
|---|---|
| **Date** | 2026-07-05 |
| **Scope** | dbt "Path D" (`meta` block) writes vs. the temporary `PERSISTED_SEVERITIES` gate in `metric_writer.py` |
| **Type** | Findings only — no remediation proposed |
| **Verdict** | **Confirmed leak.** The 2026-07-05 filter does **not** apply to dbt. dbt `info`/`warning` rows persist to `CUSTOM_METRICS` despite the gate, and the docs claiming otherwise are inaccurate. |

---

## 1. Summary

The temporary severity filter added on 2026-07-05 is a check inside the Python
metric writer (`lambdas/shared/metric_writer.py`). It is reachable **only** by
the three Lambda ingestion paths. The dbt Path D `meta`-block macro runs a raw
`INSERT INTO ... CUSTOM_METRICS` over dbt's own Snowflake connection and never
calls the writer, so it is structurally incapable of hitting the gate.

**Evidence — a live leaked row:**

```
PIPELINE_NAME = dim_braze_campaign_tags
METRIC_NAME   = dbt_model_run
SEVERITY      = info          <-- should have been dropped by the 2026-07-05 gate
IS_ALERT      = false
ENVIRONMENT   = dev
OCCURRED_AT   = Sat, 04 Jul 2026 15:16:38 GMT
PAYLOAD       = { "resource_type": "seed", "status": "success",
                  "rows_affected": 1556, "unique_id":
                  "seed.sweat_analytics_coredbt.dim_braze_campaign_tags", ... }
```

The `METRIC_NAME=dbt_model_run`, `IS_ALERT=false`, and the payload shape all
match the `monty_post_hook` macro (§4), i.e. this is a Path D direct insert, not
a Lambda write. It persisted even though its severity is `info`.

---

## 2. How the gate works, and where it lives

`lambdas/shared/metric_writer.py`:

- **Accepted severities** (`:18`):
  ```python
  ALLOWED_SEVERITIES = ("critical", "error", "warning", "info")
  ```
- **The temporary persistence gate** (`:24`):
  ```python
  PERSISTED_SEVERITIES = ("critical", "error")
  ```
- **The drop, inside `write()`** (`:96-104`):
  ```python
  if metric.severity not in PERSISTED_SEVERITIES:
      logger.info(
          "metric dropped (severity not persisted) pipeline=%s metric=%s severity=%s environment=%s",
          metric.pipeline_name, metric.metric_name, metric.severity, metric.environment,
      )
      return 0
  ```

The single INSERT that all gated writers share is at `metric_writer.py:27-32`.
**Critical point:** the gate is one `if` inside `write()`. Nothing that does not
call `metric_writer.write()` can be affected by it.

---

## 3. Which writers actually hit the gate

Only the three Lambda ingestion paths route through `metric_writer.write()`:

| Path | Entry point | Reaches the gate? |
|------|-------------|-------------------|
| failure_proxy (HTTP POST) | `lambdas/failure_proxy/handler.py:39-40` (`Metric.from_dict` → `metric_writer.write`) | Yes |
| log_scanner (CloudWatch Logs) | `lambdas/log_scanner/handler.py:46` | Yes |
| sns_subscriber (SNS) | `lambdas/sns_subscriber/handler.py:35` | Yes |
| **dbt Path D (`meta` block)** | **direct SQL — see §4** | **No** |

---

## 4. Why dbt Path D bypasses the gate — core finding

Path D is documented in `ADD_METRIC.md` §7. It "writes **directly** to
`CUSTOM_METRICS` over the existing Snowflake connection (no HTTP, no HMAC)"
(`ADD_METRIC.md:453`). The `monty_post_hook` macro emits a raw INSERT
(`ADD_METRIC.md:488-502`):

```jinja
insert into {{ var('monty_database') }}.{{ var('monty_schema') }}.CUSTOM_METRICS
  (PIPELINE_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY, RUN_ID, PAYLOAD, IS_ALERT, ENVIRONMENT)
select
  '{{ this.name }}', '{{ metric_name }}', ({{ metric_sql }})::float,
  '{{ severity }}', '{{ invocation_id }}',
  to_variant(object_construct(...)),
  {{ 'true' if is_alert else 'false' }}, '{{ environment }}'
```

The severity for that INSERT **defaults to `info`** (`ADD_METRIC.md:476`):

```jinja
{%- set severity = (meta.get('monty_severity', 'info')) | lower -%}
```

So any dbt model instrumented via Path D with no explicit `monty_severity`
emits an `info` row straight into the table — exactly the class of row the
2026-07-05 gate is meant to suppress, inserted on a code path the gate cannot
see.

**The macro is not in this repo.** `monty_post_hook.sql` / `monty_failure_hook.sql`
exist only as documentation snippets (`ADD_METRIC.md` §7, `README.md:378`,
`docs/getting-started.md`); consumers copy them into their own dbt projects'
`macros/` directory. There is therefore no in-repo artifact that the Monty
severity gate governs — Path D is enforced (or not) entirely in consumer repos.

**Not every dbt row is affected.** The companion `monty_failure_hook` macro
hard-codes `severity='error'` / `is_alert=true` (`ADD_METRIC.md:518`), which the
gate would persist anyway. dbt run-failure alerts are unaffected by the filter's
intent; the leak is specifically the non-`error` `monty_post_hook` metrics.

---

## 5. Downstream impact

The observer is the only consumer that pushes to Slack. Its poll query has no
severity predicate (`lambdas/observer/handler.py:32-39`):

```sql
SELECT ID, PIPELINE_NAME, ENVIRONMENT, METRIC_NAME, METRIC_VALUE, SEVERITY, RUN_ID, PAYLOAD
FROM MONITORING_DB.MONITORING.CUSTOM_METRICS
WHERE IS_ALERT = TRUE AND SENT_TO_SLACK = FALSE
ORDER BY OCCURRED_AT
LIMIT %s
```

Two distinct outcomes for leaked dbt rows:

- **Default Path D rows (`is_alert=false`, `ADD_METRIC.md:143`)** — never
  selected (`WHERE IS_ALERT = TRUE` excludes them). They are **never polled and
  never Slacked**, but they **do persist**. Net effect: silent row-count /
  storage growth of rows the docs promise are absent.
- **Path D rows with `monty_is_alert: true` + severity `warning`/`info`** — these
  **are** selected and **are** delivered to Slack (`#data-alerts` for prod,
  `#data-alerts-dev` for a non-`prod` dbt target, per `lambdas/observer/slack.py`
  routing). This is a direct behavioural breach of the filter's intent: the
  exact severities the gate exists to suppress reach Slack via dbt.

Storage note: `CUSTOM_METRICS` is `CLUSTER BY (IS_ALERT, SENT_TO_SLACK,
OCCURRED_AT)` (`sql/ddl/002_custom_metrics.sql:29`), so the `is_alert=false`
leaked rows cluster away from the observer's hot `IS_ALERT=true` partitions —
negligible added scan cost, but real and unbounded retention volume.

---

## 6. Documentation discrepancy

Four locations assert the filter applies universally. All are inaccurate for the
dbt path:

- `README.md:106-112` — "only `critical` and `error` rows are written … they
  never reach `CUSTOM_METRICS`."
- `README.md:644-647` — "the `info` row above will **not** be persisted … dropped
  before the INSERT."
- `ADD_METRIC.md:47-52` — "only `critical` and `error` are persisted … drops
  `warning` and `info` rows before the INSERT."
- `ADD_METRIC.md:590` (Path D `monty_severity` note) — a model defaulting to
  `info` "**will not land a row**." (It does — via the macro's own INSERT.)
- `metric_writer.py:20-24` (code comment) — describes the drop as if it covers
  all producers.

None of these acknowledge that Path D bypasses `metric_writer` entirely.

---

## 7. Test coverage gap

The gate has **no test coverage**. `metric_writer.write()`'s real body is never
executed under test — every Lambda test stubs it out:

- `tests/test_failure_proxy.py:152, 226` — `monkeypatch.setattr(handler.metric_writer, "write", ...)`
- `tests/test_log_scanner.py:156, 194` — same pattern.

There is no `tests/test_metric_writer.py`, no assertion that a `warning`/`info`
row is dropped and returns `0`, and nothing exercises the dbt direct-insert
path. The leak was invisible to the test suite.

---

## 8. Related, out of scope

The Snowflake auditor stored procedure (`sql/procedures/auditor_sp.sql`) is a
**second** direct-INSERT writer that also bypasses `metric_writer` and can write
`info` rows. It is outside this dbt-focused audit and is flagged here only so it
is not overlooked in a follow-up review.

---

## Appendix — evidence row, annotated

| Column | Value | Note |
|--------|-------|------|
| `PIPELINE_NAME` | `dim_braze_campaign_tags` | `{{ this.name }}` in `monty_post_hook` |
| `METRIC_NAME` | `dbt_model_run` | Path D metric name |
| `METRIC_VALUE` | `2561` | execution time ms |
| `SEVERITY` | `info` | **would be dropped by the gate; wasn't** |
| `RUN_ID` | `448b3ade-…` | dbt `invocation_id` |
| `PAYLOAD.resource_type` | `seed` | confirms dbt origin |
| `PAYLOAD.status` | `success` | non-failure metric |
| `IS_ALERT` | `false` | Path D default → never Slacked, but persists |
| `SENT_TO_SLACK` | `false` | consistent with `is_alert=false` |
| `ENVIRONMENT` | `dev` | dbt `target.name` |
| `OCCURRED_AT` | Sat, 04 Jul 2026 15:16:38 GMT | — |
