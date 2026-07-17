-- Monty Auditor: walks AUDIT_REGISTRY, runs each rule's SQL_CHECK, compares
-- to the threshold, and writes the result (with is_alert) to CUSTOM_METRICS.
--
-- Implemented as a Snowflake Python stored procedure so it owns its own
-- exception handling per-rule (one bad rule shouldn't kill the run) and so we
-- can emit a heartbeat metric at the end.
--
-- Run by:
--   sql/tasks/auditor_task.sql (hourly), or manually via:
--     CALL MONITORING_DB.MONITORING.RUN_AUDITOR();

USE DATABASE MONITORING_DB;
USE SCHEMA MONITORING;

CREATE OR REPLACE PROCEDURE RUN_AUDITOR()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS $$
def main(session):
    """Iterate AUDIT_REGISTRY, evaluate each rule, write to CUSTOM_METRICS."""

    # Pull all enabled rules. Cast to list so we hold no cursor across child
    # queries.
    rules = session.sql("""
        SELECT RULE_ID, PIPELINE_NAME, METRIC_NAME, SQL_CHECK,
               COMPARATOR, THRESHOLD_VALUE, SEVERITY
        FROM AUDIT_REGISTRY
        WHERE ENABLED = TRUE
    """).collect()

    rules_run = 0
    rules_failed = 0  # rules whose SQL_CHECK errored or returned non-scalar
    rules_alerted = 0  # rules whose value tripped the comparator

    for rule in rules:
        rule_id = rule['RULE_ID']
        pipeline = rule['PIPELINE_NAME']
        metric = rule['METRIC_NAME']
        sql_check = rule['SQL_CHECK']
        comparator = rule['COMPARATOR']
        threshold = float(rule['THRESHOLD_VALUE'])
        severity = rule['SEVERITY']

        try:
            rows = session.sql(sql_check).collect()
            if len(rows) != 1 or len(rows[0]) != 1:
                # Treat non-scalar as a rule-config bug. Record it as a failure
                # metric so Slack will surface the broken rule itself.
                _record_auditor_failure(
                    session, rule_id, pipeline, metric,
                    f"SQL_CHECK must return one row with one column; "
                    f"got {len(rows)} rows / "
                    f"{len(rows[0]) if rows else 0} cols"
                )
                rules_failed += 1
                continue

            value = rows[0][0]
            if value is None:
                _record_auditor_failure(
                    session, rule_id, pipeline, metric,
                    "SQL_CHECK returned NULL"
                )
                rules_failed += 1
                continue

            value_f = float(value)
            is_alert = _evaluate(value_f, comparator, threshold)

            session.sql("""
                INSERT INTO CUSTOM_METRICS
                  (PIPELINE_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY,
                   RUN_ID, PAYLOAD, IS_ALERT)
                SELECT ?, ?, ?, ?, ?, PARSE_JSON(?), ?
            """, params=[
                pipeline, metric, value_f,
                severity if is_alert else 'info',
                f'auditor-rule-{rule_id}',
                _payload_json(rule_id, comparator, threshold, value_f),
                is_alert,
            ]).collect()

            if is_alert:
                rules_alerted += 1

        except Exception as exc:  # noqa: BLE001 - we want to swallow per-rule
            _record_auditor_failure(session, rule_id, pipeline, metric, str(exc))
            rules_failed += 1

        rules_run += 1

    # Heartbeat. Missing heartbeat is itself an alert (set up via AUDIT_REGISTRY
    # rule on CUSTOM_METRICS where metric_name='auditor_heartbeat').
    session.sql("""
        INSERT INTO CUSTOM_METRICS
          (PIPELINE_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY, IS_ALERT)
        VALUES ('monty.auditor', 'auditor_heartbeat', ?, 'info', FALSE)
    """, params=[float(rules_run)]).collect()

    session.sql("""
        INSERT INTO CUSTOM_METRICS
          (PIPELINE_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY, IS_ALERT)
        VALUES ('monty.auditor', 'auditor_failures', ?, ?, ?)
    """, params=[
        float(rules_failed),
        'error' if rules_failed > 0 else 'info',
        rules_failed > 0,
    ]).collect()

    return (
        f"auditor: ran={rules_run} alerted={rules_alerted} failed={rules_failed}"
    )


def _evaluate(value, comparator, threshold):
    """Apply COMPARATOR to (value, threshold). Returns bool."""
    if comparator == '>':
        return value > threshold
    if comparator == '<':
        return value < threshold
    if comparator == '>=':
        return value >= threshold
    if comparator == '<=':
        return value <= threshold
    if comparator == '==':
        return value == threshold
    if comparator == '!=':
        return value != threshold
    raise ValueError(f"Unknown comparator: {comparator!r}")


def _payload_json(rule_id, comparator, threshold, value):
    """Build a small JSON blob for CUSTOM_METRICS.PAYLOAD."""
    import json
    return json.dumps({
        'rule_id': rule_id,
        'comparator': comparator,
        'threshold': threshold,
        'observed_value': value,
    })


def _record_auditor_failure(session, rule_id, pipeline, metric, message):
    """Insert a 'auditor_failure' alert row when a rule itself is broken."""
    import json
    session.sql("""
        INSERT INTO CUSTOM_METRICS
          (PIPELINE_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY, RUN_ID,
           PAYLOAD, IS_ALERT)
        SELECT ?, ?, NULL, 'error', ?, PARSE_JSON(?), TRUE
    """, params=[
        pipeline,
        f'auditor_failure.{metric}',
        f'auditor-rule-{rule_id}',
        json.dumps({'rule_id': rule_id, 'error': message[:4000]}),
    ]).collect()
$$;

GRANT USAGE ON PROCEDURE RUN_AUDITOR() TO ROLE MONTY_SVC_ROLE;
