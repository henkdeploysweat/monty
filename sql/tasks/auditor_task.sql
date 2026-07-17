-- Monty Auditor: hourly Snowflake Task. Calls the stored proc.
-- Edit SCHEDULE / WAREHOUSE / SUSPEND-on-failure here, then ALTER TASK ... RESUME.

USE DATABASE MONITORING_DB;
USE SCHEMA MONITORING;

CREATE OR REPLACE TASK AUDITOR_TASK
    WAREHOUSE = MONTY_WH
    SCHEDULE = 'USING CRON 0 * * * * UTC'
    -- Don't keep retrying a broken proc — the heartbeat metric will alert us.
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    COMMENT = 'Hourly: runs every enabled rule in AUDIT_REGISTRY and writes to CUSTOM_METRICS'
AS
    CALL RUN_AUDITOR();

-- Tasks are created suspended. Resume after first deploy:
ALTER TASK AUDITOR_TASK RESUME;

-- Manual run (for smoke tests):
--   EXECUTE TASK AUDITOR_TASK;
-- Or call the proc directly:
--   CALL RUN_AUDITOR();

GRANT OPERATE ON TASK AUDITOR_TASK TO ROLE MONTY_SVC_ROLE;
