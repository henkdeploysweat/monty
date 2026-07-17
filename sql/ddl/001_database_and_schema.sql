-- Monty: monitoring database, schema, role and grants.
-- Run as ACCOUNTADMIN (or a role that can CREATE DATABASE / ROLE).

CREATE DATABASE IF NOT EXISTS MONITORING_DB
    COMMENT = 'Monty: central sink for data-platform metrics and alerts';

CREATE SCHEMA IF NOT EXISTS MONITORING_DB.MONITORING
    COMMENT = 'Monty tables, procedures and tasks';

-- Service role used by the Observer / Failure-Proxy / SNS / Log-Scanner Lambdas
-- and by the Auditor stored procedure. Pipelines that need to INSERT metrics
-- (dbt, ingest Lambdas) get a narrower grant on CUSTOM_METRICS only.
CREATE ROLE IF NOT EXISTS MONTY_SVC_ROLE
    COMMENT = 'Service role used by the Monty Lambdas and the Auditor task';

CREATE ROLE IF NOT EXISTS MONTY_WRITER_ROLE
    COMMENT = 'Writer role for upstream pipelines (dbt, ingest Lambdas) inserting into CUSTOM_METRICS';

-- Warehouse used by the Auditor task and ad-hoc Monty queries.
-- XS keeps cost down; auto-suspend after 60s.
CREATE WAREHOUSE IF NOT EXISTS MONTY_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for the Monty Auditor task and Observer reads';

GRANT USAGE ON WAREHOUSE MONTY_WH TO ROLE MONTY_SVC_ROLE;
GRANT USAGE ON WAREHOUSE MONTY_WH TO ROLE MONTY_WRITER_ROLE;

GRANT USAGE ON DATABASE MONITORING_DB TO ROLE MONTY_SVC_ROLE;
GRANT USAGE ON DATABASE MONITORING_DB TO ROLE MONTY_WRITER_ROLE;

GRANT USAGE ON SCHEMA MONITORING_DB.MONITORING TO ROLE MONTY_SVC_ROLE;
GRANT USAGE ON SCHEMA MONITORING_DB.MONITORING TO ROLE MONTY_WRITER_ROLE;

-- Per-table grants follow in 002/003/004 once the tables exist.
