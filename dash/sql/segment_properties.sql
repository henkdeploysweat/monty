-- segment_properties.sql
--
-- Full property catalog for a single event, across every source that emits it.
-- This is the drill-down behind a search result: the live-generated data
-- dictionary for one event (query #2 in seg.md, scoped to one table_name).
--
-- Params: %(event)s -> exact event (table) name, e.g. 'WORKOUT_COMPLETED'
-- SOURCE: SNOWFLAKE.ACCOUNT_USAGE (grant-independent) so the drill-down works
-- for ungranted schemas too — see segment_catalog.sql for the rationale.
-- {{db}} is substituted as the database name literal (see db.SEGMENT_DB).

SELECT
    table_schema    AS source,
    table_name      AS event_name,
    column_name     AS property_name,
    data_type,
    ordinal_position
FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
WHERE table_catalog = '{{db}}'
  AND deleted IS NULL
  AND table_name = %(event)s
  AND table_schema NOT IN (
    'STRIPE','ZENDESK','__SEGMENT_REVERSE_ETL','INFORMATION_SCHEMA','PUBLIC'
  )
ORDER BY source, ordinal_position;
