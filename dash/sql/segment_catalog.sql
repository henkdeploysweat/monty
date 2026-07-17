-- segment_catalog.sql
--
-- The core "who sends what" matrix for the Segment events warehouse.
-- One row per custom event: which source schemas emit it, how many sources,
-- and how many distinct properties it carries.
--
-- SOURCE: SNOWFLAKE.ACCOUNT_USAGE, not {{db}}.INFORMATION_SCHEMA.
-- INFORMATION_SCHEMA is GRANT-FILTERED — it only shows tables the running role
-- has privileges on, so ungranted source schemas (e.g. the mobile-app sources
-- IOS_APP_PROD_SWIFT / ANDROID_APP_PRODUCTION / PROFILE_PROD) silently vanish
-- from the catalog. ACCOUNT_USAGE lists every object in the account regardless
-- of grants; the trade-off is a lag of up to ~3h, which is irrelevant for a
-- schema catalog. `deleted IS NULL` drops tables that have been removed.
--
-- {{db}} is substituted as the database name literal (see db.SEGMENT_DB).

WITH base_events AS (
    -- real event tables (not views), minus Segment's system tables
    SELECT DISTINCT table_name
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE table_catalog = '{{db}}'
      AND table_type = 'BASE TABLE'
      AND deleted IS NULL
      AND table_name NOT IN (
        'TRACKS','PAGES','SCREENS','IDENTIFIES','USERS','GROUPS',
        'USER_TRAITS','USER_IDENTIFIERS','PROFILE_MERGES',
        'PROFILE_TRAITS_UPDATES','ID_GRAPH_UPDATES',
        'EXTERNAL_ID_MAPPING_UPDATES','AUDIENCE_ENTERED','AUDIENCE_EXITED'
      )
      AND table_schema NOT IN (
        'STRIPE','ZENDESK','__SEGMENT_REVERSE_ETL','INFORMATION_SCHEMA','PUBLIC'
      )
)
SELECT
    c.table_name                                                       AS event_name,
    ARRAY_AGG(DISTINCT c.table_schema) WITHIN GROUP (ORDER BY c.table_schema) AS sources,
    COUNT(DISTINCT c.table_schema)                                     AS source_count,
    COUNT(DISTINCT c.column_name)                                      AS property_count
FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
JOIN base_events b
  ON c.table_name = b.table_name
WHERE c.table_catalog = '{{db}}'
  AND c.deleted IS NULL
  AND c.table_schema NOT IN (
    'STRIPE','ZENDESK','__SEGMENT_REVERSE_ETL','INFORMATION_SCHEMA','PUBLIC'
  )
GROUP BY c.table_name
ORDER BY event_name;
