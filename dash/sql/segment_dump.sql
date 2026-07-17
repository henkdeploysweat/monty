-- segment_dump.sql
--
-- The full flat property list for every custom Segment event, one row per
-- (source, event, property). This is what the "Save" button snapshots to a
-- local CSV so the /segment page can serve instantly without hitting the slow,
-- lagging ACCOUNT_USAGE views on every load. "Resync" re-runs it.
--
-- Same source + exclusions as segment_catalog.sql (ACCOUNT_USAGE, grant-
-- independent, deleted IS NULL). {{db}} -> the database name literal.

SELECT
    c.table_schema    AS source,
    c.table_name      AS event_name,
    c.column_name     AS property_name,
    c.data_type       AS data_type,
    c.ordinal_position AS ordinal_position
FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
JOIN SNOWFLAKE.ACCOUNT_USAGE.TABLES t
  ON c.table_catalog = t.table_catalog
 AND c.table_schema  = t.table_schema
 AND c.table_name    = t.table_name
 AND t.table_type = 'BASE TABLE'
 AND t.deleted IS NULL
WHERE c.table_catalog = '{{db}}'
  AND c.deleted IS NULL
  AND c.table_schema NOT IN (
    'STRIPE','ZENDESK','__SEGMENT_REVERSE_ETL','INFORMATION_SCHEMA','PUBLIC'
  )
  AND c.table_name NOT IN (
    'TRACKS','PAGES','SCREENS','IDENTIFIES','USERS','GROUPS',
    'USER_TRAITS','USER_IDENTIFIERS','PROFILE_MERGES',
    'PROFILE_TRAITS_UPDATES','ID_GRAPH_UPDATES',
    'EXTERNAL_ID_MAPPING_UPDATES','AUDIENCE_ENTERED','AUDIENCE_EXITED'
  )
ORDER BY event_name, source, ordinal_position;
