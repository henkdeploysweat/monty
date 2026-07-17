WITH base_events AS (
    -- real event tables (not views), minus Segment's system tables
    SELECT DISTINCT table_name
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE table_type = 'BASE TABLE'
      AND deleted IS NULL 
      AND table_catalog ='SEGMENT_EVENTS'
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
WHERE 
   c.deleted IS NULL 
   AND c.table_catalog ='SEGMENT_EVENTS'
  AND c.table_schema NOT IN (
    'STRIPE','ZENDESK','__SEGMENT_REVERSE_ETL','INFORMATION_SCHEMA','PUBLIC'
  )
GROUP BY c.table_name
ORDER BY event_name;