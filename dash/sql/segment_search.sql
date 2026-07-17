-- segment_search.sql
--
-- Search across every custom Segment event. A hit is either:
--   * the event (table) name matches the term, or
--   * the event carries a property (column) whose name matches the term.
-- For each matching event we return its source schemas, source count, and the
-- specific property names that matched (empty when it was a name-only hit).
--
-- Params: %(q)s  -> already wildcard-wrapped by the caller (percent signs around
--                   the term). NOTE: never write a bare percent sign in this file;
--                   pyformat paramstyle treats it as a placeholder and the query
--                   dies with "not enough arguments for format string".
-- SOURCE: SNOWFLAKE.ACCOUNT_USAGE (grant-independent), so ungranted source
-- schemas are searchable too — see segment_catalog.sql for the rationale.
-- {{db}} is substituted as the database name literal (see db.SEGMENT_DB).

WITH cols AS (
    SELECT
        c.table_name   AS event_name,
        c.table_schema AS source,
        c.column_name  AS property_name
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
    JOIN SNOWFLAKE.ACCOUNT_USAGE.TABLES t
      ON c.table_name = t.table_name
     AND c.table_schema = t.table_schema
     AND c.table_catalog = t.table_catalog
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
),
hit_events AS (
    -- events that match by name OR by having a matching property
    SELECT DISTINCT event_name
    FROM cols
    WHERE event_name ILIKE %(q)s
       OR property_name ILIKE %(q)s
)
SELECT
    c.event_name,
    ARRAY_AGG(DISTINCT c.source) WITHIN GROUP (ORDER BY c.source)      AS sources,
    COUNT(DISTINCT c.source)                                          AS source_count,
    ARRAY_COMPACT(
      ARRAY_AGG(DISTINCT CASE WHEN c.property_name ILIKE %(q)s
                              THEN c.property_name END)
    )                                                                 AS matched_props
FROM cols c
JOIN hit_events h
  ON c.event_name = h.event_name
GROUP BY c.event_name
ORDER BY c.event_name;
