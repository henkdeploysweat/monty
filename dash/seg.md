1. Event catalog with which sources send each event
This is the core "who sends what" matrix — flags events that only fire from one platform vs. everywhere:
sqlSELECT
  table_or_view_name AS event_name,
  ARRAY_AGG(DISTINCT schema_name) WITHIN GROUP (ORDER BY schema_name) AS sources,
  COUNT(DISTINCT schema_name) AS source_count
FROM segment_events.information_schema.tables
WHERE object_type = 'BASE TABLE'
  -- exclude Segment's standard/system tables, not "custom events"
  AND table_or_view_name NOT IN (
    'TRACKS','PAGES','SCREENS','IDENTIFIES','USERS','GROUPS',
    'USER_TRAITS','USER_IDENTIFIERS','PROFILE_MERGES',
    'PROFILE_TRAITS_UPDATES','ID_GRAPH_UPDATES',
    'EXTERNAL_ID_MAPPING_UPDATES','AUDIENCE_ENTERED','AUDIENCE_EXITED'
  )
  -- exclude non-product schemas: cloud-app sources & reverse ETL internals
  AND schema_name NOT IN ('STRIPE', 'ZENDESK', '__SEGMENT_REVERSE_ETL')
GROUP BY table_or_view_name
ORDER BY event_name;
This alone tells you a lot from your list — e.g. TRAIN_TOGETHER_PAGE_VIEWED only shows up under IOS_APP_PROD_SWIFT, while WORKOUT_COMPLETED fires from ANDROID_APP_PRODUCTION, IOS_APP_PROD_SWIFT, and PROFILE_PROD. Anything with source_count = 1 where you'd expect cross-platform parity (e.g., a core funnel event) is worth flagging to the team.
2. Full property catalog — event × source × property × type
This is your real "tracking plan," generated straight from what's actually landing, rather than what someone documented:
sqlSELECT
  table_schema  AS source,
  table_name    AS event_name,
  column_name   AS property_name,
  data_type,
  ordinal_position
FROM segment_events.information_schema.columns
WHERE table_schema NOT IN ('STRIPE', 'ZENDESK', '__SEGMENT_REVERSE_ETL')
ORDER BY event_name, source, ordinal_position;
Dump this to a sheet and you've essentially got a live-generated data dictionary. It'll also surface property drift — e.g. if CHECKOUT_STARTED has cart_id on web but not on Android.
3. Same event, different properties across sources (schema drift detector)
This flags cases where an event exists in multiple sources but the property sets don't match — usually a sign of inconsistent instrumentation:
sqlWITH event_props AS (
  SELECT table_name AS event_name, table_schema AS source,
         LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_name) AS prop_set
  FROM segment_events.information_schema.columns
  WHERE table_schema NOT IN ('STRIPE', 'ZENDESK', '__SEGMENT_REVERSE_ETL')
  GROUP BY table_name, table_schema
)
SELECT event_name, COUNT(DISTINCT prop_set) AS distinct_schema_variants,
       ARRAY_AGG(DISTINCT source) AS sources
FROM event_props
GROUP BY event_name
HAVING COUNT(DISTINCT prop_set) > 1
ORDER BY distinct_schema_variants DESC;
4. Volume & freshness per event (optional, needs dynamic SQL)
Since each event is its own table, getting row counts and last-seen timestamps across all of them requires generating a UNION ALL dynamically. Snowflake makes this easy with a two-step "generate then run" pattern:
sql-- Step 1: generate the SQL
SELECT LISTAGG(
  'SELECT ''' || table_schema || '.' || table_name || ''' AS event_source, ' ||
  'COUNT(*) AS row_count, MAX(received_at) AS last_seen ' ||
  'FROM ' || table_schema || '.' || table_name,
  ' UNION ALL '
) WITHIN GROUP (ORDER BY table_schema, table_name) AS generated_sql
FROM segment_events.information_schema.tables
WHERE object_type = 'BASE TABLE'
  AND table_or_view_name NOT IN ('IDENTIFIES','USERS','TRACKS','PAGES','SCREENS')
  AND table_schema NOT IN ('STRIPE', 'ZENDESK', '__SEGMENT_REVERSE_ETL');
Copy the resulting string out of the generated_sql column and run it as its own query — that gives you row count + last-seen per event/source, so you can spot events that have gone stale (team stopped firing them) alongside the plan.
A couple of notes on your table list specifically:

STRIPE and ZENDESK schemas aren't Segment event sources — those are Cloud App sources syncing third-party object data (customers, tickets, invoices), so I excluded them from the event catalog logic above.
__SEGMENT_REVERSE_ETL tables are Segment's internal sync-state tables (for Reverse ETL destinations), not events — also excluded.
KALYLAITSINES_COM_PROD, PAYWALL_JS_PROD, UNBOUNCE_WEBSITE_CLIENT_*, PUBLIC_WEBSITE_CLIENT_DEV, and WEBSITE all look like separate marketing/web properties — worth checking whether that fragmentation is intentional or historical sprawl, since a few of them (e.g. KAYLA_NEWSLETTER_SUBSCRIBED, TEST) look like one-off or leftover events.