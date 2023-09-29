CREATE OR REPLACE FUNCTION fact_loader.queue_table_delay_info()
RETURNS TABLE("publication_name" text,
           "queue_of_base_table_relid" regclass,
           "publisher" name,
           "source_time" timestamp with time zone)
AS
$BODY$
/***
This function exists to allow no necessary dependency
to exist on pglogical_ticker.  If the extension is used,
it will return data from its native functions, if not,
it will return a null data set matching the structure
***/
BEGIN

IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pglogical_ticker') THEN
    RETURN QUERY EXECUTE $$
    -- pglogical
    SELECT
        unnest(coalesce(subpublications,'{NULL}')) AS publication_name
      , qt.queue_of_base_table_relid
      , n.if_name AS publisher
      , t.source_time
    FROM fact_loader.queue_tables qt
      JOIN fact_loader.logical_subscription() s ON qt.pglogical_node_if_id = s.subid AND s.driver = 'pglogical'
      JOIN pglogical.node_interface n ON n.if_id = qt.pglogical_node_if_id
      JOIN pglogical_ticker.all_subscription_tickers() t ON t.provider_name = n.if_name
    UNION ALL
    -- native logical
    SELECT
        unnest(coalesce(subpublications,'{NULL}')) AS publication_name
      , qt.queue_of_base_table_relid
      , t.db AS publisher
      , t.tick_time AS source_time
    FROM fact_loader.queue_tables qt
      JOIN fact_loader.subscription_rel() psr ON psr.srrelid = qt.queue_table_relid
      JOIN fact_loader.logical_subscription() s ON psr.srsubid = s.subid
      JOIN logical_ticker.tick t ON t.db = s.dbname
    UNION ALL
    -- local
    SELECT
        NULL::text AS publication_name
      , qt.queue_of_base_table_relid
      , NULL::name AS publisher
      , now() AS source_time
    FROM fact_loader.queue_tables qt
    WHERE qt.pglogical_node_if_id IS NULL
        AND NOT EXISTS (
        SELECT 1
        FROM fact_loader.subscription_rel() psr WHERE psr.srrelid = qt.queue_table_relid
    );$$;
ELSE
    RETURN QUERY
    -- local
    SELECT
        NULL::TEXT AS publication_name
      , qt.queue_of_base_table_relid
      , NULL::NAME AS publisher
      --source_time is now() if queue tables are not pglogical-replicated, which is assumed if no ticker
      , now() AS source_time
    FROM fact_loader.queue_tables qt
    WHERE NOT EXISTS (SELECT 1 FROM fact_loader.subscription_rel() psr WHERE psr.srrelid = qt.queue_table_relid)
    UNION ALL
    -- native logical
    (WITH logical_subscription_with_db AS (
    SELECT *, (regexp_matches(subconninfo, 'dbname=(.*?)(?=\s|$)'))[1] AS db
    FROM fact_loader.logical_subscription()
    )
    SELECT
        unnest(coalesce(subpublications,'{NULL}')) AS publication_name
      , qt.queue_of_base_table_relid
      , t.db AS publisher
      , t.tick_time AS source_time
    FROM fact_loader.queue_tables qt
      JOIN fact_loader.subscription_rel() psr ON psr.srrelid = qt.queue_table_relid
      JOIN logical_subscription_with_db s ON psr.srsubid = s.subid
      JOIN logical_ticker.tick t ON t.db = s.db);
END IF;

END;
$BODY$
LANGUAGE plpgsql;
