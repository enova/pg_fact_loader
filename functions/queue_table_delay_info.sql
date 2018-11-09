CREATE OR REPLACE FUNCTION fact_loader.queue_table_delay_info()
RETURNS TABLE("replication_set_name" text,
           "queue_of_base_table_relid" regclass,
           "if_id" oid,
           "if_name" name,
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
    SELECT
        unnest(coalesce(sub_replication_sets,'{NULL}')) AS replication_set_name
      , qt.queue_of_base_table_relid
      , n.if_id
      , n.if_name
      --source_time is now() for local tables (pglogical_node_if_id is null), and based on pglogical_ticker time otherwise
      , CASE
        WHEN qt.pglogical_node_if_id IS NULL
          THEN now()
        ELSE t.source_time
        END                          AS source_time
    FROM fact_loader.queue_tables qt
      LEFT JOIN fact_loader.logical_subscription() s ON qt.pglogical_node_if_id = s.sub_origin_if
      LEFT JOIN pglogical.node_interface n ON n.if_id = qt.pglogical_node_if_id
      LEFT JOIN pglogical_ticker.all_subscription_tickers() t ON t.provider_name = n.if_name;$$;
ELSE
    RETURN QUERY
    SELECT
        NULL::TEXT AS replication_set_name
      , qt.queue_of_base_table_relid
      , NULL::OID AS if_id
      , NULL::NAME AS if_name
      --source_time is now() if queue tables are not pglogical-replicated, which is assumed if no ticker
      , now() AS source_time
    FROM fact_loader.queue_tables qt;
END IF;

END;
$BODY$
LANGUAGE plpgsql;
