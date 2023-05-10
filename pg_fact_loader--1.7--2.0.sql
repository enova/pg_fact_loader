/* pg_fact_loader--1.7--2.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fact_loader" to load this file. \quit

DROP VIEW fact_loader.queue_deps_all_with_retrieval;
DROP VIEW fact_loader.queue_deps_all;
DROP FUNCTION fact_loader.safely_terminate_workers(); 
DROP FUNCTION fact_loader.launch_workers(int);
DROP FUNCTION fact_loader.launch_worker();
DROP FUNCTION fact_loader._launch_worker(oid);
DROP FUNCTION fact_loader.queue_table_delay_info();
DROP FUNCTION fact_loader.logical_subscription();
CREATE TYPE fact_loader.driver AS ENUM ('pglogical', 'native');


/***
This function exists mostly to easily mock out for testing purposes.
 */
CREATE FUNCTION fact_loader.subscription()
RETURNS TABLE (oid OID, subpublications text[], subconninfo text)
AS $BODY$
BEGIN

RETURN QUERY
SELECT s.oid, s.subpublications, s.subconninfo FROM pg_subscription s;

END;
$BODY$
LANGUAGE plpgsql;


/***
This function exists mostly to easily mock out for testing purposes.
 */
CREATE FUNCTION fact_loader.subscription_rel()
RETURNS TABLE (srsubid OID, srrelid OID) 
AS $BODY$
BEGIN

RETURN QUERY
SELECT sr.srsubid, sr.srrelid FROM pg_subscription_rel sr;

END;
$BODY$
LANGUAGE plpgsql;


/***
This function exists mostly to easily mock out for testing purposes.
 */
CREATE FUNCTION fact_loader.logical_subscription()
RETURNS TABLE (subid OID, subpublications text[], subconninfo text, dbname text, driver fact_loader.driver)
AS $BODY$
BEGIN

IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pglogical') THEN

  RETURN QUERY EXECUTE $$
  SELECT sub_origin_if AS subid, sub_replication_sets AS subpublications, null::text AS subconninfo, null::text AS dbname, 'pglogical'::fact_loader.driver AS driver
  FROM pglogical.subscription
  UNION ALL
  SELECT oid, subpublications, subconninfo, (regexp_matches(subconninfo, 'dbname=(.*?)(?=\s|$)'))[1] AS dbname, 'native'::fact_loader.driver AS driver
  FROM fact_loader.subscription();
  $$;
ELSE
  RETURN QUERY
  SELECT oid, subpublications, subconninfo, (regexp_matches(subconninfo, 'dbname=(.*?)(?=\s|$)'))[1] AS dbname, 'native'::fact_loader.driver AS driver
  FROM fact_loader.subscription();

END IF;

END;
$BODY$
LANGUAGE plpgsql;


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


CREATE OR REPLACE VIEW fact_loader.queue_deps_all AS
WITH RECURSIVE fact_table_dep_cutoffs AS
(SELECT
    1 AS level
    , qtd.queue_table_dep_id
    , ftdqc.fact_table_dep_id
    , ftdqc.fact_table_dep_queue_table_dep_id
    --This dep_maximum_cutoff_time is being taken from the queue_table_deps, because we cannot go past when the
    --fact table has been updated
    , qtd.last_cutoff_id                        AS dep_maximum_cutoff_id
    , qtd.last_cutoff_source_time               AS dep_maximum_cutoff_time
    , ftd.parent_id                                 AS parent_fact_table_id
    , ftd.child_id                                  AS child_fact_table_id
    , ftd.child_id                                  AS base_fact_table_id
    , queue_table_id
    , relevant_change_columns
    , ftdqc.last_cutoff_id
    , ftdqc.last_cutoff_source_time
    , ftdqc.insert_merge_proid
    , ftdqc.update_merge_proid
    , ftdqc.delete_merge_proid
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.fact_table_dep_queue_table_deps ftdqc ON ftdqc.queue_table_dep_id = qtd.queue_table_dep_id
  INNER JOIN fact_loader.fact_table_deps ftd ON ftd.fact_table_dep_id = ftdqc.fact_table_dep_id
  UNION ALL
  /****
  In this recursive part, we walk UP the chain to the base level in order to get the
  last_cutoff_id and last_cutoff_source_time of parent_ids because children must never surpass those.

  The ONLY difference between this recursive part and the non-recursive part are the dep_maximum_cutoffs.
  That means we can get our resultant data below by simply selecting distinct ON the right fields and order
  by dep_maximum_cutoffs to get the most conservative cutoff window, that is, the minimum cutoff amongst
  the queue tables and any PARENT fact table cutoffs.

  That means if, for example,
    - IF a queue table has been cutoff up until 11:00:00
    - AND IF a level 1 fact table dependent on that queue table was last cutoff at 10:55:00
    - THEN a level 2 fact table dependent on level 1 fact table must not go past 10:55:00 when it is processed.
   */
  SELECT
    ftdc.level + 1 AS level
    , ftdc.queue_table_dep_id
    , ftdc.fact_table_dep_id
    , ftdc.fact_table_dep_queue_table_dep_id
    --This dep_maximum_cutoff_time is being taken from the queue_table_deps, because we cannot go past when the
    --fact table has been updated
    , ftdqc.last_cutoff_id                        AS dep_maximum_cutoff_id
    , ftdqc.last_cutoff_source_time               AS dep_maximum_cutoff_time
    , ftd.parent_id                                 AS parent_fact_table_id
    , ftd.child_id                                  AS child_fact_table_id
    , ftdc.base_fact_table_id
    , ftdc.queue_table_id
    , ftdc.relevant_change_columns
    , ftdc.last_cutoff_id
    , ftdc.last_cutoff_source_time
    , ftdc.insert_merge_proid
    , ftdc.update_merge_proid
    , ftdc.delete_merge_proid
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.fact_table_dep_queue_table_deps ftdqc ON ftdqc.queue_table_dep_id = qtd.queue_table_dep_id
  INNER JOIN fact_loader.fact_table_deps ftd ON ftd.fact_table_dep_id = ftdqc.fact_table_dep_id
  INNER JOIN fact_table_dep_cutoffs ftdc ON ftdc.parent_fact_table_id = ftd.child_id
)

, adjusted_fact_table_deps AS (
/****
The reason we look at distinct queue_table_dep_id and not simply queue_table_id
is because two parent fact tables could have differing logic for retrieving changes
for the same base queue_tables.
 */
SELECT DISTINCT ON(base_fact_table_id, queue_table_dep_id)
*
FROM fact_table_dep_cutoffs
ORDER BY base_fact_table_id, queue_table_dep_id, dep_maximum_cutoff_time
)

, queue_table_info AS (
    SELECT * FROM fact_loader.queue_table_delay_info()
)

/****
For fact tables that depend on other fact tables, we join the child fact table to the queue_table_deps of the parent
fact table, and just reuse this exactly, with these distinctions:
  - From the fact_table_dep table, we do use the proids, and the last_cutoff_id
  - We use the parent last_cutoff_source_time as the maximum_cutoff, because we can only update those records already updated on the parent
  - We pass the information of which table for which to update metadata in the end
 */
, queue_table_deps_with_nested AS (
  /****
  This part of the union is for the base level of queue_table_deps - for fact tables with no other dependent fact tables
   */
  SELECT
    queue_table_dep_id
    , NULL :: INT                                AS fact_table_dep_id
    , NULL :: INT                                AS fact_table_dep_queue_table_dep_id
    , NULL :: BIGINT                             AS dep_maximum_cutoff_id
    , NULL :: TIMESTAMPTZ                        AS dep_maximum_cutoff_time
    , fact_table_id
    , queue_table_id
    , relevant_change_columns
    , last_cutoff_id
    , last_cutoff_source_time
    , insert_merge_proid
    , update_merge_proid
    , delete_merge_proid
  FROM fact_loader.queue_table_deps
  UNION ALL
  /****
  This part of the union is for fact tables with other dependent fact tables
   */
  SELECT
    queue_table_dep_id
    , fact_table_dep_id
    , fact_table_dep_queue_table_dep_id
    , aftd.dep_maximum_cutoff_id
    , aftd.dep_maximum_cutoff_time
    , base_fact_table_id                                  AS fact_table_id
    , queue_table_id
    , relevant_change_columns
    , aftd.last_cutoff_id
    , aftd.last_cutoff_source_time
    , aftd.insert_merge_proid
    , aftd.update_merge_proid
    , aftd.delete_merge_proid
  FROM adjusted_fact_table_deps aftd
)

SELECT
  ft.fact_table_id,
  ft.fact_table_relid,
  ft.fact_table_agg_proid,
  qt.queue_table_id,
  qt.queue_table_relid,
  qt.queue_of_base_table_relid,
  qtd.relevant_change_columns,
  qtd.last_cutoff_id,
  qtd.last_cutoff_source_time,
  rt.publisher AS provider_name,
  rt.publication_name,
  qtd.dep_maximum_cutoff_id,  --Not used yet - TODO - think about if it needs to be used to filter as cutoff MAX in addition to the time filter
  LEAST(
      MIN(qtd.dep_maximum_cutoff_time)
      OVER (
        PARTITION BY qtd.fact_table_id ),
      MIN(rt.source_time)
      OVER (
        PARTITION BY qtd.fact_table_id )
  ) AS maximum_cutoff_time,
  aqt.queue_table_id_field,
  'primary_key'::name AS queue_table_key,
  'operation'::name AS queue_table_op,
  'change'::name AS queue_table_change,
  'changed_at'::name AS queue_table_timestamp,
  qt.queue_table_tz,
  aqbt.queue_of_base_table_key,
  aqbt.queue_of_base_table_key_type,
  queue_table_dep_id,
  fact_table_dep_id,
  fact_table_dep_queue_table_dep_id,
  insert_merge_proid,
  update_merge_proid,
  delete_merge_proid,
  qt.purge
FROM queue_table_deps_with_nested qtd
INNER JOIN fact_loader.fact_tables ft ON ft.fact_table_id = qtd.fact_table_id
INNER JOIN fact_loader.queue_tables qt ON qt.queue_table_id = qtd.queue_table_id
INNER JOIN queue_table_info rt ON rt.queue_of_base_table_relid = qt.queue_of_base_table_relid
INNER JOIN LATERAL
  (SELECT a.attname AS queue_of_base_table_key, format_type(atttypid, atttypmod) AS queue_of_base_table_key_type
  FROM (SELECT
          i.indrelid
          , unnest(indkey) AS ik
          , row_number()
            OVER ()        AS rn
        FROM pg_index i
        WHERE i.indrelid = qt.queue_of_base_table_relid AND i.indisprimary) pk
  INNER JOIN pg_attribute a
    ON a.attrelid = pk.indrelid AND a.attnum = pk.ik) aqbt ON TRUE
INNER JOIN LATERAL
  (SELECT a.attname AS queue_table_id_field
  FROM (SELECT
          i.indrelid
          , unnest(indkey) AS ik
          , row_number()
            OVER ()        AS rn
        FROM pg_index i
        WHERE i.indrelid = qt.queue_table_relid AND i.indisprimary) pk
  INNER JOIN pg_attribute a
    ON a.attrelid = pk.indrelid AND a.attnum = pk.ik) aqt ON TRUE
ORDER BY ft.fact_table_relid;


CREATE OR REPLACE VIEW fact_loader.queue_deps_all_with_retrieval AS
SELECT
  qtd.*,
  krs.filter_scope,
  krs.level,
  krs.return_columns, --we need not get the type separately.  It must match queue_of_base_table_key_type
  krs.is_fact_key,
  krs.join_to_relation,
  qtk.queue_table_relid AS join_to_relation_queue,
  krs.join_to_column,
  ctypes.join_column_type,
  krs.return_columns_from_join,
  ctypes.return_columns_from_join_type,
  krs.join_return_is_fact_key,
  /***
  We include this in this view def to be easily shared by all events (I, U, D) in sql_builder,
  as those may be different in terms of passing source_change_date.
   */
  format(', %s::DATE AS source_change_date',
    CASE
      WHEN krs.pass_queue_table_change_date_at_tz IS NOT NULL
        /***
        For casting queue_table_timestamp to a date, we first ensure we have it as timestamptz (objective UTC time).
        Then, we cast it to the timezone of interest on which the date should be based.
        For example, 02:00:00 UTC time on 2018-05-02 is actually 2018-05-01 in America/Chicago time.
        Thus, any date-based fact table must decide in what time zone to consider the date.
         */
        THEN format('(%s %s AT TIME ZONE %s)',
                    'q.'||quote_ident(qtd.queue_table_timestamp),
                    CASE WHEN qtd.queue_table_tz IS NULL THEN '' ELSE 'AT TIME ZONE '||quote_literal(qtd.queue_table_tz) END,
                    quote_literal(krs.pass_queue_table_change_date_at_tz))
        ELSE 'NULL'
      END) AS source_change_date_select
FROM fact_loader.queue_deps_all qtd
INNER JOIN fact_loader.key_retrieval_sequences krs ON qtd.queue_table_dep_id = krs.queue_table_dep_id
LEFT JOIN fact_loader.queue_tables qtk ON qtk.queue_of_base_table_relid = krs.join_to_relation
LEFT JOIN LATERAL
  (SELECT MAX(CASE WHEN attname = krs.join_to_column THEN format_type(atttypid, atttypmod) ELSE NULL END) AS join_column_type,
    MAX(CASE WHEN attname = krs.return_columns_from_join[1] THEN format_type(atttypid, atttypmod) ELSE NULL END) AS return_columns_from_join_type
  FROM pg_attribute a
  WHERE a.attrelid IN(krs.join_to_relation)
    /****
    We stubbornly assume that if there are multiple columns in return_columns_from_join, they all have the same type.
    Undue complexity would ensue if we did away with that rule.
     */
    AND a.attname IN(krs.join_to_column,krs.return_columns_from_join[1])) ctypes ON TRUE;


