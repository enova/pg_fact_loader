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
  rt.if_name AS provider_name,
  rt.replication_set_name,
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
