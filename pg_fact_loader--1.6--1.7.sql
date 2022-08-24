/* pg_fact_loader--1.6--1.7.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fact_loader" to load this file. \quit

DROP VIEW fact_loader.queue_deps_all_with_retrieval;
DROP VIEW fact_loader.queue_deps_all;
DROP VIEW fact_loader.prioritized_jobs;

ALTER TABLE fact_loader.fact_tables ADD COLUMN pre_execute_hook_sql TEXT;


CREATE OR REPLACE FUNCTION fact_loader.load(p_fact_table_id INT)
RETURNS VOID AS
$BODY$
DECLARE
    v_process_queue_sql text;
    v_execute_sql text;
    v_metadata_update_sql text;
    v_debug_rec record;
    v_debug_text text = '';
    v_pre_execute_hook_sql text = '';
BEGIN
/***
There are 3 basic steps to this load:
    1. Gather all queue table changes and insert them into a consolidated process_queue
    2. Update the metadata indicating the last records updated for both the queue tables and fact table
 */

/****
Get SQL to insert new data into the consolidated process_queue,
and SQL to update metadata for last_cutoffs.
 */
SELECT process_queue_sql, metadata_update_sql
INTO v_process_queue_sql, v_metadata_update_sql
FROM fact_loader.sql_builder(p_fact_table_id);

/****
Populate the consolidated queue
This just creates a temp table with all changes to be processed
 */
RAISE DEBUG 'Populating Queue for fact_table_id %: %', p_fact_table_id, v_process_queue_sql;
EXECUTE COALESCE(v_process_queue_sql, $$SELECT 'No queue data' AS result$$);

/****
 Pre-execute hook
 */
SELECT pre_execute_hook_sql INTO v_pre_execute_hook_sql
FROM fact_loader.fact_tables
WHERE fact_table_id = p_fact_table_id;

EXECUTE COALESCE(v_pre_execute_hook_sql, $$SELECT 'No pre-execute hook.' AS result$$);

/****
For DEBUG purposes only to view the actual process_queue.  Requires setting log_min_messages to DEBUG.
 */
IF current_setting('log_min_messages') = 'debug3' THEN
    INSERT INTO fact_loader.debug_process_queue (process_queue_id, fact_table_id, proid, key_value, row_created_at, row_updated_at, source_change_date)
    -- the row timestamps are not populated, so we set them here
    SELECT process_queue_id, fact_table_id, proid, key_value, now(), now(), source_change_date FROM process_queue;
END IF;

/****
With data now in the process_queue, the execute_queue function builds the SQL to execute.
Save this SQL in a variable and execute it.
If there is no data to execute, this is a no-op select statement.
 */
SELECT sql INTO v_execute_sql FROM fact_loader.execute_queue(p_fact_table_id);
RAISE DEBUG 'Executing Queue for fact_table_id %: %', p_fact_table_id, v_execute_sql;
EXECUTE COALESCE(v_execute_sql, $$SELECT 'No queue data to execute' AS result$$);

/****
With everything finished, we now update the metadata for the fact_table.
Even if no data was processed, we will still move forward last_refresh_attempted_at.

last_refresh_succeeded will be marked true always for now.  It could in the future
be used to indicate a failure in case of a caught error.
 */
RAISE DEBUG 'Updating metadata for fact_table_id %: %', p_fact_table_id, v_metadata_update_sql;
EXECUTE COALESCE(v_metadata_update_sql,
    format(
    $$UPDATE fact_loader.fact_tables ft
        SET last_refresh_attempted_at = now(),
          last_refresh_succeeded = TRUE
     WHERE fact_table_id = %s;
    $$, p_fact_table_id));

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE VIEW fact_loader.prioritized_jobs AS
WITH jobs_with_daily_variables AS (
SELECT
   ft.*,
/***
Keep all this logic of daily jobs as variables to ease visualization of logic in the next cte below!!
 */
    (--If this is the first run of a scheduled job, it is eligible
            ft.last_refresh_attempted_at IS NULL
          OR (
               --If it was last attempted successfully prior to this scheduled time only - meaning yesterday, it is eligible
                (
                 ft.last_refresh_succeeded AND
                 ft.last_refresh_attempted_at::DATE <
                  -- Timezone taken from daily_scheduled_tz if base job, otherwise look up the timezone of the base job if this is dependent
                    (now() AT TIME ZONE COALESCE(
                                          ft.daily_scheduled_tz,
                                          base.daily_scheduled_tz
                                          )
                    )::DATE
                )
              OR
               --If a job has failed and been re-enabled, it is eligible again even though it has been attempted at or after the scheduled time
                 NOT ft.last_refresh_succeeded
             )
     ) AS daily_not_attempted_today,

  (now() AT TIME ZONE ft.daily_scheduled_tz)::TIME
      BETWEEN daily_scheduled_time AND '23:59:59.999999'::TIME AS daily_scheduled_time_passed,

  base.use_daily_schedule
  AND base.last_refresh_succeeded
  AND base.last_refresh_attempted_at :: DATE = (now() AT TIME ZONE base.daily_scheduled_tz) :: DATE
    AS daily_base_job_finished,

  ft.depends_on_base_daily_job_id = ft.depends_on_parent_daily_job_id AS daily_has_only_one_parent,

  -- This should only be used in combination with daily_has_only_one_parent
  parent.use_daily_schedule
  AND parent.last_refresh_succeeded
  AND parent.last_refresh_attempted_at :: DATE = (now() AT TIME ZONE COALESCE(parent.daily_scheduled_tz, base.daily_scheduled_tz)) :: DATE
    AS parent_job_finished
FROM fact_loader.fact_tables ft
LEFT JOIN LATERAL
  (SELECT ftb.use_daily_schedule,
    ftb.last_refresh_succeeded,
    ftb.last_refresh_attempted_at,
    ftb.daily_scheduled_tz
  FROM fact_loader.fact_tables ftb
  WHERE ftb.fact_table_id = ft.depends_on_base_daily_job_id) base ON TRUE
LEFT JOIN LATERAL
  (SELECT ftp.use_daily_schedule,
    ftp.last_refresh_succeeded,
    ftp.last_refresh_attempted_at,
    ftp.daily_scheduled_tz
  FROM fact_loader.fact_tables ftp
  WHERE ftp.fact_table_id = ft.depends_on_parent_daily_job_id) parent ON TRUE
WHERE enabled
)

, jobs_with_daily_schedule_eligibility AS (
SELECT
   *,
   --Only run this job according to the same day of the daily_scheduled_time
   --according to configured timezone
   (use_daily_schedule AND daily_not_attempted_today
     AND
    (
      daily_scheduled_time_passed
      OR
     (daily_base_job_finished AND (daily_has_only_one_parent OR parent_job_finished))
    )
   ) AS daily_schedule_eligible
FROM jobs_with_daily_variables)

SELECT *
FROM jobs_with_daily_schedule_eligibility
WHERE NOT use_daily_schedule OR daily_schedule_eligible
ORDER BY
  CASE WHEN force_worker_priority THEN 0 ELSE 1 END,
  --If a job has a daily schedule, once the time has come for the next refresh,
  --prioritize it first
  CASE
    WHEN daily_schedule_eligible
    THEN (now() AT TIME ZONE daily_scheduled_tz)::TIME
    ELSE NULL
  END NULLS LAST,
  --This may be improved in the future but is a good start
  last_refresh_attempted_at NULLS FIRST,
  priority
;


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


