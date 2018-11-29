CREATE OR REPLACE FUNCTION fact_loader.sql_builder(p_fact_table_id INT)
  RETURNS TABLE(raw_queued_changes_sql text,
                gathered_queued_changes_sql text,
                process_queue_sql text,
                metadata_update_sql text) AS
$BODY$

/****
The recursive part of this CTE are only the sql_builder parts.
In Postgres, if any of your CTEs are recursive, you only use the RECURSIVE keyword on the first of a set.

The retrieval info may be the same for all 3 events (insert, update, delete), in which case filter_scope is null
Otherwise, they must be specified separately.
 */
WITH RECURSIVE queue_deps_with_insert_retrieval AS (
  SELECT *
  FROM fact_loader.queue_deps_all_with_retrieval
  WHERE (filter_scope = 'I' OR filter_scope IS NULL)
    AND fact_table_id = p_fact_table_id
)

, queue_deps_with_update_retrieval AS (
  SELECT *
  FROM fact_loader.queue_deps_all_with_retrieval
  WHERE (filter_scope = 'U' OR filter_scope IS NULL)
    AND fact_table_id = p_fact_table_id
)

, queue_deps_with_delete_retrieval AS (
  SELECT *
  FROM fact_loader.queue_deps_all_with_retrieval
  WHERE (filter_scope = 'D' OR filter_scope IS NULL)
    AND fact_table_id = p_fact_table_id
)

/****
Recursively build the SQL for any INSERT events found in the queues.

The recursive part ONLY applies to cases where multiple joins have to be made to get at the source data,
in which case there are multiple levels of key_retrieval_sequences for a given queue_table_dep_id. For an
example of this, see the test cases involving the test.order_product_promos table.
 */
, insert_sql_builder AS (
  SELECT queue_table_dep_id,
    level,
    CASE
      WHEN is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', b.'||return_columns[1]||'::TEXT AS key'
            ELSE ', unnest(array[b.'||array_to_string(return_columns, ',b.')||'])::TEXT AS key'
          END
      WHEN join_return_is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', j'||level||'.'||return_columns_from_join[1]||'::TEXT AS key'
            ELSE ', unnest(array[j'||level||'.'||array_to_string(return_columns_from_join, ',j'||level)||'])::TEXT AS key'
          END
        ELSE ''
    END AS key_select_column,
    CASE
      WHEN is_fact_key
        THEN ''
        ELSE 'INNER JOIN '||join_to_relation::TEXT||' j'||level||
             E'\n  ON b.'||quote_ident(return_columns[1])||' = j'||level||'.'||quote_ident(join_to_column)
    END AS key_retrieval_sql,
    source_change_date_select
  FROM queue_deps_with_insert_retrieval c
  WHERE level = 1
    AND fact_table_id = p_fact_table_id
  UNION ALL
  SELECT c.queue_table_dep_id,
    c.level,
    key_select_column||CASE
    WHEN c.is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||r.level||'.'||return_columns[1]||'::TEXT AS key'
          ELSE ', unnest(b.'||array_to_string(return_columns,',j'||r.level)||')::TEXT AS key'
        END
    WHEN join_return_is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||c.level||'.'||return_columns_from_join[1]||'::TEXT AS key'
          ELSE ', unnest(j'||c.level||'.'||array_to_string(return_columns_from_join,',j'||c.level)||')::TEXT AS key'
        END
      ELSE ''
    END AS key_select_column,
    key_retrieval_sql||CASE
    WHEN is_fact_key
      THEN ''
      ELSE E'\nINNER JOIN '||join_to_relation::TEXT||' j'||c.level||
           E'\n  ON j'||r.level||'.'||quote_ident(return_columns[1])||' = j'||c.level||'.'||quote_ident(join_to_column) END AS key_retrieval_sql,
    r.source_change_date_select
  FROM insert_sql_builder r
  INNER JOIN queue_deps_with_insert_retrieval c USING (queue_table_dep_id)
  WHERE c.level = r.level + 1
)

, update_sql_builder AS (
  SELECT queue_table_dep_id,
    level,
    CASE
      WHEN is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', b.'||return_columns[1]||'::TEXT AS key'
            ELSE ', unnest(array[b.'||array_to_string(return_columns, ',b.')||'])::TEXT AS key'
          END
      WHEN join_return_is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', j'||level||'.'||return_columns_from_join[1]||'::TEXT AS key'
            ELSE ', unnest(array[j'||level||'.'||array_to_string(return_columns_from_join, ',j'||level)||'])::TEXT AS key'
          END
        ELSE ''
    END AS key_select_column,
    CASE
      WHEN is_fact_key
        THEN ''
        ELSE 'INNER JOIN '||join_to_relation::TEXT||' j'||level||
             E'\n  ON b.'||quote_ident(return_columns[1])||' = j'||level||'.'||quote_ident(join_to_column)
    END AS key_retrieval_sql,
    source_change_date_select
  FROM queue_deps_with_update_retrieval c
  WHERE level = 1
    AND fact_table_id = p_fact_table_id
  UNION ALL
  SELECT c.queue_table_dep_id,
    c.level,
    key_select_column||CASE
    WHEN c.is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||r.level||'.'||return_columns[1]||'::TEXT AS key'
          ELSE ', unnest(b.'||array_to_string(return_columns,',j'||r.level)||')::TEXT AS key'
        END
    WHEN join_return_is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||c.level||'.'||return_columns_from_join[1]||'::TEXT AS key'
          ELSE ', unnest(j'||c.level||'.'||array_to_string(return_columns_from_join,',j'||c.level)||')::TEXT AS key'
        END
      ELSE ''
    END AS key_select_column,
    key_retrieval_sql||CASE
    WHEN is_fact_key
      THEN ''
      ELSE E'\nINNER JOIN '||join_to_relation::TEXT||' j'||c.level||
           E'\n  ON j'||r.level||'.'||quote_ident(return_columns[1])||' = j'||c.level||'.'||quote_ident(join_to_column) END AS key_retrieval_sql,
    r.source_change_date_select
  FROM update_sql_builder r
  INNER JOIN queue_deps_with_update_retrieval c USING (queue_table_dep_id)
  WHERE c.level = r.level + 1
)

, delete_sql_builder AS (
  SELECT queue_table_dep_id,
    level,
    --For deletes, same pattern as key_select_column but instead, we may be selecting from the audit tables instead
    CASE
      WHEN is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', q.before_change->>'||quote_literal(return_columns[1])||'::TEXT AS key'
            ELSE ', unnest(array[before_change->>'''||array_to_string(return_columns, ''', before_change->>''')||'''])::TEXT AS key'
          END
      WHEN join_return_is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', j'||level||'.'||quote_ident(return_columns_from_join[1])||'::TEXT AS key'
            ELSE ', unnest(array[j'||level||'.'||array_to_string(return_columns_from_join, ',j'||level||'.')||'])::TEXT AS key'
          END
        ELSE ''
    END AS delete_key_select_column,
    CASE
    WHEN is_fact_key
      THEN ''
      ELSE format($$
        --Join to either the base table, or the audit table, one of which
        --will be missing the key in a delete case
          INNER JOIN LATERAL (
           SELECT %s FROM %s jb
           WHERE %s = %s
           UNION ALL
           SELECT %s FROM %s jq
           WHERE operation = 'D'
            AND %s = %s) %s ON TRUE
           $$, quote_ident(return_columns_from_join[1]),
            join_to_relation::TEXT,
            (CASE WHEN level = 1 THEN '(q'||'.before_change->>'||quote_literal(return_columns[1])||')::'||join_column_type ELSE 'j'||level||'.'||quote_ident(return_columns[1]) END),
            'jb.'||quote_ident(join_to_column),
            '(before_change->>'||quote_literal(return_columns_from_join[1])||')::'||return_columns_from_join_type,
            join_to_relation_queue::TEXT,
            (CASE WHEN level = 1 THEN '(q'||'.before_change->>'||quote_literal(return_columns[1])||')::'||join_column_type ELSE 'j'||level||'.'||quote_ident(return_columns[1]) END),
            '(jq.before_change->>'||quote_literal(join_to_column)||')::'||join_column_type,
            /****
            We use the higher level here just to be consistent with aliases from insert/update key retrieval
             */
            'j'||level
            )
    END
      AS delete_key_retrieval_sql,
    source_change_date_select
  FROM queue_deps_with_delete_retrieval
  WHERE level = 1
    AND fact_table_id = p_fact_table_id
  UNION ALL
  SELECT c.queue_table_dep_id,
    c.level,
    delete_key_select_column||CASE
    WHEN c.is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||r.level||'.before_change->>'||quote_literal(return_columns[1])||'::TEXT AS key'
          ELSE ', unnest(array[before_change->>'''||array_to_string(return_columns,',j'||r.level||'.before_change->>''')||'''])::TEXT AS key'
        END
    WHEN join_return_is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||c.level||'.'||quote_ident(return_columns_from_join[1])||'::TEXT AS key'
          ELSE ', unnest(array[j'||c.level||'.'||array_to_string(return_columns_from_join,',j'||c.level)||')::TEXT AS key'
        END
      ELSE ''
    END AS delete_key_select_column,
    delete_key_retrieval_sql||CASE
    WHEN is_fact_key
      THEN ''
      ELSE format($$
        --Join to either the base table, or the audit table, one of which
        --will be missing the key in a delete case
          INNER JOIN LATERAL (
           SELECT %s FROM %s jb
           WHERE %s = %s
           UNION ALL
           SELECT %s FROM %s jq
           WHERE operation = 'D'
            AND %s = %s) %s ON TRUE
           $$, quote_ident(return_columns_from_join[1]),
            join_to_relation::TEXT,
            'j'||r.level||'.'||quote_ident(return_columns[1]),
            'jb.'||quote_ident(join_to_column),
            '(before_change->>'||quote_literal(return_columns_from_join[1])||')::'||return_columns_from_join_type,
            join_to_relation_queue::TEXT,
            'j'||r.level||'.'||quote_ident(return_columns[1]),
            '(jq.before_change->>'||quote_literal(join_to_column)||')::'||join_column_type,
            /****
            We use the higher level here just to be consistent with aliases from insert/update key retrieval
             */
            'j'||c.level
            )
    END
      AS delete_key_retrieval_sql,
    r.source_change_date_select
  FROM delete_sql_builder r
  INNER JOIN queue_deps_with_delete_retrieval c USING (queue_table_dep_id)
  WHERE c.level = r.level + 1
)

, field_vars AS (
  SELECT
    *,
    format($$
      %s AS fact_table_id,
      %s AS queue_table_dep_id,
      %s::INT AS fact_table_dep_id,
      %s::INT AS fact_table_dep_queue_table_dep_id,
      %s AS queue_table_id_field,
      q.fact_loader_batch_id,
      %s::TIMESTAMPTZ AS maximum_cutoff_time,
    -- We must not ignore ids which are above maximum_cutoff_time
    -- but below the highest id which is below maximum_cutoff_time
      MIN(q.fact_loader_batch_id)
      FILTER (
      WHERE %s %s > %s::TIMESTAMPTZ)
      OVER() AS min_missed_id
    $$,
      fact_table_id,
      queue_table_dep_id,
      (CASE WHEN fact_table_dep_id IS NULL THEN 'NULL'::TEXT ELSE fact_table_dep_id::TEXT END),
      (CASE WHEN fact_table_dep_queue_table_dep_id IS NULL THEN 'NULL'::TEXT ELSE fact_table_dep_queue_table_dep_id::TEXT END),
      'q.'||quote_ident(queue_table_id_field),
      quote_literal(maximum_cutoff_time),
      'q.'||quote_ident(queue_table_timestamp),
      CASE WHEN queue_table_tz IS NULL THEN '' ELSE 'AT TIME ZONE '||quote_literal(queue_table_tz) END,
      quote_literal(maximum_cutoff_time)
    )
      AS inner_shared_select_columns,
    $$
      fact_table_id,
      queue_table_dep_id,
      fact_table_dep_id,
      fact_table_dep_queue_table_dep_id,
      queue_table_id_field,
      fact_loader_batch_id,
      maximum_cutoff_time,
      min_missed_id
    $$
      AS outer_shared_select_columns,
    CASE WHEN queue_table_tz IS NULL THEN '' ELSE 'AT TIME ZONE '||quote_literal(queue_table_tz) END
      AS changed_at_tz_correction
  FROM fact_loader.queue_deps_all c
  WHERE c.fact_table_id = p_fact_table_id
)

, non_recursive_sql AS (
  SELECT
  /****
  Separate select list for:
    - raw queue_ids from queue tables
    - gathered data from joining queue_ids to source tables to get actual keys to update in fact tables
   */
  -- gathering all queue_ids from queue tables
    queue_table_dep_id,
    outer_shared_select_columns,
    format($$
      %s,
      %s %s AS changed_at,
      %s AS queue_table_id
    $$,
      inner_shared_select_columns,
      'q.'||quote_ident(queue_table_timestamp),
      changed_at_tz_correction,
      queue_table_id
    )
      AS inner_metadata_select_columns,
    format($$
      %s,
      queue_table_id
    $$,
      outer_shared_select_columns
    )
      AS outer_metadata_select_columns,

  -- gathering actual keys to update in fact tables by joining from queue_ids to source tables
    format($$
      %s,
      %s AS operation,
      %s %s AS changed_at,
      %s::REGPROC AS insert_merge_proid,
      %s::REGPROC AS update_merge_proid,
      %s::REGPROC AS delete_merge_proid
    $$,
      inner_shared_select_columns,
      'q.'||quote_ident(queue_table_op),
      'q.'||quote_ident(queue_table_timestamp),
      changed_at_tz_correction,
      CASE WHEN insert_merge_proid IS NULL THEN 'NULL' ELSE quote_literal(insert_merge_proid) END,
      CASE WHEN update_merge_proid IS NULL THEN 'NULL' ELSE quote_literal(update_merge_proid) END,
      CASE WHEN delete_merge_proid IS NULL THEN 'NULL' ELSE quote_literal(delete_merge_proid) END
    )
      AS inner_data_select_columns,
    format($$
      %s,
      operation,
      changed_at,
      insert_merge_proid,
      update_merge_proid,
      delete_merge_proid,
      key,
      source_change_date
    $$,
      outer_shared_select_columns
    )
      AS outer_data_select_columns,

  -- This is simply the queue table aliased as q
    format('%s q', queue_table_relid::TEXT) AS queue_table_aliased,

  -- This is the SQL to join from the queue table to the base table
    format($$
      INNER JOIN %s b
        ON q.%s::%s = b.%s
    $$,
      queue_of_base_table_relid::TEXT,
      quote_ident(queue_table_key),
      queue_of_base_table_key_type,
      quote_ident(queue_of_base_table_key))
           AS base_join_sql,

  -- This is a WHERE statement to be added to ALL gathering of new queue_ids to process.
  -- There is a further filter based on the window min_missed_id after this subquery
    format($$ %s
        $$,
        CASE
          WHEN last_cutoff_id IS NOT NULL
          THEN 'q.fact_loader_batch_id > '||last_cutoff_id
          ELSE
            'TRUE'
        END)
           AS inner_global_where_sql,
    format($$
        -- changed_at is guaranteed now to be in timestamptz - any time zone casting is only in subquery
        changed_at < %s
        AND (min_missed_id IS NULL OR (fact_loader_batch_id < min_missed_id))
        $$,
        quote_literal(c.maximum_cutoff_time)
        )
           AS outer_global_where_sql,
    format($$
      AND q.%s = 'I'
      $$,
        queue_table_op)
           AS where_for_insert_sql,
    format($$
      AND (q.%s = 'U' AND %s)
      $$,
        queue_table_op,
        CASE
          WHEN relevant_change_columns IS NULL
            THEN 'TRUE'
          ELSE
            format($$q.%s ?| '{%s}'$$, queue_table_change, array_to_string(relevant_change_columns,','))
        END)
           AS where_for_update_sql,
    format($$
      AND q.%s = 'D'
      $$,
        queue_table_op)
           AS where_for_delete_sql
  FROM field_vars c
)

, insert_sql_builder_final AS
(SELECT DISTINCT ON (queue_table_dep_id)
  *
FROM insert_sql_builder
ORDER BY queue_table_dep_id, level DESC
)

, update_sql_builder_final AS
(SELECT DISTINCT ON (queue_table_dep_id)
  *
FROM update_sql_builder
ORDER BY queue_table_dep_id, level DESC
)

, delete_sql_builder_final AS
(SELECT DISTINCT ON (queue_table_dep_id)
  *
FROM delete_sql_builder
ORDER BY queue_table_dep_id, level DESC
)

, all_queues_sql AS (
SELECT
  format($$
  SELECT %s
  FROM (
  SELECT %s
  FROM %s
  %s
  WHERE %s ) sub
  WHERE %s
  $$,
    nrs.outer_data_select_columns,
    nrs.inner_data_select_columns||isbf.key_select_column||isbf.source_change_date_select,
    nrs.queue_table_aliased||nrs.base_join_sql,
    isbf.key_retrieval_sql,
    nrs.inner_global_where_sql||nrs.where_for_insert_sql,
    nrs.outer_global_where_sql) AS queue_insert_sql,
  format($$
  SELECT %s
  FROM (
  SELECT %s
  FROM %s
  %s
  WHERE %s ) sub
  WHERE %s
  $$,
    nrs.outer_data_select_columns,
    nrs.inner_data_select_columns||usbf.key_select_column||usbf.source_change_date_select,
    nrs.queue_table_aliased||nrs.base_join_sql,
    usbf.key_retrieval_sql,
    nrs.inner_global_where_sql||nrs.where_for_update_sql,
    nrs.outer_global_where_sql) AS queue_update_sql,
  format($$
  SELECT %s
  FROM (
  SELECT %s
  FROM %s
  %s
  WHERE %s ) sub
  WHERE %s
  $$,
    nrs.outer_data_select_columns,
    nrs.inner_data_select_columns||dsbf.delete_key_select_column||usbf.source_change_date_select,
    nrs.queue_table_aliased,
    dsbf.delete_key_retrieval_sql,
    nrs.inner_global_where_sql||nrs.where_for_delete_sql,
    nrs.outer_global_where_sql) AS queue_delete_sql,
  format($$
  SELECT %s
  FROM (
  SELECT %s
  FROM %s
  WHERE %s ) sub
  WHERE %s
  $$,
    nrs.outer_metadata_select_columns,
    nrs.inner_metadata_select_columns,
    nrs.queue_table_aliased,
    nrs.inner_global_where_sql,
    nrs.outer_global_where_sql) AS queue_ids_sql
FROM non_recursive_sql nrs
INNER JOIN insert_sql_builder_final isbf ON isbf.queue_table_dep_id = nrs.queue_table_dep_id
INNER JOIN update_sql_builder_final usbf ON usbf.queue_table_dep_id = nrs.queue_table_dep_id
INNER JOIN delete_sql_builder_final dsbf ON dsbf.queue_table_dep_id = nrs.queue_table_dep_id
)

, final_queue_sql AS
(SELECT string_agg(
  /****
  This first UNION is to union together INSERT, UPDATE, and DELETE events for a single queue table
   */
  format($$
  %s
  UNION ALL
  %s
  UNION ALL
  %s
  $$,
    queue_insert_sql,
    queue_update_sql,
    queue_delete_sql)
  /****
  This second UNION as the second arg of string_agg is the union together ALL queue tables for this fact table
   */
  , E'\nUNION ALL\n') AS event_sql,
  string_agg(queue_ids_sql, E'\nUNION ALL\n') AS raw_queued_changes_sql_out
FROM all_queues_sql)

, final_outputs AS (
SELECT raw_queued_changes_sql_out,
$$
WITH all_changes AS (
($$||event_sql||$$)
ORDER BY changed_at)

, base_execution_groups AS
(SELECT fact_table_id,
    queue_table_dep_id,
    queue_table_id_field,
    operation,
    changed_at,
    source_change_date,
    insert_merge_proid,
    update_merge_proid,
    delete_merge_proid,
    maximum_cutoff_time,
    key,
    CASE WHEN operation = 'I' THEN insert_merge_proid
    WHEN operation = 'U' THEN update_merge_proid
    WHEN operation = 'D' THEN delete_merge_proid
        END AS proid,
  RANK() OVER (
    PARTITION BY
      CASE
        WHEN operation = 'I' THEN insert_merge_proid
        WHEN operation = 'U' THEN update_merge_proid
        WHEN operation = 'D' THEN delete_merge_proid
      END
  ) AS execution_group
  FROM all_changes
  WHERE key IS NOT NULL)

SELECT fact_table_id, proid, key, source_change_date
FROM base_execution_groups beg
WHERE proid IS NOT NULL
GROUP BY execution_group, fact_table_id, proid, key, source_change_date
/****
This ordering is particularly important for date-range history tables
where order of inserts is critical and usually expected to follow a pattern
***/
ORDER BY execution_group, MIN(changed_at), MIN(queue_table_id_field);
$$ AS gathered_queued_changes_sql_out
  ,

$$
DROP TABLE IF EXISTS process_queue;
CREATE TEMP TABLE process_queue
(process_queue_id serial,
 fact_table_id int,
 proid regproc,
 key_value text,
 source_change_date date);

INSERT INTO process_queue
(fact_table_id, proid, key_value, source_change_date)
$$ AS process_queue_snippet,

$$
WITH all_ids AS
($$||raw_queued_changes_sql_out||$$)

, new_metadata AS
(SELECT MAX(fact_loader_batch_id) AS last_cutoff_id,
  maximum_cutoff_time,
  queue_table_dep_id
FROM all_ids
--Exclude dependent fact tables from updates directly to queue_table_deps
WHERE fact_table_dep_id IS NULL
GROUP BY queue_table_dep_id, maximum_cutoff_time)

/****
The dependent fact table uses the same queue_table_id_field as last_cutoff
We are going to update fact_table_deps metadata instead of queue_table_deps
****/
, new_metadata_fact_dep AS
(SELECT MAX(fact_loader_batch_id) AS last_cutoff_id,
  maximum_cutoff_time,
  fact_table_dep_queue_table_dep_id
FROM all_ids
--Include dependent fact tables only
WHERE fact_table_dep_id IS NOT NULL
GROUP BY fact_table_dep_queue_table_dep_id, maximum_cutoff_time)

, update_key AS (
SELECT qdwr.queue_table_dep_id,
  --Cutoff the id to that newly found, otherwise default to last value
  COALESCE(mu.last_cutoff_id, qdwr.last_cutoff_id) AS last_cutoff_id,
  --This cutoff time must always be the same for all queue tables for given fact table.
  --Even if there are no new records, we move this forward to wherever the stream is at
  qdwr.maximum_cutoff_time AS last_cutoff_source_time
FROM fact_loader.queue_deps_all qdwr
LEFT JOIN new_metadata mu ON mu.queue_table_dep_id = qdwr.queue_table_dep_id
WHERE qdwr.fact_table_id = $$||p_fact_table_id||$$
--Exclude dependent fact tables from updates directly to queue_table_deps
  AND qdwr.fact_table_dep_id IS NULL
)

/****
This SQL also nearly matches that for the queue_table_deps but would be a little ugly to try to DRY up
****/
, update_key_fact_dep AS (
SELECT qdwr.fact_table_dep_queue_table_dep_id,
  qdwr.fact_table_id,
  COALESCE(mu.last_cutoff_id, qdwr.last_cutoff_id) AS last_cutoff_id,
  qdwr.maximum_cutoff_time AS last_cutoff_source_time
FROM fact_loader.queue_deps_all qdwr
LEFT JOIN new_metadata_fact_dep mu ON mu.fact_table_dep_queue_table_dep_id = qdwr.fact_table_dep_queue_table_dep_id
WHERE qdwr.fact_table_id = $$||p_fact_table_id||$$
--Include dependent fact tables only
  AND qdwr.fact_table_dep_id IS NOT NULL
)

, updated_queue_table_deps AS (
UPDATE fact_loader.queue_table_deps qtd
SET last_cutoff_id = uk.last_cutoff_id,
  last_cutoff_source_time = uk.last_cutoff_source_time
FROM update_key uk
WHERE qtd.queue_table_dep_id = uk.queue_table_dep_id
RETURNING qtd.*)

, updated_fact_table_deps AS (
UPDATE fact_loader.fact_table_dep_queue_table_deps ftd
SET last_cutoff_id = uk.last_cutoff_id,
  last_cutoff_source_time = uk.last_cutoff_source_time
FROM update_key_fact_dep uk
WHERE ftd.fact_table_dep_queue_table_dep_id = uk.fact_table_dep_queue_table_dep_id
RETURNING uk.*)

UPDATE fact_loader.fact_tables ft
SET last_refresh_source_cutoff = uqtd.last_cutoff_source_time,
  last_refresh_attempted_at = now(),
  last_refresh_succeeded = TRUE
FROM
(SELECT fact_table_id, last_cutoff_source_time
FROM updated_queue_table_deps
--Must use UNION to get only distinct values
UNION
SELECT fact_table_id, last_cutoff_source_time
FROM updated_fact_table_deps) uqtd
WHERE uqtd.fact_table_id = ft.fact_table_id;
$$ AS metadata_update_sql_out
FROM final_queue_sql)

SELECT raw_queued_changes_sql_out,
 gathered_queued_changes_sql_out
  ,
 format($$
  %s
  %s$$, process_queue_snippet, gathered_queued_changes_sql_out) AS process_queue_sql_out,
 metadata_update_sql_out
FROM final_outputs;

$BODY$
LANGUAGE SQL;
