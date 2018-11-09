CREATE OR REPLACE FUNCTION fact_loader.execute_queue(p_fact_table_id INT)
RETURNS TABLE (sql TEXT) AS
$BODY$
BEGIN

RETURN QUERY
WITH ordered_process_queue AS
(SELECT
   process_queue_id
   , proid
   , key_value
   , source_change_date
   , (pp.proargtypes::REGTYPE[])[0] AS proid_first_arg
 FROM process_queue pq
   LEFT JOIN pg_proc pp ON pp.oid = proid
 WHERE pq.fact_table_id = p_fact_table_id
 ORDER BY process_queue_id)

, with_rank AS
(SELECT
  /****
  If source_change_date is NULL, we assume the proid has one arg and pass it.
  If not, we assume the proid has two args and pass source_change_date as the second.
  */
   format('%s(%s::%s%s)'
          , proid::TEXT
          , 'key_value'
          , proid_first_arg
          , CASE
              WHEN source_change_date IS NOT NULL
                THEN format(', %s::DATE',quote_literal(source_change_date))
              ELSE ''
            END
        ) AS function_call,
  proid,
  process_queue_id,
  RANK() OVER (PARTITION BY proid) AS execution_group
FROM ordered_process_queue
)

, execute_sql_groups AS
(
SELECT execution_group,
format($$
SELECT process_queue_id, %s
FROM (
/****
Must wrap this to execute in order of ids
***/
SELECT *
FROM process_queue
WHERE process_queue_id BETWEEN %s AND %s
  AND fact_table_id = %s
  AND proid = %s::REGPROC
ORDER BY process_queue_id) q;
$$, function_call, MIN(process_queue_id), MAX(process_queue_id), p_fact_table_id, quote_literal(proid::TEXT)) AS execute_sql
FROM with_rank
GROUP BY execution_group, function_call, proid
ORDER BY execution_group
)

SELECT COALESCE(string_agg(execute_sql,''),'SELECT NULL') AS final_execute_sql
FROM execute_sql_groups;

END;
$BODY$
LANGUAGE plpgsql;
