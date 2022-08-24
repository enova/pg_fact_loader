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
