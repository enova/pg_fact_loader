CREATE OR REPLACE FUNCTION fact_loader.raw_queued_changes(p_fact_table_id INT)
RETURNS TABLE (fact_table_id INT,
    queue_table_dep_id INT,
    fact_table_dep_id INT,
    fact_table_dep_queue_table_dep_id INT,
    queue_table_id_field BIGINT,
    fact_loader_batch_id BIGINT,
    maximum_cutoff_time TIMESTAMPTZ,
    min_missed_id BIGINT,
    queue_table_id INT
) AS
$BODY$
DECLARE
    v_raw_sql text;
BEGIN

SELECT raw_queued_changes_sql
INTO v_raw_sql
FROM fact_loader.sql_builder(p_fact_table_id);

RETURN QUERY EXECUTE v_raw_sql;

END;
$BODY$
LANGUAGE plpgsql;
