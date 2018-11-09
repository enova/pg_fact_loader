CREATE OR REPLACE FUNCTION fact_loader.gathered_queued_changes(p_fact_table_id INT)
RETURNS TABLE (fact_table_id INT, proid REGPROC, key_value TEXT, source_change_date DATE) AS 
$BODY$
DECLARE
    v_gather_sql text;
BEGIN

SELECT gathered_queued_changes_sql
INTO v_gather_sql
FROM fact_loader.sql_builder(p_fact_table_id);

RETURN QUERY EXECUTE v_gather_sql;

END;
$BODY$
LANGUAGE plpgsql;
