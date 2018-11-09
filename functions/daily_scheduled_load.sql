CREATE OR REPLACE FUNCTION fact_loader.daily_scheduled_load(p_fact_table_id INT)
RETURNS BOOLEAN AS
$BODY$
DECLARE
    v_execute_sql text;
    v_deps regclass[];
    v_dep_delay_tolerance interval;
    v_delayed_msg text;
BEGIN
/***
There are 3 basic steps to this load:
    1. If dependencies are listed, verify they are up to date enough
    2. Execute the single daily-refresh function
    3. Update the metadata indicating the last attempt time
 */
SELECT 'SELECT '||daily_scheduled_proid::TEXT||'()',
    daily_scheduled_deps,
    daily_scheduled_dep_delay_tolerance
     INTO
    v_execute_sql,
    v_deps,
    v_dep_delay_tolerance
FROM fact_loader.fact_tables
WHERE fact_table_id = p_fact_table_id
  AND use_daily_schedule;

IF v_execute_sql IS NULL THEN
    RETURN FALSE;
END IF;

IF v_deps IS NOT NULL THEN
    WITH deps AS 
    (SELECT unnest(v_deps) AS dep)
    
    , delays AS (
    SELECT dep, now() - source_time as delay_interval 
    FROM fact_loader.queue_table_delay_info() qtd
    INNER JOIN deps d ON d.dep = qtd.queue_of_base_table_relid
    UNION ALL
    SELECT dep, now() - last_refresh_source_cutoff as delay_interval
    FROM fact_loader.fact_tables ft
    INNER JOIN deps d ON d.dep = ft.fact_table_relid
    )

    SELECT string_agg(dep::text||': Delayed '||delay_interval::text, ', ')
        INTO v_delayed_msg
    FROM delays
    WHERE delay_interval > v_dep_delay_tolerance;

    IF v_delayed_msg IS NOT NULL THEN
        RAISE EXCEPTION '%', v_delayed_msg;
    END IF;
END IF;

EXECUTE v_execute_sql;

UPDATE fact_loader.fact_tables ft
SET last_refresh_attempted_at = now(),
    last_refresh_succeeded = TRUE
WHERE fact_table_id = p_fact_table_id;

RETURN TRUE;

END;
$BODY$
LANGUAGE plpgsql;
