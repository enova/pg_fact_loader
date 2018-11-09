CREATE OR REPLACE FUNCTION fact_loader.worker()
RETURNS BOOLEAN AS
$BODY$
DECLARE
  v_fact_record RECORD;
BEGIN

/****
Acquire an advisory lock on the row indicating this job, which will cause the function
to simply return false if another session is running it concurrently.
It will be released upon transaction commit or rollback.
 */
FOR v_fact_record IN
  SELECT fact_table_id
  FROM fact_loader.prioritized_jobs
LOOP

IF fact_loader.try_load(v_fact_record.fact_table_id) THEN
--If any configured functions use temp tables,
--must discard to avoid them hanging around in the idle background worker session
  DISCARD TEMP;

--Log job times
  INSERT INTO fact_loader.fact_table_refresh_logs (fact_table_id, refresh_attempted_at, refresh_finished_at)
  VALUES (v_fact_record.fact_table_id, now(), clock_timestamp());

--Return true meaning the fact table was refreshed (this applies even if there was no new data)
  RETURN TRUE;
END IF;

END LOOP;

--If no jobs returned true, then return false
RETURN FALSE;

END;
$BODY$
LANGUAGE plpgsql;
