CREATE OR REPLACE FUNCTION fact_loader.try_load(p_fact_table_id INT)
RETURNS BOOLEAN AS
$BODY$
/***
This will be used by the worker, but can also be used safely if a DBA
wants to run a job manually.
 */
DECLARE
  c_lock_cutoff_refresh INT = 99995;
  v_err JSONB;
  v_errmsg TEXT;
  v_errdetail TEXT;
  v_errhint TEXT;
  v_errcontext TEXT;
BEGIN

-- We except rare serialization failures here which we will ignore and move to the next record
-- Anything else should be raised
BEGIN
IF EXISTS (SELECT fact_table_id
        FROM fact_loader.fact_tables
        WHERE fact_table_id = p_fact_table_id
        FOR UPDATE SKIP LOCKED) THEN
  /****
  Attempt to refresh fact_table_dep_queue_table_deps or ignore if refresh is in progress.
   */
  IF (SELECT pg_try_advisory_xact_lock(c_lock_cutoff_refresh)) THEN
    PERFORM fact_loader.refresh_fact_table_dep_queue_table_deps();
  END IF;

  --Load fact table and handle exceptions to auto-disable job and log errors in case of error
  BEGIN
    --Scheduled daily job
    IF (SELECT use_daily_schedule
        FROM fact_loader.fact_tables
        WHERE fact_table_id = p_fact_table_id) THEN

      PERFORM fact_loader.daily_scheduled_load(p_fact_table_id);

    --Queue-based job
    ELSE
      PERFORM fact_loader.load(p_fact_table_id);

      /***
      Run purge process.  This need not run every launch of worker but it should not hurt.
      It is better for it to run after the fact table load is successful so as to avoid a
      rollback and more dead bloat
       */
      PERFORM fact_loader.purge_queues();

    END IF;

    RETURN TRUE;

  EXCEPTION
    WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_errmsg = MESSAGE_TEXT,
        v_errdetail = PG_EXCEPTION_DETAIL,
        v_errhint = PG_EXCEPTION_HINT,
        v_errcontext = PG_EXCEPTION_CONTEXT;

      UPDATE fact_loader.fact_tables
      SET last_refresh_succeeded = FALSE,
          last_refresh_attempted_at = now(),
          enabled = FALSE
      WHERE fact_table_id = p_fact_table_id;

      v_err = jsonb_strip_nulls(
            jsonb_build_object(
            'Message', v_errmsg,
            'Detail', case when v_errdetail = '' then null else v_errdetail end,
            'Hint', case when v_errhint = '' then null else v_errhint end,
            'Context', case when v_errcontext = '' then null else v_errcontext end)
            );

      INSERT INTO fact_loader.fact_table_refresh_logs (fact_table_id, refresh_attempted_at, refresh_finished_at, messages)
      VALUES (p_fact_table_id, now(), clock_timestamp(), v_err);

      RETURN FALSE;
  END;
ELSE
  RETURN FALSE;
END IF;

EXCEPTION
    WHEN serialization_failure THEN
        RAISE LOG 'Serialization failure on transaction % attempting to lock % - skipping.', txid_current()::text, p_fact_table_id::text;
        RETURN FALSE;
    WHEN OTHERS THEN
        RAISE;

END;

END;
$BODY$
LANGUAGE plpgsql;
