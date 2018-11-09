CREATE OR REPLACE FUNCTION fact_loader.safely_terminate_workers()
RETURNS TABLE (number_terminated INT, number_still_live INT, pids_still_live INT[]) AS
$BODY$
/****
It is not a requirement to use this function to terminate workers.  Because workers are transactional,
you can simply terminate them and no data loss will result in pg_fact_loader.

Likewise, a hard crash of any system using pg_fact_loader will recover just fine upon re-launching workers.

Still, it is ideal to avoid bloat to cleanly terminate workers and restart them using this function to kill
them, and launch_workers(int) to re-launch them.
 */
BEGIN

RETURN QUERY
WITH try_term_pids AS (
SELECT
  pid,
  CASE WHEN
    state = 'idle' AND
    state_change BETWEEN SYMMETRIC
      now() - interval '5 seconds' AND
      now() - interval '55 seconds'
    THEN
    pg_terminate_backend(pid)
    ELSE FALSE
  END AS terminated
FROM pg_stat_activity
WHERE usename = 'postgres'
  AND query = 'SELECT fact_loader.worker();')

SELECT SUM(CASE WHEN terminated THEN 1 ELSE 0 END)::INT AS number_terminated_out,
  SUM(CASE WHEN NOT terminated THEN 1 ELSE 0 END)::INT AS number_still_live_out,
  (SELECT array_agg(pid) FROM try_term_pids WHERE NOT terminated) AS pids_still_live_out
FROM try_term_pids;

END;
$BODY$
LANGUAGE plpgsql;
