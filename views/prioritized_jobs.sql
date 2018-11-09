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
