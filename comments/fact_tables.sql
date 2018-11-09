COMMENT ON TABLE fact_loader.fact_tables IS 'Each fact table to be built via pg_fact_loader, which also drives the worker.  These are also referred to as "jobs".';
  COMMENT ON COLUMN fact_loader.fact_tables.fact_table_id IS 'Unique identifier for the fact table or job - also referred to as job_id';
  COMMENT ON COLUMN fact_loader.fact_tables.fact_table_relid IS 'The oid of the fact table itself regclass type to accept only valid relations.';
  COMMENT ON COLUMN fact_loader.fact_tables.fact_table_agg_proid IS
  $$NOT REQUIRED.  The aggregate function definition for the fact table.
  This can be used when passed to create_table_loader_function to auto-create a merge function.
  It can also be a reference for dq checks because it indicates what function returns
  the correct results for a fact table as it should appear now.$$;
  COMMENT ON COLUMN fact_loader.fact_tables.enabled IS 'Indicates whether or not the job is enabled.  The worker will skip this table unless marked TRUE.';
  COMMENT ON COLUMN fact_loader.fact_tables.priority IS 'Determines the order in which the job runs (in combination with other sorting factors)';
  COMMENT ON COLUMN fact_loader.fact_tables.force_worker_priority IS 'If marked TRUE, this fact table will be prioritized in execution order above all other factors.';
  COMMENT ON COLUMN fact_loader.fact_tables.last_refresh_source_cutoff IS 'The data cutoff time of the last refresh - only records older than this have been updated.';
  COMMENT ON COLUMN fact_loader.fact_tables.last_refresh_attempted_at IS 'The last time the worker ran on this fact table.  The oldest will be prioritized first, ahead of priority.';
  COMMENT ON COLUMN fact_loader.fact_tables.last_refresh_succeeded IS 'Whether or not the last run of the job succeeded.  NULL if it has never been run.';
  COMMENT ON COLUMN fact_loader.fact_tables.row_created_at IS 'Timestamp of when this row was first created.';
  COMMENT ON COLUMN fact_loader.fact_tables.row_updated_at IS 'Timestamp of when this row was last updated (this is updated via trigger).';
  COMMENT ON COLUMN fact_loader.fact_tables.use_daily_schedule IS 'If TRUE, this job is scheduled to run daily instead of using queue tables according to other daily column configuration.  Also must be marked TRUE for dependent jobs.';
  COMMENT ON COLUMN fact_loader.fact_tables.daily_scheduled_time IS 'The time of day *after which* to run the job (the system will attempt to run until midnight). If you have a chain of daily scheduled jobs, only the base job has time filled in.';
  COMMENT ON COLUMN fact_loader.fact_tables.daily_scheduled_tz IS 'The timezone your time is in.  This is critical to know when to allow a daily refresh from the standpoint of the business logic you require for a timezone-based date.';
  COMMENT ON COLUMN fact_loader.fact_tables.daily_scheduled_proid IS
  $$The single function oid to execute at the scheduled time.  No arguments supported. It is assumed to contain all the
  logic necessary to add any new daily entries, if applicable.  See the unit tests in sql/16_1_2_features.sql for examples.$$;
  COMMENT ON COLUMN fact_loader.fact_tables.depends_on_base_daily_job_id IS 'For jobs that depend on other daily scheduled jobs only. This is the fact_table_id of the FIRST job in a chain which is actually the only one with a scheduled_time.';
  COMMENT ON COLUMN fact_loader.fact_tables.depends_on_parent_daily_job_id IS 'For jobs that depend on other daily scheduled jobs only. Immediate parent which must complete before this job will run.';
  COMMENT ON COLUMN fact_loader.fact_tables.daily_scheduled_deps IS 'OPTIONAL for daily scheduled jobs.  The only purpose of this column is to consider if we should wait to run a scheduled job because dependent tables are out of date.  This is a regclass array of tables that this scheduled job depends on, which will only be considered if they are either listed in fact_loader.queue_tables or fact_loader.fact_tables.  If the former, replication delay will be considered (if table is not local).  If the latter, last_refresh_source_cutoff will be considered.  Works in combination with daily_scheduled_dep_delay_tolerance which says how much time delay is tolerated.  Job will FAIL if the time delay constraint is not met for all tables - this is intended to be configured as a rare occurrence and thus we want to raise an alarm about it.';
  COMMENT ON COLUMN fact_loader.fact_tables.daily_scheduled_dep_delay_tolerance IS 'OPTIONAL for daily scheduled jobs.  Amount of time interval allowed that dependent tables can be out of date before running this job.  For example, if 10 minutes, then if ANY of the dependent tables are more than 10 minutes out of date, this job will FAIL if the time delay constraint is not met for all tables - this is intended to be configured as a rare occurrence and thus we want to raise an alarm about it.';
