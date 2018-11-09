COMMENT ON TABLE fact_loader.fact_table_refresh_logs IS 'Used to log both job run times and exceptions.';
  COMMENT ON COLUMN fact_loader.fact_table_refresh_logs.fact_table_refresh_log_id IS 'Unique identifier,';
  COMMENT ON COLUMN fact_loader.fact_table_refresh_logs.fact_table_id IS 'Fact table that created the log.';
  COMMENT ON COLUMN fact_loader.fact_table_refresh_logs.refresh_attempted_at IS 'The time of the attempt (transaction begin time), which can be correlated to fact_table.last_refresh_attempted_at (see also unresolved_failures).';
  COMMENT ON COLUMN fact_loader.fact_table_refresh_logs.refresh_finished_at IS 'The transaction commit time of the attempt, which can be used with refresh_attempted_at to get actual run time.';
  COMMENT ON COLUMN fact_loader.fact_table_refresh_logs.messages IS 'Only for failures - Error message content in JSON format - including message, message detail, context, and hint.';
