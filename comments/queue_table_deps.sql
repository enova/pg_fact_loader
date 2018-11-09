COMMENT ON TABLE fact_loader.queue_table_deps IS
$$Ties together which fact tables depend on which queue tables, along with holding
information on the last cutoff ids for each queue table.  **NOTE** that anything that exists in
queue_table_dep is assumed to be require its queue data not to be pruned even if the fact_tables
job is disabled.  That means that even if a job is disabled, you will not lose data, but you will also
have your queue tables building up in size until you either enable (successfully) or drop the job.
The regression suite in ./sql and ./expected has abundant examples of different configurations.$$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.queue_table_dep_id IS 'Unique identifier.';
  COMMENT ON COLUMN fact_loader.queue_table_deps.fact_table_id IS 'Fact table to tie together with a queue table it depends on.';
  COMMENT ON COLUMN fact_loader.queue_table_deps.queue_table_id IS 'Queue table to tie together with a fact table that needs its changes.';
  COMMENT ON COLUMN fact_loader.queue_table_deps.relevant_change_columns IS
  $$Optional. For UPDATE changes to data, you can specify to only consider changes
  to these columns as sufficient to update the fact table.
  If NULL, all columns will be considered as potentially changing the fact table data.$$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.last_cutoff_id IS
  $$The last fact_loader_batch_id of the queue table that was processed for this queue table - fact table pair.
  After this job runs, records that have this id and lower are eligible to be pruned,
  assuming no other fact tables also depend on those same records.
  The next time the job runs, only records after this id are considered.$$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.last_cutoff_source_time IS
  $$The source data change time of the last queue table record that was processed for this
  queue table - fact table pair.  This helps pg_fact_loader synchronize time across
  multiple queue tables and only pull changes that are early enough, and not purge
  records that are later than these cutoff times.  THIS DOES NOT DETERMINE filter conditions
  for the starting point at which to pull new records as does last_cutoff_id - it is only
  used as an ending-point barrier.
  $$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.insert_merge_proid IS
  $$Function oid to execute on insert events - accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - fact table pair
  is configured in key_retrieval_sequences. NULL to ignore insert events.$$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.update_merge_proid IS
  $$Function oid to execute on update events - accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - fact table pair
  is configured in key_retrieval_sequences. NULL to ignore update events.$$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.delete_merge_proid IS
  $$Function oid to execute on delete events - accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - fact table pair
  is configured in key_retrieval_sequences. NULL to ignore delete events.$$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.row_created_at IS 'Timestamp of when this row was first created.';
  COMMENT ON COLUMN fact_loader.queue_table_deps.row_updated_at IS 'Timestamp of when this row was last updated (this is updated via trigger).';
