COMMENT ON TABLE fact_loader.key_retrieval_sequences IS
$$How to go from a change in the queue table itself to retrieve the key
that needs to be updated in the fact table.  That key specifically will be passed
to the insert/update/delete merge_proids configured in queue_table_deps.  When multiple joins
are required to get there, you will have more than one key_retrieval_sequence for a
single queue_table_dep.  You can also optionally have a different key_retrieval_sequence
if your insert/update/delete merge_proids don't all accept the exact same field as an arg.
NOTE - The regression suite in ./sql and ./expected has abundant examples of different configurations.$$;
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.key_retrieval_sequence_id IS 'Unique identifier.';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.queue_table_dep_id IS 'Which fact table - queue table record this is for (queue_table_deps)';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.filter_scope IS
  $$NULL or one of I, U, D.  Optional and likely rare.  By default, this key_retrieval_sequence
  will tell pg_fact_loader how to get the key for all events - insert, update, delete.
  But if your insert/update/delete merge_proids don't all accept the exact same field as an arg,
  you will have to tell it a different way to retrieve the different I, U, D events on separate rows.
  The regression suite has examples of this.$$;
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.level IS
  $$Default 1. When there are multiple joins required to retrieve a key,
  this indicates the order in which to perform the joins.  It will start at level 1,
  then the return_columns_from_join field will be used to join to the join_to_relation - join_to_column
  for the level 2 record, and so on.$$;
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.return_columns IS
  $$What field to return from the base table (if this is level 1), or (if this level 2+)
  this should be the same as the return_columns_from_join from the previous level.$$;
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.is_fact_key IS 'Only true if the base table itself contains the key. If return_columns contains the keys to pass into the functions without any additional join, TRUE.  Otherwise, FALSE if you need to join to get more information.';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.join_to_relation IS 'Join from the base table (or if this is level 2+, the join_to_relation from the previous level) to this table to get the key or to do yet a further join.';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.join_to_column IS 'Join to this column of join_to_relation.';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.return_columns_from_join IS 'Return these columns from join_to_relation.';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.join_return_is_fact_key IS 'If return_columns_from_join are your fact keys, true.  Otherwise false, and that means you need another level to get your key.';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.pass_queue_table_change_date_at_tz IS
  $$If this is set to a time zone, then the changed_at field will be cast to this time zone and then cast to a date,
  for the purpose of creating a date-range based fact table.
  For casting queue_table_timestamp to a date, we first ensure we have it as timestamptz (objective UTC time).
  Then, we cast it to the timezone of interest on which the date should be based.
  For example, 02:00:00 UTC time on 2018-05-02 is actually 2018-05-01 in America/Chicago time.
  Thus, any date-based fact table must decide in what time zone to consider the date.$$;
