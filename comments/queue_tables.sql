COMMENT ON TABLE fact_loader.queue_tables IS 'Each queue table along with the base table to which it belongs.';
  COMMENT ON COLUMN fact_loader.queue_tables.queue_table_id IS 'Unique identifier for queue tables.';
  COMMENT ON COLUMN fact_loader.queue_tables.queue_table_relid IS 'The oid of the queue table itself regclass type to accept only valid relations.';
  COMMENT ON COLUMN fact_loader.queue_tables.queue_of_base_table_relid IS 'The oid of the base table for which the queue table contains an audited log of changes.  regclass type to accept only valid relations.';
  COMMENT ON COLUMN fact_loader.queue_tables.pglogical_node_if_id IS
  $$Optional - If NULL, we assume this is a local queue table and we need not synchronize time
  for potential replication delay.  For use with tables that are replicated via pglogical.
  This is the pglogical.node_interface of the table.  This also requires pglogical_ticker
  and is used to synchronize time and ensure we don't continue to move forward
  in time when replication is delayed for this queue table.$$;
  COMMENT ON COLUMN fact_loader.queue_tables.queue_table_tz IS
  $$**NOTE CAREFULLY** - If this is NULL, it assumes that changed_at in the queue
  tables is stored in TIMESTAMPTZ.  If it IS set, it assumes you are telling it that changed_at is
  of TIMESTAMP data type which is stored in the provided time zone of queue_table_tz.$$;
  COMMENT ON COLUMN fact_loader.queue_tables.row_created_at IS 'Timestamp of when this row was first created.';
  COMMENT ON COLUMN fact_loader.queue_tables.row_updated_at IS 'Timestamp of when this row was last updated (this is updated via trigger).';
  COMMENT ON COLUMN fact_loader.queue_tables.purge IS 'Default is true because we prune queue tables as data is no longer needed. Can be set to false and no pruning will happen on this table.';
