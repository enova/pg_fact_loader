COMMENT ON TABLE fact_loader.fact_table_deps IS 'For queue-based fact tables that depend on other fact table changes ONLY. Add those dependencies here.';
  COMMENT ON COLUMN fact_loader.fact_table_deps.fact_table_dep_id IS 'Unique identifier.';
  COMMENT ON COLUMN fact_loader.fact_table_deps.parent_id IS 'The parent fact_table_id that the child depends on.';
  COMMENT ON COLUMN fact_loader.fact_table_deps.child_id IS 'The child fact_table_id that will run only after the parent is updated.';
  COMMENT ON COLUMN fact_loader.fact_table_deps.default_insert_merge_proid IS
  $$Default function to use for insert events to update child tables.
  This may need to be modified for each individual inherited fact_table_dep_queue_table_deps
  if that generalization isn't possible. See the regression suite in ./sql and ./expected for examples.$$;
  COMMENT ON COLUMN fact_loader.fact_table_deps.default_update_merge_proid IS
  $$Default function to use for update events to update child tables.
  This may need to be modified for each individual inherited fact_table_dep_queue_table_deps
  if that generalization isn't possible. See the regression suite in ./sql and ./expected for examples.$$;
  COMMENT ON COLUMN fact_loader.fact_table_deps.default_delete_merge_proid IS
  $$Default function to use for delete events to update child tables.
  This may need to be modified for each individual inherited fact_table_dep_queue_table_deps
  if that generalization isn't possible. See the regression suite in ./sql and ./expected for examples.$$;
  COMMENT ON COLUMN fact_loader.fact_table_deps.row_created_at IS 'Timestamp of when this row was first created.';
  COMMENT ON COLUMN fact_loader.fact_table_deps.row_updated_at IS 'Timestamp of when this row was last updated (this is updated via trigger).';
