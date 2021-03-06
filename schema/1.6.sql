DROP VIEW fact_loader.queue_deps_all_with_retrieval;
DROP VIEW fact_loader.queue_deps_all;
DROP VIEW fact_loader.prioritized_jobs;

-- Must ensure we have the fully schema-qualified regprod before converting to text
SET search_path TO '';
ALTER TABLE fact_loader.debug_process_queue ALTER COLUMN proid TYPE TEXT;
ALTER TABLE fact_loader.debug_process_queue ADD CONSTRAINT check_proid CHECK (COALESCE(proid::REGPROC, 'boolin') IS NOT NULL);
ALTER TABLE fact_loader.fact_table_dep_queue_table_deps ALTER COLUMN delete_merge_proid TYPE TEXT;
ALTER TABLE fact_loader.fact_table_dep_queue_table_deps ADD CONSTRAINT check_delete_merge_proid CHECK (COALESCE(delete_merge_proid::REGPROC, 'boolin') IS NOT NULL);
ALTER TABLE fact_loader.fact_table_dep_queue_table_deps ALTER COLUMN insert_merge_proid TYPE TEXT;
ALTER TABLE fact_loader.fact_table_dep_queue_table_deps ADD CONSTRAINT check_insert_merge_proid CHECK (COALESCE(insert_merge_proid::REGPROC, 'boolin') IS NOT NULL);
ALTER TABLE fact_loader.fact_table_dep_queue_table_deps ALTER COLUMN update_merge_proid TYPE TEXT;
ALTER TABLE fact_loader.fact_table_dep_queue_table_deps ADD CONSTRAINT check_update_merge_proid CHECK (COALESCE(update_merge_proid::REGPROC, 'boolin') IS NOT NULL);
ALTER TABLE fact_loader.fact_table_deps ALTER COLUMN default_delete_merge_proid TYPE TEXT;
ALTER TABLE fact_loader.fact_table_deps ADD CONSTRAINT check_default_delete_merge_proid CHECK (COALESCE(default_delete_merge_proid::REGPROC, 'boolin') IS NOT NULL);
ALTER TABLE fact_loader.fact_table_deps ALTER COLUMN default_insert_merge_proid TYPE TEXT;
ALTER TABLE fact_loader.fact_table_deps ADD CONSTRAINT check_default_insert_merge_proid CHECK (COALESCE(default_insert_merge_proid::REGPROC, 'boolin') IS NOT NULL);
ALTER TABLE fact_loader.fact_table_deps ALTER COLUMN default_update_merge_proid TYPE TEXT;
ALTER TABLE fact_loader.fact_table_deps ADD CONSTRAINT check_default_update_merge_proid CHECK (COALESCE(default_update_merge_proid::REGPROC, 'boolin') IS NOT NULL);
ALTER TABLE fact_loader.fact_tables ALTER COLUMN daily_scheduled_proid TYPE TEXT;
ALTER TABLE fact_loader.fact_tables ADD CONSTRAINT check_daily_scheduled_proid CHECK (COALESCE(daily_scheduled_proid::REGPROC, 'boolin') IS NOT NULL);
ALTER TABLE fact_loader.fact_tables ALTER COLUMN fact_table_agg_proid TYPE TEXT;
ALTER TABLE fact_loader.fact_tables ADD CONSTRAINT check_fact_table_agg_proid CHECK (COALESCE(fact_table_agg_proid::REGPROC, 'boolin') IS NOT NULL);
ALTER TABLE fact_loader.queue_table_deps ALTER COLUMN delete_merge_proid TYPE TEXT;
ALTER TABLE fact_loader.queue_table_deps ADD CONSTRAINT check_delete_merge_proid CHECK (COALESCE(delete_merge_proid::REGPROC, 'boolin') IS NOT NULL);
ALTER TABLE fact_loader.queue_table_deps ALTER COLUMN insert_merge_proid TYPE TEXT;
ALTER TABLE fact_loader.queue_table_deps ADD CONSTRAINT check_insert_merge_proid CHECK (COALESCE(insert_merge_proid::REGPROC, 'boolin') IS NOT NULL);
ALTER TABLE fact_loader.queue_table_deps ALTER COLUMN update_merge_proid TYPE TEXT;
ALTER TABLE fact_loader.queue_table_deps ADD CONSTRAINT check_update_merge_proid CHECK (COALESCE(update_merge_proid::REGPROC, 'boolin') IS NOT NULL);
RESET search_path;
