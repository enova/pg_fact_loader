DROP VIEW fact_loader.queue_deps_all_with_retrieval;
DROP VIEW fact_loader.queue_deps_all;
DROP VIEW fact_loader.prioritized_jobs;

ALTER TABLE fact_loader.fact_tables ADD COLUMN pre_execute_hook_sql TEXT;
