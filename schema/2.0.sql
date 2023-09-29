DROP VIEW fact_loader.queue_deps_all_with_retrieval;
DROP VIEW fact_loader.queue_deps_all;
DROP FUNCTION fact_loader.safely_terminate_workers(); 
DROP FUNCTION fact_loader.launch_workers(int);
DROP FUNCTION fact_loader.launch_worker();
DROP FUNCTION fact_loader._launch_worker(oid);
DROP FUNCTION fact_loader.queue_table_delay_info();
DROP FUNCTION fact_loader.logical_subscription();
CREATE TYPE fact_loader.driver AS ENUM ('pglogical', 'native');
