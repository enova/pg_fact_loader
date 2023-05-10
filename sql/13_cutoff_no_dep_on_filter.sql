SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

/***
Based on our config, no actual changes will be processed based on these updates.
But we still want the queue to be cleared.
 */
UPDATE test.customers SET customer_number = customer_number||'1';

SELECT COUNT(1) FROM test_audit_raw.customers_audit;

SELECT test.tick();
SELECT fact_loader.worker() FROM generate_series(1,6);

--Should now handle dep fact tables
SELECT test.tick();
SELECT fact_loader.worker() FROM generate_series(1,6);
SELECT test.tick();
SELECT fact_loader.worker() FROM generate_series(1,6);

SELECT fact_loader.purge_queues('0 seconds'::INTERVAL);
SELECT COUNT(1) FROM test_audit_raw.customers_audit;

