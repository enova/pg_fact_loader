SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

/***
Based on our config, this should not create an ERROR but should not do anything.
 */
INSERT INTO test.orders (order_id, customer_id, order_date, total)
VALUES (5, NULL, '2018-04-10', 100.00);

SELECT COUNT(1) FROM test_audit_raw.orders_audit;

/****
We limit everything to this 1 table because the above grossly violates our schema and will create
errors on other tables.  We just want to verify that this actually runs without error when processed.
 */
UPDATE fact_loader.fact_tables SET force_worker_priority = TRUE WHERE fact_table_relid = 'test_fact.orders_fact'::REGCLASS;

SELECT test.tick();
SELECT fact_loader.worker() FROM generate_series(1,6);

SELECT order_id, customer_id, order_date, total, is_reorder
FROM test_fact.orders_fact
ORDER BY order_id;

SELECT fact_loader.purge_queues('0 seconds'::INTERVAL);

SELECT COUNT(1) FROM test_audit_raw.orders_audit;

TRUNCATE test_audit_raw.orders_audit;

UPDATE fact_loader.fact_tables SET force_worker_priority = FALSE WHERE fact_table_relid = 'test_fact.orders_fact'::REGCLASS;