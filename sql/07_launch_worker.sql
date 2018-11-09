SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

--Make one change to guarantee we want to see a fact table change, ensure rep stream is up to date
UPDATE test.customers SET phone = '0001234577' WHERE customer_id = 10;
SELECT pglogical_ticker.tick();

--Ensure this one table is prioritized
UPDATE fact_loader.fact_tables SET force_worker_priority = TRUE WHERE fact_table_relid = 'test_fact.customers_fact'::REGCLASS;

DROP TABLE IF EXISTS foo_worker;
CREATE TEMP TABLE foo_worker AS
SELECT fact_loader.launch_worker() AS pid;
--This should be enough time for the worker to run once.
SELECT pg_sleep(3);

SELECT customer_id, phone, age, last_order_id, order_product_count, order_product_promo_ids
FROM test_fact.customers_fact
ORDER BY customer_id;

UPDATE fact_loader.fact_tables SET force_worker_priority = FALSE WHERE fact_table_relid = 'test_fact.customers_fact'::REGCLASS;

--Terminate the worker
SELECT pg_terminate_backend(pid) FROM foo_worker;

--Let it shutdown
SELECT pg_sleep(1);