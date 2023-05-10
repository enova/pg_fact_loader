SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

DELETE FROM test.customers WHERE customer_id = 3;
SELECT pg_sleep(1);
SELECT test.tick();

SELECT fact_loader.worker();
SELECT fact_loader.worker();
SELECT fact_loader.worker();
SELECT fact_loader.worker();
SELECT fact_loader.worker();
SELECT fact_loader.worker();
SELECT fact_loader.worker();

SELECT customer_id, phone, age, last_order_id, order_product_count, order_product_promo_ids
FROM test_fact.customers_fact
ORDER BY customer_id;

SELECT order_id, customer_id, order_date, total, is_reorder
FROM test_fact.orders_fact
ORDER BY order_id;

SELECT order_id, customer_id, phone, age, max_order_date, min_total
FROM test_fact.customersorders_fact
ORDER BY order_id;

SELECT email_id, read, promo_count
FROM test_fact.emails_fact
ORDER BY email_id;

SELECT order_id, customer_id, order_date, total, is_reorder, num_emails, num_read
FROM test_fact.order_emails_fact
ORDER BY order_id;

SELECT customer_id, as_of_date, total_orders, last_order_date
FROM test_fact.customer_order_history_fact
ORDER BY customer_id, as_of_date;

SELECT customer_id, rows_in_customersorders_fact
FROM test_fact.customersorders_summary_fact
ORDER BY customer_id;

SELECT COUNT(1) FROM test_audit_raw.customers_audit;

--We call this explicitly, because the worker will take the default add_interval of 1 hour, thus
--won't see any actual purging in the test suite.
SELECT fact_loader.purge_queues('0 seconds'::INTERVAL);

SELECT COUNT(1) FROM test_audit_raw.customers_audit;

DELETE FROM test.reorders;
SELECT pg_sleep(1);
SELECT test.tick();

SELECT fact_loader.worker();
SELECT fact_loader.worker();
SELECT fact_loader.worker();
SELECT fact_loader.worker();
SELECT fact_loader.worker();
SELECT fact_loader.worker();

SELECT order_id, customer_id, order_date, total, is_reorder
FROM test_fact.orders_fact
ORDER BY order_id;

SELECT order_id, customer_id, order_date, total, is_reorder, num_emails, num_read
FROM test_fact.order_emails_fact
ORDER BY order_id;
