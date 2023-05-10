SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

/***
Try this odd case to be sure we process all events in order correctly 
***/
UPDATE test.orders
SET total = 1000.00
WHERE order_id = 3;

DELETE FROM test.orders WHERE order_id = 3;

INSERT INTO test.orders (order_id, customer_id, order_date, total)
VALUES (3, 5, '2018-04-12', 2000.00);

--Move the mock replication stream forward to now
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
