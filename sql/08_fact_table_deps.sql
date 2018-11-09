SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

--Enable the dep tables - which thereby should be placed first in line!
UPDATE fact_loader.fact_tables ft SET enabled = TRUE
WHERE EXISTS (SELECT 1 FROM fact_loader.fact_table_deps d WHERE d.child_id = ft.fact_table_id);

SELECT fact_loader.worker();
SELECT order_id, customer_id, phone, age, max_order_date, min_total
FROM test_fact.customersorders_fact
ORDER BY order_id;

SELECT fact_loader.worker();
SELECT order_id, customer_id, order_date, total, is_reorder, num_emails, num_read
FROM test_fact.order_emails_fact
ORDER BY order_id;

/****
Nested fact table deps
 */
SELECT fact_loader.worker();
SELECT customer_id, rows_in_customersorders_fact
FROM test_fact.customersorders_summary_fact
ORDER BY customer_id;
