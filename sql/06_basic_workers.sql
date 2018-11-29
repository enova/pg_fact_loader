SET client_min_messages TO warning;

-- Client time zone should not change functionality of worker - use a different one here
SET TIMEZONE TO 'UTC';

--Enable all except dep tables for now
UPDATE fact_loader.fact_tables ft SET enabled = TRUE
WHERE NOT EXISTS (SELECT 1 FROM fact_loader.fact_table_deps d WHERE d.child_id = ft.fact_table_id);

--Move the mock replication stream forward to now
SELECT pglogical_ticker.tick();

SELECT fact_loader.worker();
SELECT customer_id, phone, age, last_order_id, order_product_count, order_product_promo_ids
FROM test_fact.customers_fact
ORDER BY customer_id;

--test debugging feature on this table
SET log_min_messages TO debug3;
SELECT fact_loader.worker();
SELECT order_id, customer_id, order_date, total, is_reorder
FROM test_fact.orders_fact
ORDER BY order_id;
RESET log_min_messages;
DO $$
BEGIN
    IF NOT (SELECT COUNT(1) FROM fact_loader.debug_process_queue) = 3 THEN
        RAISE EXCEPTION '%', 'No worky';
    END IF; 
END$$;


SELECT fact_loader.worker();
SELECT order_id, customer_id, phone, age, max_order_date, min_total
FROM test_fact.customersorders_fact
ORDER BY order_id;

SELECT fact_loader.worker();
SELECT email_id, read, promo_count
FROM test_fact.emails_fact
ORDER BY email_id;

SELECT fact_loader.worker();
SELECT order_id, customer_id, order_date, total, is_reorder, num_emails, num_read
FROM test_fact.order_emails_fact
ORDER BY order_id;

SELECT fact_loader.worker();
SELECT customer_id, as_of_date, total_orders, last_order_date
FROM test_fact.customer_order_history_fact
ORDER BY customer_id, as_of_date;

-- Set time zone back to America/Chicago because the audit data is being logged at that time zone
-- (another great reason NEVER to use timestamp, but functionality we need at any rate)
SET TIMEZONE TO 'America/Chicago';
UPDATE test.customers SET age = 40 WHERE customer_id = 2;
-- We need to make deletes handled with recursive joins as well first before testing this
-- DELETE FROM test.customers WHERE customer_id = 3;
/****
This should not update the fact table, because the replication stream is behind these last 2 updates
 */
SELECT fact_loader.worker();
SELECT customer_id, phone, age, last_order_id, order_product_count, order_product_promo_ids
FROM test_fact.customers_fact
ORDER BY customer_id;

UPDATE fact_loader.fact_tables SET force_worker_priority = TRUE WHERE fact_table_relid = 'test_fact.customers_fact'::REGCLASS;
SELECT pglogical_ticker.tick();
SELECT fact_loader.worker();
SELECT customer_id, phone, age, last_order_id, order_product_count, order_product_promo_ids
FROM test_fact.customers_fact
ORDER BY customer_id;

--This would simulate an application's changes being out of order now
UPDATE test.customers SET age = 41 WHERE customer_id = 2;
SELECT pglogical_ticker.tick();
SELECT fact_loader.worker();

--Pretend the transaction for this began before the update above - by lowering the actual audit_id and tx time
UPDATE test.customers SET age = 42 WHERE customer_id = 2;
UPDATE test_audit_raw.customers_audit
SET customers_audit_id = customers_audit_id - 1000, changed_at = changed_at - interval '1 minute'
WHERE customers_audit_id = (SELECT MAX(customers_audit_id) FROM test_audit_raw.customers_audit);

--However, we assume fact_loader_batch_id is still in order because we have a single-threaded
--predicatable order with pglogical or a local queue table fed by pg_fact_loader 

--This will be missed by version 1.2, but not 1.3
SELECT pglogical_ticker.tick();
SELECT fact_loader.worker();

SELECT (age = 42) AS age_is_updated
FROM test_fact.customers_fact
WHERE customer_id = 2
ORDER BY customer_id;

ALTER EXTENSION pg_fact_loader UPDATE;

UPDATE fact_loader.fact_tables SET force_worker_priority = FALSE WHERE fact_table_relid = 'test_fact.customers_fact'::REGCLASS;
