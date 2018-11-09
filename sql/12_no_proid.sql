SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

/****
This makes no sense in reality for the fact table, but we are trying to simulate the potential issue
 */
WITH to_update AS
(SELECT qtd.queue_table_dep_id
FROM fact_loader.queue_table_deps qtd
INNER JOIN fact_loader.fact_tables ft ON ft.fact_table_id = qtd.fact_table_id
INNER JOIN fact_loader.queue_tables qt ON qt.queue_table_id = qtd.queue_table_id
WHERE ft.fact_table_relid = 'test_fact.emails_fact'::REGCLASS
  AND qt.queue_table_relid = 'test_audit_raw.emails_audit'::REGCLASS)

UPDATE fact_loader.queue_table_deps qtd
SET insert_merge_proid = NULL
FROM to_update tu WHERE tu.queue_table_dep_id = qtd.queue_table_dep_id;

--We have configured for this NOT to show up as a change to the fact table
INSERT INTO test.emails (email_id, customer_id, read)
VALUES (2, 6, true),
  (3, 7, false);

--The bug would have caused this to be missed
UPDATE test.emails SET read = FALSE WHERE email_id = 1;

--We have configured for this NOT to show up as a change to the fact table
INSERT INTO test.emails (email_id, customer_id, read)
VALUES (4, 8, true),
  (5, 9, false);

SELECT pglogical_ticker.tick();
SELECT fact_loader.worker() FROM generate_series(1,6);

SELECT email_id, read, promo_count
FROM test_fact.emails_fact
ORDER BY email_id;

SELECT pglogical_ticker.tick();
SELECT fact_loader.worker() FROM generate_series(1,6);

SELECT fact_loader.purge_queues('0 seconds'::INTERVAL);
SELECT COUNT(1) FROM test_audit_raw.emails_audit;

/****
Now fix what we broke
 */
WITH to_update AS
(SELECT qtd.queue_table_dep_id
FROM fact_loader.queue_table_deps qtd
INNER JOIN fact_loader.fact_tables ft ON ft.fact_table_id = qtd.fact_table_id
INNER JOIN fact_loader.queue_tables qt ON qt.queue_table_id = qtd.queue_table_id
WHERE ft.fact_table_relid = 'test_fact.emails_fact'::REGCLASS
  AND qt.queue_table_relid = 'test_audit_raw.emails_audit'::REGCLASS)

UPDATE fact_loader.queue_table_deps qtd
SET insert_merge_proid = 'test_fact.emails_fact_merge'::REGPROC
FROM to_update tu WHERE tu.queue_table_dep_id = qtd.queue_table_dep_id;

SELECT test_fact.emails_fact_merge(email_id)
FROM test.emails;

SELECT test_fact.order_emails_fact_merge(customer_id)
FROM test.customers c
WHERE EXISTS (SELECT 1 FROM test.emails e WHERE e.customer_id = c.customer_id);

SELECT email_id, read, promo_count
FROM test_fact.emails_fact
ORDER BY email_id;

SELECT order_id, customer_id, order_date, total, is_reorder, num_emails, num_read
FROM test_fact.order_emails_fact
ORDER BY order_id;