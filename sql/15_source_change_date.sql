SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

/****
This example tests not only using the queue_table timestamp to build a date-based table,
but doing that in a different time zone than we might expect, just to show the functionality.

So we are going to show a history table of customers from the perspective of the UK
 */
CREATE TABLE test_fact.customers_history_uktime_fact (customer_id INT, as_of_date DATERANGE, customer_number text, phone TEXT, age INT, PRIMARY KEY (customer_id, as_of_date));
CREATE FUNCTION test_fact.customers_history_uktime_fact_merge(p_customer_id INT, p_as_of_date DATE)
RETURNS VOID AS
$BODY$
BEGIN

  WITH it_really_changed AS (
  SELECT customer_id, daterange(p_as_of_date, 'infinity') AS as_of_date, customer_number, phone, age FROM test.customers WHERE customer_id = p_customer_id
  EXCEPT
  SELECT customer_id, as_of_date, customer_number, phone, age FROM test_fact.customers_history_uktime_fact
  WHERE customer_id = p_customer_id
    AND upper(as_of_date) = 'infinity'
  )

  , ended_last_fact AS
  (UPDATE test_fact.customers_history_uktime_fact f
      SET as_of_date = daterange(lower(f.as_of_date), lower(irc.as_of_date))
   FROM it_really_changed irc
    WHERE f.customer_id = irc.customer_id
      AND lower(f.as_of_date) <> lower(irc.as_of_date)
      AND upper(f.as_of_date) = 'infinity'
    RETURNING *)

  INSERT INTO test_fact.customers_history_uktime_fact AS f
    (customer_id, as_of_date, customer_number, phone, age)
  SELECT
    customer_id,
    as_of_date,
    customer_number,
    phone,
    age
  FROM it_really_changed nes
  ON CONFLICT (customer_id, as_of_date)
  DO UPDATE
  SET
    customer_number = EXCLUDED.customer_number
    , phone = EXCLUDED.phone
    , age = EXCLUDED.age;

END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION test_fact.customers_history_uktime_fact_delete(p_customer_id INT)
RETURNS VOID AS
$BODY$
BEGIN

DELETE FROM test_fact.customers_history_uktime_fact WHERE customer_id = p_customer_id;

END;
$BODY$
LANGUAGE plpgsql;

INSERT INTO fact_loader.fact_tables
(fact_table_relid, priority)
VALUES ('test_fact.customers_history_uktime_fact'::REGCLASS, 8);

WITH queue_tables_with_proids AS (
  SELECT
    *,
    'test_fact.customers_history_uktime_fact_merge'::REGPROC AS insert_merge_proid,
    'test_fact.customers_history_uktime_fact_merge'::REGPROC AS update_merge_proid,
    'test_fact.customers_history_uktime_fact_delete'::REGPROC AS delete_merge_proid
  FROM fact_loader.queue_tables
  WHERE queue_of_base_table_relid IN
    /***
    These are the tables that are involved in test_fact.customers_fact_aggregator
    Find this out for each function in order to properly configure all possible changes
    that could affect the tables
     */
        ('test.customers'::REGCLASS)
  )

  INSERT INTO fact_loader.queue_table_deps
  (fact_table_id, queue_table_id, insert_merge_proid, update_merge_proid, delete_merge_proid)
  SELECT
    fact_table_id, queue_tables_with_proids.queue_table_id, insert_merge_proid, update_merge_proid, delete_merge_proid
  FROM fact_loader.fact_tables
  CROSS JOIN queue_tables_with_proids
  WHERE fact_table_relid = 'test_fact.customers_history_uktime_fact'::REGCLASS;

  --Key retrieval for updates
  INSERT INTO fact_loader.key_retrieval_sequences (
    queue_table_dep_id,
    filter_scope,
    level,
    return_columns,
    is_fact_key,
    join_to_relation,
    join_to_column,
    return_columns_from_join,
    join_return_is_fact_key,
    pass_queue_table_change_date_at_tz)
  SELECT
    queue_table_dep_id,
    evts.evt,
    1,
    '{customer_id}'::name[],
    true,
    null,
    null,
    null::name[],
    null::boolean,
    --THIS is the key of which time zone the date is seen from
    'Europe/London'::TEXT
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
  INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
  CROSS JOIN (VALUES ('I'),('U')) evts (evt)
  WHERE fact_table_relid = 'test_fact.customers_history_uktime_fact'::REGCLASS
    AND queue_of_base_table_relid IN('test.customers'::REGCLASS)
  UNION ALL
  SELECT
    queue_table_dep_id,
    'D',
    1,
    '{customer_id}'::name[],
    true,
    null,
    null,
    null::name[],
    null::boolean,
    null::TEXT
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
  INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
  WHERE fact_table_relid = 'test_fact.customers_history_uktime_fact'::REGCLASS
    AND queue_of_base_table_relid IN('test.customers'::REGCLASS);

SELECT test_fact.customers_history_uktime_fact_merge(customer_id, '2018-04-22'::DATE)
FROM test.customers;

UPDATE test.customers SET customer_number = customer_number||'a' WHERE customer_id BETWEEN 1 AND 5;
UPDATE test.customers SET customer_number = customer_number||'b' WHERE customer_id BETWEEN 1 AND 5;
UPDATE test.customers SET customer_number = customer_number||'c' WHERE customer_id BETWEEN 6 AND 10;
UPDATE test.customers SET customer_number = customer_number||'d' WHERE customer_id BETWEEN 6 AND 10;
UPDATE test.customers SET customer_number = customer_number||'e' WHERE customer_id BETWEEN 1 AND 5;

/****
Now we have to mock that this actually happened on different days.
 */
UPDATE test_audit_raw.customers_audit SET changed_at = '2018-04-24'::DATE WHERE change ->> 'customer_number' ~ '1a$';
UPDATE test_audit_raw.customers_audit SET changed_at = '2018-04-24'::DATE WHERE change ->> 'customer_number' ~ '1ab$';
UPDATE test_audit_raw.customers_audit SET changed_at = '2018-04-25'::DATE WHERE change ->> 'customer_number' ~ '1c$';
UPDATE test_audit_raw.customers_audit SET changed_at = '2018-04-26'::DATE WHERE change ->> 'customer_number' ~ '1cd$';
UPDATE test_audit_raw.customers_audit SET changed_at = '2018-04-27'::DATE WHERE change ->> 'customer_number' ~ '1abe$';

--Ensure this one table is prioritized
UPDATE fact_loader.fact_tables SET force_worker_priority = TRUE, enabled = TRUE WHERE fact_table_relid = 'test_fact.customers_history_uktime_fact'::REGCLASS;

SELECT test.tick();

DO $$
BEGIN
    IF NOT (SELECT COUNT(1) FROM fact_loader.gathered_queued_changes((SELECT fact_table_id FROM fact_loader.fact_tables WHERE fact_table_relid = 'test_fact.customers_history_uktime_fact'::REGCLASS))) = 18 THEN
        RAISE EXCEPTION '%', 'No worky';
    END IF;
END$$;

SELECT fact_loader.worker();

SELECT * FROM test_fact.customers_history_uktime_fact ORDER BY upper(as_of_date), customer_id;

--Let's verify the current records are the same as the actual table
SELECT customer_id, customer_number, phone, age FROM test.customers
INTERSECT
SELECT customer_id, customer_number, phone, age FROM test_fact.customers_history_uktime_fact
WHERE upper(as_of_date) = 'infinity'
ORDER BY customer_id;

UPDATE fact_loader.fact_tables SET force_worker_priority = FALSE WHERE fact_table_relid = 'test_fact.customers_history_uktime_fact'::REGCLASS;
