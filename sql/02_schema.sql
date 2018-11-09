SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

DROP SCHEMA IF EXISTS test, test_fact, audit, test_audit_raw CASCADE ;
TRUNCATE fact_loader.fact_tables CASCADE;
TRUNCATE fact_loader.queue_tables CASCADE;

--We use no serial/identity types here purely to be able to have consistency across multiple re-testing
CREATE SCHEMA test;
CREATE TABLE test.customers (customer_id INT PRIMARY KEY, customer_number text, phone TEXT, age INT);
CREATE TABLE test.orders (order_id INT PRIMARY KEY, customer_id INT REFERENCES test.customers (customer_id) ON DELETE CASCADE, order_date DATE, total NUMERIC(10,2), row_updated_at TIMESTAMPTZ);
CREATE TABLE test.emails (email_id INT PRIMARY KEY, customer_id INT REFERENCES test.customers (customer_id) ON DELETE CASCADE, read BOOLEAN);
CREATE TABLE test.promos (promo_id INT PRIMARY KEY, description TEXT);
CREATE TABLE test.email_promos (email_promo_id INT PRIMARY KEY, email_id INT REFERENCES test.emails (email_id) ON DELETE CASCADE, promo_id INT REFERENCES test.promos (promo_id) ON DELETE CASCADE);
CREATE TABLE test.products (product_id INT PRIMARY KEY, product_name NAME);

CREATE TABLE test.order_products (order_product_id INT PRIMARY KEY, order_id INT REFERENCES test.orders (order_id) ON DELETE CASCADE, product_id INT REFERENCES test.products (product_id) ON DELETE CASCADE);

--This table will test having to do multiple joins from changes to a table - join to orders, join to customers, in order to update customers_fact
CREATE TABLE test.order_product_promos (order_product_promo_id INT PRIMARY KEY, order_product_id INT NOT NULL REFERENCES test.order_products (order_product_id) ON DELETE CASCADE, promo_id INT NOT NULL REFERENCES test.promos (promo_id) ON DELETE CASCADE);

--This table will test multiple columns referring to a key of a fact table (orders.order_id)
CREATE TABLE test.reorders (reorder_id INT PRIMARY KEY, base_order_id INT REFERENCES test.orders (order_id) ON DELETE CASCADE, reorder_from_id INT REFERENCES test.orders (order_id) ON DELETE CASCADE, reorder_to_id INT REFERENCES test.orders (order_id) ON DELETE CASCADE);

CREATE SCHEMA test_fact;
CREATE TABLE test_fact.customers_fact (customer_id INT PRIMARY KEY, phone TEXT, age INT, last_order_id INT, order_product_count INT, order_product_promo_ids INT[], row_updated_at TIMESTAMPTZ);
CREATE TABLE test_fact.orders_fact (order_id INT PRIMARY KEY, customer_id INT, order_date DATE, total NUMERIC(10,2), is_reorder BOOLEAN, row_updated_at TIMESTAMPTZ);

--This is a silly dependent fact table definition, but will test correct updating of a fact table that depends on other fact tables
CREATE TABLE test_fact.customersorders_fact (order_id INT PRIMARY KEY, customer_id INT, phone TEXT, age INT, max_order_date DATE, min_total NUMERIC(10,2), row_updated_at TIMESTAMPTZ);

--This fact table def is an example of both a fact and base table dependency
CREATE TABLE test_fact.order_emails_fact (order_id INT PRIMARY KEY, customer_id INT, order_date DATE, total NUMERIC(10,2), is_reorder BOOLEAN, num_emails INT, num_read INT, row_updated_at TIMESTAMPTZ);

--This fact table tests nested fact table deps
CREATE TABLE test_fact.customersorders_summary_fact (customer_id INT PRIMARY KEY, rows_in_customersorders_fact INT);

--This fact table depends only on customers, which other fact tables depend on, and also emails, which the customers and test_fact.orders_fact do not depend on
CREATE TABLE test_fact.emails_fact (email_id INT PRIMARY KEY, read BOOLEAN, promo_count INT);

--This is to test range value tables
CREATE TABLE test_fact.customer_order_history_fact (as_of_date daterange, customer_id INT, total_orders INT, last_order_date DATE, row_updated_at TIMESTAMPTZ, PRIMARY KEY (customer_id, as_of_date));

CREATE OR REPLACE FUNCTION test_fact.customers_fact_aggregator(p_customer_id INT)
RETURNS SETOF test_fact.customers_fact
AS
$BODY$
BEGIN
  RETURN QUERY
  SELECT customer_id, phone, age, os.last_order_id, ops.order_product_count::INT, oppi.order_product_promo_ids, now() AS row_updated_at
  FROM test.customers c
  LEFT JOIN LATERAL
    (SELECT MAX(order_id) AS last_order_id
    FROM test.orders o
    WHERE o.customer_id = c.customer_id) os ON TRUE
  LEFT JOIN LATERAL
    (SELECT COUNT(1) AS order_product_count
     FROM test.orders o
     INNER JOIN test.order_products op ON op.order_id = o.order_id
     WHERE o.customer_id = c.customer_id
    ) ops ON TRUE
  LEFT JOIN LATERAL
    (SELECT array_agg(opp.promo_id ORDER BY opp.promo_id) AS order_product_promo_ids
     FROM test.order_product_promos opp
    INNER JOIN test.order_products op ON opp.order_product_id = op.order_product_id
    INNER JOIN test.orders o ON op.order_id = o.order_id
    WHERE o.customer_id = c.customer_id) oppi ON TRUE
  WHERE customer_id = p_customer_id;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION test_fact.customers_fact_delete(p_customer_id INT)
RETURNS VOID
AS
$BODY$
BEGIN

  DELETE
  FROM test_fact.customers_fact c
  WHERE customer_id = p_customer_id;

END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION test_fact.orders_fact_aggregator(p_customer_id INT)
RETURNS SETOF test_fact.orders_fact
AS
$BODY$
BEGIN
  RETURN QUERY
  SELECT order_id, customer_id, order_date, total, is_reorder, now() AS row_updated_at
  FROM test.orders o
  LEFT JOIN LATERAL
    (SELECT EXISTS (SELECT 1 FROM test.reorders ro WHERE ro.reorder_to_id = o.order_id) AS is_reorder) ros ON TRUE
  WHERE customer_id = p_customer_id;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION test_fact.orders_fact_delete(p_order_id INT)
RETURNS VOID
AS
$BODY$
BEGIN

  DELETE
  FROM test_fact.orders_fact c
  WHERE order_id = p_order_id;

END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_fact.customersorders_fact_aggregator(p_customer_id INT)
RETURNS SETOF test_fact.customersorders_fact
AS
$BODY$
BEGIN
  RETURN QUERY
  SELECT order_id, customer_id, phone, age, MAX(order_date), MIN(total)::NUMERIC(10,2), now() AS row_updated_at
  FROM test_fact.customers_fact ff
  INNER JOIN test_fact.orders_fact bf USING (customer_id)
  WHERE ff.customer_id = p_customer_id
  GROUP BY order_id, customer_id, phone, age;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION test_fact.customersorders_fact_delete(p_customer_id INT)
RETURNS VOID
AS
$BODY$
BEGIN

  DELETE
  FROM test_fact.customersorders_fact c
  WHERE customer_id = p_customer_id;

END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_fact.customersorders_summary_fact_aggregator(p_customer_id INT)
RETURNS SETOF test_fact.customersorders_summary_fact
AS
$BODY$
BEGIN
  RETURN QUERY
  SELECT customer_id, COUNT(1)::INT AS rows_in_customersorders_fact
  FROM test_fact.customersorders_fact
  WHERE customer_id = p_customer_id
  GROUP BY customer_id;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION test_fact.customersorders_summary_fact_delete(p_customer_id INT)
RETURNS VOID
AS
$BODY$
BEGIN

  DELETE
  FROM test_fact.customersorders_summary_fact c
  WHERE customer_id = p_customer_id;

END;
$BODY$
LANGUAGE plpgsql;

/***
This fact table def is an example of both a fact and base table dependency
 */
CREATE OR REPLACE FUNCTION test_fact.order_emails_fact_aggregator(p_customer_id INT)
RETURNS SETOF test_fact.order_emails_fact
AS
$BODY$
BEGIN
  RETURN QUERY
  SELECT order_id, customer_id, order_date, total, is_reorder, es.num_emails::INT, es.num_read::INT, now() AS row_updated_at
  FROM test_fact.orders_fact of
  LEFT JOIN LATERAL
    (SELECT COUNT(1) AS num_emails, SUM(CASE WHEN read THEN 1 ELSE 0 END) AS num_read
     FROM test.emails e
     WHERE e.customer_id = of.customer_id) es ON TRUE
  WHERE of.customer_id = p_customer_id;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION test_fact.order_emails_fact_delete(p_order_id INT)
RETURNS VOID
AS
$BODY$
BEGIN

  DELETE
  FROM test_fact.order_emails_fact c
  WHERE order_id = p_order_id;

END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_fact.emails_fact_aggregator(p_email_id INT)
RETURNS SETOF test_fact.emails_fact
AS
$BODY$
BEGIN
  RETURN QUERY
  SELECT email_id, read, promo_count::INT
  FROM test.emails e
  LEFT JOIN LATERAL
    (SELECT COUNT(1) AS promo_count
     FROM test.email_promos ep
     WHERE ep.email_id = e.email_id) eps ON TRUE
  WHERE email_id = p_email_id;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION test_fact.emails_fact_delete(p_email_id INT)
RETURNS VOID
AS
$BODY$
BEGIN

  DELETE
  FROM test_fact.emails_fact c
  WHERE email_id = p_email_id;

END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION test_fact.customer_order_history_fact_merge(p_order_id INT)
RETURNS VOID
AS
$BODY$
BEGIN

  PERFORM test_fact.customer_order_history_fact_record_merge(o.*)
  FROM test.orders o
  WHERE order_id = p_order_id;

END;
$BODY$
LANGUAGE plpgsql;

--TODO - this assumes inserts always have a greater or equal order_date - but is that just implementation?
CREATE FUNCTION test_fact.customer_order_history_fact_record_merge(p_order test.orders)
RETURNS VOID
AS
$BODY$
DECLARE
    v_add_to_total_orders integer = 1;
BEGIN
  
  WITH ended_last_fact AS
  (UPDATE test_fact.customer_order_history_fact
      SET as_of_date = daterange(lower(as_of_date), p_order.order_date)
        , row_updated_at = p_order.row_updated_at
      WHERE customer_id = p_order.customer_id
        AND lower(as_of_date) <> p_order.order_date
        AND upper(as_of_date) = 'infinity'
      RETURNING *)
  
  INSERT INTO test_fact.customer_order_history_fact AS f
  (as_of_date, customer_id, total_orders, last_order_date, row_updated_at)
  SELECT daterange(p_order.order_date, 'infinity'),
    p_order.customer_id,
    COALESCE(ended_last_fact.total_orders, 0) + v_add_to_total_orders AS total_orders,
    p_order.order_date,
    now()
  FROM (SELECT p_order.customer_id) nes
  LEFT JOIN ended_last_fact ON nes.customer_id = ended_last_fact.customer_id
  ON CONFLICT (customer_id, as_of_date)
  DO UPDATE
  SET
    total_orders = f.total_orders + v_add_to_total_orders
    , last_order_date = p_order.order_date
    , row_updated_at = now();

END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION test_fact.customer_order_history_fact_update(p_order_id INT)
RETURNS VOID
AS
$BODY$
DECLARE
  v_customer_id INT = (SELECT customer_id FROM test.orders WHERE order_id = p_order_id);
BEGIN
  --For simplicities sake for this unusual event, just drop and rebuild history
  DELETE FROM test_fact.customer_order_history_fact cohf WHERE customer_id = v_customer_id;

  PERFORM test_fact.customer_order_history_fact_record_merge(o_ordered.*)
  FROM
  (SELECT *
  FROM test.orders
  WHERE customer_id = v_customer_id
  ORDER BY order_id) o_ordered;

END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION test_fact.customer_order_history_fact_delete(p_customer_id INT)
RETURNS VOID
AS
$BODY$
BEGIN
  --For simplicities sake for this unusual event, just drop and rebuild history
  DELETE FROM test_fact.customer_order_history_fact cohf WHERE customer_id = p_customer_id;

  PERFORM test_fact.customer_order_history_fact_record_merge(o_ordered.*)
  FROM
  (SELECT *
  FROM test.orders
  WHERE customer_id = p_customer_id
  ORDER BY order_id) o_ordered;

END;
$BODY$
LANGUAGE plpgsql;

SELECT fact_loader.create_table_loader_function((schemaname||'.'||relname||'_aggregator')::REGPROC,relid,'{row_updated_at}')
FROM pg_stat_user_tables
WHERE relname IN('customers_fact','orders_fact','customersorders_fact','emails_fact','order_emails_fact','customersorders_summary_fact')
ORDER BY schemaname, relname;

INSERT INTO fact_loader.fact_tables
(fact_table_relid, fact_table_agg_proid, priority)
VALUES ('test_fact.customers_fact'::REGCLASS, 'test_fact.customers_fact_aggregator'::REGPROC, 1);

INSERT INTO fact_loader.fact_tables
(fact_table_relid, fact_table_agg_proid, priority)
VALUES ('test_fact.orders_fact'::REGCLASS, 'test_fact.orders_fact_aggregator'::REGPROC, 2);

--TODO feature
INSERT INTO fact_loader.fact_tables
(fact_table_relid, fact_table_agg_proid, priority)
VALUES ('test_fact.customersorders_fact'::REGCLASS, 'test_fact.customersorders_fact_aggregator'::REGPROC, 3);

INSERT INTO fact_loader.fact_tables
(fact_table_relid, fact_table_agg_proid, priority)
VALUES ('test_fact.emails_fact' ::REGCLASS, 'test_fact.emails_fact_aggregator'::REGPROC, 4);

--TODO feature
INSERT INTO fact_loader.fact_tables
(fact_table_relid, fact_table_agg_proid, priority)
VALUES ('test_fact.order_emails_fact' ::REGCLASS, 'test_fact.order_emails_fact_aggregator'::REGPROC, 5);

--TODO feature
INSERT INTO fact_loader.fact_tables
(fact_table_relid, fact_table_agg_proid, priority)
VALUES ('test_fact.customer_order_history_fact' ::REGCLASS, NULL, 6);

--Nested fact table deps
INSERT INTO fact_loader.fact_tables
(fact_table_relid, fact_table_agg_proid, priority)
VALUES ('test_fact.customersorders_summary_fact' ::REGCLASS, 'test_fact.customersorders_summary_fact_aggregator'::REGPROC, 7);
