SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

--Test at least a column as timestamptz
ALTER TABLE test_audit_raw.emails_audit ALTER COLUMN changed_at TYPE TIMESTAMPTZ;

INSERT INTO test.customers (customer_id, customer_number, phone, age)
SELECT generate_series,
  'cust2018'||generate_series,
  '000123456'||RIGHT(generate_series::TEXT,1),
  35
FROM generate_series(1,10);

INSERT INTO test.products (product_id, product_name)
VALUES (1,'Sour Candy'),
  (2,'Pool Table'),
  (3,'One Pocket Book'),
  (4,'Fast Turbo Car'),
  (5,'Black Cassock'),
  (6,'Pacifier'),
  (7,'Book Light'),
  (8,'A Dozen Roses');

INSERT INTO test.orders (order_id, customer_id, order_date, total)
VALUES (1, 1, '2018-04-10', 100.00),
  (2, 3, '2018-04-11', 200.00),
  (3, 5, '2018-04-12', 2000.00);

INSERT INTO test.order_products (order_product_id, order_id, product_id)
VALUES (1, 1, 1),
  (2, 1, 3),
  (3, 1, 5),
  (4, 2, 7),
  (5, 2, 8),
  (6, 3, 2);

INSERT INTO test.promos (promo_id, description)
VALUES (1, '50% off 9 foot pool table with real Italian slate');

INSERT INTO test.emails (email_id, customer_id, read)
VALUES (1, 5, true);

INSERT INTO test.email_promos (email_promo_id, email_id, promo_id)
VALUES (1, 1, 1);

INSERT INTO test.order_product_promos (order_product_promo_id, order_product_id, promo_id)
VALUES (1, 6, 1);

INSERT INTO test.orders (order_id, customer_id, order_date, total)
VALUES (4, 1, '2018-04-13', 100.00);
INSERT INTO test.reorders (reorder_id, base_order_id, reorder_from_id, reorder_to_id)
VALUES (1, 1, 1, 4);

INSERT INTO fact_loader.queue_tables (queue_table_relid, queue_of_base_table_relid, pglogical_node_if_id, queue_table_tz)
SELECT st.relid::REGCLASS, sts.relid::REGCLASS, 0, CASE WHEN st.relname = 'emails_audit' THEN NULL ELSE 'America/Chicago' END
FROM (SELECT c.oid AS relid, c.relname, n.nspname AS schemaname FROM pg_class c INNER JOIN pg_namespace n ON n.oid = c.relnamespace) st
INNER JOIN (SELECT c.oid AS relid, c.relname, n.nspname AS schemaname FROM pg_class c INNER JOIN pg_namespace n ON n.oid = c.relnamespace) sts ON sts.schemaname||'_audit_raw' = st.schemaname AND sts.relname||'_audit' = st.relname
WHERE st.schemaname = 'test_audit_raw';

SELECT fact_loader.add_batch_id_fields();
/****
Configuration for customers_fact
 */

  --Queue tables
  WITH queue_tables_with_proids AS (
  SELECT
    *,

    'test_fact.customers_fact_merge'::REGPROC AS insert_merge_proid,
    'test_fact.customers_fact_merge'::REGPROC AS update_merge_proid,
    CASE
      WHEN queue_of_base_table_relid = 'test.customers'::REGCLASS
        THEN 'test_fact.customers_fact_delete'::REGPROC
      ELSE 'test_fact.customers_fact_merge'::REGPROC
    END AS delete_merge_proid,

    CASE
      WHEN queue_of_base_table_relid = 'test.customers'::REGCLASS
        THEN '{phone, age}'::TEXT[]
      WHEN queue_of_base_table_relid = 'test.orders'::REGCLASS
        --This update may be implausible, but would affect the fact table
        THEN '{customer_id}'::TEXT[]
      --Let's just consider that any update to the other tables should cause concern and we want to be safe and refresh all
      ELSE NULL
    END AS relevant_change_columns
  FROM fact_loader.queue_tables
  WHERE queue_of_base_table_relid IN
    /***
    These are the tables that are involved in test_fact.customers_fact_aggregator
    Find this out for each function in order to properly configure all possible changes
    that could affect the tables
     */
        ('test.customers'::REGCLASS,
         'test.orders'::REGCLASS,
         'test.order_products'::REGCLASS,
         'test.order_product_promos'::REGCLASS)
  )

  INSERT INTO fact_loader.queue_table_deps
  (fact_table_id, queue_table_id, relevant_change_columns, insert_merge_proid, update_merge_proid, delete_merge_proid)
  SELECT
    fact_table_id, queue_tables_with_proids.queue_table_id, relevant_change_columns, insert_merge_proid, update_merge_proid, delete_merge_proid
  FROM fact_loader.fact_tables
  CROSS JOIN queue_tables_with_proids
  WHERE fact_table_relid = 'test_fact.customers_fact'::REGCLASS;

  --Key retrieval for updates
  INSERT INTO fact_loader.key_retrieval_sequences (
    queue_table_dep_id,
    level,
    return_columns,
    is_fact_key,
    join_to_relation,
    join_to_column,
    return_columns_from_join,
    join_return_is_fact_key)
  SELECT
    queue_table_dep_id,
    1,
    '{customer_id}'::name[],
    true,
    null,
    null,
    null::name[],
    null
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
  INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
  WHERE fact_table_relid = 'test_fact.customers_fact'::REGCLASS
    AND queue_of_base_table_relid IN(
        'test.customers'::REGCLASS,
         'test.orders'::REGCLASS)
  UNION ALL
  SELECT
    queue_table_dep_id,
    1,
    '{order_id}',
    false,
    'test.orders'::REGCLASS,
    'order_id',
    '{customer_id}',
    true
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
  INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
  WHERE fact_table_relid = 'test_fact.customers_fact'::REGCLASS
    AND queue_of_base_table_relid IN(
        'test.order_products'::REGCLASS)
  UNION ALL
  /****
  These 2 are an example of a dependency requiring multiple joins
  to get the customer_id key needed to update the customers_fact table
   */
  SELECT
    queue_table_dep_id,
    1,
    '{order_product_id}',
    false,
    'test.order_products'::REGCLASS,
    'order_product_id',
    '{order_id}',
    false
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
  INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
  WHERE fact_table_relid = 'test_fact.customers_fact'::REGCLASS
    AND queue_of_base_table_relid IN(
        'test.order_product_promos'::REGCLASS)
  UNION ALL
  SELECT
    queue_table_dep_id,
    2,
    '{order_id}',
    false,
    'test.orders'::REGCLASS,
    'order_id',
    '{customer_id}',
    true
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
  INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
  WHERE fact_table_relid = 'test_fact.customers_fact'::REGCLASS
    AND queue_of_base_table_relid IN(
        'test.order_product_promos'::REGCLASS);

/****
Configuration for orders_fact
 */
  --Queue tables
  INSERT INTO fact_loader.queue_table_deps
    (
    fact_table_id,
    queue_table_id,
    relevant_change_columns,
    insert_merge_proid,
    update_merge_proid,
    delete_merge_proid
    )
  SELECT
    fact_table_id
    ,(SELECT queue_table_id
      FROM fact_loader.queue_tables
      WHERE queue_of_base_table_relid IN ('test.orders'::REGCLASS))
    , '{order_date, total}'::TEXT[]
    , 'test_fact.orders_fact_merge'::REGPROC AS insert_merge_proid
    , 'test_fact.orders_fact_merge'::REGPROC AS update_merge_proid
    , 'test_fact.orders_fact_delete'::REGPROC AS delete_merge_proid
  FROM fact_loader.fact_tables
  WHERE fact_table_relid = 'test_fact.orders_fact'::REGCLASS;

  INSERT INTO fact_loader.queue_table_deps
    (
    fact_table_id,
    queue_table_id,
    relevant_change_columns,
    insert_merge_proid,
    update_merge_proid,
    delete_merge_proid
    )
  SELECT
    fact_table_id
    ,(SELECT queue_table_id
      FROM fact_loader.queue_tables
      WHERE queue_of_base_table_relid IN ('test.reorders'::REGCLASS))
    , NULL
    , 'test_fact.orders_fact_merge'::REGPROC AS insert_merge_proid
    , 'test_fact.orders_fact_merge'::REGPROC AS update_merge_proid
    , 'test_fact.orders_fact_merge'::REGPROC AS delete_merge_proid
  FROM fact_loader.fact_tables
  WHERE fact_table_relid = 'test_fact.orders_fact'::REGCLASS;

  --Key retrieval
  INSERT INTO fact_loader.key_retrieval_sequences (
  queue_table_dep_id,
  filter_scope,
  level,
  return_columns,
  is_fact_key,
  join_to_relation,
  join_to_column,
  return_columns_from_join,
  join_return_is_fact_key)
  SELECT
    queue_table_dep_id,
    evts.evt,
    1,
    evts.return_columns,
    true,
    null,
    null,
    null::name[],
    null::boolean
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
  INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
  CROSS JOIN (VALUES
                ('I','{customer_id}'::name[]),
                ('U','{customer_id}'::name[]),
                ('D','{order_id}'::name[])) evts (evt, return_columns)
  WHERE fact_table_relid = 'test_fact.orders_fact'::REGCLASS
    AND queue_of_base_table_relid IN(
        'test.orders'::REGCLASS)
  UNION ALL
  SELECT
    queue_table_dep_id,
    NULL,
    1,
    '{base_order_id,reorder_from_id,reorder_to_id}',
    false,
    'test.orders',
    'order_id',
    '{customer_id}'::name[],
    true
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
  INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
  WHERE fact_table_relid = 'test_fact.orders_fact'::REGCLASS
    AND queue_of_base_table_relid IN(
        'test.reorders'::REGCLASS);

/****
Configuration for customersorders_fact_aggregator
 */
  --Only deps in fact_table_deps for this fact table because it depends on no queue tables directly
  --TODO - revisit and add delete functions as appropriate
  INSERT INTO fact_loader.fact_table_deps (parent_id, child_id, default_insert_merge_proid, default_update_merge_proid, default_delete_merge_proid)
  VALUES ((SELECT fact_table_id FROM fact_loader.fact_tables WHERE fact_table_relid = 'test_fact.customers_fact'::REGCLASS),(SELECT fact_table_id FROM fact_loader.fact_tables WHERE fact_table_relid = 'test_fact.customersorders_fact'::REGCLASS),'test_fact.customersorders_fact_merge','test_fact.customersorders_fact_merge','test_fact.customersorders_fact_delete'),
    ((SELECT fact_table_id FROM fact_loader.fact_tables WHERE fact_table_relid = 'test_fact.orders_fact'::REGCLASS),(SELECT fact_table_id FROM fact_loader.fact_tables WHERE fact_table_relid = 'test_fact.customersorders_fact'::REGCLASS),'test_fact.customersorders_fact_merge','test_fact.customersorders_fact_merge','test_fact.customersorders_fact_delete');


/****
Configuration for order_emails_fact
 */
  --Queue tables
  WITH queue_tables_with_proids AS (
  SELECT
    *,
    'test_fact.order_emails_fact_merge'::REGPROC AS insert_merge_proid,
    'test_fact.order_emails_fact_merge'::REGPROC AS update_merge_proid,
    'test_fact.order_emails_fact_merge'::REGPROC AS delete_merge_proid
  FROM fact_loader.queue_tables
  WHERE queue_of_base_table_relid IN('test.emails'::REGCLASS)
  )

  INSERT INTO fact_loader.queue_table_deps
  (fact_table_id, queue_table_id, insert_merge_proid, update_merge_proid, delete_merge_proid)
  SELECT
    fact_table_id, queue_tables_with_proids.queue_table_id, insert_merge_proid, update_merge_proid, delete_merge_proid
  FROM fact_loader.fact_tables
  CROSS JOIN queue_tables_with_proids
  WHERE fact_table_relid = 'test_fact.order_emails_fact'::REGCLASS;

  --Key retrieval for updates
  INSERT INTO fact_loader.key_retrieval_sequences (
    queue_table_dep_id,
    level,
    return_columns,
    is_fact_key)
  SELECT
    queue_table_dep_id,
    1,
    '{customer_id}'::name[],
    true
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
  INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
  WHERE fact_table_relid = 'test_fact.order_emails_fact'::REGCLASS
    AND queue_of_base_table_relid IN('test.emails'::REGCLASS);

  --Fact table deps
  INSERT INTO fact_loader.fact_table_deps (parent_id, child_id, default_insert_merge_proid, default_update_merge_proid, default_delete_merge_proid)
  VALUES ((SELECT fact_table_id FROM fact_loader.fact_tables WHERE fact_table_relid = 'test_fact.orders_fact'::REGCLASS),(SELECT fact_table_id FROM fact_loader.fact_tables WHERE fact_table_relid = 'test_fact.order_emails_fact'::REGCLASS),'test_fact.order_emails_fact_merge','test_fact.order_emails_fact_merge','test_fact.order_emails_fact_delete');

/****
Configuration for emails_fact
 */
  --Queue tables
  WITH queue_tables_with_proids AS (
  SELECT
    *,

    'test_fact.emails_fact_merge'::REGPROC AS insert_merge_proid,
    'test_fact.emails_fact_merge'::REGPROC AS update_merge_proid,
    CASE
      WHEN queue_of_base_table_relid = 'test.emails'::REGCLASS
        THEN 'test_fact.emails_fact_delete'::REGPROC
      ELSE 'test_fact.emails_fact_merge'::REGPROC
    END AS delete_merge_proid
  FROM fact_loader.queue_tables
  WHERE queue_of_base_table_relid IN
    /***
    These are the tables that are involved in test_fact.customers_fact_aggregator
    Find this out for each function in order to properly configure all possible changes
    that could affect the tables
     */
        ('test.emails'::REGCLASS,
         'test.email_promos'::REGCLASS)
  )

  INSERT INTO fact_loader.queue_table_deps
  (fact_table_id, queue_table_id, insert_merge_proid, update_merge_proid, delete_merge_proid)
  SELECT
    fact_table_id, queue_tables_with_proids.queue_table_id, insert_merge_proid, update_merge_proid, delete_merge_proid
  FROM fact_loader.fact_tables
  CROSS JOIN queue_tables_with_proids
  WHERE fact_table_relid = 'test_fact.emails_fact'::REGCLASS;

  --Key retrieval for updates
  INSERT INTO fact_loader.key_retrieval_sequences (
    queue_table_dep_id,
    level,
    return_columns,
    is_fact_key)
  SELECT
    queue_table_dep_id,
    1,
    '{email_id}'::name[],
    true
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
  INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
  WHERE fact_table_relid = 'test_fact.emails_fact'::REGCLASS
    AND queue_of_base_table_relid IN(
        'test.emails'::REGCLASS,
         'test.email_promos'::REGCLASS);

/****
Configuration for customer_order_history_fact
 */
--Queue tables
  WITH qt AS (
  SELECT *,
  'test_fact.customer_order_history_fact_merge'::REGPROC AS insert_merge_proid,
  'test_fact.customer_order_history_fact_update'::REGPROC AS update_merge_proid,
  'test_fact.customer_order_history_fact_delete'::REGPROC AS delete_merge_proid
  FROM fact_loader.queue_tables
  WHERE queue_of_base_table_relid IN
    ('test.orders'::REGCLASS)
  )

  INSERT INTO fact_loader.queue_table_deps (fact_table_id, queue_table_id, insert_merge_proid, update_merge_proid, delete_merge_proid)
  SELECT fact_table_id, qt.queue_table_id, insert_merge_proid, update_merge_proid, delete_merge_proid
  FROM fact_loader.fact_tables
  CROSS JOIN qt
  WHERE fact_table_relid = 'test_fact.customer_order_history_fact'::REGCLASS;

  /****
  For this fact table, we need a different key_retrieval for deletes, so we enter all 3 separately
   */
  INSERT INTO fact_loader.key_retrieval_sequences (
  queue_table_dep_id,
  filter_scope,
  level,
  return_columns,
  is_fact_key)
  SELECT
    queue_table_dep_id,
    evts.evt,
    1,
    '{order_id}'::name[],
    true
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
  INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
  CROSS JOIN (VALUES ('I'),('U')) evts (evt)
  WHERE fact_table_relid = 'test_fact.customer_order_history_fact'::REGCLASS
    AND queue_of_base_table_relid IN('test.orders'::REGCLASS)
  UNION ALL
  SELECT
    queue_table_dep_id,
    'D',
    1,
    '{customer_id}'::name[],
    true
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
  INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
  WHERE fact_table_relid = 'test_fact.customer_order_history_fact'::REGCLASS
    AND queue_of_base_table_relid IN('test.orders'::REGCLASS);

/****
Configuration for test_fact.customersorders_summary_fact
 */
INSERT INTO fact_loader.fact_table_deps (parent_id, child_id, default_insert_merge_proid, default_update_merge_proid, default_delete_merge_proid)
VALUES ((SELECT fact_table_id
         FROM fact_loader.fact_tables
         WHERE fact_table_relid = 'test_fact.customersorders_fact' :: REGCLASS),
        (SELECT fact_table_id
         FROM fact_loader.fact_tables
         WHERE fact_table_relid =
               'test_fact.customersorders_summary_fact' :: REGCLASS),
        'test_fact.customersorders_summary_fact_merge',
        'test_fact.customersorders_summary_fact_merge',
        'test_fact.customersorders_summary_fact_delete');

/****
Because we need to manually adjust the dependent fact table config for at least one table, we do this manually
  1. Now that configs are all in place, run fact_loader.refresh_fact_table_dep_queue_cutoffs(); to build the deps table
  2. Query based on fact_table_relid and queue_table_relid to find the correct fact_table_dep_queue_table_dep_id to update
  3. Set this dep to have a different delete function for this queue table
 */
SELECT fact_loader.refresh_fact_table_dep_queue_table_deps();

WITH to_update AS (
  SELECT ftdqtd.fact_table_dep_queue_table_dep_id
    , qtd.queue_table_id
    , qt.queue_table_relid
    , ft.fact_table_id
  FROM fact_loader.fact_table_deps ftd
  INNER JOIN fact_loader.fact_tables ft ON ft.fact_table_id = ftd.child_id
  INNER JOIN fact_loader.fact_table_dep_queue_table_deps ftdqtd ON ftdqtd.fact_table_dep_id = ftd.fact_table_dep_id
  INNER JOIN fact_loader.queue_table_deps qtd ON qtd.queue_table_dep_id = ftdqtd.queue_table_dep_id
  INNER JOIN fact_loader.queue_tables qt ON qt.queue_table_id = qtd.queue_table_id
  WHERE fact_table_relid = 'test_fact.order_emails_fact'::REGCLASS
    AND qt.queue_table_relid = 'test_audit_raw.reorders_audit'::REGCLASS
)

UPDATE fact_loader.fact_table_dep_queue_table_deps
SET delete_merge_proid = 'test_fact.order_emails_fact_merge'
WHERE fact_table_dep_queue_table_dep_id = (SELECT fact_table_dep_queue_table_dep_id FROM to_update);

/****
Both of these next 2 are the same situation because one depends on the other
****/
WITH to_update AS (
  SELECT ftdqtd.fact_table_dep_queue_table_dep_id
    , qtd.queue_table_id
    , qt.queue_table_relid
    , ft.fact_table_id
    , ft.fact_table_relid
  FROM fact_loader.fact_table_deps ftd
  INNER JOIN fact_loader.fact_tables ft ON ft.fact_table_id = ftd.child_id
  INNER JOIN fact_loader.fact_table_dep_queue_table_deps ftdqtd ON ftdqtd.fact_table_dep_id = ftd.fact_table_dep_id
  INNER JOIN fact_loader.queue_table_deps qtd ON qtd.queue_table_dep_id = ftdqtd.queue_table_dep_id
  INNER JOIN fact_loader.queue_tables qt ON qt.queue_table_id = qtd.queue_table_id
  WHERE fact_table_relid = 'test_fact.customersorders_fact'::REGCLASS
    AND qt.queue_table_relid IN('test_audit_raw.reorders_audit'::REGCLASS,'test_audit_raw.order_product_promos_audit'::REGCLASS,'test_audit_raw.order_products_audit'::REGCLASS)
)

UPDATE fact_loader.fact_table_dep_queue_table_deps
SET delete_merge_proid = 'test_fact.customersorders_fact_merge'
WHERE fact_table_dep_queue_table_dep_id IN (SELECT fact_table_dep_queue_table_dep_id FROM to_update);

WITH to_update AS (
  SELECT ftdqtd.fact_table_dep_queue_table_dep_id
    , qtd.queue_table_id
    , qt.queue_table_relid
    , ft.fact_table_id
    , ft.fact_table_relid
  FROM fact_loader.fact_table_deps ftd
  INNER JOIN fact_loader.fact_tables ft ON ft.fact_table_id = ftd.child_id
  INNER JOIN fact_loader.fact_table_dep_queue_table_deps ftdqtd ON ftdqtd.fact_table_dep_id = ftd.fact_table_dep_id
  INNER JOIN fact_loader.queue_table_deps qtd ON qtd.queue_table_dep_id = ftdqtd.queue_table_dep_id
  INNER JOIN fact_loader.queue_tables qt ON qt.queue_table_id = qtd.queue_table_id
  WHERE fact_table_relid = 'test_fact.customersorders_summary_fact'::REGCLASS
    AND qt.queue_table_relid IN('test_audit_raw.reorders_audit'::REGCLASS,'test_audit_raw.order_product_promos_audit'::REGCLASS,'test_audit_raw.order_products_audit'::REGCLASS)
)

UPDATE fact_loader.fact_table_dep_queue_table_deps
SET delete_merge_proid = 'test_fact.customersorders_summary_fact_merge'
WHERE fact_table_dep_queue_table_dep_id IN (SELECT fact_table_dep_queue_table_dep_id FROM to_update);

/****
DEMO

SELECT * FROM fact_loader.fact_tables
ORDER BY priority;

SELECT *
FROM fact_loader.queue_tables ORDER BY queue_table_relid::REGCLASS::TEXT;

SELECT
  ft.fact_table_relid,
  qt.queue_table_relid,
  krs.*
FROM fact_loader.key_retrieval_sequences krs
INNER JOIN fact_loader.queue_table_deps qtd USING (queue_table_dep_id)
INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
ORDER BY qtd.queue_table_dep_id, krs.filter_scope, krs.level;

SELECT qtd.queue_table_dep_id,
  ft.fact_table_relid,
  qt.queue_table_relid,
  qtd.relevant_change_columns,
  qtd.last_cutoff_id,
  qtd.last_cutoff_source_time,
  qtd.insert_merge_proid,
  qtd.update_merge_proid,
  qtd.delete_merge_proid
FROM fact_loader.queue_table_deps qtd
INNER JOIN fact_loader.fact_tables ft USING (fact_table_id)
INNER JOIN fact_loader.queue_tables qt USING (queue_table_id)
ORDER BY ft.fact_table_relid::TEXT, qt.queue_table_relid::TEXT;
 */
