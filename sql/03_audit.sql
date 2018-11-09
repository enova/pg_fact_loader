SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

CREATE EXTENSION IF NOT EXISTS hstore;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE OR REPLACE FUNCTION audit.no_dml_on_audit_table()
RETURNS TRIGGER AS
$$
BEGIN
  RAISE EXCEPTION 'No common-case updates/deletes/truncates allowed on audit table';
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

/***
TO BUILD THE REST OF THE AUDIT SQL:

SELECT format('./audit.sh %s %s %s >> sql/03_audit.sql', schemaname, relname, pkey) AS script
FROM pg_stat_user_tables st
INNER JOIN LATERAL
  (SELECT a.attname AS pkey
  FROM (SELECT
          i.indrelid
          , unnest(indkey) AS ik
          , row_number()
            OVER ()        AS rn
        FROM pg_index i
        WHERE i.indrelid = st.relid AND i.indisprimary) pk
  INNER JOIN pg_attribute a
    ON a.attrelid = pk.indrelid AND a.attnum = pk.ik) aft ON TRUE
WHERE st.schemaname = 'test'
ORDER BY schemaname, relname;
 */
CREATE SCHEMA IF NOT EXISTS test_audit_raw;

CREATE TABLE test_audit_raw.customers_audit (
    customers_audit_id BIGSERIAL PRIMARY KEY,
    changed_at timestamp without time zone NOT NULL,
    operation character varying(1) NOT NULL,
    row_before_change jsonb,
    change jsonb,
    primary_key text,
    before_change jsonb
);

CREATE OR REPLACE FUNCTION "test_audit_raw"."audit_test_customers"()
      RETURNS TRIGGER AS
      $$
      DECLARE
        value_row HSTORE = hstore(NULL);
        new_row HSTORE = hstore(NULL);
        audit_id BIGINT;
      BEGIN
        SELECT nextval('test_audit_raw.customers_audit_customers_audit_id_seq') INTO audit_id;
        IF (TG_OP = 'UPDATE') THEN
          new_row = hstore(NEW);
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h.h)).key AS key, substring((each(h.h)).value FROM 1 FOR 500) AS value FROM (SELECT hstore(OLD) - hstore(NEW) AS h) h) sq;
          IF new_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."customers_audit"("customers_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), new_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."customers_audit"("customers_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), NULL);
          END IF;
        ELSIF (TG_OP = 'INSERT') THEN
          value_row = hstore(NEW);
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."customers_audit"("customers_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."customers_audit"("customers_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'DELETE') THEN
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h)).key AS key, substring((each(h)).value FROM 1 FOR 500) AS value FROM hstore(OLD) h) sq;
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."customers_audit"("customers_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."customers_audit"("customers_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'TRUNCATE') THEN
          INSERT INTO "test_audit_raw"."customers_audit"("customers_audit_id", changed_at, operation, before_change, change, primary_key)
          VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
        ELSE
          RETURN NULL;
        END IF;
        RETURN NULL;
      END;
      $$
      LANGUAGE plpgsql;

CREATE TRIGGER row_audit_star AFTER INSERT OR DELETE OR UPDATE ON test.customers FOR EACH ROW EXECUTE PROCEDURE "test_audit_raw"."audit_test_customers" ('customer_id');
CREATE SCHEMA IF NOT EXISTS test_audit_raw;

CREATE TABLE test_audit_raw.email_promos_audit (
    email_promos_audit_id BIGSERIAL PRIMARY KEY,
    changed_at timestamp without time zone NOT NULL,
    operation character varying(1) NOT NULL,
    row_before_change jsonb,
    change jsonb,
    primary_key text,
    before_change jsonb
);

CREATE OR REPLACE FUNCTION "test_audit_raw"."audit_test_email_promos"()
      RETURNS TRIGGER AS
      $$
      DECLARE
        value_row HSTORE = hstore(NULL);
        new_row HSTORE = hstore(NULL);
        audit_id BIGINT;
      BEGIN
        SELECT nextval('test_audit_raw.email_promos_audit_email_promos_audit_id_seq') INTO audit_id;
        IF (TG_OP = 'UPDATE') THEN
          new_row = hstore(NEW);
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h.h)).key AS key, substring((each(h.h)).value FROM 1 FOR 500) AS value FROM (SELECT hstore(OLD) - hstore(NEW) AS h) h) sq;
          IF new_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."email_promos_audit"("email_promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), new_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."email_promos_audit"("email_promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), NULL);
          END IF;
        ELSIF (TG_OP = 'INSERT') THEN
          value_row = hstore(NEW);
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."email_promos_audit"("email_promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."email_promos_audit"("email_promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'DELETE') THEN
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h)).key AS key, substring((each(h)).value FROM 1 FOR 500) AS value FROM hstore(OLD) h) sq;
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."email_promos_audit"("email_promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."email_promos_audit"("email_promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'TRUNCATE') THEN
          INSERT INTO "test_audit_raw"."email_promos_audit"("email_promos_audit_id", changed_at, operation, before_change, change, primary_key)
          VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
        ELSE
          RETURN NULL;
        END IF;
        RETURN NULL;
      END;
      $$
      LANGUAGE plpgsql;

CREATE TRIGGER row_audit_star AFTER INSERT OR DELETE OR UPDATE ON test.email_promos FOR EACH ROW EXECUTE PROCEDURE "test_audit_raw"."audit_test_email_promos" ('email_promo_id');
CREATE SCHEMA IF NOT EXISTS test_audit_raw;

CREATE TABLE test_audit_raw.emails_audit (
    emails_audit_id BIGSERIAL PRIMARY KEY,
    changed_at timestamp without time zone NOT NULL,
    operation character varying(1) NOT NULL,
    row_before_change jsonb,
    change jsonb,
    primary_key text,
    before_change jsonb
);

CREATE OR REPLACE FUNCTION "test_audit_raw"."audit_test_emails"()
      RETURNS TRIGGER AS
      $$
      DECLARE
        value_row HSTORE = hstore(NULL);
        new_row HSTORE = hstore(NULL);
        audit_id BIGINT;
      BEGIN
        SELECT nextval('test_audit_raw.emails_audit_emails_audit_id_seq') INTO audit_id;
        IF (TG_OP = 'UPDATE') THEN
          new_row = hstore(NEW);
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h.h)).key AS key, substring((each(h.h)).value FROM 1 FOR 500) AS value FROM (SELECT hstore(OLD) - hstore(NEW) AS h) h) sq;
          IF new_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."emails_audit"("emails_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), new_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."emails_audit"("emails_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), NULL);
          END IF;
        ELSIF (TG_OP = 'INSERT') THEN
          value_row = hstore(NEW);
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."emails_audit"("emails_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."emails_audit"("emails_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'DELETE') THEN
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h)).key AS key, substring((each(h)).value FROM 1 FOR 500) AS value FROM hstore(OLD) h) sq;
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."emails_audit"("emails_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."emails_audit"("emails_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'TRUNCATE') THEN
          INSERT INTO "test_audit_raw"."emails_audit"("emails_audit_id", changed_at, operation, before_change, change, primary_key)
          VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
        ELSE
          RETURN NULL;
        END IF;
        RETURN NULL;
      END;
      $$
      LANGUAGE plpgsql;

CREATE TRIGGER row_audit_star AFTER INSERT OR DELETE OR UPDATE ON test.emails FOR EACH ROW EXECUTE PROCEDURE "test_audit_raw"."audit_test_emails" ('email_id');
CREATE SCHEMA IF NOT EXISTS test_audit_raw;

CREATE TABLE test_audit_raw.order_product_promos_audit (
    order_product_promos_audit_id BIGSERIAL PRIMARY KEY,
    changed_at timestamp without time zone NOT NULL,
    operation character varying(1) NOT NULL,
    row_before_change jsonb,
    change jsonb,
    primary_key text,
    before_change jsonb
);

CREATE OR REPLACE FUNCTION "test_audit_raw"."audit_test_order_product_promos"()
      RETURNS TRIGGER AS
      $$
      DECLARE
        value_row HSTORE = hstore(NULL);
        new_row HSTORE = hstore(NULL);
        audit_id BIGINT;
      BEGIN
        SELECT nextval('test_audit_raw.order_product_promos_audit_order_product_promos_audit_id_seq') INTO audit_id;
        IF (TG_OP = 'UPDATE') THEN
          new_row = hstore(NEW);
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h.h)).key AS key, substring((each(h.h)).value FROM 1 FOR 500) AS value FROM (SELECT hstore(OLD) - hstore(NEW) AS h) h) sq;
          IF new_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."order_product_promos_audit"("order_product_promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), new_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."order_product_promos_audit"("order_product_promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), NULL);
          END IF;
        ELSIF (TG_OP = 'INSERT') THEN
          value_row = hstore(NEW);
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."order_product_promos_audit"("order_product_promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."order_product_promos_audit"("order_product_promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'DELETE') THEN
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h)).key AS key, substring((each(h)).value FROM 1 FOR 500) AS value FROM hstore(OLD) h) sq;
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."order_product_promos_audit"("order_product_promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."order_product_promos_audit"("order_product_promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'TRUNCATE') THEN
          INSERT INTO "test_audit_raw"."order_product_promos_audit"("order_product_promos_audit_id", changed_at, operation, before_change, change, primary_key)
          VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
        ELSE
          RETURN NULL;
        END IF;
        RETURN NULL;
      END;
      $$
      LANGUAGE plpgsql;

CREATE TRIGGER row_audit_star AFTER INSERT OR DELETE OR UPDATE ON test.order_product_promos FOR EACH ROW EXECUTE PROCEDURE "test_audit_raw"."audit_test_order_product_promos" ('order_product_promo_id');
CREATE SCHEMA IF NOT EXISTS test_audit_raw;

CREATE TABLE test_audit_raw.order_products_audit (
    order_products_audit_id BIGSERIAL PRIMARY KEY,
    changed_at timestamp without time zone NOT NULL,
    operation character varying(1) NOT NULL,
    row_before_change jsonb,
    change jsonb,
    primary_key text,
    before_change jsonb
);

CREATE OR REPLACE FUNCTION "test_audit_raw"."audit_test_order_products"()
      RETURNS TRIGGER AS
      $$
      DECLARE
        value_row HSTORE = hstore(NULL);
        new_row HSTORE = hstore(NULL);
        audit_id BIGINT;
      BEGIN
        SELECT nextval('test_audit_raw.order_products_audit_order_products_audit_id_seq') INTO audit_id;
        IF (TG_OP = 'UPDATE') THEN
          new_row = hstore(NEW);
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h.h)).key AS key, substring((each(h.h)).value FROM 1 FOR 500) AS value FROM (SELECT hstore(OLD) - hstore(NEW) AS h) h) sq;
          IF new_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."order_products_audit"("order_products_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), new_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."order_products_audit"("order_products_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), NULL);
          END IF;
        ELSIF (TG_OP = 'INSERT') THEN
          value_row = hstore(NEW);
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."order_products_audit"("order_products_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."order_products_audit"("order_products_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'DELETE') THEN
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h)).key AS key, substring((each(h)).value FROM 1 FOR 500) AS value FROM hstore(OLD) h) sq;
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."order_products_audit"("order_products_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."order_products_audit"("order_products_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'TRUNCATE') THEN
          INSERT INTO "test_audit_raw"."order_products_audit"("order_products_audit_id", changed_at, operation, before_change, change, primary_key)
          VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
        ELSE
          RETURN NULL;
        END IF;
        RETURN NULL;
      END;
      $$
      LANGUAGE plpgsql;

CREATE TRIGGER row_audit_star AFTER INSERT OR DELETE OR UPDATE ON test.order_products FOR EACH ROW EXECUTE PROCEDURE "test_audit_raw"."audit_test_order_products" ('order_product_id');
CREATE SCHEMA IF NOT EXISTS test_audit_raw;

CREATE TABLE test_audit_raw.orders_audit (
    orders_audit_id BIGSERIAL PRIMARY KEY,
    changed_at timestamp without time zone NOT NULL,
    operation character varying(1) NOT NULL,
    row_before_change jsonb,
    change jsonb,
    primary_key text,
    before_change jsonb
);

CREATE OR REPLACE FUNCTION "test_audit_raw"."audit_test_orders"()
      RETURNS TRIGGER AS
      $$
      DECLARE
        value_row HSTORE = hstore(NULL);
        new_row HSTORE = hstore(NULL);
        audit_id BIGINT;
      BEGIN
        SELECT nextval('test_audit_raw.orders_audit_orders_audit_id_seq') INTO audit_id;
        IF (TG_OP = 'UPDATE') THEN
          new_row = hstore(NEW);
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h.h)).key AS key, substring((each(h.h)).value FROM 1 FOR 500) AS value FROM (SELECT hstore(OLD) - hstore(NEW) AS h) h) sq;
          IF new_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."orders_audit"("orders_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), new_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."orders_audit"("orders_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), NULL);
          END IF;
        ELSIF (TG_OP = 'INSERT') THEN
          value_row = hstore(NEW);
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."orders_audit"("orders_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."orders_audit"("orders_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'DELETE') THEN
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h)).key AS key, substring((each(h)).value FROM 1 FOR 500) AS value FROM hstore(OLD) h) sq;
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."orders_audit"("orders_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."orders_audit"("orders_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'TRUNCATE') THEN
          INSERT INTO "test_audit_raw"."orders_audit"("orders_audit_id", changed_at, operation, before_change, change, primary_key)
          VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
        ELSE
          RETURN NULL;
        END IF;
        RETURN NULL;
      END;
      $$
      LANGUAGE plpgsql;

CREATE TRIGGER row_audit_star AFTER INSERT OR DELETE OR UPDATE ON test.orders FOR EACH ROW EXECUTE PROCEDURE "test_audit_raw"."audit_test_orders" ('order_id');
CREATE SCHEMA IF NOT EXISTS test_audit_raw;

CREATE TABLE test_audit_raw.products_audit (
    products_audit_id BIGSERIAL PRIMARY KEY,
    changed_at timestamp without time zone NOT NULL,
    operation character varying(1) NOT NULL,
    row_before_change jsonb,
    change jsonb,
    primary_key text,
    before_change jsonb
);

CREATE OR REPLACE FUNCTION "test_audit_raw"."audit_test_products"()
      RETURNS TRIGGER AS
      $$
      DECLARE
        value_row HSTORE = hstore(NULL);
        new_row HSTORE = hstore(NULL);
        audit_id BIGINT;
      BEGIN
        SELECT nextval('test_audit_raw.products_audit_products_audit_id_seq') INTO audit_id;
        IF (TG_OP = 'UPDATE') THEN
          new_row = hstore(NEW);
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h.h)).key AS key, substring((each(h.h)).value FROM 1 FOR 500) AS value FROM (SELECT hstore(OLD) - hstore(NEW) AS h) h) sq;
          IF new_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."products_audit"("products_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), new_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."products_audit"("products_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), NULL);
          END IF;
        ELSIF (TG_OP = 'INSERT') THEN
          value_row = hstore(NEW);
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."products_audit"("products_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."products_audit"("products_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'DELETE') THEN
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h)).key AS key, substring((each(h)).value FROM 1 FOR 500) AS value FROM hstore(OLD) h) sq;
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."products_audit"("products_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."products_audit"("products_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'TRUNCATE') THEN
          INSERT INTO "test_audit_raw"."products_audit"("products_audit_id", changed_at, operation, before_change, change, primary_key)
          VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
        ELSE
          RETURN NULL;
        END IF;
        RETURN NULL;
      END;
      $$
      LANGUAGE plpgsql;

CREATE TRIGGER row_audit_star AFTER INSERT OR DELETE OR UPDATE ON test.products FOR EACH ROW EXECUTE PROCEDURE "test_audit_raw"."audit_test_products" ('product_id');
CREATE SCHEMA IF NOT EXISTS test_audit_raw;

CREATE TABLE test_audit_raw.promos_audit (
    promos_audit_id BIGSERIAL PRIMARY KEY,
    changed_at timestamp without time zone NOT NULL,
    operation character varying(1) NOT NULL,
    row_before_change jsonb,
    change jsonb,
    primary_key text,
    before_change jsonb
);

CREATE OR REPLACE FUNCTION "test_audit_raw"."audit_test_promos"()
      RETURNS TRIGGER AS
      $$
      DECLARE
        value_row HSTORE = hstore(NULL);
        new_row HSTORE = hstore(NULL);
        audit_id BIGINT;
      BEGIN
        SELECT nextval('test_audit_raw.promos_audit_promos_audit_id_seq') INTO audit_id;
        IF (TG_OP = 'UPDATE') THEN
          new_row = hstore(NEW);
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h.h)).key AS key, substring((each(h.h)).value FROM 1 FOR 500) AS value FROM (SELECT hstore(OLD) - hstore(NEW) AS h) h) sq;
          IF new_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."promos_audit"("promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), new_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."promos_audit"("promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), NULL);
          END IF;
        ELSIF (TG_OP = 'INSERT') THEN
          value_row = hstore(NEW);
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."promos_audit"("promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."promos_audit"("promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'DELETE') THEN
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h)).key AS key, substring((each(h)).value FROM 1 FOR 500) AS value FROM hstore(OLD) h) sq;
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."promos_audit"("promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."promos_audit"("promos_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'TRUNCATE') THEN
          INSERT INTO "test_audit_raw"."promos_audit"("promos_audit_id", changed_at, operation, before_change, change, primary_key)
          VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
        ELSE
          RETURN NULL;
        END IF;
        RETURN NULL;
      END;
      $$
      LANGUAGE plpgsql;

CREATE TRIGGER row_audit_star AFTER INSERT OR DELETE OR UPDATE ON test.promos FOR EACH ROW EXECUTE PROCEDURE "test_audit_raw"."audit_test_promos" ('promo_id');
CREATE SCHEMA IF NOT EXISTS test_audit_raw;

CREATE TABLE test_audit_raw.reorders_audit (
    reorders_audit_id BIGSERIAL PRIMARY KEY,
    changed_at timestamp without time zone NOT NULL,
    operation character varying(1) NOT NULL,
    row_before_change jsonb,
    change jsonb,
    primary_key text,
    before_change jsonb
);

CREATE OR REPLACE FUNCTION "test_audit_raw"."audit_test_reorders"()
      RETURNS TRIGGER AS
      $$
      DECLARE
        value_row HSTORE = hstore(NULL);
        new_row HSTORE = hstore(NULL);
        audit_id BIGINT;
      BEGIN
        SELECT nextval('test_audit_raw.reorders_audit_reorders_audit_id_seq') INTO audit_id;
        IF (TG_OP = 'UPDATE') THEN
          new_row = hstore(NEW);
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h.h)).key AS key, substring((each(h.h)).value FROM 1 FOR 500) AS value FROM (SELECT hstore(OLD) - hstore(NEW) AS h) h) sq;
          IF new_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."reorders_audit"("reorders_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), new_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."reorders_audit"("reorders_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), hstore_to_jsonb(hstore(NEW) - hstore(OLD)), NULL);
          END IF;
        ELSIF (TG_OP = 'INSERT') THEN
          value_row = hstore(NEW);
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."reorders_audit"("reorders_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."reorders_audit"("reorders_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'DELETE') THEN
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h)).key AS key, substring((each(h)).value FROM 1 FOR 500) AS value FROM hstore(OLD) h) sq;
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "test_audit_raw"."reorders_audit"("reorders_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "test_audit_raw"."reorders_audit"("reorders_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_jsonb(value_row), NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'TRUNCATE') THEN
          INSERT INTO "test_audit_raw"."reorders_audit"("reorders_audit_id", changed_at, operation, before_change, change, primary_key)
          VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
        ELSE
          RETURN NULL;
        END IF;
        RETURN NULL;
      END;
      $$
      LANGUAGE plpgsql;

CREATE TRIGGER row_audit_star AFTER INSERT OR DELETE OR UPDATE ON test.reorders FOR EACH ROW EXECUTE PROCEDURE "test_audit_raw"."audit_test_reorders" ('reorder_id');
