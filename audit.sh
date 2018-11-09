#!/usr/bin/env bash

set -eu

#TO BUILD THE REST OF THE AUDIT SQL:
#
#SELECT format('./audit.sh %s %s %s >> sql/03_audit.sql', schemaname, relname, pkey) AS script
#FROM pg_stat_user_tables st
#INNER JOIN LATERAL
#  (SELECT a.attname AS pkey
#  FROM (SELECT
#          i.indrelid
#          , unnest(indkey) AS ik
#          , row_number()
#            OVER ()        AS rn
#        FROM pg_index i
#        WHERE i.indrelid = st.relid AND i.indisprimary) pk
#  INNER JOIN pg_attribute a
#    ON a.attrelid = pk.indrelid AND a.attnum = pk.ik) aft ON TRUE
#WHERE st.schemaname = 'test';
#
#./audit.sh test customers customer_id >> sql/03_audit.sql
#./audit.sh test orders order_id >> sql/03_audit.sql
#./audit.sh test emails email_id >> sql/03_audit.sql
#./audit.sh test email_promos email_promo_id >> sql/03_audit.sql
#./audit.sh test products product_id >> sql/03_audit.sql
#./audit.sh test order_products order_product_id >> sql/03_audit.sql
#./audit.sh test reorders reorder_id >> sql/03_audit.sql

sql() {
schema=$1
audit_schema="${schema}_audit_raw"
table=$2
primary_key=$3
sequence_name="${table}_audit_${table}_audit_id_seq"
client_query="NULL"
json_type="jsonb"
cat << EOM
CREATE SCHEMA IF NOT EXISTS ${audit_schema};

CREATE TABLE ${audit_schema}.${table}_audit (
    ${table}_audit_id BIGSERIAL PRIMARY KEY,
    changed_at timestamp without time zone NOT NULL,
    operation character varying(1) NOT NULL,
    row_before_change jsonb,
    change jsonb,
    primary_key text,
    before_change jsonb
);

CREATE OR REPLACE FUNCTION "${schema}_audit_raw"."audit_${schema}_${table}"()
      RETURNS TRIGGER AS
      \$\$
      DECLARE
        value_row HSTORE = hstore(NULL);
        new_row HSTORE = hstore(NULL);
        audit_id BIGINT;
      BEGIN
        SELECT nextval('${audit_schema}.${sequence_name}') INTO audit_id;
        IF (TG_OP = 'UPDATE') THEN
          new_row = hstore(NEW);
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h.h)).key AS key, substring((each(h.h)).value FROM 1 FOR 500) AS value FROM (SELECT hstore(OLD) - hstore(NEW) AS h) h) sq;
          IF new_row ? TG_ARGV[0] THEN
            INSERT INTO "${schema}_audit_raw"."${table}_audit"("${table}_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_${json_type}(value_row), hstore_to_${json_type}(hstore(NEW) - hstore(OLD)), new_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "${schema}_audit_raw"."${table}_audit"("${table}_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_${json_type}(value_row), hstore_to_${json_type}(hstore(NEW) - hstore(OLD)), NULL);
          END IF;
        ELSIF (TG_OP = 'INSERT') THEN
          value_row = hstore(NEW);
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "${schema}_audit_raw"."${table}_audit"("${table}_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "${schema}_audit_raw"."${table}_audit"("${table}_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'DELETE') THEN
          SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h)).key AS key, substring((each(h)).value FROM 1 FOR 500) AS value FROM hstore(OLD) h) sq;
          IF value_row ? TG_ARGV[0] THEN
            INSERT INTO "${schema}_audit_raw"."${table}_audit"("${table}_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_${json_type}(value_row), NULL, value_row -> TG_ARGV[0]);
          ELSE
            INSERT INTO "${schema}_audit_raw"."${table}_audit"("${table}_audit_id", changed_at, operation, before_change, change, primary_key)
            VALUES(audit_id, now(), substring(TG_OP,1,1), hstore_to_${json_type}(value_row), NULL, NULL);
          END IF;
        ELSIF (TG_OP = 'TRUNCATE') THEN
          INSERT INTO "${schema}_audit_raw"."${table}_audit"("${table}_audit_id", changed_at, operation, before_change, change, primary_key)
          VALUES(audit_id, now(), substring(TG_OP,1,1), NULL, NULL, NULL);
        ELSE
          RETURN NULL;
        END IF;
        RETURN NULL;
      END;
      \$\$
      LANGUAGE plpgsql;

CREATE TRIGGER row_audit_star AFTER INSERT OR DELETE OR UPDATE ON ${schema}.$table FOR EACH ROW EXECUTE PROCEDURE "${schema}_audit_raw"."audit_${schema}_${table}" ('${primary_key}');
EOM
}

sql $1 $2 $3 
