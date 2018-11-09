/* pg_fact_loader--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fact_loader" to load this file. \quit

CREATE FUNCTION fact_loader._launch_worker(oid)
  RETURNS pg_catalog.INT4 STRICT
AS 'MODULE_PATHNAME', 'pg_fact_loader_worker'
LANGUAGE C;

CREATE FUNCTION fact_loader.launch_worker()
  RETURNS pg_catalog.INT4 STRICT
AS 'SELECT fact_loader._launch_worker(oid) FROM pg_database WHERE datname = current_database();'
LANGUAGE SQL;

CREATE TABLE fact_loader.fact_tables (
  fact_table_id SERIAL PRIMARY KEY,
  fact_table_relid REGCLASS NOT NULL,
  fact_table_agg_proid REGPROC NULL, --This may only be used to generate a merge function but is not used in automation
  enabled BOOLEAN NOT NULL DEFAULT FALSE,
  priority INT,
  attempt_number INT,
  retries_allowed INT DEFAULT 0,
  force_worker_priority BOOLEAN NOT NULL DEFAULT FALSE,
  last_refresh_source_cutoff TIMESTAMPTZ,
  last_refresh_attempted_at TIMESTAMPTZ,
  --TODO - answer if we want the worker to bail or record messages on ERROR (or both)
  last_refresh_succeeded BOOLEAN,
  row_created_at TIMESTAMPTZ DEFAULT NOW(),
  row_updated_at TIMESTAMPTZ,
  CONSTRAINT unique_fact_tables UNIQUE (fact_table_relid)
);
SELECT pg_catalog.pg_extension_config_dump('fact_loader.fact_tables', '');

CREATE TABLE fact_loader.fact_table_deps (
  fact_table_dep_id SERIAL PRIMARY KEY,
  parent_id INT NOT NULL REFERENCES fact_loader.fact_tables (fact_table_id),
  child_id INT NOT NULL REFERENCES fact_loader.fact_tables (fact_table_id),
  /*****
  In very many cases, you will use the same procs for insert, update, and delete
  even with multiple dependencies.  This is why you must give defaults here which
  will be used to auto-populate fact_loader.fact_table_dep_queue_table_deps which
  can be overridden if necessary for each queue table.

  After you configure all of your fact tables and queue tables, run the function
  refresh_fact_table_dep_queue_table_deps manually to populate fact_table_dep_queue_table_deps,
  then make any changes as necessary.

  You can see an example of this in the test suite
  "seeds" file.  You can also see an override example with order_emails_fact having a
  different proc for orders and reorders delete cases.
   */
  default_insert_merge_proid REGPROC NOT NULL,
  default_update_merge_proid REGPROC NOT NULL,
  default_delete_merge_proid REGPROC NOT NULL,
  row_created_at TIMESTAMPTZ DEFAULT NOW(),
  row_updated_at TIMESTAMPTZ,
  CONSTRAINT unique_fact_deps UNIQUE (parent_id, child_id)
);
SELECT pg_catalog.pg_extension_config_dump('fact_loader.fact_table_deps', '');

CREATE TABLE fact_loader.queue_tables (
  queue_table_id SERIAL PRIMARY KEY,
  queue_table_relid REGCLASS NOT NULL,
  queue_of_base_table_relid REGCLASS NOT NULL,
  /****
  NOTE - the reason for this config existing here is that we have no built-in way
  in pglogical to know which tables belong to which pglogical node.  Therefore, we
  need to configure that.  We hope that some time down the road, this will change,
  and we can derive this information.
   */
  pglogical_node_if_id INT NOT NULL,
  --This is the timezone for the changed_at column - if null, we assume it is timestamptz (we could check that actually)
  queue_table_tz TEXT,
  row_created_at TIMESTAMPTZ DEFAULT NOW(),
  row_updated_at TIMESTAMPTZ,
  CONSTRAINT unique_queue_table UNIQUE (queue_table_relid),
  CONSTRAINT unique_base_table UNIQUE (queue_of_base_table_relid)
);
COMMENT ON COLUMN fact_loader.queue_tables.pglogical_node_if_id IS $$The reason for this config existing here is that we have no built-in way
  in pglogical to know which tables belong to which pglogical node.  Therefore, we
  need to configure that.  We hope that some time down the road, this will change,
  and we can derive this information.$$;
SELECT pg_catalog.pg_extension_config_dump('fact_loader.queue_tables', '');

CREATE TABLE fact_loader.queue_table_deps (
  queue_table_dep_id SERIAL PRIMARY KEY,
  fact_table_id INT NOT NULL REFERENCES fact_loader.fact_tables (fact_table_id),
  queue_table_id INT NOT NULL REFERENCES fact_loader.queue_tables (queue_table_id),
  relevant_change_columns NAME[],
  last_cutoff_id BIGINT,
  last_cutoff_source_time TIMESTAMPTZ,
  insert_merge_proid REGPROC NOT NULL,
  update_merge_proid REGPROC NOT NULL,
  delete_merge_proid REGPROC NOT NULL,
  row_created_at TIMESTAMPTZ DEFAULT NOW(),
  row_updated_at TIMESTAMPTZ,
  CONSTRAINT unique_queue_deps UNIQUE (fact_table_id, queue_table_id)
);
SELECT pg_catalog.pg_extension_config_dump('fact_loader.queue_table_deps', '');

CREATE TABLE fact_loader.key_retrieval_sequences (
  key_retrieval_sequence_id SERIAL PRIMARY KEY,
  queue_table_dep_id INT NOT NULL REFERENCES fact_loader.queue_table_deps (queue_table_dep_id),
  /****
  In almost all cases, we only need to write one way to retrieve keys.  The only exception is, for
  example, when in a delete case, you need to pass a different field (customer_id instead of order_id)
  to the delete_merge_proid function.  You then need a different key_retrieval_sequence to handle
  a different field name for this delete case.

  By default this is NULL, meaning there is no filter, meaning the sequence applies to all events I, U, D.
  Otherwise, you can add scopes in which case you must have one for each of 'I','U','D'.
   */
  filter_scope CHAR(1) NULL,
  level INT NOT NULL,
  return_columns NAME[] NOT NULL,
  is_fact_key BOOLEAN NOT NULL,
  join_to_relation REGCLASS NULL,
  join_to_column NAME NULL,
  return_columns_from_join NAME[] NULL,
  join_return_is_fact_key BOOLEAN NULL,
  CONSTRAINT unique_retrievals UNIQUE (queue_table_dep_id, filter_scope, level),
  CONSTRAINT valid_scopes CHECK (filter_scope IN ('I','U','D'))
);

SELECT pg_catalog.pg_extension_config_dump('fact_loader.key_retrieval_sequences', '');

CREATE TABLE fact_loader.fact_table_dep_queue_table_deps
(
  fact_table_dep_queue_table_dep_id SERIAL PRIMARY KEY,
  fact_table_dep_id              INT REFERENCES fact_loader.fact_table_deps (fact_table_dep_id),
  queue_table_dep_id             INT REFERENCES fact_loader.queue_table_deps (queue_table_dep_id),
  last_cutoff_id                 BIGINT,
  last_cutoff_source_time        TIMESTAMPTZ,
  insert_merge_proid             REGPROC NOT NULL,
  update_merge_proid             REGPROC NOT NULL,
  delete_merge_proid             REGPROC NOT NULL,
  row_created_at                 TIMESTAMPTZ DEFAULT NOW(),
  row_updated_at                 TIMESTAMPTZ,
  CONSTRAINT unique_cutoffs UNIQUE (fact_table_dep_id, queue_table_dep_id)
);

CREATE OR REPLACE FUNCTION fact_loader.unique_scopes()
RETURNS TRIGGER AS
$BODY$
BEGIN
  IF (NEW.filter_scope IS NULL AND EXISTS (
    SELECT 1
    FROM fact_loader.key_retrieval_sequences
    WHERE queue_table_dep_id <> NEW.queue_table_dep_id
      AND NEW.filter_scope IS NOT NULL
      )) OR
      (NEW.filter_scope IS NOT NULL AND EXISTS (
    SELECT 1
    FROM fact_loader.key_retrieval_sequences
    WHERE queue_table_dep_id <> NEW.queue_table_dep_id
      AND NEW.filter_scope IS NULL
      ))
  THEN
    RAISE EXCEPTION $$You must either use a NULL filter_scope to cover all 3 events I, U, D
    or you must specify all 3 events separately I, U, D (For queue_table_dep_id %).
    $$, NEW.queue_table_dep_id;
  END IF;
  RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER unique_scopes
BEFORE INSERT OR UPDATE ON fact_loader.key_retrieval_sequences
FOR EACH ROW EXECUTE PROCEDURE fact_loader.unique_scopes();

/***
This table is unlogged because it only has data mid-transaction and should always be empty
 */
CREATE UNLOGGED TABLE fact_loader.process_queue (
  process_queue_id BIGSERIAL PRIMARY KEY,
  fact_table_id INT NOT NULL REFERENCES fact_loader.fact_tables (fact_table_id),
  proid REGPROC NOT NULL,
  key_value TEXT NOT NULL,
  row_created_at TIMESTAMPTZ DEFAULT NOW(),
  row_updated_at TIMESTAMPTZ
);

CREATE OR REPLACE FUNCTION fact_loader.set_row_updated_at_to_now()
RETURNS TRIGGER AS
$BODY$
BEGIN
  NEW.row_updated_at = now();
  RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER set_row_updated_at_to_now
BEFORE INSERT OR UPDATE ON fact_loader.fact_tables
FOR EACH ROW
WHEN (NEW.row_updated_at IS DISTINCT FROM now())
EXECUTE PROCEDURE fact_loader.set_row_updated_at_to_now();

CREATE TRIGGER set_row_updated_at_to_now
BEFORE INSERT OR UPDATE ON fact_loader.fact_table_deps
FOR EACH ROW
WHEN (NEW.row_updated_at IS DISTINCT FROM now())
EXECUTE PROCEDURE fact_loader.set_row_updated_at_to_now();

CREATE TRIGGER set_row_updated_at_to_now
BEFORE INSERT OR UPDATE ON fact_loader.queue_tables
FOR EACH ROW
WHEN (NEW.row_updated_at IS DISTINCT FROM now())
EXECUTE PROCEDURE fact_loader.set_row_updated_at_to_now();

CREATE TRIGGER set_row_updated_at_to_now
BEFORE INSERT OR UPDATE ON fact_loader.queue_table_deps
FOR EACH ROW
WHEN (NEW.row_updated_at IS DISTINCT FROM now())
EXECUTE PROCEDURE fact_loader.set_row_updated_at_to_now();

CREATE TRIGGER set_row_updated_at_to_now
BEFORE INSERT OR UPDATE ON fact_loader.fact_table_dep_queue_table_deps
FOR EACH ROW
WHEN (NEW.row_updated_at IS DISTINCT FROM now())
EXECUTE PROCEDURE fact_loader.set_row_updated_at_to_now();

CREATE TRIGGER set_row_updated_at_to_now
BEFORE INSERT OR UPDATE ON fact_loader.process_queue
FOR EACH ROW
WHEN (NEW.row_updated_at IS DISTINCT FROM now())
EXECUTE PROCEDURE fact_loader.set_row_updated_at_to_now();

--TODO
-- CREATE TRIGGER verify_columns_are_really_columns
