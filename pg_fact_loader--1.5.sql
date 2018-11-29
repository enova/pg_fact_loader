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

CREATE TYPE fact_loader.table_load_type AS ENUM('delta','full_refresh');


CREATE OR REPLACE FUNCTION fact_loader.create_table_loader_function
(p_source_proc REGPROC,
 p_destination_relation REGCLASS,
 p_ignore_diff_for_columns TEXT[])
RETURNS REGPROC AS
$BODY$
DECLARE
  v_new_proc TEXT;
  v_sql TEXT;
BEGIN

/****
Find the primary key for the destination table.  This is required.
If the destination table does not have a primary key, it should.

This is partly for simplicity, and partly to encourage good practice
that we build and refresh tables based on chosen primary key to match
records 1 for 1, which is basic DB design 101.
 */
  SELECT function_name, function_sql
  INTO v_new_proc, v_sql
  FROM fact_loader.table_loader_function(p_source_proc, p_destination_relation, p_ignore_diff_for_columns);

  EXECUTE v_sql;

  RETURN v_new_proc::REGPROC;

END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fact_loader.execute_queue(p_fact_table_id INT)
RETURNS TABLE (sql TEXT) AS
$BODY$
BEGIN

RETURN QUERY
WITH ordered_process_queue AS
(SELECT process_queue_id, proid, key_value,
  --TODO - either infer the data type of the function args, which is not super easy with postgres,
  --or add configuration fields for the name and data type of these.  This will suffice for now
  --because we only have integer args for all functions
   'integer' AS queue_of_base_table_key_type
FROM fact_loader.process_queue pq
WHERE pq.fact_table_id = p_fact_table_id
ORDER BY process_queue_id)

, with_rank AS
(SELECT format('%s(%s::%s)', proid::TEXT, 'key_value', queue_of_base_table_key_type) AS function_call,
  process_queue_id,
  RANK() OVER (PARTITION BY proid) AS execution_group
FROM ordered_process_queue
)

, execute_sql_groups AS
(
SELECT execution_group,
format($$
WITH newly_processed AS (
SELECT process_queue_id, %s
FROM (
/****
Must wrap this to execute in order of ids
***/
SELECT *
FROM fact_loader.process_queue
WHERE process_queue_id BETWEEN %s AND %s
  AND fact_table_id = %s
ORDER BY process_queue_id) q
)

DELETE FROM fact_loader.process_queue pq USING newly_processed np
WHERE np.process_queue_id = pq.process_queue_id;
$$, function_call, MIN(process_queue_id), MAX(process_queue_id), p_fact_table_id) AS execute_sql
FROM with_rank
GROUP BY execution_group, function_call
ORDER BY execution_group
)

SELECT COALESCE(string_agg(execute_sql,''),'SELECT NULL') AS final_execute_sql
FROM execute_sql_groups;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.execute_table_loader
(p_source_relation REGCLASS,
 p_destination_relation REGCLASS,
 p_ignore_diff_for_columns TEXT[],
 p_load_type fact_loader.table_load_type,
 p_ignore_unmapped_columns BOOLEAN = FALSE)
RETURNS TABLE (upserted INT, deleted INT, truncated BOOLEAN, pct_dest NUMERIC(8,2)) AS
$BODY$
/***
  The SQL executed within this container is the actual
  load to the destination table, and assumes that 'prepare'
  phase has already been run, which is supposed to have gathered
  the actual minimal delta and determine what to do here.
 */
DECLARE
  v_sql TEXT;
  v_unmapped_src_columns TEXT[];
  v_unmapped_dest_columns TEXT[];
BEGIN
  SELECT execute_sql, unmapped_src_columns, unmapped_dest_columns INTO v_sql, v_unmapped_src_columns, v_unmapped_dest_columns
  FROM fact_loader.table_loader(
      p_source_relation,
      p_destination_relation,
      p_ignore_diff_for_columns,
      p_load_type);
  PERFORM fact_loader.table_loader_validator(p_source_relation,
                                        p_destination_relation,
                                        v_unmapped_src_columns,
                                        v_unmapped_dest_columns,
                                        p_ignore_unmapped_columns);
  RAISE LOG 'Executing SQL: %', v_sql;
  EXECUTE v_sql;

  RETURN QUERY
  SELECT * FROM count_tracker;

END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fact_loader.load(p_fact_table_id INT)
RETURNS VOID AS
$BODY$
DECLARE
    v_insert_to_process_queue_sql text;
    v_execute_sql text;
    v_metadata_update_sql text;
    v_debug_rec record;
    v_debug_text text = '';
BEGIN
/***
There are 3 basic steps to this load:
    1. Gather all queue table changes and insert them into a consolidated process_queue
    2. Using the process_queue data, execute the delta load of the fact table
    3. Update the metadata indicating the last records updated for both the queue tables and fact table
 */

/****
Get SQL to insert new data into the consolidated process_queue,
and SQL to update metadata for last_cutoffs.
 */
SELECT insert_to_process_queue_sql, metadata_update_sql
INTO v_insert_to_process_queue_sql, v_metadata_update_sql
FROM fact_loader.sql_builder(p_fact_table_id);

/****
Populate the consolidated queue
 */
RAISE LOG 'Populating Queue for fact_table_id %: %', p_fact_table_id, v_insert_to_process_queue_sql;
EXECUTE COALESCE(v_insert_to_process_queue_sql, $$SELECT 'No queue data' AS result$$);

/****
For DEBUG purposes only to view the actual process_queue.  Requires setting log_min_messages to DEBUG.
 */
IF current_setting('log_min_messages') LIKE 'debug%' THEN
    FOR v_debug_rec IN
        SELECT * FROM fact_loader.process_queue
    LOOP
        v_debug_text = v_debug_text||E'\n'||format('%s', v_debug_rec.process_queue_id||chr(9)||v_debug_rec.fact_table_id||chr(9)||v_debug_rec.proid||chr(9)||v_debug_rec.key_value);
    END LOOP;
    IF v_debug_text <> '' THEN
        v_debug_text = E'\n'||format('%s',
                                     (SELECT string_agg(column_name,chr(9))
                                      FROM information_schema.columns
                                      WHERE table_name = 'process_queue'
                                        AND table_schema = 'fact_loader'
                                        AND column_name NOT LIKE 'row_%_at'))
            ||v_debug_text;
        RAISE DEBUG '%', v_debug_text;
    END IF;
END IF;

/****
With data now in the process_queue, the execute_queue function builds the SQL to execute.
Save this SQL in a variable and execute it.
If there is no data to execute, this is a no-op select statement.
 */
SELECT sql INTO v_execute_sql FROM fact_loader.execute_queue(p_fact_table_id);
RAISE LOG 'Executing Queue for fact_table_id %: %', p_fact_table_id, v_execute_sql;
EXECUTE COALESCE(v_execute_sql, $$SELECT 'No queue data to execute' AS result$$);

/****
With everything finished, we now update the metadata for the fact_table.
Even if no data was processed, we will still move forward last_refresh_attempted_at.

last_refresh_succeeded will be marked true always for now.  It could in the future
be used to indicate a failure in case of a caught error.
 */
RAISE LOG 'Updating metadata for fact_table_id %: %', p_fact_table_id, v_metadata_update_sql;
EXECUTE COALESCE(v_metadata_update_sql,
    format(
    $$UPDATE fact_loader.fact_tables ft
        SET last_refresh_attempted_at = now(),
          last_refresh_succeeded = TRUE
     WHERE fact_table_id = %s;
    $$, p_fact_table_id));

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.prepare_table_loader
(p_source_relation REGCLASS,
 p_destination_relation REGCLASS,
 p_ignore_diff_for_columns TEXT[],
 p_load_type fact_loader.table_load_type,
 p_ignore_unmapped_columns BOOLEAN = FALSE)
RETURNS TABLE (upserted INT, deleted INT, truncated BOOLEAN, pct_dest NUMERIC(8,2)) AS
$BODY$
/***
  The SQL executed within this container is not going
  to lock any of the destination table for writing, which
  is precisely why it is separated from the 'execute' phase
  which actually writes to the table in the shortest transaction
  possible.
 */
DECLARE
  v_sql TEXT;
  v_unmapped_src_columns TEXT[];
  v_unmapped_dest_columns TEXT[];
BEGIN
  SELECT prepare_sql, unmapped_src_columns, unmapped_dest_columns INTO v_sql, v_unmapped_src_columns, v_unmapped_dest_columns
  FROM fact_loader.table_loader(
      p_source_relation,
      p_destination_relation,
      p_ignore_diff_for_columns,
      p_load_type);
  PERFORM fact_loader.table_loader_validator(p_source_relation,
                                        p_destination_relation,
                                        v_unmapped_src_columns,
                                        v_unmapped_dest_columns,
                                        p_ignore_unmapped_columns);
  RAISE LOG 'Executing SQL: %', v_sql;
  EXECUTE v_sql;

  RETURN QUERY
  SELECT * FROM count_tracker;

END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fact_loader.purge_queues
(p_add_interval INTERVAL = '1 hour')
RETURNS VOID AS
$BODY$
/*****
The interval overlap is only important for delete cases in which you may need to join
to another audit table in order to get a deleted row's data.  1 hour is somewhat arbitrary,
but in the delete case, any related deleted rows would seem to normally appear very close to
another relation's deleted rows.  1 hour is probably generous but also safe.
 */
DECLARE
  v_sql TEXT;
BEGIN

WITH eligible_queue_tables_for_purge AS
(SELECT
  /****
  This logic should handle dependent fact tables as well,
  because they share the same queue tables but they have separately
  logged last_cutoffs.
   */
   qt.queue_table_relid
   , qt.queue_table_id_field
   , queue_table_timestamp
   , queue_table_tz
   , MIN(last_cutoff_id)          AS min_cutoff_id
   , MIN(last_cutoff_source_time) AS min_source_time
 FROM fact_loader.queue_deps_all qt
 WHERE qt.last_cutoff_id IS NOT NULL
  /***
  There must be no other fact tables using the same queue
  which have not yet been processed at all
   */
  AND NOT EXISTS
   (SELECT 1
    FROM fact_loader.queue_deps_all qtdx
    WHERE qtdx.queue_table_id = qt.queue_table_id
          AND qtdx.last_cutoff_id IS NULL)
 GROUP BY qt.queue_table_relid
   , qt.queue_table_id_field
   , queue_table_timestamp
   , queue_table_tz)

SELECT
  string_agg(
    format($$
    DELETE FROM %s
    WHERE %s <= %s
      AND %s %s < (%s::TIMESTAMPTZ - interval %s);
    $$,
      queue_table_relid,
      queue_table_id_field,
      min_cutoff_id,
      quote_ident(queue_table_timestamp),
      CASE WHEN queue_table_tz IS NULL THEN '' ELSE 'AT TIME ZONE '||quote_literal(queue_table_tz) END,
      quote_literal(min_source_time),
      quote_literal(p_add_interval::TEXT)
      )
    , E'\n\n')
  INTO v_sql
FROM eligible_queue_tables_for_purge;

IF v_sql IS NOT NULL THEN
  RAISE LOG 'Purging Queue: %', v_sql;
  EXECUTE v_sql;
END IF;

END;
$BODY$
LANGUAGE plpgsql;


CREATE FUNCTION fact_loader.refresh_fact_table_dep_queue_table_deps()
RETURNS VOID AS
$BODY$
BEGIN
/****
This function will be used to refresh the fact_table_dep_queue_table_deps table.
The purpose of this table is to easily figure out queue data for fact tables that depend on other fact tables.
This will be run with every call of load().
This may not be the most efficient method, but it is certainly reliable and fast.
 */

/****
Recursively find all fact table deps including nested ones (fact tables that depend on other fact tables)
to build the fact_table_dep_queue_table_deps table.
 */
WITH RECURSIVE all_fact_table_deps AS (
  SELECT
    qtd.queue_table_dep_id
    , ftd.fact_table_dep_id
    , parent_id                                 AS parent_fact_table_id
    , child_id                                  AS fact_table_id
    , qtd.queue_table_id
    , qt.queue_table_relid
    , ftp.fact_table_relid AS parent_fact_table
    , ftc.fact_table_relid AS child_fact_table
    , ftd.default_insert_merge_proid
    , ftd.default_update_merge_proid
    , ftd.default_delete_merge_proid
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt ON qtd.queue_table_id = qt.queue_table_id
  INNER JOIN fact_loader.fact_table_deps ftd ON ftd.parent_id = qtd.fact_table_id
  INNER JOIN fact_loader.fact_tables ftp USING (fact_table_id)
  INNER JOIN fact_loader.fact_tables ftc ON ftc.fact_table_id = ftd.child_id
  UNION ALL
  SELECT
    qtd.queue_table_dep_id
    , ftd.fact_table_dep_id
    , parent_id                                 AS parent_fact_table_id
    , child_id                                  AS fact_table_id
    , qtd.queue_table_id
    , qt.queue_table_relid
    , ftp.fact_table_relid AS parent_fact_table
    , ft.fact_table_relid AS child_fact_table
    , ftd.default_insert_merge_proid
    , ftd.default_update_merge_proid
    , ftd.default_delete_merge_proid
  FROM all_fact_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt ON qtd.queue_table_id = qt.queue_table_id
  INNER JOIN fact_loader.fact_table_deps ftd ON ftd.parent_id = qtd.fact_table_id
  INNER JOIN fact_loader.fact_tables ftp ON ftp.fact_table_id = ftd.parent_id
  INNER JOIN fact_loader.fact_tables ft ON ft.fact_table_id = ftd.child_id
)

/****
Remove fact_table_dep_queue_table_deps that no longer exist if applicable
 */
, removed AS (
  DELETE FROM fact_loader.fact_table_dep_queue_table_deps ftdqc
  WHERE NOT EXISTS(SELECT 1
                   FROM all_fact_table_deps aftd
                   WHERE aftd.fact_table_dep_id = ftdqc.fact_table_dep_id
                    AND aftd.queue_table_dep_id = ftdqc.queue_table_dep_id)
)

/****
Add any new keys or ignore if they already exist
 */
INSERT INTO fact_loader.fact_table_dep_queue_table_deps
(fact_table_dep_id, queue_table_dep_id, insert_merge_proid, update_merge_proid, delete_merge_proid)
SELECT fact_table_dep_id, queue_table_dep_id, default_insert_merge_proid, default_update_merge_proid, default_delete_merge_proid
FROM all_fact_table_deps
ON CONFLICT (fact_table_dep_id, queue_table_dep_id)
DO NOTHING;

END;
$BODY$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION fact_loader.table_loader
(p_source_relation REGCLASS,
 p_destination_relation REGCLASS,
 p_ignore_diff_for_columns TEXT[],
 p_load_type fact_loader.table_load_type)
RETURNS TABLE (prepare_sql text, execute_sql text, unmapped_src_columns text[], unmapped_dest_columns text[]) AS
$BODY$
DECLARE
  v_pkey_fields TEXT[];
BEGIN

/****
Find the primary key for the destination table.  This is required.
If the destination table does not have a primary key, it should.

This is partly for simplicity, and partly to encourage good practice
that we build and refresh tables based on chosen primary key to match
records 1 for 1, which is basic DB design 101.
 */
SELECT array_agg(a.attname ORDER BY pk.rn) INTO v_pkey_fields
FROM (SELECT
        i.indrelid
        , unnest(indkey) AS ik
        , row_number()
          OVER ()        AS rn
      FROM pg_index i
      WHERE i.indrelid = p_destination_relation AND i.indisprimary) pk
INNER JOIN pg_attribute a
  ON a.attrelid = pk.indrelid AND a.attnum = pk.ik;

RETURN QUERY
WITH source_columns AS
(
  SELECT column_name, ordinal_position, CASE WHEN column_name = ANY(v_pkey_fields) THEN TRUE ELSE FALSE END AS pkey_field
  FROM information_schema.columns c
  INNER JOIN pg_class pc ON pc.relname = c.table_name AND pc.oid = p_source_relation
  INNER JOIN pg_namespace n ON n.oid = pc.relnamespace AND c.table_schema = n.nspname
  ORDER BY ordinal_position
)

, destination_columns AS
(
  SELECT column_name, ordinal_position, CASE WHEN column_name = ANY(v_pkey_fields) THEN TRUE ELSE FALSE END AS pkey_field
  FROM information_schema.columns c
  INNER JOIN pg_class pc ON pc.relname = c.table_name AND pc.oid = p_destination_relation
  INNER JOIN pg_namespace n ON n.oid = pc.relnamespace AND c.table_schema = n.nspname
  ORDER BY ordinal_position
)

, unmapped_source_columns AS
(
  SELECT array_agg(s.column_name::text) AS unmapped_columns_src
  FROM source_columns s
  WHERE NOT EXISTS
  (SELECT 1 FROM destination_columns d WHERE d.column_name = s.column_name)
)

, unmapped_dest_columns AS
(
  SELECT array_agg(d.column_name::text) AS unmapped_columns_dest
  FROM destination_columns d
  WHERE NOT EXISTS
  (SELECT 1 FROM source_columns s WHERE d.column_name = s.column_name)
)

, pkeys AS
(
  SELECT
    string_agg(quote_ident(pkey_field),E'\n, ') AS pkey_fields,
    string_agg($$s.$$||quote_ident(pkey_field)||$$ = d.$$||quote_ident(pkey_field),E'\nAND ') AS pkey_join
  FROM
  (SELECT unnest AS pkey_field
  FROM unnest(v_pkey_fields)) pk
)

, info AS
(
    SELECT
        string_agg(
          CASE
            WHEN sc.column_name IS NOT NULL
              THEN dc.column_name
            ELSE
              NULL
          END, E'\n  , '
          ORDER BY dc.ordinal_position
        )
          AS matching_column_list
      , string_agg(
          CASE
            WHEN sc.column_name IS NOT NULL
             AND (p_ignore_diff_for_columns IS NULL
                  OR sc.column_name != ALL (p_ignore_diff_for_columns)
                 )
              THEN dc.column_name
            ELSE
              NULL
          END, E'\n  , '
          ORDER BY dc.ordinal_position
        )
        AS matching_column_list_without_ignored
      , string_agg(
          CASE
            WHEN sc.column_name IS NOT NULL
             AND NOT dc.pkey_field
              THEN dc.column_name || ' = EXCLUDED.' || dc.column_name
            ELSE
              NULL
          END, E'\n  , '
          ORDER BY dc.ordinal_position
        )
        AS upsert_list
      , pkeys.pkey_fields
      , pkeys.pkey_join
    FROM destination_columns dc
      CROSS JOIN pkeys
      LEFT JOIN source_columns sc ON dc.column_name = sc.column_name
    GROUP BY pkeys.pkey_fields,
      pkeys.pkey_join
)

, sql_snippets AS
(
  SELECT
    $$
    DROP TABLE IF EXISTS count_tracker;
    CREATE TEMP TABLE count_tracker (upserted INT, deleted INT, truncated BOOLEAN, pct_dest NUMERIC(8,2));
    INSERT INTO count_tracker VALUES (NULL, NULL, FALSE, NULL);
    $$::TEXT
      AS count_tracker_sql
    , $$
    DROP TABLE IF EXISTS actual_delta;
    CREATE TEMP TABLE actual_delta AS
    WITH final_diff AS (
    SELECT $$||pkey_fields||$$
    FROM
    (SELECT $$||matching_column_list_without_ignored||$$
    FROM $$||p_source_relation::TEXT||$$
    EXCEPT
    SELECT $$||matching_column_list_without_ignored||$$
    FROM $$||p_destination_relation::TEXT||$$ d $$
      AS actual_delta_sql

    , $$
    DROP TABLE IF EXISTS removed_keys;
    CREATE TEMP TABLE removed_keys AS
    SELECT $$||pkey_fields||$$
    FROM $$||p_destination_relation::TEXT||$$ d
    WHERE NOT EXISTS (SELECT 1 FROM $$||p_source_relation::TEXT||$$ s WHERE $$||pkey_join||$$);
    $$
      AS removed_keys_sql

    , $$
    ) full_diff)

    --This extra step is necessarily precisely because we may want to not except every column, like load_dttm
    SELECT *
    FROM $$||p_source_relation::TEXT||$$ s
    WHERE EXISTS (
      SELECT 1
      FROM final_diff d
      WHERE $$||pkey_join||$$
    );
    $$
      AS except_join_to_source_sql

    , $$
    /***
      We add the exists here because we are only looking for column-level differences
      for the given keys that have changed.  This may be a very small portion of the
      table.  Without the exists clause, this second part of EXCEPT would do a full
      table scan unnecessarily.
    ***/
    WHERE EXISTS (SELECT 1 FROM $$||p_source_relation::TEXT||$$ s WHERE $$||pkey_join||$$)$$
      AS key_join_exists_sql

    , $$
    /***
      We add a primary key to the actual_delta table to ensure there are no duplicate keys.
    ***/
    ALTER TABLE actual_delta ADD PRIMARY KEY ($$||pkey_fields||$$);
    $$
      AS add_delta_pkey_sql

    , $$
    /****
      This part is not implemented yet, but partially complete.
      If we decide we want to figure out that >50% of the table will be updated, we could decide
      to truncate.  But then we have to balance the desire for that with more read queries to
      figure it out.

      To implement, add the type full_refresh_truncate to fact_loader.table_load_type, and uncomment code.
      We would also have to add the logic to find actual keys added, then subtract it from actual_delta
      to get the net updates expected.  If this is over 50%, we should truncate and re-insert all data.
    ***/
    DROP TABLE IF EXISTS percent_of_destination;
    CREATE TEMP TABLE percent_of_destination AS
    SELECT
    (((SELECT COUNT(1) FROM actual_delta) - (SELECT COUNT(1) FROM added_keys))::NUMERIC /
    (SELECT COUNT(1) FROM $$||p_destination_relation::TEXT||$$)::NUMERIC)::NUMERIC(8,2) AS pct;

    UPDATE count_tracker SET pct_dest = (SELECT pct FROM percent_of_destination);
    $$
      AS percent_change_sql

    ,$$
    DO $LOCK_SAFE_DDL$
    BEGIN
    SET lock_timeout TO '10ms';
    IF (SELECT pct FROM percent_of_destination) >= 0.5 THEN
      LOOP
        BEGIN
          TRUNCATE $$||p_destination_relation::TEXT||$$;
          UPDATE count_tracker SET truncated = true;
          EXIT;
        EXCEPTION
          WHEN lock_not_available
            THEN RAISE WARNING 'Could not obtain immediate lock for SQL %, retrying', p_sql;
            PERFORM pg_sleep(3);
          WHEN OTHERS THEN
            RAISE;
        END;
      END LOOP;
    END IF;
    RESET lock_timeout;
    END
    $LOCK_SAFE_DDL$
    ;
    $$
      AS lock_safe_truncate_sql

    ,$$
    --Delete keys that are no longer in your new version
    DELETE FROM $$||p_destination_relation::TEXT||$$ d
    WHERE EXISTS
        (SELECT 1 FROM removed_keys s WHERE $$||pkey_join||$$);

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    UPDATE count_tracker SET deleted = v_row_count;
    $$
      AS delete_sql

    ,$$
    INSERT INTO $$||p_destination_relation::TEXT||$$ AS t ($$||
    matching_column_list||$$)
    SELECT $$||matching_column_list||
    $$ FROM actual_delta
    ON CONFLICT ($$||pkey_fields||$$)
    DO UPDATE
    SET $$||upsert_list||$$
    ;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    UPDATE count_tracker SET upserted = v_row_count;
    $$
      AS upsert_sql
  FROM info
)

SELECT
  count_tracker_sql||
  CASE
/*** not implemented truncate pattern
    WHEN p_load_type IN('full_refresh','full_refresh_truncate') THEN
***/
    WHEN p_load_type = 'full_refresh' THEN
      removed_keys_sql||actual_delta_sql||except_join_to_source_sql||add_delta_pkey_sql||$$;$$
    WHEN p_load_type = 'delta' THEN
      actual_delta_sql||key_join_exists_sql||except_join_to_source_sql||add_delta_pkey_sql||$$;$$
  END||$$

    $$||
/*** not implemented truncate pattern
    CASE
    WHEN p_load_type = 'full_refresh_truncate' THEN
      percent_change_sql
    ELSE
    ''
  END
***/
  ''
  AS prepare_sql
  , $$
  --THIS SHOULD BE RUN IN A TRANSACTION
  DO $SCRIPT$
  DECLARE
    v_row_count INT;
    v_results RECORD;
  BEGIN
  $$||
  CASE
/*** not implemented truncate pattern
    WHEN p_load_type = 'full_refresh_truncate' THEN
      lock_safe_truncate_sql||delete_sql||upsert_sql
***/
    WHEN p_load_type = 'full_refresh' THEN
      delete_sql||upsert_sql
  WHEN p_load_type = 'delta' THEN
      upsert_sql
  END||$$

  FOR v_results IN SELECT * FROM count_tracker LOOP
    RAISE LOG 'upserted: %, deleted: %, truncated: %, pct_dest: %',
    v_results.upserted, v_results.deleted, v_results.truncated, v_results.pct_dest;
  END LOOP;

  END
  $SCRIPT$;

  $$ AS execute_sql

  , (SELECT unmapped_columns_src FROM unmapped_source_columns) AS unmapped_src_columns
  , (SELECT unmapped_columns_dest FROM unmapped_dest_columns) AS unmapped_dest_columns
FROM sql_snippets;

END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fact_loader.table_loader_function
(p_source_proc REGPROC,
 p_destination_relation REGCLASS,
 p_ignore_diff_for_columns TEXT[])
RETURNS TABLE (function_name text, function_sql text) AS
$BODY$
BEGIN

/****
Find the primary key for the destination table.  This is required.
If the destination table does not have a primary key, it should.

This is partly for simplicity, and partly to encourage good practice
that we build and refresh tables based on chosen primary key to match
records 1 for 1, which is basic DB design 101.
 */
RETURN QUERY
WITH get_pkey_fields AS (
SELECT
  a.attname,
  format_type(a.atttypid, a.atttypmod) AS atttype,
  pk.rn
FROM (SELECT
        i.indrelid
        , unnest(indkey) AS ik
        , row_number()
          OVER ()        AS rn
      FROM pg_index i
      WHERE i.indrelid = p_destination_relation AND i.indisprimary) pk
INNER JOIN pg_attribute a
  ON a.attrelid = pk.indrelid AND a.attnum = pk.ik)

, pkey_fields_sorted AS
(SELECT array_agg(attname ORDER BY rn) AS pkey_fields FROM get_pkey_fields)

, function_args AS
(SELECT regexp_matches(pg_get_function_identity_arguments(p_source_proc),'(?:^|, )(\w+)','g') AS arg)

, function_schema AS
(SELECT string_agg(arg[1],', ') AS arg_params,
 pg_get_function_identity_arguments(p_source_proc) AS arg_defs
 FROM function_args)

, destination_columns AS
(
  SELECT c.table_schema, c.table_name, column_name, ordinal_position, CASE WHEN gpf.attname IS NOT NULL THEN TRUE ELSE FALSE END AS pkey_field
  FROM information_schema.columns c
  INNER JOIN pg_class pc ON pc.relname = c.table_name AND pc.oid = p_destination_relation
  INNER JOIN pg_namespace n ON n.oid = pc.relnamespace AND c.table_schema = n.nspname
  LEFT JOIN get_pkey_fields gpf ON gpf.attname = c.column_name
  ORDER BY ordinal_position
)

, pkeys AS
(
  SELECT
    string_agg(quote_ident(pkey_field),E'\n, ') AS pkey_fields,
    string_agg(quote_ident(pkey_field)||' '||pkey_type,', ') AS pkey_fields_ddl,
    string_agg($$s.$$||quote_ident(pkey_field)||$$ = d.$$||quote_ident(pkey_field),E'\nAND ') AS pkey_join
  FROM
  (SELECT attname AS pkey_field,
    atttype AS pkey_type
  FROM get_pkey_fields
  ORDER BY rn) pk
)

, info AS
(
    SELECT
        string_agg(
          dc.column_name, E'\n  , '
          ORDER BY dc.ordinal_position
        )
          AS matching_column_list
      , string_agg(
          CASE
            WHEN (p_ignore_diff_for_columns IS NULL
                  OR dc.column_name != ALL (p_ignore_diff_for_columns)
                 )
              THEN dc.column_name
            ELSE
              NULL
          END, E'\n  , '
          ORDER BY dc.ordinal_position
        )
        AS matching_column_list_without_ignored
      , string_agg(
          CASE
            WHEN NOT dc.pkey_field
              THEN dc.column_name || ' = EXCLUDED.' || dc.column_name
            ELSE
              NULL
          END, E'\n  , '
          ORDER BY dc.ordinal_position
        )
        AS upsert_list
      , pkeys.pkey_fields
      , pkeys.pkey_fields_ddl
      , pkeys.pkey_join
      , quote_ident(dc.table_schema)||'.'||quote_ident(table_name||'_merge') AS proposed_function_name
      , fs.arg_params
      , fs.arg_defs
    FROM destination_columns dc
      CROSS JOIN pkeys
      CROSS JOIN function_schema fs
    GROUP BY pkeys.pkey_fields,
      pkeys.pkey_fields_ddl,
      pkeys.pkey_join,
      quote_ident(dc.table_schema)||'.'||quote_ident(table_name||'_merge'),
      fs.arg_params,
      fs.arg_defs
)

, sql_snippets AS
(
  SELECT
    proposed_function_name
    , $$
    CREATE OR REPLACE FUNCTION $$||proposed_function_name||$$($$||arg_defs||$$)
    RETURNS VOID AS
    $FUNC$
    BEGIN
    $$::TEXT AS function_start
    , $$
    END;
    $FUNC$
    LANGUAGE plpgsql;
    $$::TEXT AS function_end
    , $$
    WITH actual_delta AS (
    $$::TEXT AS actual_delta_cte

    , $$
    WITH data AS (
    SELECT * FROM $$||p_source_proc::TEXT||$$($$||arg_params||$$)
    )

    , final_diff AS (
    SELECT $$||pkey_fields||$$
    FROM
    (SELECT $$||matching_column_list_without_ignored||$$
    FROM data
    EXCEPT
    SELECT $$||matching_column_list_without_ignored||$$
    FROM $$||p_destination_relation::TEXT||$$ d $$
      AS actual_delta_sql

    , $$
    ) full_diff)

    --This extra step is necessarily precisely because we may want to not except every column, like load_dttm
    SELECT *
    FROM data s
    WHERE EXISTS (
      SELECT 1
      FROM final_diff d
      WHERE $$||pkey_join||$$
    )
    $$
      AS except_join_to_source_sql

    , $$
    /***
      We add the exists here because we are only looking for column-level differences
      for the given keys that have changed.  This may be a very small portion of the
      table.  Without the exists clause, this second part of EXCEPT would do a full
      table scan unnecessarily.
    ***/
    WHERE EXISTS (SELECT 1 FROM data s WHERE $$||pkey_join||$$)$$
      AS key_join_exists_sql

    ,$$
    INSERT INTO $$||p_destination_relation::TEXT||$$ AS t ($$||
    matching_column_list||$$)
    SELECT $$||matching_column_list||
    $$ FROM actual_delta
    ON CONFLICT ($$||pkey_fields||$$)
    DO UPDATE
    SET $$||upsert_list||$$
    ;
    $$
      AS upsert_sql
  FROM info
)

SELECT
  proposed_function_name AS function_name
  , function_start||actual_delta_cte||actual_delta_sql||except_join_to_source_sql||')'||upsert_sql||function_end
      AS function_sql
FROM sql_snippets;

END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fact_loader.table_loader_validator
(p_source_relation REGCLASS,
  p_destination_relation REGCLASS,
  p_unmapped_src_columns TEXT[],
  p_unmapped_dest_columns TEXT[],
  p_ignore_unmapped_columns BOOLEAN)
RETURNS VOID AS
$BODY$
DECLARE v_messages TEXT = '';
BEGIN

IF NOT p_ignore_unmapped_columns AND p_unmapped_src_columns IS NOT NULL THEN
  v_messages = format($$You have unmapped columns (%s) in the source table %s.
  All source columns must be named identically to destination
  in order to map.

  If you are certain you want to ignore these columns, meaning they
  will not update anything in destination table %s, add the final argument
  to this function as TRUE.  $$
    , array_to_string(p_unmapped_src_columns,', ')
    , p_source_relation::TEXT
    , p_destination_relation::TEXT);
END IF;
IF NOT p_ignore_unmapped_columns AND p_unmapped_dest_columns IS NOT NULL THEN
  v_messages = v_messages||format($$

  You have unmapped columns (%s) in the destination table %s.
  All destination columns must be named identically to source
  in order to map.

  If you are certain you want to ignore these columns, meaning the source
  table %s does not contain all columns in destination table, add the final argument
  to this function as TRUE.$$
    , array_to_string(p_unmapped_dest_columns,', ')
    , p_destination_relation::TEXT
    , p_source_relation::TEXT);
END IF;
IF v_messages <> '' THEN
  RAISE EXCEPTION '%', v_messages;
END IF;

END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fact_loader.worker()
RETURNS BOOLEAN AS
$BODY$
DECLARE
  v_fact_record RECORD;
  c_lock_cutoff_refresh INT = 99995;
BEGIN


/****
Attempt to refresh fact_table_dep_queue_table_deps or ignore if refresh is in progress.
 */
IF (SELECT pg_try_advisory_xact_lock(c_lock_cutoff_refresh)) THEN
  PERFORM fact_loader.refresh_fact_table_dep_queue_table_deps();
END IF;

/****
Acquire an advisory lock on the row indicating this job, which will cause the function
to simply return false if another session is running it concurrently.
It will be released upon transaction commit or rollback.
 */
FOR v_fact_record IN
  SELECT fact_table_id
  FROM fact_loader.fact_tables
  WHERE enabled
  ORDER BY
    CASE WHEN force_worker_priority THEN 0 ELSE 1 END,
    --This may be improved in the future but is a good start
    last_refresh_attempted_at NULLS FIRST,
    priority
LOOP

IF (SELECT pg_try_advisory_xact_lock(fact_table_id)
        FROM fact_loader.fact_tables
        WHERE fact_table_id = v_fact_record.fact_table_id) THEN
  --Load fact table
  PERFORM fact_loader.load(v_fact_record.fact_table_id);

  /***
  Run purge process.  This need not run every launch of worker but it should not hurt.
  It is better for it to run after the fact table load is successful so as to avoid a
  rollback and more dead bloat
   */
  PERFORM fact_loader.purge_queues();

  RETURN TRUE;
END IF;

END LOOP;

RETURN FALSE;

END;
$BODY$
LANGUAGE plpgsql;


/* pg_fact_loader--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fact_loader" to load this file. \quit

ALTER TABLE fact_loader.key_retrieval_sequences ADD COLUMN pass_queue_table_change_date_at_tz TEXT NULL;
COMMENT ON COLUMN fact_loader.key_retrieval_sequences.pass_queue_table_change_date_at_tz
IS
$$If this is set to a time zone, then the changed_at field will be cast to this time zone and then cast to a date, for the purpose of creating a date-range based fact table.
For casting queue_table_timestamp to a date, we first ensure we have it as timestamptz (objective UTC time).
Then, we cast it to the timezone of interest on which the date should be based.
For example, 02:00:00 UTC time on 2018-05-02 is actually 2018-05-01 in America/Chicago time.
Thus, any date-based fact table must decide in what time zone to consider the date.$$;

ALTER TABLE fact_loader.key_retrieval_sequences ADD CONSTRAINT verify_valid_tz CHECK (pass_queue_table_change_date_at_tz IS NULL OR (now() AT TIME ZONE pass_queue_table_change_date_at_tz IS NOT NULL));

--This check constraint could have been added in v. 1.0
ALTER TABLE fact_loader.queue_tables ADD CONSTRAINT verify_valid_tz CHECK (queue_table_tz IS NULL OR (now() AT TIME ZONE queue_table_tz IS NOT NULL));

ALTER TABLE fact_loader.process_queue ADD COLUMN source_change_date DATE NULL;
COMMENT ON COLUMN fact_loader.process_queue.source_change_date
IS
'Corresponds to fact_loader.key_retrieval_sequences.pass_queue_table_change_date_at_tz.  If this field is populated, a function will be expected that has args (key_value, source_change_date) based on this process_queue table.';

--This should have already been added in v. 1.0
SELECT pg_catalog.pg_extension_config_dump('fact_loader.fact_table_dep_queue_table_deps', '');

ALTER TABLE fact_loader.queue_table_deps
  ALTER COLUMN insert_merge_proid DROP NOT NULL,
  ALTER COLUMN update_merge_proid DROP NOT NULL,
  ALTER COLUMN delete_merge_proid DROP NOT NULL;

ALTER TABLE fact_loader.fact_table_dep_queue_table_deps
  ALTER COLUMN insert_merge_proid DROP NOT NULL,
  ALTER COLUMN update_merge_proid DROP NOT NULL,
  ALTER COLUMN delete_merge_proid DROP NOT NULL;

ALTER TABLE fact_loader.fact_table_deps
  ALTER COLUMN default_insert_merge_proid DROP NOT NULL,
  ALTER COLUMN default_update_merge_proid DROP NOT NULL,
  ALTER COLUMN default_delete_merge_proid DROP NOT NULL;





CREATE OR REPLACE FUNCTION fact_loader.execute_queue(p_fact_table_id INT)
RETURNS TABLE (sql TEXT) AS
$BODY$
BEGIN

RETURN QUERY
WITH ordered_process_queue AS
(SELECT
   process_queue_id
   , proid
   , key_value
   , source_change_date
   , (pp.proargtypes::REGTYPE[])[0] AS proid_first_arg
 FROM fact_loader.process_queue pq
   LEFT JOIN pg_proc pp ON pp.oid = proid
 WHERE pq.fact_table_id = p_fact_table_id
 ORDER BY process_queue_id)

, with_rank AS
(SELECT
  /****
  If source_change_date is NULL, we assume the proid has one arg and pass it.
  If not, we assume the proid has two args and pass source_change_date as the second.
  */
   format('%s(%s::%s%s)'
          , proid::TEXT
          , 'key_value'
          , proid_first_arg
          , CASE
              WHEN source_change_date IS NOT NULL
                THEN format(', %s::DATE',quote_literal(source_change_date))
              ELSE ''
            END
        ) AS function_call,
  proid,
  process_queue_id,
  RANK() OVER (PARTITION BY proid) AS execution_group
FROM ordered_process_queue
)

, execute_sql_groups AS
(
SELECT execution_group,
format($$
WITH newly_processed AS (
SELECT process_queue_id, %s
FROM (
/****
Must wrap this to execute in order of ids
***/
SELECT *
FROM fact_loader.process_queue
WHERE process_queue_id BETWEEN %s AND %s
  AND fact_table_id = %s
  AND proid = %s::REGPROC
ORDER BY process_queue_id) q
)

DELETE FROM fact_loader.process_queue pq USING newly_processed np
WHERE np.process_queue_id = pq.process_queue_id;
$$, function_call, MIN(process_queue_id), MAX(process_queue_id), p_fact_table_id, quote_literal(proid::TEXT)) AS execute_sql
FROM with_rank
GROUP BY execution_group, function_call, proid
ORDER BY execution_group
)

SELECT COALESCE(string_agg(execute_sql,''),'SELECT NULL') AS final_execute_sql
FROM execute_sql_groups;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.table_loader_function
(p_source_proc REGPROC,
 p_destination_relation REGCLASS,
 p_ignore_diff_for_columns TEXT[])
RETURNS TABLE (function_name text, function_sql text) AS
$BODY$
BEGIN

/****
Find the primary key for the destination table.  This is required.
If the destination table does not have a primary key, it should.

This is partly for simplicity, and partly to encourage good practice
that we build and refresh tables based on chosen primary key to match
records 1 for 1, which is basic DB design 101.
 */
RETURN QUERY
WITH get_pkey_fields AS (
SELECT
  a.attname,
  format_type(a.atttypid, a.atttypmod) AS atttype,
  pk.rn
FROM (SELECT
        i.indrelid
        , unnest(indkey) AS ik
        , row_number()
          OVER ()        AS rn
      FROM pg_index i
      WHERE i.indrelid = p_destination_relation AND i.indisprimary) pk
INNER JOIN pg_attribute a
  ON a.attrelid = pk.indrelid AND a.attnum = pk.ik)

, pkey_fields_sorted AS
(SELECT array_agg(attname ORDER BY rn) AS pkey_fields FROM get_pkey_fields)

, function_args AS
(SELECT regexp_matches(pg_get_function_identity_arguments(p_source_proc),'(?:^|, )(\w+)','g') AS arg)

, function_schema AS
(SELECT string_agg(arg[1],', ') AS arg_params,
 pg_get_function_identity_arguments(p_source_proc) AS arg_defs
 FROM function_args)

, destination_columns AS
(
  SELECT c.table_schema, c.table_name, column_name, ordinal_position, CASE WHEN gpf.attname IS NOT NULL THEN TRUE ELSE FALSE END AS pkey_field
  FROM information_schema.columns c
  INNER JOIN pg_class pc ON pc.relname = c.table_name AND pc.oid = p_destination_relation
  INNER JOIN pg_namespace n ON n.oid = pc.relnamespace AND c.table_schema = n.nspname
  LEFT JOIN get_pkey_fields gpf ON gpf.attname = c.column_name
  ORDER BY ordinal_position
)

, pkeys AS
(
  SELECT
    string_agg(quote_ident(pkey_field),E'\n, ') AS pkey_fields,
    string_agg(quote_ident(pkey_field)||' '||pkey_type,', ') AS pkey_fields_ddl,
    string_agg($$s.$$||quote_ident(pkey_field)||$$ = d.$$||quote_ident(pkey_field),E'\nAND ') AS pkey_join,
    string_agg($$d.$$||quote_ident(pkey_field)||$$ = $$||(SELECT arg_params FROM function_schema),E'\nAND ') AS pkey_join_to_arg
  FROM
  (SELECT attname AS pkey_field,
    atttype AS pkey_type
  FROM get_pkey_fields
  ORDER BY rn) pk
)

, info AS
(
    SELECT
        string_agg(
          dc.column_name, E'\n  , '
          ORDER BY dc.ordinal_position
        )
          AS matching_column_list
      , string_agg(
          CASE
            WHEN (p_ignore_diff_for_columns IS NULL
                  OR dc.column_name != ALL (p_ignore_diff_for_columns)
                 )
              THEN dc.column_name
            ELSE
              NULL
          END, E'\n  , '
          ORDER BY dc.ordinal_position
        )
        AS matching_column_list_without_ignored
      , string_agg(
          CASE
            WHEN NOT dc.pkey_field
              THEN dc.column_name || ' = EXCLUDED.' || dc.column_name
            ELSE
              NULL
          END, E'\n  , '
          ORDER BY dc.ordinal_position
        )
        AS upsert_list
      , pkeys.pkey_fields
      , pkeys.pkey_fields_ddl
      , pkeys.pkey_join
      , quote_ident(dc.table_schema)||'.'||quote_ident(table_name||'_merge') AS proposed_function_name
      , fs.arg_params
      , fs.arg_defs
      , pkey_join_to_arg
    FROM destination_columns dc
      CROSS JOIN pkeys
      CROSS JOIN function_schema fs
    GROUP BY pkeys.pkey_fields,
      pkeys.pkey_fields_ddl,
      pkeys.pkey_join,
      quote_ident(dc.table_schema)||'.'||quote_ident(table_name||'_merge'),
      fs.arg_params,
      fs.arg_defs,
      pkey_join_to_arg
)

, sql_snippets AS
(
  SELECT
    proposed_function_name
    , $$
    CREATE OR REPLACE FUNCTION $$||proposed_function_name||$$($$||arg_defs||$$)
    RETURNS VOID AS
    $FUNC$
    BEGIN
    $$::TEXT AS function_start
    , $$
    END;
    $FUNC$
    LANGUAGE plpgsql;
    $$::TEXT AS function_end
    , $$
    WITH actual_delta AS (
    $$::TEXT AS actual_delta_cte

    , $$
    WITH data AS (
    SELECT * FROM $$||p_source_proc::TEXT||$$($$||arg_params||$$)
    )

    , final_diff AS (
    SELECT $$||pkey_fields||$$
    FROM
    (SELECT $$||matching_column_list_without_ignored||$$
    FROM data
    EXCEPT
    SELECT $$||matching_column_list_without_ignored||$$
    FROM $$||p_destination_relation::TEXT||$$ d
    WHERE $$||pkey_join_to_arg
      AS actual_delta_sql

    , $$
    ) full_diff)

    --This extra step is necessarily precisely because we may want to not except every column, like load_dttm
    SELECT *
    FROM data s
    WHERE EXISTS (
      SELECT 1
      FROM final_diff d
      WHERE $$||pkey_join||$$
    )
    $$
      AS except_join_to_source_sql

    , $$
    /***
      We add the exists here because we are only looking for column-level differences
      for the given keys that have changed.  This may be a very small portion of the
      table.  Without the exists clause, this second part of EXCEPT would do a full
      table scan unnecessarily.
    ***/
    WHERE EXISTS (SELECT 1 FROM data s WHERE $$||pkey_join||$$)$$
      AS key_join_exists_sql

    ,$$
    INSERT INTO $$||p_destination_relation::TEXT||$$ AS t ($$||
    matching_column_list||$$)
    SELECT $$||matching_column_list||
    $$ FROM actual_delta
    ON CONFLICT ($$||pkey_fields||$$)
    DO UPDATE
    SET $$||upsert_list||$$
    ;
    $$
      AS upsert_sql
  FROM info
)

SELECT
  proposed_function_name AS function_name
  , function_start||actual_delta_cte||actual_delta_sql||except_join_to_source_sql||')'||upsert_sql||function_end
      AS function_sql
FROM sql_snippets;

END;
$BODY$
LANGUAGE plpgsql;

/* pg_fact_loader--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fact_loader" to load this file. \quit

--To support non-replicated queue tables
ALTER TABLE fact_loader.queue_tables
  ALTER COLUMN pglogical_node_if_id DROP NOT NULL;

CREATE TABLE fact_loader.fact_table_refresh_logs
(fact_table_refresh_log_id SERIAL PRIMARY KEY,
fact_table_id INT REFERENCES fact_loader.fact_tables (fact_table_id),
refresh_attempted_at TIMESTAMPTZ,
messages TEXT);

ALTER TABLE fact_loader.fact_tables
  ADD COLUMN use_daily_schedule BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN daily_scheduled_time TIME NULL,
  ADD COLUMN daily_scheduled_tz TEXT NULL,
  ADD COLUMN daily_scheduled_proid REGPROC,
  ADD CONSTRAINT verify_valid_daily_tz
    CHECK (daily_scheduled_tz IS NULL OR
           (now() AT TIME ZONE daily_scheduled_tz IS NOT NULL)),
  ADD CONSTRAINT daily_schedule_configured_correctly
    CHECK ((NOT use_daily_schedule) OR
           (use_daily_schedule
            AND daily_scheduled_time IS NOT NULL
            AND daily_scheduled_tz IS NOT NULL
            AND daily_scheduled_proid IS NOT NULL));



CREATE OR REPLACE VIEW fact_loader.unresolved_failures AS
SELECT ft.fact_table_id,
  fact_table_relid,
  refresh_attempted_at,
  messages
FROM fact_loader.fact_tables ft
INNER JOIN fact_loader.fact_table_refresh_logs ftrl
  ON ft.fact_table_id = ftrl.fact_table_id
  AND ft.last_refresh_attempted_at = ftrl.refresh_attempted_at
WHERE NOT enabled
  AND NOT last_refresh_succeeded;

CREATE OR REPLACE VIEW fact_loader.prioritized_jobs AS
SELECT *
FROM fact_loader.fact_tables
WHERE enabled
  AND (NOT use_daily_schedule OR
       --Only run this job according to the same day of the daily_scheduled_time
       --according to configured timezone
       (
         (last_refresh_attempted_at IS NULL
          OR last_refresh_attempted_at::DATE < (now() AT TIME ZONE daily_scheduled_tz)::DATE
         )
          AND (now() AT TIME ZONE daily_scheduled_tz)::TIME
            BETWEEN daily_scheduled_time AND '23:59:59.999999'::TIME
       )
      )
ORDER BY
  CASE WHEN force_worker_priority THEN 0 ELSE 1 END,
  --If a job has a daily schedule, once the time has come for the next refresh,
  --prioritize it first
  CASE
    WHEN (use_daily_schedule AND
          (last_refresh_attempted_at IS NULL
            OR last_refresh_attempted_at::DATE < (now() AT TIME ZONE daily_scheduled_tz)::DATE
           )
          AND (now() AT TIME ZONE daily_scheduled_tz)::TIME
            BETWEEN daily_scheduled_time AND '23:59:59.999999'::TIME)
    THEN (now() AT TIME ZONE daily_scheduled_tz)::TIME
    ELSE NULL
  END NULLS LAST,
  --This may be improved in the future but is a good start
  last_refresh_attempted_at NULLS FIRST,
  priority
;


CREATE OR REPLACE FUNCTION fact_loader.daily_scheduled_load(p_fact_table_id INT)
RETURNS BOOLEAN AS
$BODY$
DECLARE
    v_execute_sql text;
BEGIN
/***
There are 2 basic steps to this load:
    1. Execute the single daily-refresh function
    2. Update the metadata indicating the last attempt time
 */
SELECT 'SELECT '||daily_scheduled_proid::TEXT||'()' INTO v_execute_sql
FROM fact_loader.fact_tables
WHERE fact_table_id = p_fact_table_id
  AND use_daily_schedule;

IF v_execute_sql IS NULL THEN
    RETURN FALSE;
END IF;

EXECUTE v_execute_sql;

UPDATE fact_loader.fact_tables ft
SET last_refresh_attempted_at = now(),
    last_refresh_succeeded = TRUE
WHERE fact_table_id = p_fact_table_id;

RETURN TRUE;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.try_load(p_fact_table_id INT)
RETURNS BOOLEAN AS
$BODY$
/***
This will be used by the worker, but can also be used safely if a DBA
wants to run a job manually.
 */
DECLARE
  c_lock_cutoff_refresh INT = 99995;
BEGIN

IF (SELECT pg_try_advisory_xact_lock(fact_table_id)
        FROM fact_loader.fact_tables
        WHERE fact_table_id = p_fact_table_id) THEN
  /****
  Attempt to refresh fact_table_dep_queue_table_deps or ignore if refresh is in progress.
   */
  IF (SELECT pg_try_advisory_xact_lock(c_lock_cutoff_refresh)) THEN
    PERFORM fact_loader.refresh_fact_table_dep_queue_table_deps();
  END IF;

  --Load fact table and handle exceptions to auto-disable job and log errors in case of error
  BEGIN
    --Scheduled daily job
    IF (SELECT use_daily_schedule
        FROM fact_loader.fact_tables
        WHERE fact_table_id = p_fact_table_id) THEN

      PERFORM fact_loader.daily_scheduled_load(p_fact_table_id);

    --Queue-based job
    ELSE
      PERFORM fact_loader.load(p_fact_table_id);

      /***
      Run purge process.  This need not run every launch of worker but it should not hurt.
      It is better for it to run after the fact table load is successful so as to avoid a
      rollback and more dead bloat
       */
      PERFORM fact_loader.purge_queues();

    END IF;

    RETURN TRUE;

  EXCEPTION
    WHEN OTHERS THEN
      UPDATE fact_loader.fact_tables
      SET last_refresh_succeeded = FALSE,
          last_refresh_attempted_at = now(),
          enabled = FALSE
      WHERE fact_table_id = p_fact_table_id;

      INSERT INTO fact_loader.fact_table_refresh_logs (fact_table_id, refresh_attempted_at, messages)
      VALUES (p_fact_table_id, now(), SQLERRM);

      RETURN FALSE;
  END;
ELSE
  RETURN FALSE;
END IF;

END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fact_loader.worker()
RETURNS BOOLEAN AS
$BODY$
DECLARE
  v_fact_record RECORD;
BEGIN

/****
Acquire an advisory lock on the row indicating this job, which will cause the function
to simply return false if another session is running it concurrently.
It will be released upon transaction commit or rollback.
 */
FOR v_fact_record IN
  SELECT fact_table_id
  FROM fact_loader.prioritized_jobs
LOOP

IF fact_loader.try_load(v_fact_record.fact_table_id) THEN
  RETURN TRUE;
END IF;

END LOOP;

--If no jobs returned true, then return false
RETURN FALSE;

END;
$BODY$
LANGUAGE plpgsql;


/* pg_fact_loader--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fact_loader" to load this file. \quit

DROP VIEW IF EXISTS fact_loader.queue_deps_all_with_retrieval;
DROP VIEW IF EXISTS fact_loader.queue_deps_all;
DROP VIEW IF EXISTS fact_loader.logical_subscription;
DROP VIEW IF EXISTS fact_loader.prioritized_jobs;
DROP VIEW IF EXISTS fact_loader.unresolved_failures;
DROP FUNCTION IF EXISTS fact_loader.sql_builder(int);
CREATE OR REPLACE FUNCTION fact_loader.add_batch_id_fields()
RETURNS VOID
AS $BODY$
DECLARE
  v_rec RECORD;
  v_sql TEXT;
BEGIN

FOR v_rec IN
  SELECT queue_table_relid
  FROM fact_loader.queue_tables qt
  INNER JOIN pg_class c ON c.oid = qt.queue_table_relid
  INNER JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE NOT EXISTS
    (SELECT 1
    FROM information_schema.columns col
    WHERE col.column_name = 'fact_loader_batch_id'
      AND col.table_schema = n.nspname
      AND col.table_name = c.relname)
LOOP

v_sql = format($F$
ALTER TABLE %s
ADD COLUMN fact_loader_batch_id
BIGINT
DEFAULT nextval('fact_loader.batch_id');
$F$,
v_rec.queue_table_relid::text,
v_rec.queue_table_relid::text);

RAISE LOG 'ADDING fact_loader_batch_id COLUMN TO queue table %: %', v_rec.queue_table_relid::text, v_sql;
EXECUTE v_sql;

END LOOP;

END
$BODY$
LANGUAGE plpgsql;


ALTER TABLE fact_loader.queue_tables ADD COLUMN purge BOOLEAN NOT NULL DEFAULT TRUE;
UPDATE fact_loader.fact_table_refresh_logs SET messages = jsonb_build_object('Message', messages) WHERE messages IS NOT NULL;
--Will be re-added via \i in sql file
ALTER TABLE fact_loader.fact_table_refresh_logs ALTER COLUMN messages TYPE jsonb USING messages::jsonb;

--This was a problem from the start
ALTER TABLE fact_loader.queue_tables ALTER COLUMN pglogical_node_if_id TYPE OID;

--This should have been done from the start
SELECT pg_catalog.pg_extension_config_dump('fact_loader.fact_table_dep_queue_table_de_fact_table_dep_queue_table_de_seq', '');
SELECT pg_catalog.pg_extension_config_dump('fact_loader.fact_table_deps_fact_table_dep_id_seq', '');
SELECT pg_catalog.pg_extension_config_dump('fact_loader.fact_tables_fact_table_id_seq', '');
SELECT pg_catalog.pg_extension_config_dump('fact_loader.key_retrieval_sequences_key_retrieval_sequence_id_seq', '');
SELECT pg_catalog.pg_extension_config_dump('fact_loader.queue_table_deps_queue_table_dep_id_seq', '');
SELECT pg_catalog.pg_extension_config_dump('fact_loader.queue_tables_queue_table_id_seq', '');

--No indexes or anything but allow debugging
CREATE UNLOGGED TABLE fact_loader.debug_process_queue (LIKE fact_loader.process_queue);
ALTER TABLE fact_loader.debug_process_queue ADD PRIMARY KEY (process_queue_id);

-- Now a temp table to avoid serialization contention
DROP TABLE fact_loader.process_queue;

--Make this a trigger to check dep fact tables
ALTER TABLE fact_loader.fact_tables ADD COLUMN depends_on_base_daily_job_id INT REFERENCES fact_loader.fact_tables (fact_table_id);
ALTER TABLE fact_loader.fact_tables ADD COLUMN depends_on_parent_daily_job_id INT REFERENCES fact_loader.fact_tables (fact_table_id);
ALTER TABLE fact_loader.fact_tables DROP CONSTRAINT daily_schedule_configured_correctly;
ALTER TABLE fact_loader.fact_tables ADD CONSTRAINT daily_schedule_configured_correctly CHECK (NOT use_daily_schedule OR (use_daily_schedule AND ((daily_scheduled_time IS NOT NULL AND daily_scheduled_tz IS NOT NULL AND daily_scheduled_proid IS NOT NULL) OR (depends_on_base_daily_job_id IS NOT NULL AND depends_on_parent_daily_job_id IS NOT NULL))));

--These columns have never been used
ALTER TABLE fact_loader.fact_tables DROP COLUMN attempt_number, DROP COLUMN retries_allowed;

--This is the usual case and makes sense
ALTER TABLE fact_loader.key_retrieval_sequences ALTER COLUMN level SET DEFAULT 1;

--Need to have a more reliable dependency knowledge for scheduled jobs
ALTER TABLE fact_loader.fact_tables ADD COLUMN daily_scheduled_deps REGCLASS[];
ALTER TABLE fact_loader.fact_tables ADD COLUMN daily_scheduled_dep_delay_tolerance INTERVAL;
ALTER TABLE fact_loader.fact_tables ADD CONSTRAINT daily_deps_correctly_configured CHECK ((daily_scheduled_deps IS NULL AND daily_scheduled_dep_delay_tolerance IS NULL) OR (daily_scheduled_deps IS NOT NULL AND daily_scheduled_dep_delay_tolerance IS NOT NULL));

--Log all events and add pruning
ALTER TABLE fact_loader.fact_table_refresh_logs ADD COLUMN refresh_finished_at TIMESTAMPTZ;
ALTER TABLE fact_loader.fact_table_refresh_logs ALTER COLUMN fact_table_refresh_log_id TYPE BIGINT;

-- Handle race conditions by changing to batch usage
CREATE SEQUENCE fact_loader.batch_id;
SELECT fact_loader.add_batch_id_fields();


CREATE OR REPLACE FUNCTION fact_loader.queue_table_delay_info()
RETURNS TABLE("replication_set_name" text,
           "queue_of_base_table_relid" regclass,
           "if_id" oid,
           "if_name" name,
           "source_time" timestamp with time zone)
AS
$BODY$
/***
This function exists to allow no necessary dependency
to exist on pglogical_ticker.  If the extension is used,
it will return data from its native functions, if not,
it will return a null data set matching the structure
***/
BEGIN

IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pglogical_ticker') THEN
    RETURN QUERY EXECUTE $$
    SELECT
        unnest(coalesce(sub_replication_sets,'{NULL}')) AS replication_set_name
      , qt.queue_of_base_table_relid
      , n.if_id
      , n.if_name
      --source_time is now() for local tables (pglogical_node_if_id is null), and based on pglogical_ticker time otherwise
      , CASE
        WHEN qt.pglogical_node_if_id IS NULL
          THEN now()
        ELSE t.source_time
        END                          AS source_time
    FROM fact_loader.queue_tables qt
      LEFT JOIN fact_loader.logical_subscription() s ON qt.pglogical_node_if_id = s.sub_origin_if
      LEFT JOIN pglogical.node_interface n ON n.if_id = qt.pglogical_node_if_id
      LEFT JOIN pglogical_ticker.all_subscription_tickers() t ON t.provider_name = n.if_name;$$;
ELSE
    RETURN QUERY
    SELECT
        NULL::TEXT AS replication_set_name
      , qt.queue_of_base_table_relid
      , NULL::OID AS if_id
      , NULL::NAME AS if_name
      --source_time is now() if queue tables are not pglogical-replicated, which is assumed if no ticker
      , now() AS source_time
    FROM fact_loader.queue_tables qt;
END IF;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE VIEW fact_loader.queue_deps_all AS
WITH RECURSIVE fact_table_dep_cutoffs AS
(SELECT
    1 AS level
    , qtd.queue_table_dep_id
    , ftdqc.fact_table_dep_id
    , ftdqc.fact_table_dep_queue_table_dep_id
    --This dep_maximum_cutoff_time is being taken from the queue_table_deps, because we cannot go past when the
    --fact table has been updated
    , qtd.last_cutoff_id                        AS dep_maximum_cutoff_id
    , qtd.last_cutoff_source_time               AS dep_maximum_cutoff_time
    , ftd.parent_id                                 AS parent_fact_table_id
    , ftd.child_id                                  AS child_fact_table_id
    , ftd.child_id                                  AS base_fact_table_id
    , queue_table_id
    , relevant_change_columns
    , ftdqc.last_cutoff_id
    , ftdqc.last_cutoff_source_time
    , ftdqc.insert_merge_proid
    , ftdqc.update_merge_proid
    , ftdqc.delete_merge_proid
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.fact_table_dep_queue_table_deps ftdqc ON ftdqc.queue_table_dep_id = qtd.queue_table_dep_id
  INNER JOIN fact_loader.fact_table_deps ftd ON ftd.fact_table_dep_id = ftdqc.fact_table_dep_id
  UNION ALL
  /****
  In this recursive part, we walk UP the chain to the base level in order to get the
  last_cutoff_id and last_cutoff_source_time of parent_ids because children must never surpass those.

  The ONLY difference between this recursive part and the non-recursive part are the dep_maximum_cutoffs.
  That means we can get our resultant data below by simply selecting distinct ON the right fields and order
  by dep_maximum_cutoffs to get the most conservative cutoff window, that is, the minimum cutoff amongst
  the queue tables and any PARENT fact table cutoffs.

  That means if, for example,
    - IF a queue table has been cutoff up until 11:00:00
    - AND IF a level 1 fact table dependent on that queue table was last cutoff at 10:55:00
    - THEN a level 2 fact table dependent on level 1 fact table must not go past 10:55:00 when it is processed.
   */
  SELECT
    ftdc.level + 1 AS level
    , ftdc.queue_table_dep_id
    , ftdc.fact_table_dep_id
    , ftdc.fact_table_dep_queue_table_dep_id
    --This dep_maximum_cutoff_time is being taken from the queue_table_deps, because we cannot go past when the
    --fact table has been updated
    , ftdqc.last_cutoff_id                        AS dep_maximum_cutoff_id
    , ftdqc.last_cutoff_source_time               AS dep_maximum_cutoff_time
    , ftd.parent_id                                 AS parent_fact_table_id
    , ftd.child_id                                  AS child_fact_table_id
    , ftdc.base_fact_table_id
    , ftdc.queue_table_id
    , ftdc.relevant_change_columns
    , ftdc.last_cutoff_id
    , ftdc.last_cutoff_source_time
    , ftdc.insert_merge_proid
    , ftdc.update_merge_proid
    , ftdc.delete_merge_proid
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.fact_table_dep_queue_table_deps ftdqc ON ftdqc.queue_table_dep_id = qtd.queue_table_dep_id
  INNER JOIN fact_loader.fact_table_deps ftd ON ftd.fact_table_dep_id = ftdqc.fact_table_dep_id
  INNER JOIN fact_table_dep_cutoffs ftdc ON ftdc.parent_fact_table_id = ftd.child_id
)

, adjusted_fact_table_deps AS (
/****
The reason we look at distinct queue_table_dep_id and not simply queue_table_id
is because two parent fact tables could have differing logic for retrieving changes
for the same base queue_tables.
 */
SELECT DISTINCT ON(base_fact_table_id, queue_table_dep_id)
*
FROM fact_table_dep_cutoffs
ORDER BY base_fact_table_id, queue_table_dep_id, dep_maximum_cutoff_time
)

, queue_table_info AS (
    SELECT * FROM fact_loader.queue_table_delay_info()
)

/****
For fact tables that depend on other fact tables, we join the child fact table to the queue_table_deps of the parent
fact table, and just reuse this exactly, with these distinctions:
  - From the fact_table_dep table, we do use the proids, and the last_cutoff_id
  - We use the parent last_cutoff_source_time as the maximum_cutoff, because we can only update those records already updated on the parent
  - We pass the information of which table for which to update metadata in the end
 */
, queue_table_deps_with_nested AS (
  /****
  This part of the union is for the base level of queue_table_deps - for fact tables with no other dependent fact tables
   */
  SELECT
    queue_table_dep_id
    , NULL :: INT                                AS fact_table_dep_id
    , NULL :: INT                                AS fact_table_dep_queue_table_dep_id
    , NULL :: BIGINT                             AS dep_maximum_cutoff_id
    , NULL :: TIMESTAMPTZ                        AS dep_maximum_cutoff_time
    , fact_table_id
    , queue_table_id
    , relevant_change_columns
    , last_cutoff_id
    , last_cutoff_source_time
    , insert_merge_proid
    , update_merge_proid
    , delete_merge_proid
  FROM fact_loader.queue_table_deps
  UNION ALL
  /****
  This part of the union is for fact tables with other dependent fact tables
   */
  SELECT
    queue_table_dep_id
    , fact_table_dep_id
    , fact_table_dep_queue_table_dep_id
    , aftd.dep_maximum_cutoff_id
    , aftd.dep_maximum_cutoff_time
    , base_fact_table_id                                  AS fact_table_id
    , queue_table_id
    , relevant_change_columns
    , aftd.last_cutoff_id
    , aftd.last_cutoff_source_time
    , aftd.insert_merge_proid
    , aftd.update_merge_proid
    , aftd.delete_merge_proid
  FROM adjusted_fact_table_deps aftd
)

SELECT
  ft.fact_table_id,
  ft.fact_table_relid,
  ft.fact_table_agg_proid,
  qt.queue_table_id,
  qt.queue_table_relid,
  qt.queue_of_base_table_relid,
  qtd.relevant_change_columns,
  qtd.last_cutoff_id,
  qtd.last_cutoff_source_time,
  rt.if_name AS provider_name,
  rt.replication_set_name,
  qtd.dep_maximum_cutoff_id,  --Not used yet - TODO - think about if it needs to be used to filter as cutoff MAX in addition to the time filter
  LEAST(
      MIN(qtd.dep_maximum_cutoff_time)
      OVER (
        PARTITION BY qtd.fact_table_id ),
      MIN(rt.source_time)
      OVER (
        PARTITION BY qtd.fact_table_id )
  ) AS maximum_cutoff_time,
  aqt.queue_table_id_field,
  'primary_key'::name AS queue_table_key,
  'operation'::name AS queue_table_op,
  'change'::name AS queue_table_change,
  'changed_at'::name AS queue_table_timestamp,
  qt.queue_table_tz,
  aqbt.queue_of_base_table_key,
  aqbt.queue_of_base_table_key_type,
  queue_table_dep_id,
  fact_table_dep_id,
  fact_table_dep_queue_table_dep_id,
  insert_merge_proid,
  update_merge_proid,
  delete_merge_proid,
  qt.purge
FROM queue_table_deps_with_nested qtd
INNER JOIN fact_loader.fact_tables ft ON ft.fact_table_id = qtd.fact_table_id
INNER JOIN fact_loader.queue_tables qt ON qt.queue_table_id = qtd.queue_table_id
INNER JOIN queue_table_info rt ON rt.queue_of_base_table_relid = qt.queue_of_base_table_relid
INNER JOIN LATERAL
  (SELECT a.attname AS queue_of_base_table_key, format_type(atttypid, atttypmod) AS queue_of_base_table_key_type
  FROM (SELECT
          i.indrelid
          , unnest(indkey) AS ik
          , row_number()
            OVER ()        AS rn
        FROM pg_index i
        WHERE i.indrelid = qt.queue_of_base_table_relid AND i.indisprimary) pk
  INNER JOIN pg_attribute a
    ON a.attrelid = pk.indrelid AND a.attnum = pk.ik) aqbt ON TRUE
INNER JOIN LATERAL
  (SELECT a.attname AS queue_table_id_field
  FROM (SELECT
          i.indrelid
          , unnest(indkey) AS ik
          , row_number()
            OVER ()        AS rn
        FROM pg_index i
        WHERE i.indrelid = qt.queue_table_relid AND i.indisprimary) pk
  INNER JOIN pg_attribute a
    ON a.attrelid = pk.indrelid AND a.attnum = pk.ik) aqt ON TRUE
ORDER BY ft.fact_table_relid;


CREATE OR REPLACE VIEW fact_loader.queue_deps_all_with_retrieval AS
SELECT
  qtd.*,
  krs.filter_scope,
  krs.level,
  krs.return_columns, --we need not get the type separately.  It must match queue_of_base_table_key_type
  krs.is_fact_key,
  krs.join_to_relation,
  qtk.queue_table_relid AS join_to_relation_queue,
  krs.join_to_column,
  ctypes.join_column_type,
  krs.return_columns_from_join,
  ctypes.return_columns_from_join_type,
  krs.join_return_is_fact_key,
  /***
  We include this in this view def to be easily shared by all events (I, U, D) in sql_builder,
  as those may be different in terms of passing source_change_date.
   */
  format(', %s::DATE AS source_change_date',
    CASE
      WHEN krs.pass_queue_table_change_date_at_tz IS NOT NULL
        /***
        For casting queue_table_timestamp to a date, we first ensure we have it as timestamptz (objective UTC time).
        Then, we cast it to the timezone of interest on which the date should be based.
        For example, 02:00:00 UTC time on 2018-05-02 is actually 2018-05-01 in America/Chicago time.
        Thus, any date-based fact table must decide in what time zone to consider the date.
         */
        THEN format('(%s %s AT TIME ZONE %s)',
                    'q.'||quote_ident(qtd.queue_table_timestamp),
                    CASE WHEN qtd.queue_table_tz IS NULL THEN '' ELSE 'AT TIME ZONE '||quote_literal(qtd.queue_table_tz) END,
                    quote_literal(krs.pass_queue_table_change_date_at_tz))
        ELSE 'NULL'
      END) AS source_change_date_select
FROM fact_loader.queue_deps_all qtd
INNER JOIN fact_loader.key_retrieval_sequences krs ON qtd.queue_table_dep_id = krs.queue_table_dep_id
LEFT JOIN fact_loader.queue_tables qtk ON qtk.queue_of_base_table_relid = krs.join_to_relation
LEFT JOIN LATERAL
  (SELECT MAX(CASE WHEN attname = krs.join_to_column THEN format_type(atttypid, atttypmod) ELSE NULL END) AS join_column_type,
    MAX(CASE WHEN attname = krs.return_columns_from_join[1] THEN format_type(atttypid, atttypmod) ELSE NULL END) AS return_columns_from_join_type
  FROM pg_attribute a
  WHERE a.attrelid IN(krs.join_to_relation)
    /****
    We stubbornly assume that if there are multiple columns in return_columns_from_join, they all have the same type.
    Undue complexity would ensue if we did away with that rule.
     */
    AND a.attname IN(krs.join_to_column,krs.return_columns_from_join[1])) ctypes ON TRUE;


CREATE OR REPLACE FUNCTION fact_loader.purge_queues
(p_add_interval INTERVAL = '1 hour')
RETURNS VOID AS
$BODY$
/*****
The interval overlap is only important for delete cases in which you may need to join
to another audit table in order to get a deleted row's data.  1 hour is somewhat arbitrary,
but in the delete case, any related deleted rows would seem to normally appear very close to
another relation's deleted rows.  1 hour is probably generous but also safe.
 */
DECLARE
  v_sql TEXT;
BEGIN

WITH eligible_queue_tables_for_purge AS
(SELECT
  /****
  This logic should handle dependent fact tables as well,
  because they share the same queue tables but they have separately
  logged last_cutoffs.
   */
   qt.queue_table_relid
   , queue_table_timestamp
   , queue_table_tz
   , MIN(last_cutoff_id)          AS min_cutoff_id
   , MIN(last_cutoff_source_time) AS min_source_time
 FROM fact_loader.queue_deps_all qt
 WHERE qt.last_cutoff_id IS NOT NULL AND qt.purge
  /***
  There must be no other fact tables using the same queue
  which have not yet been processed at all
   */
  AND NOT EXISTS
   (SELECT 1
    FROM fact_loader.queue_deps_all qtdx
    WHERE qtdx.queue_table_id = qt.queue_table_id
          AND qtdx.last_cutoff_id IS NULL)
 GROUP BY qt.queue_table_relid
   , queue_table_timestamp
   , queue_table_tz)

SELECT
  string_agg(
    format($$
    DELETE FROM %s
    WHERE %s IN
      (SELECT %s
      FROM %s
      WHERE %s <= %s
      AND %s %s < (%s::TIMESTAMPTZ - interval %s)
      FOR UPDATE SKIP LOCKED
      );
    $$,
      queue_table_relid,
      'fact_loader_batch_id',
      'fact_loader_batch_id',
      queue_table_relid,
      'fact_loader_batch_id',
      min_cutoff_id,
      quote_ident(queue_table_timestamp),
      CASE WHEN queue_table_tz IS NULL THEN '' ELSE 'AT TIME ZONE '||quote_literal(queue_table_tz) END,
      quote_literal(min_source_time),
      quote_literal(p_add_interval::TEXT)
      )
    , E'\n\n')
  INTO v_sql
FROM eligible_queue_tables_for_purge;

IF v_sql IS NOT NULL THEN
    RAISE DEBUG 'Purging Queue: %', v_sql;
    BEGIN
        EXECUTE v_sql;
    EXCEPTION
        WHEN serialization_failure THEN
            RAISE LOG 'Serialization failure in queue purging for transaction % - skipping.', txid_current()::text;
        WHEN OTHERS THEN
            RAISE;
    END;
END IF;

END;
$BODY$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION fact_loader.worker()
RETURNS BOOLEAN AS
$BODY$
DECLARE
  v_fact_record RECORD;
BEGIN

/****
Acquire an advisory lock on the row indicating this job, which will cause the function
to simply return false if another session is running it concurrently.
It will be released upon transaction commit or rollback.
 */
FOR v_fact_record IN
  SELECT fact_table_id
  FROM fact_loader.prioritized_jobs
LOOP

IF fact_loader.try_load(v_fact_record.fact_table_id) THEN
--If any configured functions use temp tables,
--must discard to avoid them hanging around in the idle background worker session
  DISCARD TEMP;

--Log job times
  INSERT INTO fact_loader.fact_table_refresh_logs (fact_table_id, refresh_attempted_at, refresh_finished_at)
  VALUES (v_fact_record.fact_table_id, now(), clock_timestamp());

--Return true meaning the fact table was refreshed (this applies even if there was no new data)
  RETURN TRUE;
END IF;

END LOOP;

--If no jobs returned true, then return false
RETURN FALSE;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.try_load(p_fact_table_id INT)
RETURNS BOOLEAN AS
$BODY$
/***
This will be used by the worker, but can also be used safely if a DBA
wants to run a job manually.
 */
DECLARE
  c_lock_cutoff_refresh INT = 99995;
  v_err JSONB;
  v_errmsg TEXT;
  v_errdetail TEXT;
  v_errhint TEXT;
  v_errcontext TEXT;
BEGIN

-- We except rare serialization failures here which we will ignore and move to the next record
-- Anything else should be raised
BEGIN
IF EXISTS (SELECT fact_table_id
        FROM fact_loader.fact_tables
        WHERE fact_table_id = p_fact_table_id
        FOR UPDATE SKIP LOCKED) THEN
  /****
  Attempt to refresh fact_table_dep_queue_table_deps or ignore if refresh is in progress.
   */
  IF (SELECT pg_try_advisory_xact_lock(c_lock_cutoff_refresh)) THEN
    PERFORM fact_loader.refresh_fact_table_dep_queue_table_deps();
  END IF;

  --Load fact table and handle exceptions to auto-disable job and log errors in case of error
  BEGIN
    --Scheduled daily job
    IF (SELECT use_daily_schedule
        FROM fact_loader.fact_tables
        WHERE fact_table_id = p_fact_table_id) THEN

      PERFORM fact_loader.daily_scheduled_load(p_fact_table_id);

    --Queue-based job
    ELSE
      PERFORM fact_loader.load(p_fact_table_id);

      /***
      Run purge process.  This need not run every launch of worker but it should not hurt.
      It is better for it to run after the fact table load is successful so as to avoid a
      rollback and more dead bloat
       */
      PERFORM fact_loader.purge_queues();

    END IF;

    RETURN TRUE;

  EXCEPTION
    WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_errmsg = MESSAGE_TEXT,
        v_errdetail = PG_EXCEPTION_DETAIL,
        v_errhint = PG_EXCEPTION_HINT,
        v_errcontext = PG_EXCEPTION_CONTEXT;

      UPDATE fact_loader.fact_tables
      SET last_refresh_succeeded = FALSE,
          last_refresh_attempted_at = now(),
          enabled = FALSE
      WHERE fact_table_id = p_fact_table_id;

      v_err = jsonb_strip_nulls(
            jsonb_build_object(
            'Message', v_errmsg,
            'Detail', case when v_errdetail = '' then null else v_errdetail end,
            'Hint', case when v_errhint = '' then null else v_errhint end,
            'Context', case when v_errcontext = '' then null else v_errcontext end)
            );

      INSERT INTO fact_loader.fact_table_refresh_logs (fact_table_id, refresh_attempted_at, refresh_finished_at, messages)
      VALUES (p_fact_table_id, now(), clock_timestamp(), v_err);

      RETURN FALSE;
  END;
ELSE
  RETURN FALSE;
END IF;

EXCEPTION
    WHEN serialization_failure THEN
        RAISE LOG 'Serialization failure on transaction % attempting to lock % - skipping.', txid_current()::text, p_fact_table_id::text;
        RETURN FALSE;
    WHEN OTHERS THEN
        RAISE;

END;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.load(p_fact_table_id INT)
RETURNS VOID AS
$BODY$
DECLARE
    v_process_queue_sql text;
    v_execute_sql text;
    v_metadata_update_sql text;
    v_debug_rec record;
    v_debug_text text = '';
BEGIN
/***
There are 3 basic steps to this load:
    1. Gather all queue table changes and insert them into a consolidated process_queue
    2. Update the metadata indicating the last records updated for both the queue tables and fact table
 */

/****
Get SQL to insert new data into the consolidated process_queue,
and SQL to update metadata for last_cutoffs.
 */
SELECT process_queue_sql, metadata_update_sql
INTO v_process_queue_sql, v_metadata_update_sql
FROM fact_loader.sql_builder(p_fact_table_id);

/****
Populate the consolidated queue
This just creates a temp table with all changes to be processed
 */
RAISE DEBUG 'Populating Queue for fact_table_id %: %', p_fact_table_id, v_process_queue_sql;
EXECUTE COALESCE(v_process_queue_sql, $$SELECT 'No queue data' AS result$$);

/****
For DEBUG purposes only to view the actual process_queue.  Requires setting log_min_messages to DEBUG.
 */
IF current_setting('log_min_messages') = 'debug3' THEN
    INSERT INTO fact_loader.debug_process_queue
    SELECT * FROM process_queue;
END IF;

/****
With data now in the process_queue, the execute_queue function builds the SQL to execute.
Save this SQL in a variable and execute it.
If there is no data to execute, this is a no-op select statement.
 */
SELECT sql INTO v_execute_sql FROM fact_loader.execute_queue(p_fact_table_id);
RAISE DEBUG 'Executing Queue for fact_table_id %: %', p_fact_table_id, v_execute_sql;
EXECUTE COALESCE(v_execute_sql, $$SELECT 'No queue data to execute' AS result$$);

/****
With everything finished, we now update the metadata for the fact_table.
Even if no data was processed, we will still move forward last_refresh_attempted_at.

last_refresh_succeeded will be marked true always for now.  It could in the future
be used to indicate a failure in case of a caught error.
 */
RAISE DEBUG 'Updating metadata for fact_table_id %: %', p_fact_table_id, v_metadata_update_sql;
EXECUTE COALESCE(v_metadata_update_sql,
    format(
    $$UPDATE fact_loader.fact_tables ft
        SET last_refresh_attempted_at = now(),
          last_refresh_succeeded = TRUE
     WHERE fact_table_id = %s;
    $$, p_fact_table_id));

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE VIEW fact_loader.unresolved_failures AS
SELECT ft.fact_table_id,
  fact_table_relid,
  refresh_attempted_at,
  messages
FROM fact_loader.fact_tables ft
INNER JOIN fact_loader.fact_table_refresh_logs ftrl
  ON ft.fact_table_id = ftrl.fact_table_id
  AND ft.last_refresh_attempted_at = ftrl.refresh_attempted_at
WHERE NOT enabled
  AND NOT last_refresh_succeeded;

CREATE OR REPLACE FUNCTION fact_loader.safely_terminate_workers()
RETURNS TABLE (number_terminated INT, number_still_live INT, pids_still_live INT[]) AS
$BODY$
/****
It is not a requirement to use this function to terminate workers.  Because workers are transactional,
you can simply terminate them and no data loss will result in pg_fact_loader.

Likewise, a hard crash of any system using pg_fact_loader will recover just fine upon re-launching workers.

Still, it is ideal to avoid bloat to cleanly terminate workers and restart them using this function to kill
them, and launch_workers(int) to re-launch them.
 */
BEGIN

RETURN QUERY
WITH try_term_pids AS (
SELECT
  pid,
  CASE WHEN
    state = 'idle' AND
    state_change BETWEEN SYMMETRIC
      now() - interval '5 seconds' AND
      now() - interval '55 seconds'
    THEN
    pg_terminate_backend(pid)
    ELSE FALSE
  END AS terminated
FROM pg_stat_activity
WHERE usename = 'postgres'
  AND query = 'SELECT fact_loader.worker();')

SELECT SUM(CASE WHEN terminated THEN 1 ELSE 0 END)::INT AS number_terminated_out,
  SUM(CASE WHEN NOT terminated THEN 1 ELSE 0 END)::INT AS number_still_live_out,
  (SELECT array_agg(pid) FROM try_term_pids WHERE NOT terminated) AS pids_still_live_out
FROM try_term_pids;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.launch_workers(number_to_launch int)
RETURNS INT[] AS
$BODY$
DECLARE
  v_pids INT[];
BEGIN

FOR i IN 1..number_to_launch
LOOP
  v_pids = array_append(v_pids, fact_loader.launch_worker());
/*
It's not strictly required to not launch all workers simultaneously, but it's
also a little more invasive to do that, probably requiring more advisory lock skips.
Better to just sleep 1 second between launches.
 */
PERFORM pg_sleep(1);
END LOOP;

RETURN v_pids;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE VIEW fact_loader.prioritized_jobs AS
WITH jobs_with_daily_variables AS (
SELECT
   ft.*,
/***
Keep all this logic of daily jobs as variables to ease visualization of logic in the next cte below!!
 */
    (--If this is the first run of a scheduled job, it is eligible
            ft.last_refresh_attempted_at IS NULL
          OR (
               --If it was last attempted successfully prior to this scheduled time only - meaning yesterday, it is eligible
                (
                 ft.last_refresh_succeeded AND
                 ft.last_refresh_attempted_at::DATE <
                  -- Timezone taken from daily_scheduled_tz if base job, otherwise look up the timezone of the base job if this is dependent
                    (now() AT TIME ZONE COALESCE(
                                          ft.daily_scheduled_tz,
                                          base.daily_scheduled_tz
                                          )
                    )::DATE
                )
              OR
               --If a job has failed and been re-enabled, it is eligible again even though it has been attempted at or after the scheduled time
                 NOT ft.last_refresh_succeeded
             )
     ) AS daily_not_attempted_today,

  (now() AT TIME ZONE ft.daily_scheduled_tz)::TIME
      BETWEEN daily_scheduled_time AND '23:59:59.999999'::TIME AS daily_scheduled_time_passed,

  base.use_daily_schedule
  AND base.last_refresh_succeeded
  AND base.last_refresh_attempted_at :: DATE = (now() AT TIME ZONE base.daily_scheduled_tz) :: DATE
    AS daily_base_job_finished,

  ft.depends_on_base_daily_job_id = ft.depends_on_parent_daily_job_id AS daily_has_only_one_parent,

  -- This should only be used in combination with daily_has_only_one_parent
  parent.use_daily_schedule
  AND parent.last_refresh_succeeded
  AND parent.last_refresh_attempted_at :: DATE = (now() AT TIME ZONE COALESCE(parent.daily_scheduled_tz, base.daily_scheduled_tz)) :: DATE
    AS parent_job_finished
FROM fact_loader.fact_tables ft
LEFT JOIN LATERAL
  (SELECT ftb.use_daily_schedule,
    ftb.last_refresh_succeeded,
    ftb.last_refresh_attempted_at,
    ftb.daily_scheduled_tz
  FROM fact_loader.fact_tables ftb
  WHERE ftb.fact_table_id = ft.depends_on_base_daily_job_id) base ON TRUE
LEFT JOIN LATERAL
  (SELECT ftp.use_daily_schedule,
    ftp.last_refresh_succeeded,
    ftp.last_refresh_attempted_at,
    ftp.daily_scheduled_tz
  FROM fact_loader.fact_tables ftp
  WHERE ftp.fact_table_id = ft.depends_on_parent_daily_job_id) parent ON TRUE
WHERE enabled
)

, jobs_with_daily_schedule_eligibility AS (
SELECT
   *,
   --Only run this job according to the same day of the daily_scheduled_time
   --according to configured timezone
   (use_daily_schedule AND daily_not_attempted_today
     AND
    (
      daily_scheduled_time_passed
      OR
     (daily_base_job_finished AND (daily_has_only_one_parent OR parent_job_finished))
    )
   ) AS daily_schedule_eligible
FROM jobs_with_daily_variables)

SELECT *
FROM jobs_with_daily_schedule_eligibility
WHERE NOT use_daily_schedule OR daily_schedule_eligible
ORDER BY
  CASE WHEN force_worker_priority THEN 0 ELSE 1 END,
  --If a job has a daily schedule, once the time has come for the next refresh,
  --prioritize it first
  CASE
    WHEN daily_schedule_eligible
    THEN (now() AT TIME ZONE daily_scheduled_tz)::TIME
    ELSE NULL
  END NULLS LAST,
  --This may be improved in the future but is a good start
  last_refresh_attempted_at NULLS FIRST,
  priority
;


CREATE OR REPLACE FUNCTION fact_loader.sql_builder(p_fact_table_id INT)
  RETURNS TABLE(raw_queued_changes_sql text,
                gathered_queued_changes_sql text,
                process_queue_sql text,
                metadata_update_sql text) AS
$BODY$

/****
The recursive part of this CTE are only the sql_builder parts.
In Postgres, if any of your CTEs are recursive, you only use the RECURSIVE keyword on the first of a set.

The retrieval info may be the same for all 3 events (insert, update, delete), in which case filter_scope is null
Otherwise, they must be specified separately.
 */
WITH RECURSIVE queue_deps_with_insert_retrieval AS (
  SELECT *
  FROM fact_loader.queue_deps_all_with_retrieval
  WHERE (filter_scope = 'I' OR filter_scope IS NULL)
    AND fact_table_id = p_fact_table_id
)

, queue_deps_with_update_retrieval AS (
  SELECT *
  FROM fact_loader.queue_deps_all_with_retrieval
  WHERE (filter_scope = 'U' OR filter_scope IS NULL)
    AND fact_table_id = p_fact_table_id
)

, queue_deps_with_delete_retrieval AS (
  SELECT *
  FROM fact_loader.queue_deps_all_with_retrieval
  WHERE (filter_scope = 'D' OR filter_scope IS NULL)
    AND fact_table_id = p_fact_table_id
)

/****
Recursively build the SQL for any INSERT events found in the queues.

The recursive part ONLY applies to cases where multiple joins have to be made to get at the source data,
in which case there are multiple levels of key_retrieval_sequences for a given queue_table_dep_id. For an
example of this, see the test cases involving the test.order_product_promos table.
 */
, insert_sql_builder AS (
  SELECT queue_table_dep_id,
    level,
    CASE
      WHEN is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', b.'||return_columns[1]||'::TEXT AS key'
            ELSE ', unnest(array[b.'||array_to_string(return_columns, ',b.')||'])::TEXT AS key'
          END
      WHEN join_return_is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', j'||level||'.'||return_columns_from_join[1]||'::TEXT AS key'
            ELSE ', unnest(array[j'||level||'.'||array_to_string(return_columns_from_join, ',j'||level)||'])::TEXT AS key'
          END
        ELSE ''
    END AS key_select_column,
    CASE
      WHEN is_fact_key
        THEN ''
        ELSE 'INNER JOIN '||join_to_relation::TEXT||' j'||level||
             E'\n  ON b.'||quote_ident(return_columns[1])||' = j'||level||'.'||quote_ident(join_to_column)
    END AS key_retrieval_sql,
    source_change_date_select
  FROM queue_deps_with_insert_retrieval c
  WHERE level = 1
    AND fact_table_id = p_fact_table_id
  UNION ALL
  SELECT c.queue_table_dep_id,
    c.level,
    key_select_column||CASE
    WHEN c.is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||r.level||'.'||return_columns[1]||'::TEXT AS key'
          ELSE ', unnest(b.'||array_to_string(return_columns,',j'||r.level)||')::TEXT AS key'
        END
    WHEN join_return_is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||c.level||'.'||return_columns_from_join[1]||'::TEXT AS key'
          ELSE ', unnest(j'||c.level||'.'||array_to_string(return_columns_from_join,',j'||c.level)||')::TEXT AS key'
        END
      ELSE ''
    END AS key_select_column,
    key_retrieval_sql||CASE
    WHEN is_fact_key
      THEN ''
      ELSE E'\nINNER JOIN '||join_to_relation::TEXT||' j'||c.level||
           E'\n  ON j'||r.level||'.'||quote_ident(return_columns[1])||' = j'||c.level||'.'||quote_ident(join_to_column) END AS key_retrieval_sql,
    r.source_change_date_select
  FROM insert_sql_builder r
  INNER JOIN queue_deps_with_insert_retrieval c USING (queue_table_dep_id)
  WHERE c.level = r.level + 1
)

, update_sql_builder AS (
  SELECT queue_table_dep_id,
    level,
    CASE
      WHEN is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', b.'||return_columns[1]||'::TEXT AS key'
            ELSE ', unnest(array[b.'||array_to_string(return_columns, ',b.')||'])::TEXT AS key'
          END
      WHEN join_return_is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', j'||level||'.'||return_columns_from_join[1]||'::TEXT AS key'
            ELSE ', unnest(array[j'||level||'.'||array_to_string(return_columns_from_join, ',j'||level)||'])::TEXT AS key'
          END
        ELSE ''
    END AS key_select_column,
    CASE
      WHEN is_fact_key
        THEN ''
        ELSE 'INNER JOIN '||join_to_relation::TEXT||' j'||level||
             E'\n  ON b.'||quote_ident(return_columns[1])||' = j'||level||'.'||quote_ident(join_to_column)
    END AS key_retrieval_sql,
    source_change_date_select
  FROM queue_deps_with_update_retrieval c
  WHERE level = 1
    AND fact_table_id = p_fact_table_id
  UNION ALL
  SELECT c.queue_table_dep_id,
    c.level,
    key_select_column||CASE
    WHEN c.is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||r.level||'.'||return_columns[1]||'::TEXT AS key'
          ELSE ', unnest(b.'||array_to_string(return_columns,',j'||r.level)||')::TEXT AS key'
        END
    WHEN join_return_is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||c.level||'.'||return_columns_from_join[1]||'::TEXT AS key'
          ELSE ', unnest(j'||c.level||'.'||array_to_string(return_columns_from_join,',j'||c.level)||')::TEXT AS key'
        END
      ELSE ''
    END AS key_select_column,
    key_retrieval_sql||CASE
    WHEN is_fact_key
      THEN ''
      ELSE E'\nINNER JOIN '||join_to_relation::TEXT||' j'||c.level||
           E'\n  ON j'||r.level||'.'||quote_ident(return_columns[1])||' = j'||c.level||'.'||quote_ident(join_to_column) END AS key_retrieval_sql,
    r.source_change_date_select
  FROM update_sql_builder r
  INNER JOIN queue_deps_with_update_retrieval c USING (queue_table_dep_id)
  WHERE c.level = r.level + 1
)

, delete_sql_builder AS (
  SELECT queue_table_dep_id,
    level,
    --For deletes, same pattern as key_select_column but instead, we may be selecting from the audit tables instead
    CASE
      WHEN is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', q.before_change->>'||quote_literal(return_columns[1])||'::TEXT AS key'
            ELSE ', unnest(array[before_change->>'''||array_to_string(return_columns, ''', before_change->>''')||'''])::TEXT AS key'
          END
      WHEN join_return_is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', j'||level||'.'||quote_ident(return_columns_from_join[1])||'::TEXT AS key'
            ELSE ', unnest(array[j'||level||'.'||array_to_string(return_columns_from_join, ',j'||level||'.')||'])::TEXT AS key'
          END
        ELSE ''
    END AS delete_key_select_column,
    CASE
    WHEN is_fact_key
      THEN ''
      ELSE format($$
        --Join to either the base table, or the audit table, one of which
        --will be missing the key in a delete case
          INNER JOIN LATERAL (
           SELECT %s FROM %s jb
           WHERE %s = %s
           UNION ALL
           SELECT %s FROM %s jq
           WHERE operation = 'D'
            AND %s = %s) %s ON TRUE
           $$, quote_ident(return_columns_from_join[1]),
            join_to_relation::TEXT,
            (CASE WHEN level = 1 THEN '(q'||'.before_change->>'||quote_literal(return_columns[1])||')::'||join_column_type ELSE 'j'||level||'.'||quote_ident(return_columns[1]) END),
            'jb.'||quote_ident(join_to_column),
            '(before_change->>'||quote_literal(return_columns_from_join[1])||')::'||return_columns_from_join_type,
            join_to_relation_queue::TEXT,
            (CASE WHEN level = 1 THEN '(q'||'.before_change->>'||quote_literal(return_columns[1])||')::'||join_column_type ELSE 'j'||level||'.'||quote_ident(return_columns[1]) END),
            '(jq.before_change->>'||quote_literal(join_to_column)||')::'||join_column_type,
            /****
            We use the higher level here just to be consistent with aliases from insert/update key retrieval
             */
            'j'||level
            )
    END
      AS delete_key_retrieval_sql,
    source_change_date_select
  FROM queue_deps_with_delete_retrieval
  WHERE level = 1
    AND fact_table_id = p_fact_table_id
  UNION ALL
  SELECT c.queue_table_dep_id,
    c.level,
    delete_key_select_column||CASE
    WHEN c.is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||r.level||'.before_change->>'||quote_literal(return_columns[1])||'::TEXT AS key'
          ELSE ', unnest(array[before_change->>'''||array_to_string(return_columns,',j'||r.level||'.before_change->>''')||'''])::TEXT AS key'
        END
    WHEN join_return_is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||c.level||'.'||quote_ident(return_columns_from_join[1])||'::TEXT AS key'
          ELSE ', unnest(array[j'||c.level||'.'||array_to_string(return_columns_from_join,',j'||c.level)||')::TEXT AS key'
        END
      ELSE ''
    END AS delete_key_select_column,
    delete_key_retrieval_sql||CASE
    WHEN is_fact_key
      THEN ''
      ELSE format($$
        --Join to either the base table, or the audit table, one of which
        --will be missing the key in a delete case
          INNER JOIN LATERAL (
           SELECT %s FROM %s jb
           WHERE %s = %s
           UNION ALL
           SELECT %s FROM %s jq
           WHERE operation = 'D'
            AND %s = %s) %s ON TRUE
           $$, quote_ident(return_columns_from_join[1]),
            join_to_relation::TEXT,
            'j'||r.level||'.'||quote_ident(return_columns[1]),
            'jb.'||quote_ident(join_to_column),
            '(before_change->>'||quote_literal(return_columns_from_join[1])||')::'||return_columns_from_join_type,
            join_to_relation_queue::TEXT,
            'j'||r.level||'.'||quote_ident(return_columns[1]),
            '(jq.before_change->>'||quote_literal(join_to_column)||')::'||join_column_type,
            /****
            We use the higher level here just to be consistent with aliases from insert/update key retrieval
             */
            'j'||c.level
            )
    END
      AS delete_key_retrieval_sql,
    r.source_change_date_select
  FROM delete_sql_builder r
  INNER JOIN queue_deps_with_delete_retrieval c USING (queue_table_dep_id)
  WHERE c.level = r.level + 1
)

, non_recursive_sql AS (
  SELECT
  /****
  Separate select list for:
    - raw queue_ids from queue tables
    - gathered data from joining queue_ids to source tables to get actual keys to update in fact tables
   */
  -- gathering all queue_ids from queue tables
    queue_table_dep_id,
    format($$
    %s AS fact_table_id,
    %s AS queue_table_id,
    %s AS queue_table_dep_id,
    %s::INT AS fact_table_dep_id,
    %s::INT AS fact_table_dep_queue_table_dep_id,
    %s AS queue_table_id_field,
    q.fact_loader_batch_id,
    %s::TIMESTAMPTZ AS maximum_cutoff_time $$,
    fact_table_id,
    queue_table_id,
    queue_table_dep_id,
    (CASE WHEN fact_table_dep_id IS NULL THEN 'NULL'::TEXT ELSE fact_table_dep_id::TEXT END),
    (CASE WHEN fact_table_dep_queue_table_dep_id IS NULL THEN 'NULL'::TEXT ELSE fact_table_dep_queue_table_dep_id::TEXT END),
    'q.'||quote_ident(queue_table_id_field),
    quote_literal(maximum_cutoff_time))
      AS metadata_select_columns,

  -- gathering actual keys to update in fact tables by joining from queue_ids to source tables
    format($$
    %s AS fact_table_id,
    %s AS queue_table_dep_id,
    %s::INT AS fact_table_dep_id,
    %s::INT AS fact_table_dep_queue_table_dep_id,
    %s AS queue_table_id_field,
    q.fact_loader_batch_id,
    %s AS operation,
    %s %s AS changed_at,
    %s::REGPROC AS insert_merge_proid,
    %s::REGPROC AS update_merge_proid,
    %s::REGPROC AS delete_merge_proid,
    %s::TIMESTAMPTZ AS maximum_cutoff_time $$,
    fact_table_id,
    queue_table_dep_id,
    (CASE WHEN fact_table_dep_id IS NULL THEN 'NULL'::TEXT ELSE fact_table_dep_id::TEXT END),
    (CASE WHEN fact_table_dep_queue_table_dep_id IS NULL THEN 'NULL'::TEXT ELSE fact_table_dep_queue_table_dep_id::TEXT END),
    'q.'||quote_ident(queue_table_id_field),
    'q.'||quote_ident(queue_table_op),
    'q.'||quote_ident(queue_table_timestamp),
    CASE WHEN queue_table_tz IS NULL THEN '' ELSE 'AT TIME ZONE '||quote_literal(queue_table_tz) END,
    CASE WHEN insert_merge_proid IS NULL THEN 'NULL' ELSE quote_literal(insert_merge_proid) END,
    CASE WHEN update_merge_proid IS NULL THEN 'NULL' ELSE quote_literal(update_merge_proid) END,
    CASE WHEN delete_merge_proid IS NULL THEN 'NULL' ELSE quote_literal(delete_merge_proid) END,
    quote_literal(maximum_cutoff_time))
      AS global_select_columns,

  -- This is simply the queue table aliased as q
    format('%s q', queue_table_relid::TEXT) AS queue_table_aliased,

  -- This is the SQL to join from the queue table to the base table
    E'\nINNER JOIN '||queue_of_base_table_relid::TEXT||' b'||
    E'\n  ON q.'||quote_ident(queue_table_key)||'::'||queue_of_base_table_key_type||' = b.'||quote_ident(queue_of_base_table_key)
           AS base_join_sql,

  -- This is a WHERE statement to be added to ALL gathering of new queue_ids to process.
    format($$ %s
        AND q.%s < %s %s
        $$,
        CASE
          WHEN last_cutoff_id IS NOT NULL
          THEN 'q.fact_loader_batch_id > '||last_cutoff_id
          ELSE
            'TRUE'
        END,
        quote_ident(c.queue_table_timestamp),
        quote_literal(c.maximum_cutoff_time),
        CASE WHEN queue_table_tz IS NULL THEN '' ELSE 'AT TIME ZONE '||quote_literal(queue_table_tz) END) AS global_where_sql,
    format($$
      AND q.%s = 'I'
      $$,
        queue_table_op)
           AS where_for_insert_sql,
    format($$
      AND (q.%s = 'U' AND %s)
      $$,
        queue_table_op,
        CASE
          WHEN relevant_change_columns IS NULL
            THEN 'TRUE'
          ELSE
            format($$q.%s ?| '{%s}'$$, queue_table_change, array_to_string(relevant_change_columns,','))
        END)
           AS where_for_update_sql,
    format($$
      AND q.%s = 'D'
      $$,
        queue_table_op)
           AS where_for_delete_sql
  FROM fact_loader.queue_deps_all c
  WHERE c.fact_table_id = p_fact_table_id
)

, insert_sql_builder_final AS
(SELECT DISTINCT ON (queue_table_dep_id)
  *
FROM insert_sql_builder
ORDER BY queue_table_dep_id, level DESC
)

, update_sql_builder_final AS
(SELECT DISTINCT ON (queue_table_dep_id)
  *
FROM update_sql_builder
ORDER BY queue_table_dep_id, level DESC
)

, delete_sql_builder_final AS
(SELECT DISTINCT ON (queue_table_dep_id)
  *
FROM delete_sql_builder
ORDER BY queue_table_dep_id, level DESC
)

, all_queues_sql AS (
SELECT
  format($$
  SELECT %s
  FROM %s
  %s
  WHERE %s
  $$,
    nrs.global_select_columns||isbf.key_select_column||isbf.source_change_date_select,
    nrs.queue_table_aliased||nrs.base_join_sql,
    isbf.key_retrieval_sql,
    nrs.global_where_sql||nrs.where_for_insert_sql) AS queue_insert_sql,
  format($$
  SELECT %s
  FROM %s
  %s
  WHERE %s
  $$,
    nrs.global_select_columns||usbf.key_select_column||usbf.source_change_date_select,
    nrs.queue_table_aliased||nrs.base_join_sql,
    usbf.key_retrieval_sql,
    nrs.global_where_sql||nrs.where_for_update_sql) AS queue_update_sql,
  format($$
  SELECT %s
  FROM %s
  %s
  WHERE %s
  $$,
    nrs.global_select_columns||dsbf.delete_key_select_column||usbf.source_change_date_select,
    nrs.queue_table_aliased,
    dsbf.delete_key_retrieval_sql,
    nrs.global_where_sql||nrs.where_for_delete_sql) AS queue_delete_sql,
  format($$
  SELECT %s
  FROM %s
  WHERE %s
  $$,
    nrs.metadata_select_columns,
    nrs.queue_table_aliased,
    nrs.global_where_sql) AS queue_ids_sql
FROM non_recursive_sql nrs
INNER JOIN insert_sql_builder_final isbf ON isbf.queue_table_dep_id = nrs.queue_table_dep_id
INNER JOIN update_sql_builder_final usbf ON usbf.queue_table_dep_id = nrs.queue_table_dep_id
INNER JOIN delete_sql_builder_final dsbf ON dsbf.queue_table_dep_id = nrs.queue_table_dep_id
)

, final_queue_sql AS
(SELECT string_agg(
  /****
  This first UNION is to union together INSERT, UPDATE, and DELETE events for a single queue table
   */
  format($$
  %s
  UNION ALL
  %s
  UNION ALL
  %s
  $$,
    queue_insert_sql,
    queue_update_sql,
    queue_delete_sql)
  /****
  This second UNION as the second arg of string_agg is the union together ALL queue tables for this fact table
   */
  , E'\nUNION ALL\n') AS event_sql,
  string_agg(queue_ids_sql, E'\nUNION ALL\n') AS raw_queued_changes_sql_out
FROM all_queues_sql)

, final_outputs AS (
SELECT raw_queued_changes_sql_out,
$$
WITH all_changes AS (
($$||event_sql||$$)
ORDER BY changed_at)

, base_execution_groups AS
(SELECT fact_table_id,
    queue_table_dep_id,
    queue_table_id_field,
    operation,
    changed_at,
    source_change_date,
    insert_merge_proid,
    update_merge_proid,
    delete_merge_proid,
    maximum_cutoff_time,
    key,
    CASE WHEN operation = 'I' THEN insert_merge_proid
    WHEN operation = 'U' THEN update_merge_proid
    WHEN operation = 'D' THEN delete_merge_proid
        END AS proid,
  RANK() OVER (
    PARTITION BY
      CASE
        WHEN operation = 'I' THEN insert_merge_proid
        WHEN operation = 'U' THEN update_merge_proid
        WHEN operation = 'D' THEN delete_merge_proid
      END
  ) AS execution_group
  FROM all_changes
  WHERE key IS NOT NULL)

SELECT fact_table_id, proid, key, source_change_date
FROM base_execution_groups beg
WHERE proid IS NOT NULL
GROUP BY execution_group, fact_table_id, proid, key, source_change_date
/****
This ordering is particularly important for date-range history tables
where order of inserts is critical and usually expected to follow a pattern
***/
ORDER BY execution_group, MIN(changed_at), MIN(queue_table_id_field);
$$ AS gathered_queued_changes_sql_out
  ,

$$
DROP TABLE IF EXISTS process_queue;
CREATE TEMP TABLE process_queue
(process_queue_id serial,
 fact_table_id int,
 proid regproc,
 key_value text,
 source_change_date date);

INSERT INTO process_queue
(fact_table_id, proid, key_value, source_change_date)
$$ AS process_queue_snippet,

$$
WITH all_ids AS
($$||raw_queued_changes_sql_out||$$)

, new_metadata AS
(SELECT MAX(fact_loader_batch_id) AS last_cutoff_id,
  maximum_cutoff_time,
  queue_table_dep_id
FROM all_ids
--Exclude dependent fact tables from updates directly to queue_table_deps
WHERE fact_table_dep_id IS NULL
GROUP BY queue_table_dep_id, maximum_cutoff_time)

/****
The dependent fact table uses the same queue_table_id_field as last_cutoff
We are going to update fact_table_deps metadata instead of queue_table_deps
****/
, new_metadata_fact_dep AS
(SELECT MAX(fact_loader_batch_id) AS last_cutoff_id,
  maximum_cutoff_time,
  fact_table_dep_queue_table_dep_id
FROM all_ids
--Include dependent fact tables only
WHERE fact_table_dep_id IS NOT NULL
GROUP BY fact_table_dep_queue_table_dep_id, maximum_cutoff_time)

, update_key AS (
SELECT qdwr.queue_table_dep_id,
  --Cutoff the id to that newly found, otherwise default to last value
  COALESCE(mu.last_cutoff_id, qdwr.last_cutoff_id) AS last_cutoff_id,
  --This cutoff time must always be the same for all queue tables for given fact table.
  --Even if there are no new records, we move this forward to wherever the stream is at
  qdwr.maximum_cutoff_time AS last_cutoff_source_time
FROM fact_loader.queue_deps_all qdwr
LEFT JOIN new_metadata mu ON mu.queue_table_dep_id = qdwr.queue_table_dep_id
WHERE qdwr.fact_table_id = $$||p_fact_table_id||$$
--Exclude dependent fact tables from updates directly to queue_table_deps
  AND qdwr.fact_table_dep_id IS NULL
)

/****
This SQL also nearly matches that for the queue_table_deps but would be a little ugly to try to DRY up
****/
, update_key_fact_dep AS (
SELECT qdwr.fact_table_dep_queue_table_dep_id,
  qdwr.fact_table_id,
  COALESCE(mu.last_cutoff_id, qdwr.last_cutoff_id) AS last_cutoff_id,
  qdwr.maximum_cutoff_time AS last_cutoff_source_time
FROM fact_loader.queue_deps_all qdwr
LEFT JOIN new_metadata_fact_dep mu ON mu.fact_table_dep_queue_table_dep_id = qdwr.fact_table_dep_queue_table_dep_id
WHERE qdwr.fact_table_id = $$||p_fact_table_id||$$
--Include dependent fact tables only
  AND qdwr.fact_table_dep_id IS NOT NULL
)

, updated_queue_table_deps AS (
UPDATE fact_loader.queue_table_deps qtd
SET last_cutoff_id = uk.last_cutoff_id,
  last_cutoff_source_time = uk.last_cutoff_source_time
FROM update_key uk
WHERE qtd.queue_table_dep_id = uk.queue_table_dep_id
RETURNING qtd.*)

, updated_fact_table_deps AS (
UPDATE fact_loader.fact_table_dep_queue_table_deps ftd
SET last_cutoff_id = uk.last_cutoff_id,
  last_cutoff_source_time = uk.last_cutoff_source_time
FROM update_key_fact_dep uk
WHERE ftd.fact_table_dep_queue_table_dep_id = uk.fact_table_dep_queue_table_dep_id
RETURNING uk.*)

UPDATE fact_loader.fact_tables ft
SET last_refresh_source_cutoff = uqtd.last_cutoff_source_time,
  last_refresh_attempted_at = now(),
  last_refresh_succeeded = TRUE
FROM
(SELECT fact_table_id, last_cutoff_source_time
FROM updated_queue_table_deps
--Must use UNION to get only distinct values
UNION
SELECT fact_table_id, last_cutoff_source_time
FROM updated_fact_table_deps) uqtd
WHERE uqtd.fact_table_id = ft.fact_table_id;
$$ AS metadata_update_sql_out
FROM final_queue_sql)

SELECT raw_queued_changes_sql_out,
 gathered_queued_changes_sql_out
  ,
 format($$
  %s
  %s$$, process_queue_snippet, gathered_queued_changes_sql_out) AS process_queue_sql_out,
 metadata_update_sql_out
FROM final_outputs;

$BODY$
LANGUAGE SQL;


CREATE OR REPLACE FUNCTION fact_loader.execute_queue(p_fact_table_id INT)
RETURNS TABLE (sql TEXT) AS
$BODY$
BEGIN

RETURN QUERY
WITH ordered_process_queue AS
(SELECT
   process_queue_id
   , proid
   , key_value
   , source_change_date
   , (pp.proargtypes::REGTYPE[])[0] AS proid_first_arg
 FROM process_queue pq
   LEFT JOIN pg_proc pp ON pp.oid = proid
 WHERE pq.fact_table_id = p_fact_table_id
 ORDER BY process_queue_id)

, with_rank AS
(SELECT
  /****
  If source_change_date is NULL, we assume the proid has one arg and pass it.
  If not, we assume the proid has two args and pass source_change_date as the second.
  */
   format('%s(%s::%s%s)'
          , proid::TEXT
          , 'key_value'
          , proid_first_arg
          , CASE
              WHEN source_change_date IS NOT NULL
                THEN format(', %s::DATE',quote_literal(source_change_date))
              ELSE ''
            END
        ) AS function_call,
  proid,
  process_queue_id,
  RANK() OVER (PARTITION BY proid) AS execution_group
FROM ordered_process_queue
)

, execute_sql_groups AS
(
SELECT execution_group,
format($$
SELECT process_queue_id, %s
FROM (
/****
Must wrap this to execute in order of ids
***/
SELECT *
FROM process_queue
WHERE process_queue_id BETWEEN %s AND %s
  AND fact_table_id = %s
  AND proid = %s::REGPROC
ORDER BY process_queue_id) q;
$$, function_call, MIN(process_queue_id), MAX(process_queue_id), p_fact_table_id, quote_literal(proid::TEXT)) AS execute_sql
FROM with_rank
GROUP BY execution_group, function_call, proid
ORDER BY execution_group
)

SELECT COALESCE(string_agg(execute_sql,''),'SELECT NULL') AS final_execute_sql
FROM execute_sql_groups;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.raw_queued_changes(p_fact_table_id INT)
RETURNS TABLE (fact_table_id INT,
    queue_table_id INT,
    queue_table_dep_id INT,
    fact_table_dep_id INT,
    fact_table_dep_queue_table_dep_id INT,
    queue_table_id_field BIGINT,
    fact_loader_batch_id BIGINT,
    maximum_cutoff_time TIMESTAMPTZ) AS
$BODY$
DECLARE
    v_raw_sql text;
BEGIN

SELECT raw_queued_changes_sql
INTO v_raw_sql
FROM fact_loader.sql_builder(p_fact_table_id);

RETURN QUERY EXECUTE v_raw_sql;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.gathered_queued_changes(p_fact_table_id INT)
RETURNS TABLE (fact_table_id INT, proid REGPROC, key_value TEXT, source_change_date DATE) AS 
$BODY$
DECLARE
    v_gather_sql text;
BEGIN

SELECT gathered_queued_changes_sql
INTO v_gather_sql
FROM fact_loader.sql_builder(p_fact_table_id);

RETURN QUERY EXECUTE v_gather_sql;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.daily_scheduled_load(p_fact_table_id INT)
RETURNS BOOLEAN AS
$BODY$
DECLARE
    v_execute_sql text;
    v_deps regclass[];
    v_dep_delay_tolerance interval;
    v_delayed_msg text;
BEGIN
/***
There are 3 basic steps to this load:
    1. If dependencies are listed, verify they are up to date enough
    2. Execute the single daily-refresh function
    3. Update the metadata indicating the last attempt time
 */
SELECT 'SELECT '||daily_scheduled_proid::TEXT||'()',
    daily_scheduled_deps,
    daily_scheduled_dep_delay_tolerance
     INTO
    v_execute_sql,
    v_deps,
    v_dep_delay_tolerance
FROM fact_loader.fact_tables
WHERE fact_table_id = p_fact_table_id
  AND use_daily_schedule;

IF v_execute_sql IS NULL THEN
    RETURN FALSE;
END IF;

IF v_deps IS NOT NULL THEN
    WITH deps AS 
    (SELECT unnest(v_deps) AS dep)
    
    , delays AS (
    SELECT dep, now() - source_time as delay_interval 
    FROM fact_loader.queue_table_delay_info() qtd
    INNER JOIN deps d ON d.dep = qtd.queue_of_base_table_relid
    UNION ALL
    SELECT dep, now() - last_refresh_source_cutoff as delay_interval
    FROM fact_loader.fact_tables ft
    INNER JOIN deps d ON d.dep = ft.fact_table_relid
    )

    SELECT string_agg(dep::text||': Delayed '||delay_interval::text, ', ')
        INTO v_delayed_msg
    FROM delays
    WHERE delay_interval > v_dep_delay_tolerance;

    IF v_delayed_msg IS NOT NULL THEN
        RAISE EXCEPTION '%', v_delayed_msg;
    END IF;
END IF;

EXECUTE v_execute_sql;

UPDATE fact_loader.fact_tables ft
SET last_refresh_attempted_at = now(),
    last_refresh_succeeded = TRUE
WHERE fact_table_id = p_fact_table_id;

RETURN TRUE;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.fact_table_refresh_logs_pruner() RETURNS trigger
LANGUAGE plpgsql
AS $$

declare
        step int := 1000;
        -- step should equal the firing frequency in trigger definition
        overdrive int := 2;
        -- overdrive times step = max rows (see below)

        max_rows int := step * overdrive;
        rows int;

begin
        delete from fact_loader.fact_table_refresh_logs
        where fact_table_refresh_log_id in (
                select fact_table_refresh_log_id
                from fact_loader.fact_table_refresh_logs
                where refresh_attempted_at < now() - '90 days'::interval
                -- do not do the literal interval value above as a declare parameter
                order by fact_table_refresh_log_id
                limit max_rows
                for update skip locked
        );

        get diagnostics rows = row_count;
        return null;
end
$$;

CREATE TRIGGER fact_table_refresh_logs_pruner
AFTER INSERT ON fact_loader.fact_table_refresh_logs
FOR EACH ROW WHEN ((new.fact_table_refresh_log_id % 1000::bigint) = 0)
EXECUTE PROCEDURE fact_loader.fact_table_refresh_logs_pruner();


/***
This function exists mostly to easily mock out for testing purposes.
 */
CREATE FUNCTION fact_loader.logical_subscription()
RETURNS TABLE (sub_origin_if OID, sub_replication_sets text[])
AS $BODY$
BEGIN

IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pglogical') THEN

  RETURN QUERY EXECUTE $$
  SELECT sub_origin_if, sub_replication_sets
  FROM pglogical.subscription;
  $$;
ELSE
  RETURN QUERY
  SELECT NULL::OID, NULL::TEXT[];

END IF;

END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fact_loader.queue_table_delay_info()
RETURNS TABLE("replication_set_name" text,
           "queue_of_base_table_relid" regclass,
           "if_id" oid,
           "if_name" name,
           "source_time" timestamp with time zone)
AS
$BODY$
/***
This function exists to allow no necessary dependency
to exist on pglogical_ticker.  If the extension is used,
it will return data from its native functions, if not,
it will return a null data set matching the structure
***/
BEGIN

IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pglogical_ticker') THEN
    RETURN QUERY EXECUTE $$
    SELECT
        unnest(coalesce(sub_replication_sets,'{NULL}')) AS replication_set_name
      , qt.queue_of_base_table_relid
      , n.if_id
      , n.if_name
      --source_time is now() for local tables (pglogical_node_if_id is null), and based on pglogical_ticker time otherwise
      , CASE
        WHEN qt.pglogical_node_if_id IS NULL
          THEN now()
        ELSE t.source_time
        END                          AS source_time
    FROM fact_loader.queue_tables qt
      LEFT JOIN fact_loader.logical_subscription() s ON qt.pglogical_node_if_id = s.sub_origin_if
      LEFT JOIN pglogical.node_interface n ON n.if_id = qt.pglogical_node_if_id
      LEFT JOIN pglogical_ticker.all_subscription_tickers() t ON t.provider_name = n.if_name;$$;
ELSE
    RETURN QUERY
    SELECT
        NULL::TEXT AS replication_set_name
      , qt.queue_of_base_table_relid
      , NULL::OID AS if_id
      , NULL::NAME AS if_name
      --source_time is now() if queue tables are not pglogical-replicated, which is assumed if no ticker
      , now() AS source_time
    FROM fact_loader.queue_tables qt;
END IF;

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.refresh_fact_table_dep_queue_table_deps()
RETURNS VOID AS
$BODY$
BEGIN
/****
This function will be used to refresh the fact_table_dep_queue_table_deps table.
The purpose of this table is to easily figure out queue data for fact tables that depend on other fact tables.
This will be run with every call of load().
This may not be the most efficient method, but it is certainly reliable and fast.
 */

/****
Recursively find all fact table deps including nested ones (fact tables that depend on other fact tables)
to build the fact_table_dep_queue_table_deps table.
 */
WITH RECURSIVE all_fact_table_deps AS (
  SELECT
    qtd.queue_table_dep_id
    , ftd.fact_table_dep_id
    , parent_id                                 AS parent_fact_table_id
    , child_id                                  AS fact_table_id
    , qtd.queue_table_id
    , qt.queue_table_relid
    , ftp.fact_table_relid AS parent_fact_table
    , ftc.fact_table_relid AS child_fact_table
    , ftd.default_insert_merge_proid
    , ftd.default_update_merge_proid
    , ftd.default_delete_merge_proid
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt ON qtd.queue_table_id = qt.queue_table_id
  INNER JOIN fact_loader.fact_table_deps ftd ON ftd.parent_id = qtd.fact_table_id
  INNER JOIN fact_loader.fact_tables ftp USING (fact_table_id)
  INNER JOIN fact_loader.fact_tables ftc ON ftc.fact_table_id = ftd.child_id
  UNION ALL
  SELECT
    qtd.queue_table_dep_id
    , ftd.fact_table_dep_id
    , parent_id                                 AS parent_fact_table_id
    , child_id                                  AS fact_table_id
    , qtd.queue_table_id
    , qt.queue_table_relid
    , ftp.fact_table_relid AS parent_fact_table
    , ft.fact_table_relid AS child_fact_table
    , ftd.default_insert_merge_proid
    , ftd.default_update_merge_proid
    , ftd.default_delete_merge_proid
  FROM all_fact_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt ON qtd.queue_table_id = qt.queue_table_id
  INNER JOIN fact_loader.fact_table_deps ftd ON ftd.parent_id = qtd.fact_table_id
  INNER JOIN fact_loader.fact_tables ftp ON ftp.fact_table_id = ftd.parent_id
  INNER JOIN fact_loader.fact_tables ft ON ft.fact_table_id = ftd.child_id
)

/****
Remove fact_table_dep_queue_table_deps that no longer exist if applicable
 */
, removed AS (
  DELETE FROM fact_loader.fact_table_dep_queue_table_deps ftdqc
  WHERE NOT EXISTS(SELECT 1
                   FROM all_fact_table_deps aftd
                   WHERE aftd.fact_table_dep_id = ftdqc.fact_table_dep_id
                    AND aftd.queue_table_dep_id = ftdqc.queue_table_dep_id)
)

/****
Add any new keys or ignore if they already exist
Add not exists because we think allowing all records to insert and conflict could be cause
of serialization errors in repeatable read isolation.
 */
INSERT INTO fact_loader.fact_table_dep_queue_table_deps
(fact_table_dep_id, queue_table_dep_id, insert_merge_proid, update_merge_proid, delete_merge_proid)
SELECT fact_table_dep_id, queue_table_dep_id, default_insert_merge_proid, default_update_merge_proid, default_delete_merge_proid
FROM all_fact_table_deps new
WHERE NOT EXISTS
    (SELECT 1
    FROM fact_loader.fact_table_dep_queue_table_deps existing
    WHERE existing.fact_table_dep_id = new.fact_table_dep_id
      AND existing.queue_table_dep_id = new.queue_table_dep_id)
ON CONFLICT (fact_table_dep_id, queue_table_dep_id)
DO NOTHING;

END;
$BODY$
LANGUAGE plpgsql;


-- These fields now becomes based on batch, not based on queue_table_id_field
DO $BODY$
DECLARE
    v_rec RECORD;
    v_sql TEXT;
BEGIN

FOR v_rec IN
    SELECT
      format($$
      UPDATE fact_loader.%s
      SET last_cutoff_id =
      (SELECT fact_loader_batch_id
      FROM %s
      WHERE %s = %s)
      WHERE %s = %s;
      $$,
        CASE WHEN fact_table_dep_id IS NULL THEN 'queue_table_deps' ELSE 'fact_table_dep_queue_table_deps' END,
        queue_table_relid::text,
        queue_table_id_field::text,
        last_cutoff_id::text,
        CASE WHEN fact_table_dep_id IS NULL THEN 'queue_table_dep_id' ELSE 'fact_table_dep_queue_table_dep_id' END,
        CASE WHEN fact_table_dep_id IS NULL THEN queue_table_dep_id ELSE fact_table_dep_queue_table_dep_id END
      ) AS sql
    FROM fact_loader.queue_deps_all
    WHERE last_cutoff_id IS NOT NULL
LOOP

v_sql = v_rec.sql;
RAISE LOG 'Updating Extension pg_fact_loader Executed: %', v_sql;
EXECUTE v_sql;

END LOOP;

END$BODY$;


COMMENT ON TABLE fact_loader.debug_process_queue IS 'A mirror of process_queue for debugging only (unlogged) - only populated with log_min_duration set to DEBUG.';


COMMENT ON TABLE fact_loader.fact_table_dep_queue_table_deps IS
$$Data in this table is by default auto-generated by refresh_fact_table_dep_queue_table_deps() only for queue-based fact tables that depend on other fact table changes.
Each row represents a parent's queue_table_dep, updates of which will trickle down to this dependent fact table.  Even though the default proids
from fact_table_deps are used initially, they may not be appropriate as generalized across all of these queue_table_deps.
The proids may need to be overridden for individual fact_table_dep_queue_table_deps if that generalization isn't possible.
See the regression suite in ./sql and ./expected for examples of this.
$$;
  COMMENT ON COLUMN fact_loader.fact_table_dep_queue_table_deps.fact_table_dep_queue_table_dep_id IS 'Unique identifier';
  COMMENT ON COLUMN fact_loader.fact_table_dep_queue_table_deps.fact_table_dep_id IS 'fact_table_dep for this specific dependency.';
  COMMENT ON COLUMN fact_loader.fact_table_dep_queue_table_deps.queue_table_dep_id IS 'Inherited queue_table_dep that this dependent fact table depends on.';
  COMMENT ON COLUMN fact_loader.fact_table_dep_queue_table_deps.last_cutoff_id IS
  $$This is unique and maintained separately from last_cutoff_id in queue_table_deps,
  as it refers to the last_cutoff_id for this dependent fact table.  It is the last fact_loader_batch_id of
  the queue table that was processed for this queue table - dependent fact table pair.
  After this job runs, records that have this id and lower are eligible to be pruned,
  assuming no other fact tables also depend on those same records.
  The next time the job runs, only records after this id are considered.$$;
  COMMENT ON COLUMN fact_loader.fact_table_dep_queue_table_deps.last_cutoff_source_time IS
  $$This is unique and maintained separately from last_cutoff_source_time in queue_table_deps,
  as it refers to the last_cutoff_source_time for this dependent fact table.  It is the source data
  change time of the last queue table record that was processed for this queue table - dependent fact table pair.
  This helps pg_fact_loader synchronize time across multiple queue tables and only pull changes
  that are early enough, and not purge records that are later than these cutoff times.  It will also
  never go past its parent(s) in time.  THIS DOES NOT DETERMINE filter conditions
  for the starting point at which to pull new records as does last_cutoff_id - it is only
  used as an ending-point barrier.
  $$;
  COMMENT ON COLUMN fact_loader.fact_table_dep_queue_table_deps.insert_merge_proid IS
  $$Initially populated by default_insert_merge_proid from fact_table_deps, but can be
  overridden if a different proid is required. This is the function oid to execute on
  INSERT events *for this dependent fact table* - it accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - dependent fact table pair
  is configured in key_retrieval_sequences *for the parent(s)*. NULL to ignore insert events.
  See the regression suite in ./sql and ./expected for examples of this.$$;
  COMMENT ON COLUMN fact_loader.fact_table_dep_queue_table_deps.update_merge_proid IS
  $$Initially populated by default_update_merge_proid from fact_table_deps, but can be
  overridden if a different proid is required. This is the function oid to execute on
  UPDATE events *for this dependent fact table* - it accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - dependent fact table pair
  is configured in key_retrieval_sequences *for the parent(s)*. NULL to ignore insert events.
  See the regression suite in ./sql and ./expected for examples of this.$$;
  COMMENT ON COLUMN fact_loader.fact_table_dep_queue_table_deps.delete_merge_proid IS
  $$Initially populated by default_delete_merge_proid from fact_table_deps, but can be
  overridden if a different proid is required. This is the function oid to execute on
  DELETE events *for this dependent fact table* - it accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - dependent fact table pair
  is configured in key_retrieval_sequences *for the parent(s)*. NULL to ignore insert events.
  See the regression suite in ./sql and ./expected for examples of this.$$;
  COMMENT ON COLUMN fact_loader.fact_table_dep_queue_table_deps.row_created_at IS 'Timestamp of when this row was first created.';
  COMMENT ON COLUMN fact_loader.fact_table_dep_queue_table_deps.row_updated_at IS 'Timestamp of when this row was last updated (this is updated via trigger).';


COMMENT ON TABLE fact_loader.fact_table_deps IS 'For queue-based fact tables that depend on other fact table changes ONLY. Add those dependencies here.';
  COMMENT ON COLUMN fact_loader.fact_table_deps.fact_table_dep_id IS 'Unique identifier.';
  COMMENT ON COLUMN fact_loader.fact_table_deps.parent_id IS 'The parent fact_table_id that the child depends on.';
  COMMENT ON COLUMN fact_loader.fact_table_deps.child_id IS 'The child fact_table_id that will run only after the parent is updated.';
  COMMENT ON COLUMN fact_loader.fact_table_deps.default_insert_merge_proid IS
  $$Default function to use for insert events to update child tables.
  This may need to be modified for each individual inherited fact_table_dep_queue_table_deps
  if that generalization isn't possible. See the regression suite in ./sql and ./expected for examples.$$;
  COMMENT ON COLUMN fact_loader.fact_table_deps.default_update_merge_proid IS
  $$Default function to use for update events to update child tables.
  This may need to be modified for each individual inherited fact_table_dep_queue_table_deps
  if that generalization isn't possible. See the regression suite in ./sql and ./expected for examples.$$;
  COMMENT ON COLUMN fact_loader.fact_table_deps.default_delete_merge_proid IS
  $$Default function to use for delete events to update child tables.
  This may need to be modified for each individual inherited fact_table_dep_queue_table_deps
  if that generalization isn't possible. See the regression suite in ./sql and ./expected for examples.$$;
  COMMENT ON COLUMN fact_loader.fact_table_deps.row_created_at IS 'Timestamp of when this row was first created.';
  COMMENT ON COLUMN fact_loader.fact_table_deps.row_updated_at IS 'Timestamp of when this row was last updated (this is updated via trigger).';


COMMENT ON TABLE fact_loader.fact_table_refresh_logs IS 'Used to log both job run times and exceptions.';
  COMMENT ON COLUMN fact_loader.fact_table_refresh_logs.fact_table_refresh_log_id IS 'Unique identifier,';
  COMMENT ON COLUMN fact_loader.fact_table_refresh_logs.fact_table_id IS 'Fact table that created the log.';
  COMMENT ON COLUMN fact_loader.fact_table_refresh_logs.refresh_attempted_at IS 'The time of the attempt (transaction begin time), which can be correlated to fact_table.last_refresh_attempted_at (see also unresolved_failures).';
  COMMENT ON COLUMN fact_loader.fact_table_refresh_logs.refresh_finished_at IS 'The transaction commit time of the attempt, which can be used with refresh_attempted_at to get actual run time.';
  COMMENT ON COLUMN fact_loader.fact_table_refresh_logs.messages IS 'Only for failures - Error message content in JSON format - including message, message detail, context, and hint.';


COMMENT ON TABLE fact_loader.fact_tables IS 'Each fact table to be built via pg_fact_loader, which also drives the worker.  These are also referred to as "jobs".';
  COMMENT ON COLUMN fact_loader.fact_tables.fact_table_id IS 'Unique identifier for the fact table or job - also referred to as job_id';
  COMMENT ON COLUMN fact_loader.fact_tables.fact_table_relid IS 'The oid of the fact table itself regclass type to accept only valid relations.';
  COMMENT ON COLUMN fact_loader.fact_tables.fact_table_agg_proid IS
  $$NOT REQUIRED.  The aggregate function definition for the fact table.
  This can be used when passed to create_table_loader_function to auto-create a merge function.
  It can also be a reference for dq checks because it indicates what function returns
  the correct results for a fact table as it should appear now.$$;
  COMMENT ON COLUMN fact_loader.fact_tables.enabled IS 'Indicates whether or not the job is enabled.  The worker will skip this table unless marked TRUE.';
  COMMENT ON COLUMN fact_loader.fact_tables.priority IS 'Determines the order in which the job runs (in combination with other sorting factors)';
  COMMENT ON COLUMN fact_loader.fact_tables.force_worker_priority IS 'If marked TRUE, this fact table will be prioritized in execution order above all other factors.';
  COMMENT ON COLUMN fact_loader.fact_tables.last_refresh_source_cutoff IS 'The data cutoff time of the last refresh - only records older than this have been updated.';
  COMMENT ON COLUMN fact_loader.fact_tables.last_refresh_attempted_at IS 'The last time the worker ran on this fact table.  The oldest will be prioritized first, ahead of priority.';
  COMMENT ON COLUMN fact_loader.fact_tables.last_refresh_succeeded IS 'Whether or not the last run of the job succeeded.  NULL if it has never been run.';
  COMMENT ON COLUMN fact_loader.fact_tables.row_created_at IS 'Timestamp of when this row was first created.';
  COMMENT ON COLUMN fact_loader.fact_tables.row_updated_at IS 'Timestamp of when this row was last updated (this is updated via trigger).';
  COMMENT ON COLUMN fact_loader.fact_tables.use_daily_schedule IS 'If TRUE, this job is scheduled to run daily instead of using queue tables according to other daily column configuration.  Also must be marked TRUE for dependent jobs.';
  COMMENT ON COLUMN fact_loader.fact_tables.daily_scheduled_time IS 'The time of day *after which* to run the job (the system will attempt to run until midnight). If you have a chain of daily scheduled jobs, only the base job has time filled in.';
  COMMENT ON COLUMN fact_loader.fact_tables.daily_scheduled_tz IS 'The timezone your time is in.  This is critical to know when to allow a daily refresh from the standpoint of the business logic you require for a timezone-based date.';
  COMMENT ON COLUMN fact_loader.fact_tables.daily_scheduled_proid IS
  $$The single function oid to execute at the scheduled time.  No arguments supported. It is assumed to contain all the
  logic necessary to add any new daily entries, if applicable.  See the unit tests in sql/16_1_2_features.sql for examples.$$;
  COMMENT ON COLUMN fact_loader.fact_tables.depends_on_base_daily_job_id IS 'For jobs that depend on other daily scheduled jobs only. This is the fact_table_id of the FIRST job in a chain which is actually the only one with a scheduled_time.';
  COMMENT ON COLUMN fact_loader.fact_tables.depends_on_parent_daily_job_id IS 'For jobs that depend on other daily scheduled jobs only. Immediate parent which must complete before this job will run.';
  COMMENT ON COLUMN fact_loader.fact_tables.daily_scheduled_deps IS 'OPTIONAL for daily scheduled jobs.  The only purpose of this column is to consider if we should wait to run a scheduled job because dependent tables are out of date.  This is a regclass array of tables that this scheduled job depends on, which will only be considered if they are either listed in fact_loader.queue_tables or fact_loader.fact_tables.  If the former, replication delay will be considered (if table is not local).  If the latter, last_refresh_source_cutoff will be considered.  Works in combination with daily_scheduled_dep_delay_tolerance which says how much time delay is tolerated.  Job will FAIL if the time delay constraint is not met for all tables - this is intended to be configured as a rare occurrence and thus we want to raise an alarm about it.';
  COMMENT ON COLUMN fact_loader.fact_tables.daily_scheduled_dep_delay_tolerance IS 'OPTIONAL for daily scheduled jobs.  Amount of time interval allowed that dependent tables can be out of date before running this job.  For example, if 10 minutes, then if ANY of the dependent tables are more than 10 minutes out of date, this job will FAIL if the time delay constraint is not met for all tables - this is intended to be configured as a rare occurrence and thus we want to raise an alarm about it.';


COMMENT ON TABLE fact_loader.key_retrieval_sequences IS
$$How to go from a change in the queue table itself to retrieve the key
that needs to be updated in the fact table.  That key specifically will be passed
to the insert/update/delete merge_proids configured in queue_table_deps.  When multiple joins
are required to get there, you will have more than one key_retrieval_sequence for a
single queue_table_dep.  You can also optionally have a different key_retrieval_sequence
if your insert/update/delete merge_proids don't all accept the exact same field as an arg.
NOTE - The regression suite in ./sql and ./expected has abundant examples of different configurations.$$;
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.key_retrieval_sequence_id IS 'Unique identifier.';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.queue_table_dep_id IS 'Which fact table - queue table record this is for (queue_table_deps)';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.filter_scope IS
  $$NULL or one of I, U, D.  Optional and likely rare.  By default, this key_retrieval_sequence
  will tell pg_fact_loader how to get the key for all events - insert, update, delete.
  But if your insert/update/delete merge_proids don't all accept the exact same field as an arg,
  you will have to tell it a different way to retrieve the different I, U, D events on separate rows.
  The regression suite has examples of this.$$;
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.level IS
  $$Default 1. When there are multiple joins required to retrieve a key,
  this indicates the order in which to perform the joins.  It will start at level 1,
  then the return_columns_from_join field will be used to join to the join_to_relation - join_to_column
  for the level 2 record, and so on.$$;
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.return_columns IS
  $$What field to return from the base table (if this is level 1), or (if this level 2+)
  this should be the same as the return_columns_from_join from the previous level.$$;
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.is_fact_key IS 'Only true if the base table itself contains the key. If return_columns contains the keys to pass into the functions without any additional join, TRUE.  Otherwise, FALSE if you need to join to get more information.';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.join_to_relation IS 'Join from the base table (or if this is level 2+, the join_to_relation from the previous level) to this table to get the key or to do yet a further join.';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.join_to_column IS 'Join to this column of join_to_relation.';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.return_columns_from_join IS 'Return these columns from join_to_relation.';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.join_return_is_fact_key IS 'If return_columns_from_join are your fact keys, true.  Otherwise false, and that means you need another level to get your key.';
  COMMENT ON COLUMN fact_loader.key_retrieval_sequences.pass_queue_table_change_date_at_tz IS
  $$If this is set to a time zone, then the changed_at field will be cast to this time zone and then cast to a date,
  for the purpose of creating a date-range based fact table.
  For casting queue_table_timestamp to a date, we first ensure we have it as timestamptz (objective UTC time).
  Then, we cast it to the timezone of interest on which the date should be based.
  For example, 02:00:00 UTC time on 2018-05-02 is actually 2018-05-01 in America/Chicago time.
  Thus, any date-based fact table must decide in what time zone to consider the date.$$;


COMMENT ON VIEW fact_loader.queue_deps_all IS 'A view which gathers all fact table data in order to process queued changes and update it, including nested dependencies.';


COMMENT ON VIEW fact_loader.queue_deps_all_with_retrieval IS 'The master view which builds on queue_deps_all to include key_retrieval_sequences.  This is the main view used by sql_builder(int) to gather all queued changes.';


COMMENT ON TABLE fact_loader.queue_table_deps IS
$$Ties together which fact tables depend on which queue tables, along with holding
information on the last cutoff ids for each queue table.  **NOTE** that anything that exists in
queue_table_dep is assumed to be require its queue data not to be pruned even if the fact_tables
job is disabled.  That means that even if a job is disabled, you will not lose data, but you will also
have your queue tables building up in size until you either enable (successfully) or drop the job.
The regression suite in ./sql and ./expected has abundant examples of different configurations.$$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.queue_table_dep_id IS 'Unique identifier.';
  COMMENT ON COLUMN fact_loader.queue_table_deps.fact_table_id IS 'Fact table to tie together with a queue table it depends on.';
  COMMENT ON COLUMN fact_loader.queue_table_deps.queue_table_id IS 'Queue table to tie together with a fact table that needs its changes.';
  COMMENT ON COLUMN fact_loader.queue_table_deps.relevant_change_columns IS
  $$Optional. For UPDATE changes to data, you can specify to only consider changes
  to these columns as sufficient to update the fact table.
  If NULL, all columns will be considered as potentially changing the fact table data.$$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.last_cutoff_id IS
  $$The last fact_loader_batch_id of the queue table that was processed for this queue table - fact table pair.
  After this job runs, records that have this id and lower are eligible to be pruned,
  assuming no other fact tables also depend on those same records.
  The next time the job runs, only records after this id are considered.$$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.last_cutoff_source_time IS
  $$The source data change time of the last queue table record that was processed for this
  queue table - fact table pair.  This helps pg_fact_loader synchronize time across
  multiple queue tables and only pull changes that are early enough, and not purge
  records that are later than these cutoff times.  THIS DOES NOT DETERMINE filter conditions
  for the starting point at which to pull new records as does last_cutoff_id - it is only
  used as an ending-point barrier.
  $$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.insert_merge_proid IS
  $$Function oid to execute on insert events - accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - fact table pair
  is configured in key_retrieval_sequences. NULL to ignore insert events.$$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.update_merge_proid IS
  $$Function oid to execute on update events - accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - fact table pair
  is configured in key_retrieval_sequences. NULL to ignore update events.$$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.delete_merge_proid IS
  $$Function oid to execute on delete events - accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - fact table pair
  is configured in key_retrieval_sequences. NULL to ignore delete events.$$;
  COMMENT ON COLUMN fact_loader.queue_table_deps.row_created_at IS 'Timestamp of when this row was first created.';
  COMMENT ON COLUMN fact_loader.queue_table_deps.row_updated_at IS 'Timestamp of when this row was last updated (this is updated via trigger).';


COMMENT ON TABLE fact_loader.queue_tables IS 'Each queue table along with the base table to which it belongs.';
  COMMENT ON COLUMN fact_loader.queue_tables.queue_table_id IS 'Unique identifier for queue tables.';
  COMMENT ON COLUMN fact_loader.queue_tables.queue_table_relid IS 'The oid of the queue table itself regclass type to accept only valid relations.';
  COMMENT ON COLUMN fact_loader.queue_tables.queue_of_base_table_relid IS 'The oid of the base table for which the queue table contains an audited log of changes.  regclass type to accept only valid relations.';
  COMMENT ON COLUMN fact_loader.queue_tables.pglogical_node_if_id IS
  $$Optional - If NULL, we assume this is a local queue table and we need not synchronize time
  for potential replication delay.  For use with tables that are replicated via pglogical.
  This is the pglogical.node_interface of the table.  This also requires pglogical_ticker
  and is used to synchronize time and ensure we don't continue to move forward
  in time when replication is delayed for this queue table.$$;
  COMMENT ON COLUMN fact_loader.queue_tables.queue_table_tz IS
  $$**NOTE CAREFULLY** - If this is NULL, it assumes that changed_at in the queue
  tables is stored in TIMESTAMPTZ.  If it IS set, it assumes you are telling it that changed_at is
  of TIMESTAMP data type which is stored in the provided time zone of queue_table_tz.$$;
  COMMENT ON COLUMN fact_loader.queue_tables.row_created_at IS 'Timestamp of when this row was first created.';
  COMMENT ON COLUMN fact_loader.queue_tables.row_updated_at IS 'Timestamp of when this row was last updated (this is updated via trigger).';
  COMMENT ON COLUMN fact_loader.queue_tables.purge IS 'Default is true because we prune queue tables as data is no longer needed. Can be set to false and no pruning will happen on this table.';


COMMENT ON VIEW fact_loader.unresolved_failures IS 'Will only show fact table and error messages for a job that just failed and has not been re-enabled since last failure.  Useful for monitoring.';


/* pg_fact_loader--1.3--1.4.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fact_loader" to load this file. \quit

DROP FUNCTION fact_loader.raw_queued_changes(int);
ALTER TABLE fact_loader.debug_process_queue DROP CONSTRAINT debug_process_queue_pkey;


CREATE OR REPLACE FUNCTION fact_loader.load(p_fact_table_id INT)
RETURNS VOID AS
$BODY$
DECLARE
    v_process_queue_sql text;
    v_execute_sql text;
    v_metadata_update_sql text;
    v_debug_rec record;
    v_debug_text text = '';
BEGIN
/***
There are 3 basic steps to this load:
    1. Gather all queue table changes and insert them into a consolidated process_queue
    2. Update the metadata indicating the last records updated for both the queue tables and fact table
 */

/****
Get SQL to insert new data into the consolidated process_queue,
and SQL to update metadata for last_cutoffs.
 */
SELECT process_queue_sql, metadata_update_sql
INTO v_process_queue_sql, v_metadata_update_sql
FROM fact_loader.sql_builder(p_fact_table_id);

/****
Populate the consolidated queue
This just creates a temp table with all changes to be processed
 */
RAISE DEBUG 'Populating Queue for fact_table_id %: %', p_fact_table_id, v_process_queue_sql;
EXECUTE COALESCE(v_process_queue_sql, $$SELECT 'No queue data' AS result$$);

/****
For DEBUG purposes only to view the actual process_queue.  Requires setting log_min_messages to DEBUG.
 */
IF current_setting('log_min_messages') = 'debug3' THEN
    INSERT INTO fact_loader.debug_process_queue (process_queue_id, fact_table_id, proid, key_value, row_created_at, row_updated_at, source_change_date)
    -- the row timestamps are not populated, so we set them here
    SELECT process_queue_id, fact_table_id, proid, key_value, now(), now(), source_change_date FROM process_queue;
END IF;

/****
With data now in the process_queue, the execute_queue function builds the SQL to execute.
Save this SQL in a variable and execute it.
If there is no data to execute, this is a no-op select statement.
 */
SELECT sql INTO v_execute_sql FROM fact_loader.execute_queue(p_fact_table_id);
RAISE DEBUG 'Executing Queue for fact_table_id %: %', p_fact_table_id, v_execute_sql;
EXECUTE COALESCE(v_execute_sql, $$SELECT 'No queue data to execute' AS result$$);

/****
With everything finished, we now update the metadata for the fact_table.
Even if no data was processed, we will still move forward last_refresh_attempted_at.

last_refresh_succeeded will be marked true always for now.  It could in the future
be used to indicate a failure in case of a caught error.
 */
RAISE DEBUG 'Updating metadata for fact_table_id %: %', p_fact_table_id, v_metadata_update_sql;
EXECUTE COALESCE(v_metadata_update_sql,
    format(
    $$UPDATE fact_loader.fact_tables ft
        SET last_refresh_attempted_at = now(),
          last_refresh_succeeded = TRUE
     WHERE fact_table_id = %s;
    $$, p_fact_table_id));

END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fact_loader.sql_builder(p_fact_table_id INT)
  RETURNS TABLE(raw_queued_changes_sql text,
                gathered_queued_changes_sql text,
                process_queue_sql text,
                metadata_update_sql text) AS
$BODY$

/****
The recursive part of this CTE are only the sql_builder parts.
In Postgres, if any of your CTEs are recursive, you only use the RECURSIVE keyword on the first of a set.

The retrieval info may be the same for all 3 events (insert, update, delete), in which case filter_scope is null
Otherwise, they must be specified separately.
 */
WITH RECURSIVE queue_deps_with_insert_retrieval AS (
  SELECT *
  FROM fact_loader.queue_deps_all_with_retrieval
  WHERE (filter_scope = 'I' OR filter_scope IS NULL)
    AND fact_table_id = p_fact_table_id
)

, queue_deps_with_update_retrieval AS (
  SELECT *
  FROM fact_loader.queue_deps_all_with_retrieval
  WHERE (filter_scope = 'U' OR filter_scope IS NULL)
    AND fact_table_id = p_fact_table_id
)

, queue_deps_with_delete_retrieval AS (
  SELECT *
  FROM fact_loader.queue_deps_all_with_retrieval
  WHERE (filter_scope = 'D' OR filter_scope IS NULL)
    AND fact_table_id = p_fact_table_id
)

/****
Recursively build the SQL for any INSERT events found in the queues.

The recursive part ONLY applies to cases where multiple joins have to be made to get at the source data,
in which case there are multiple levels of key_retrieval_sequences for a given queue_table_dep_id. For an
example of this, see the test cases involving the test.order_product_promos table.
 */
, insert_sql_builder AS (
  SELECT queue_table_dep_id,
    level,
    CASE
      WHEN is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', b.'||return_columns[1]||'::TEXT AS key'
            ELSE ', unnest(array[b.'||array_to_string(return_columns, ',b.')||'])::TEXT AS key'
          END
      WHEN join_return_is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', j'||level||'.'||return_columns_from_join[1]||'::TEXT AS key'
            ELSE ', unnest(array[j'||level||'.'||array_to_string(return_columns_from_join, ',j'||level)||'])::TEXT AS key'
          END
        ELSE ''
    END AS key_select_column,
    CASE
      WHEN is_fact_key
        THEN ''
        ELSE 'INNER JOIN '||join_to_relation::TEXT||' j'||level||
             E'\n  ON b.'||quote_ident(return_columns[1])||' = j'||level||'.'||quote_ident(join_to_column)
    END AS key_retrieval_sql,
    source_change_date_select
  FROM queue_deps_with_insert_retrieval c
  WHERE level = 1
    AND fact_table_id = p_fact_table_id
  UNION ALL
  SELECT c.queue_table_dep_id,
    c.level,
    key_select_column||CASE
    WHEN c.is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||r.level||'.'||return_columns[1]||'::TEXT AS key'
          ELSE ', unnest(b.'||array_to_string(return_columns,',j'||r.level)||')::TEXT AS key'
        END
    WHEN join_return_is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||c.level||'.'||return_columns_from_join[1]||'::TEXT AS key'
          ELSE ', unnest(j'||c.level||'.'||array_to_string(return_columns_from_join,',j'||c.level)||')::TEXT AS key'
        END
      ELSE ''
    END AS key_select_column,
    key_retrieval_sql||CASE
    WHEN is_fact_key
      THEN ''
      ELSE E'\nINNER JOIN '||join_to_relation::TEXT||' j'||c.level||
           E'\n  ON j'||r.level||'.'||quote_ident(return_columns[1])||' = j'||c.level||'.'||quote_ident(join_to_column) END AS key_retrieval_sql,
    r.source_change_date_select
  FROM insert_sql_builder r
  INNER JOIN queue_deps_with_insert_retrieval c USING (queue_table_dep_id)
  WHERE c.level = r.level + 1
)

, update_sql_builder AS (
  SELECT queue_table_dep_id,
    level,
    CASE
      WHEN is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', b.'||return_columns[1]||'::TEXT AS key'
            ELSE ', unnest(array[b.'||array_to_string(return_columns, ',b.')||'])::TEXT AS key'
          END
      WHEN join_return_is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', j'||level||'.'||return_columns_from_join[1]||'::TEXT AS key'
            ELSE ', unnest(array[j'||level||'.'||array_to_string(return_columns_from_join, ',j'||level)||'])::TEXT AS key'
          END
        ELSE ''
    END AS key_select_column,
    CASE
      WHEN is_fact_key
        THEN ''
        ELSE 'INNER JOIN '||join_to_relation::TEXT||' j'||level||
             E'\n  ON b.'||quote_ident(return_columns[1])||' = j'||level||'.'||quote_ident(join_to_column)
    END AS key_retrieval_sql,
    source_change_date_select
  FROM queue_deps_with_update_retrieval c
  WHERE level = 1
    AND fact_table_id = p_fact_table_id
  UNION ALL
  SELECT c.queue_table_dep_id,
    c.level,
    key_select_column||CASE
    WHEN c.is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||r.level||'.'||return_columns[1]||'::TEXT AS key'
          ELSE ', unnest(b.'||array_to_string(return_columns,',j'||r.level)||')::TEXT AS key'
        END
    WHEN join_return_is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||c.level||'.'||return_columns_from_join[1]||'::TEXT AS key'
          ELSE ', unnest(j'||c.level||'.'||array_to_string(return_columns_from_join,',j'||c.level)||')::TEXT AS key'
        END
      ELSE ''
    END AS key_select_column,
    key_retrieval_sql||CASE
    WHEN is_fact_key
      THEN ''
      ELSE E'\nINNER JOIN '||join_to_relation::TEXT||' j'||c.level||
           E'\n  ON j'||r.level||'.'||quote_ident(return_columns[1])||' = j'||c.level||'.'||quote_ident(join_to_column) END AS key_retrieval_sql,
    r.source_change_date_select
  FROM update_sql_builder r
  INNER JOIN queue_deps_with_update_retrieval c USING (queue_table_dep_id)
  WHERE c.level = r.level + 1
)

, delete_sql_builder AS (
  SELECT queue_table_dep_id,
    level,
    --For deletes, same pattern as key_select_column but instead, we may be selecting from the audit tables instead
    CASE
      WHEN is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', q.before_change->>'||quote_literal(return_columns[1])||'::TEXT AS key'
            ELSE ', unnest(array[before_change->>'''||array_to_string(return_columns, ''', before_change->>''')||'''])::TEXT AS key'
          END
      WHEN join_return_is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', j'||level||'.'||quote_ident(return_columns_from_join[1])||'::TEXT AS key'
            ELSE ', unnest(array[j'||level||'.'||array_to_string(return_columns_from_join, ',j'||level||'.')||'])::TEXT AS key'
          END
        ELSE ''
    END AS delete_key_select_column,
    CASE
    WHEN is_fact_key
      THEN ''
      ELSE format($$
        --Join to either the base table, or the audit table, one of which
        --will be missing the key in a delete case
          INNER JOIN LATERAL (
           SELECT %s FROM %s jb
           WHERE %s = %s
           UNION ALL
           SELECT %s FROM %s jq
           WHERE operation = 'D'
            AND %s = %s) %s ON TRUE
           $$, quote_ident(return_columns_from_join[1]),
            join_to_relation::TEXT,
            (CASE WHEN level = 1 THEN '(q'||'.before_change->>'||quote_literal(return_columns[1])||')::'||join_column_type ELSE 'j'||level||'.'||quote_ident(return_columns[1]) END),
            'jb.'||quote_ident(join_to_column),
            '(before_change->>'||quote_literal(return_columns_from_join[1])||')::'||return_columns_from_join_type,
            join_to_relation_queue::TEXT,
            (CASE WHEN level = 1 THEN '(q'||'.before_change->>'||quote_literal(return_columns[1])||')::'||join_column_type ELSE 'j'||level||'.'||quote_ident(return_columns[1]) END),
            '(jq.before_change->>'||quote_literal(join_to_column)||')::'||join_column_type,
            /****
            We use the higher level here just to be consistent with aliases from insert/update key retrieval
             */
            'j'||level
            )
    END
      AS delete_key_retrieval_sql,
    source_change_date_select
  FROM queue_deps_with_delete_retrieval
  WHERE level = 1
    AND fact_table_id = p_fact_table_id
  UNION ALL
  SELECT c.queue_table_dep_id,
    c.level,
    delete_key_select_column||CASE
    WHEN c.is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||r.level||'.before_change->>'||quote_literal(return_columns[1])||'::TEXT AS key'
          ELSE ', unnest(array[before_change->>'''||array_to_string(return_columns,',j'||r.level||'.before_change->>''')||'''])::TEXT AS key'
        END
    WHEN join_return_is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||c.level||'.'||quote_ident(return_columns_from_join[1])||'::TEXT AS key'
          ELSE ', unnest(array[j'||c.level||'.'||array_to_string(return_columns_from_join,',j'||c.level)||')::TEXT AS key'
        END
      ELSE ''
    END AS delete_key_select_column,
    delete_key_retrieval_sql||CASE
    WHEN is_fact_key
      THEN ''
      ELSE format($$
        --Join to either the base table, or the audit table, one of which
        --will be missing the key in a delete case
          INNER JOIN LATERAL (
           SELECT %s FROM %s jb
           WHERE %s = %s
           UNION ALL
           SELECT %s FROM %s jq
           WHERE operation = 'D'
            AND %s = %s) %s ON TRUE
           $$, quote_ident(return_columns_from_join[1]),
            join_to_relation::TEXT,
            'j'||r.level||'.'||quote_ident(return_columns[1]),
            'jb.'||quote_ident(join_to_column),
            '(before_change->>'||quote_literal(return_columns_from_join[1])||')::'||return_columns_from_join_type,
            join_to_relation_queue::TEXT,
            'j'||r.level||'.'||quote_ident(return_columns[1]),
            '(jq.before_change->>'||quote_literal(join_to_column)||')::'||join_column_type,
            /****
            We use the higher level here just to be consistent with aliases from insert/update key retrieval
             */
            'j'||c.level
            )
    END
      AS delete_key_retrieval_sql,
    r.source_change_date_select
  FROM delete_sql_builder r
  INNER JOIN queue_deps_with_delete_retrieval c USING (queue_table_dep_id)
  WHERE c.level = r.level + 1
)

, field_vars AS (
  SELECT
    *,
    format($$
      %s AS fact_table_id,
      %s AS queue_table_dep_id,
      %s::INT AS fact_table_dep_id,
      %s::INT AS fact_table_dep_queue_table_dep_id,
      %s AS queue_table_id_field,
      q.fact_loader_batch_id,
      %s::TIMESTAMPTZ AS maximum_cutoff_time,
    -- We must not ignore ids which are above maximum_cutoff_time
    -- but below the highest id which is below maximum_cutoff_time
      MIN(q.fact_loader_batch_id)
      FILTER (
      WHERE %s %s > %s::TIMESTAMPTZ)
      OVER() AS min_missed_id
    $$,
      fact_table_id,
      queue_table_dep_id,
      (CASE WHEN fact_table_dep_id IS NULL THEN 'NULL'::TEXT ELSE fact_table_dep_id::TEXT END),
      (CASE WHEN fact_table_dep_queue_table_dep_id IS NULL THEN 'NULL'::TEXT ELSE fact_table_dep_queue_table_dep_id::TEXT END),
      'q.'||quote_ident(queue_table_id_field),
      quote_literal(maximum_cutoff_time),
      'q.'||quote_ident(queue_table_timestamp),
      CASE WHEN queue_table_tz IS NULL THEN '' ELSE 'AT TIME ZONE '||quote_literal(queue_table_tz) END,
      quote_literal(maximum_cutoff_time)
    )
      AS inner_shared_select_columns,
    $$
      fact_table_id,
      queue_table_dep_id,
      fact_table_dep_id,
      fact_table_dep_queue_table_dep_id,
      queue_table_id_field,
      fact_loader_batch_id,
      maximum_cutoff_time,
      min_missed_id
    $$
      AS outer_shared_select_columns,
    CASE WHEN queue_table_tz IS NULL THEN '' ELSE 'AT TIME ZONE '||quote_literal(queue_table_tz) END
      AS changed_at_tz_correction
  FROM fact_loader.queue_deps_all c
  WHERE c.fact_table_id = p_fact_table_id
)

, non_recursive_sql AS (
  SELECT
  /****
  Separate select list for:
    - raw queue_ids from queue tables
    - gathered data from joining queue_ids to source tables to get actual keys to update in fact tables
   */
  -- gathering all queue_ids from queue tables
    queue_table_dep_id,
    outer_shared_select_columns,
    format($$
      %s,
      %s %s AS changed_at,
      %s AS queue_table_id
    $$,
      inner_shared_select_columns,
      'q.'||quote_ident(queue_table_timestamp),
      changed_at_tz_correction,
      queue_table_id
    )
      AS inner_metadata_select_columns,
    format($$
      %s,
      queue_table_id
    $$,
      outer_shared_select_columns
    )
      AS outer_metadata_select_columns,

  -- gathering actual keys to update in fact tables by joining from queue_ids to source tables
    format($$
      %s,
      %s AS operation,
      %s %s AS changed_at,
      %s::REGPROC AS insert_merge_proid,
      %s::REGPROC AS update_merge_proid,
      %s::REGPROC AS delete_merge_proid
    $$,
      inner_shared_select_columns,
      'q.'||quote_ident(queue_table_op),
      'q.'||quote_ident(queue_table_timestamp),
      changed_at_tz_correction,
      CASE WHEN insert_merge_proid IS NULL THEN 'NULL' ELSE quote_literal(insert_merge_proid) END,
      CASE WHEN update_merge_proid IS NULL THEN 'NULL' ELSE quote_literal(update_merge_proid) END,
      CASE WHEN delete_merge_proid IS NULL THEN 'NULL' ELSE quote_literal(delete_merge_proid) END
    )
      AS inner_data_select_columns,
    format($$
      %s,
      operation,
      changed_at,
      insert_merge_proid,
      update_merge_proid,
      delete_merge_proid,
      key,
      source_change_date
    $$,
      outer_shared_select_columns
    )
      AS outer_data_select_columns,

  -- This is simply the queue table aliased as q
    format('%s q', queue_table_relid::TEXT) AS queue_table_aliased,

  -- This is the SQL to join from the queue table to the base table
    format($$
      INNER JOIN %s b
        ON q.%s::%s = b.%s
    $$,
      queue_of_base_table_relid::TEXT,
      quote_ident(queue_table_key),
      queue_of_base_table_key_type,
      quote_ident(queue_of_base_table_key))
           AS base_join_sql,

  -- This is a WHERE statement to be added to ALL gathering of new queue_ids to process.
  -- There is a further filter based on the window min_missed_id after this subquery
    format($$ %s
        $$,
        CASE
          WHEN last_cutoff_id IS NOT NULL
          THEN 'q.fact_loader_batch_id > '||last_cutoff_id
          ELSE
            'TRUE'
        END)
           AS inner_global_where_sql,
    format($$
        %s < %s %s
        AND (min_missed_id IS NULL OR (fact_loader_batch_id < min_missed_id))
        $$,
        quote_ident(c.queue_table_timestamp),
        quote_literal(c.maximum_cutoff_time),
        changed_at_tz_correction)
           AS outer_global_where_sql,
    format($$
      AND q.%s = 'I'
      $$,
        queue_table_op)
           AS where_for_insert_sql,
    format($$
      AND (q.%s = 'U' AND %s)
      $$,
        queue_table_op,
        CASE
          WHEN relevant_change_columns IS NULL
            THEN 'TRUE'
          ELSE
            format($$q.%s ?| '{%s}'$$, queue_table_change, array_to_string(relevant_change_columns,','))
        END)
           AS where_for_update_sql,
    format($$
      AND q.%s = 'D'
      $$,
        queue_table_op)
           AS where_for_delete_sql
  FROM field_vars c
)

, insert_sql_builder_final AS
(SELECT DISTINCT ON (queue_table_dep_id)
  *
FROM insert_sql_builder
ORDER BY queue_table_dep_id, level DESC
)

, update_sql_builder_final AS
(SELECT DISTINCT ON (queue_table_dep_id)
  *
FROM update_sql_builder
ORDER BY queue_table_dep_id, level DESC
)

, delete_sql_builder_final AS
(SELECT DISTINCT ON (queue_table_dep_id)
  *
FROM delete_sql_builder
ORDER BY queue_table_dep_id, level DESC
)

, all_queues_sql AS (
SELECT
  format($$
  SELECT %s
  FROM (
  SELECT %s
  FROM %s
  %s
  WHERE %s ) sub
  WHERE %s
  $$,
    nrs.outer_data_select_columns,
    nrs.inner_data_select_columns||isbf.key_select_column||isbf.source_change_date_select,
    nrs.queue_table_aliased||nrs.base_join_sql,
    isbf.key_retrieval_sql,
    nrs.inner_global_where_sql||nrs.where_for_insert_sql,
    nrs.outer_global_where_sql) AS queue_insert_sql,
  format($$
  SELECT %s
  FROM (
  SELECT %s
  FROM %s
  %s
  WHERE %s ) sub
  WHERE %s
  $$,
    nrs.outer_data_select_columns,
    nrs.inner_data_select_columns||usbf.key_select_column||usbf.source_change_date_select,
    nrs.queue_table_aliased||nrs.base_join_sql,
    usbf.key_retrieval_sql,
    nrs.inner_global_where_sql||nrs.where_for_update_sql,
    nrs.outer_global_where_sql) AS queue_update_sql,
  format($$
  SELECT %s
  FROM (
  SELECT %s
  FROM %s
  %s
  WHERE %s ) sub
  WHERE %s
  $$,
    nrs.outer_data_select_columns,
    nrs.inner_data_select_columns||dsbf.delete_key_select_column||usbf.source_change_date_select,
    nrs.queue_table_aliased,
    dsbf.delete_key_retrieval_sql,
    nrs.inner_global_where_sql||nrs.where_for_delete_sql,
    nrs.outer_global_where_sql) AS queue_delete_sql,
  format($$
  SELECT %s
  FROM (
  SELECT %s
  FROM %s
  WHERE %s ) sub
  WHERE %s
  $$,
    nrs.outer_metadata_select_columns,
    nrs.inner_metadata_select_columns,
    nrs.queue_table_aliased,
    nrs.inner_global_where_sql,
    nrs.outer_global_where_sql) AS queue_ids_sql
FROM non_recursive_sql nrs
INNER JOIN insert_sql_builder_final isbf ON isbf.queue_table_dep_id = nrs.queue_table_dep_id
INNER JOIN update_sql_builder_final usbf ON usbf.queue_table_dep_id = nrs.queue_table_dep_id
INNER JOIN delete_sql_builder_final dsbf ON dsbf.queue_table_dep_id = nrs.queue_table_dep_id
)

, final_queue_sql AS
(SELECT string_agg(
  /****
  This first UNION is to union together INSERT, UPDATE, and DELETE events for a single queue table
   */
  format($$
  %s
  UNION ALL
  %s
  UNION ALL
  %s
  $$,
    queue_insert_sql,
    queue_update_sql,
    queue_delete_sql)
  /****
  This second UNION as the second arg of string_agg is the union together ALL queue tables for this fact table
   */
  , E'\nUNION ALL\n') AS event_sql,
  string_agg(queue_ids_sql, E'\nUNION ALL\n') AS raw_queued_changes_sql_out
FROM all_queues_sql)

, final_outputs AS (
SELECT raw_queued_changes_sql_out,
$$
WITH all_changes AS (
($$||event_sql||$$)
ORDER BY changed_at)

, base_execution_groups AS
(SELECT fact_table_id,
    queue_table_dep_id,
    queue_table_id_field,
    operation,
    changed_at,
    source_change_date,
    insert_merge_proid,
    update_merge_proid,
    delete_merge_proid,
    maximum_cutoff_time,
    key,
    CASE WHEN operation = 'I' THEN insert_merge_proid
    WHEN operation = 'U' THEN update_merge_proid
    WHEN operation = 'D' THEN delete_merge_proid
        END AS proid,
  RANK() OVER (
    PARTITION BY
      CASE
        WHEN operation = 'I' THEN insert_merge_proid
        WHEN operation = 'U' THEN update_merge_proid
        WHEN operation = 'D' THEN delete_merge_proid
      END
  ) AS execution_group
  FROM all_changes
  WHERE key IS NOT NULL)

SELECT fact_table_id, proid, key, source_change_date
FROM base_execution_groups beg
WHERE proid IS NOT NULL
GROUP BY execution_group, fact_table_id, proid, key, source_change_date
/****
This ordering is particularly important for date-range history tables
where order of inserts is critical and usually expected to follow a pattern
***/
ORDER BY execution_group, MIN(changed_at), MIN(queue_table_id_field);
$$ AS gathered_queued_changes_sql_out
  ,

$$
DROP TABLE IF EXISTS process_queue;
CREATE TEMP TABLE process_queue
(process_queue_id serial,
 fact_table_id int,
 proid regproc,
 key_value text,
 source_change_date date);

INSERT INTO process_queue
(fact_table_id, proid, key_value, source_change_date)
$$ AS process_queue_snippet,

$$
WITH all_ids AS
($$||raw_queued_changes_sql_out||$$)

, new_metadata AS
(SELECT MAX(fact_loader_batch_id) AS last_cutoff_id,
  maximum_cutoff_time,
  queue_table_dep_id
FROM all_ids
--Exclude dependent fact tables from updates directly to queue_table_deps
WHERE fact_table_dep_id IS NULL
GROUP BY queue_table_dep_id, maximum_cutoff_time)

/****
The dependent fact table uses the same queue_table_id_field as last_cutoff
We are going to update fact_table_deps metadata instead of queue_table_deps
****/
, new_metadata_fact_dep AS
(SELECT MAX(fact_loader_batch_id) AS last_cutoff_id,
  maximum_cutoff_time,
  fact_table_dep_queue_table_dep_id
FROM all_ids
--Include dependent fact tables only
WHERE fact_table_dep_id IS NOT NULL
GROUP BY fact_table_dep_queue_table_dep_id, maximum_cutoff_time)

, update_key AS (
SELECT qdwr.queue_table_dep_id,
  --Cutoff the id to that newly found, otherwise default to last value
  COALESCE(mu.last_cutoff_id, qdwr.last_cutoff_id) AS last_cutoff_id,
  --This cutoff time must always be the same for all queue tables for given fact table.
  --Even if there are no new records, we move this forward to wherever the stream is at
  qdwr.maximum_cutoff_time AS last_cutoff_source_time
FROM fact_loader.queue_deps_all qdwr
LEFT JOIN new_metadata mu ON mu.queue_table_dep_id = qdwr.queue_table_dep_id
WHERE qdwr.fact_table_id = $$||p_fact_table_id||$$
--Exclude dependent fact tables from updates directly to queue_table_deps
  AND qdwr.fact_table_dep_id IS NULL
)

/****
This SQL also nearly matches that for the queue_table_deps but would be a little ugly to try to DRY up
****/
, update_key_fact_dep AS (
SELECT qdwr.fact_table_dep_queue_table_dep_id,
  qdwr.fact_table_id,
  COALESCE(mu.last_cutoff_id, qdwr.last_cutoff_id) AS last_cutoff_id,
  qdwr.maximum_cutoff_time AS last_cutoff_source_time
FROM fact_loader.queue_deps_all qdwr
LEFT JOIN new_metadata_fact_dep mu ON mu.fact_table_dep_queue_table_dep_id = qdwr.fact_table_dep_queue_table_dep_id
WHERE qdwr.fact_table_id = $$||p_fact_table_id||$$
--Include dependent fact tables only
  AND qdwr.fact_table_dep_id IS NOT NULL
)

, updated_queue_table_deps AS (
UPDATE fact_loader.queue_table_deps qtd
SET last_cutoff_id = uk.last_cutoff_id,
  last_cutoff_source_time = uk.last_cutoff_source_time
FROM update_key uk
WHERE qtd.queue_table_dep_id = uk.queue_table_dep_id
RETURNING qtd.*)

, updated_fact_table_deps AS (
UPDATE fact_loader.fact_table_dep_queue_table_deps ftd
SET last_cutoff_id = uk.last_cutoff_id,
  last_cutoff_source_time = uk.last_cutoff_source_time
FROM update_key_fact_dep uk
WHERE ftd.fact_table_dep_queue_table_dep_id = uk.fact_table_dep_queue_table_dep_id
RETURNING uk.*)

UPDATE fact_loader.fact_tables ft
SET last_refresh_source_cutoff = uqtd.last_cutoff_source_time,
  last_refresh_attempted_at = now(),
  last_refresh_succeeded = TRUE
FROM
(SELECT fact_table_id, last_cutoff_source_time
FROM updated_queue_table_deps
--Must use UNION to get only distinct values
UNION
SELECT fact_table_id, last_cutoff_source_time
FROM updated_fact_table_deps) uqtd
WHERE uqtd.fact_table_id = ft.fact_table_id;
$$ AS metadata_update_sql_out
FROM final_queue_sql)

SELECT raw_queued_changes_sql_out,
 gathered_queued_changes_sql_out
  ,
 format($$
  %s
  %s$$, process_queue_snippet, gathered_queued_changes_sql_out) AS process_queue_sql_out,
 metadata_update_sql_out
FROM final_outputs;

$BODY$
LANGUAGE SQL;


CREATE OR REPLACE FUNCTION fact_loader.raw_queued_changes(p_fact_table_id INT)
RETURNS TABLE (fact_table_id INT,
    queue_table_dep_id INT,
    fact_table_dep_id INT,
    fact_table_dep_queue_table_dep_id INT,
    queue_table_id_field BIGINT,
    fact_loader_batch_id BIGINT,
    maximum_cutoff_time TIMESTAMPTZ,
    min_missed_id BIGINT,
    queue_table_id INT
) AS
$BODY$
DECLARE
    v_raw_sql text;
BEGIN

SELECT raw_queued_changes_sql
INTO v_raw_sql
FROM fact_loader.sql_builder(p_fact_table_id);

RETURN QUERY EXECUTE v_raw_sql;

END;
$BODY$
LANGUAGE plpgsql;


COMMENT ON VIEW fact_loader.queue_deps_all IS 'A view which gathers all fact table data in order to process queued changes and update it, including nested dependencies.';


/* pg_fact_loader--1.4--1.5.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fact_loader" to load this file. \quit

CREATE OR REPLACE FUNCTION fact_loader.sql_builder(p_fact_table_id INT)
  RETURNS TABLE(raw_queued_changes_sql text,
                gathered_queued_changes_sql text,
                process_queue_sql text,
                metadata_update_sql text) AS
$BODY$

/****
The recursive part of this CTE are only the sql_builder parts.
In Postgres, if any of your CTEs are recursive, you only use the RECURSIVE keyword on the first of a set.

The retrieval info may be the same for all 3 events (insert, update, delete), in which case filter_scope is null
Otherwise, they must be specified separately.
 */
WITH RECURSIVE queue_deps_with_insert_retrieval AS (
  SELECT *
  FROM fact_loader.queue_deps_all_with_retrieval
  WHERE (filter_scope = 'I' OR filter_scope IS NULL)
    AND fact_table_id = p_fact_table_id
)

, queue_deps_with_update_retrieval AS (
  SELECT *
  FROM fact_loader.queue_deps_all_with_retrieval
  WHERE (filter_scope = 'U' OR filter_scope IS NULL)
    AND fact_table_id = p_fact_table_id
)

, queue_deps_with_delete_retrieval AS (
  SELECT *
  FROM fact_loader.queue_deps_all_with_retrieval
  WHERE (filter_scope = 'D' OR filter_scope IS NULL)
    AND fact_table_id = p_fact_table_id
)

/****
Recursively build the SQL for any INSERT events found in the queues.

The recursive part ONLY applies to cases where multiple joins have to be made to get at the source data,
in which case there are multiple levels of key_retrieval_sequences for a given queue_table_dep_id. For an
example of this, see the test cases involving the test.order_product_promos table.
 */
, insert_sql_builder AS (
  SELECT queue_table_dep_id,
    level,
    CASE
      WHEN is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', b.'||return_columns[1]||'::TEXT AS key'
            ELSE ', unnest(array[b.'||array_to_string(return_columns, ',b.')||'])::TEXT AS key'
          END
      WHEN join_return_is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', j'||level||'.'||return_columns_from_join[1]||'::TEXT AS key'
            ELSE ', unnest(array[j'||level||'.'||array_to_string(return_columns_from_join, ',j'||level)||'])::TEXT AS key'
          END
        ELSE ''
    END AS key_select_column,
    CASE
      WHEN is_fact_key
        THEN ''
        ELSE 'INNER JOIN '||join_to_relation::TEXT||' j'||level||
             E'\n  ON b.'||quote_ident(return_columns[1])||' = j'||level||'.'||quote_ident(join_to_column)
    END AS key_retrieval_sql,
    source_change_date_select
  FROM queue_deps_with_insert_retrieval c
  WHERE level = 1
    AND fact_table_id = p_fact_table_id
  UNION ALL
  SELECT c.queue_table_dep_id,
    c.level,
    key_select_column||CASE
    WHEN c.is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||r.level||'.'||return_columns[1]||'::TEXT AS key'
          ELSE ', unnest(b.'||array_to_string(return_columns,',j'||r.level)||')::TEXT AS key'
        END
    WHEN join_return_is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||c.level||'.'||return_columns_from_join[1]||'::TEXT AS key'
          ELSE ', unnest(j'||c.level||'.'||array_to_string(return_columns_from_join,',j'||c.level)||')::TEXT AS key'
        END
      ELSE ''
    END AS key_select_column,
    key_retrieval_sql||CASE
    WHEN is_fact_key
      THEN ''
      ELSE E'\nINNER JOIN '||join_to_relation::TEXT||' j'||c.level||
           E'\n  ON j'||r.level||'.'||quote_ident(return_columns[1])||' = j'||c.level||'.'||quote_ident(join_to_column) END AS key_retrieval_sql,
    r.source_change_date_select
  FROM insert_sql_builder r
  INNER JOIN queue_deps_with_insert_retrieval c USING (queue_table_dep_id)
  WHERE c.level = r.level + 1
)

, update_sql_builder AS (
  SELECT queue_table_dep_id,
    level,
    CASE
      WHEN is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', b.'||return_columns[1]||'::TEXT AS key'
            ELSE ', unnest(array[b.'||array_to_string(return_columns, ',b.')||'])::TEXT AS key'
          END
      WHEN join_return_is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', j'||level||'.'||return_columns_from_join[1]||'::TEXT AS key'
            ELSE ', unnest(array[j'||level||'.'||array_to_string(return_columns_from_join, ',j'||level)||'])::TEXT AS key'
          END
        ELSE ''
    END AS key_select_column,
    CASE
      WHEN is_fact_key
        THEN ''
        ELSE 'INNER JOIN '||join_to_relation::TEXT||' j'||level||
             E'\n  ON b.'||quote_ident(return_columns[1])||' = j'||level||'.'||quote_ident(join_to_column)
    END AS key_retrieval_sql,
    source_change_date_select
  FROM queue_deps_with_update_retrieval c
  WHERE level = 1
    AND fact_table_id = p_fact_table_id
  UNION ALL
  SELECT c.queue_table_dep_id,
    c.level,
    key_select_column||CASE
    WHEN c.is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||r.level||'.'||return_columns[1]||'::TEXT AS key'
          ELSE ', unnest(b.'||array_to_string(return_columns,',j'||r.level)||')::TEXT AS key'
        END
    WHEN join_return_is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||c.level||'.'||return_columns_from_join[1]||'::TEXT AS key'
          ELSE ', unnest(j'||c.level||'.'||array_to_string(return_columns_from_join,',j'||c.level)||')::TEXT AS key'
        END
      ELSE ''
    END AS key_select_column,
    key_retrieval_sql||CASE
    WHEN is_fact_key
      THEN ''
      ELSE E'\nINNER JOIN '||join_to_relation::TEXT||' j'||c.level||
           E'\n  ON j'||r.level||'.'||quote_ident(return_columns[1])||' = j'||c.level||'.'||quote_ident(join_to_column) END AS key_retrieval_sql,
    r.source_change_date_select
  FROM update_sql_builder r
  INNER JOIN queue_deps_with_update_retrieval c USING (queue_table_dep_id)
  WHERE c.level = r.level + 1
)

, delete_sql_builder AS (
  SELECT queue_table_dep_id,
    level,
    --For deletes, same pattern as key_select_column but instead, we may be selecting from the audit tables instead
    CASE
      WHEN is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', q.before_change->>'||quote_literal(return_columns[1])||'::TEXT AS key'
            ELSE ', unnest(array[before_change->>'''||array_to_string(return_columns, ''', before_change->>''')||'''])::TEXT AS key'
          END
      WHEN join_return_is_fact_key
        THEN
          CASE
            WHEN array_length(return_columns, 1) = 1
            THEN ', j'||level||'.'||quote_ident(return_columns_from_join[1])||'::TEXT AS key'
            ELSE ', unnest(array[j'||level||'.'||array_to_string(return_columns_from_join, ',j'||level||'.')||'])::TEXT AS key'
          END
        ELSE ''
    END AS delete_key_select_column,
    CASE
    WHEN is_fact_key
      THEN ''
      ELSE format($$
        --Join to either the base table, or the audit table, one of which
        --will be missing the key in a delete case
          INNER JOIN LATERAL (
           SELECT %s FROM %s jb
           WHERE %s = %s
           UNION ALL
           SELECT %s FROM %s jq
           WHERE operation = 'D'
            AND %s = %s) %s ON TRUE
           $$, quote_ident(return_columns_from_join[1]),
            join_to_relation::TEXT,
            (CASE WHEN level = 1 THEN '(q'||'.before_change->>'||quote_literal(return_columns[1])||')::'||join_column_type ELSE 'j'||level||'.'||quote_ident(return_columns[1]) END),
            'jb.'||quote_ident(join_to_column),
            '(before_change->>'||quote_literal(return_columns_from_join[1])||')::'||return_columns_from_join_type,
            join_to_relation_queue::TEXT,
            (CASE WHEN level = 1 THEN '(q'||'.before_change->>'||quote_literal(return_columns[1])||')::'||join_column_type ELSE 'j'||level||'.'||quote_ident(return_columns[1]) END),
            '(jq.before_change->>'||quote_literal(join_to_column)||')::'||join_column_type,
            /****
            We use the higher level here just to be consistent with aliases from insert/update key retrieval
             */
            'j'||level
            )
    END
      AS delete_key_retrieval_sql,
    source_change_date_select
  FROM queue_deps_with_delete_retrieval
  WHERE level = 1
    AND fact_table_id = p_fact_table_id
  UNION ALL
  SELECT c.queue_table_dep_id,
    c.level,
    delete_key_select_column||CASE
    WHEN c.is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||r.level||'.before_change->>'||quote_literal(return_columns[1])||'::TEXT AS key'
          ELSE ', unnest(array[before_change->>'''||array_to_string(return_columns,',j'||r.level||'.before_change->>''')||'''])::TEXT AS key'
        END
    WHEN join_return_is_fact_key
      THEN
        CASE
          WHEN array_length(return_columns, 1) = 1
          THEN ', j'||c.level||'.'||quote_ident(return_columns_from_join[1])||'::TEXT AS key'
          ELSE ', unnest(array[j'||c.level||'.'||array_to_string(return_columns_from_join,',j'||c.level)||')::TEXT AS key'
        END
      ELSE ''
    END AS delete_key_select_column,
    delete_key_retrieval_sql||CASE
    WHEN is_fact_key
      THEN ''
      ELSE format($$
        --Join to either the base table, or the audit table, one of which
        --will be missing the key in a delete case
          INNER JOIN LATERAL (
           SELECT %s FROM %s jb
           WHERE %s = %s
           UNION ALL
           SELECT %s FROM %s jq
           WHERE operation = 'D'
            AND %s = %s) %s ON TRUE
           $$, quote_ident(return_columns_from_join[1]),
            join_to_relation::TEXT,
            'j'||r.level||'.'||quote_ident(return_columns[1]),
            'jb.'||quote_ident(join_to_column),
            '(before_change->>'||quote_literal(return_columns_from_join[1])||')::'||return_columns_from_join_type,
            join_to_relation_queue::TEXT,
            'j'||r.level||'.'||quote_ident(return_columns[1]),
            '(jq.before_change->>'||quote_literal(join_to_column)||')::'||join_column_type,
            /****
            We use the higher level here just to be consistent with aliases from insert/update key retrieval
             */
            'j'||c.level
            )
    END
      AS delete_key_retrieval_sql,
    r.source_change_date_select
  FROM delete_sql_builder r
  INNER JOIN queue_deps_with_delete_retrieval c USING (queue_table_dep_id)
  WHERE c.level = r.level + 1
)

, field_vars AS (
  SELECT
    *,
    format($$
      %s AS fact_table_id,
      %s AS queue_table_dep_id,
      %s::INT AS fact_table_dep_id,
      %s::INT AS fact_table_dep_queue_table_dep_id,
      %s AS queue_table_id_field,
      q.fact_loader_batch_id,
      %s::TIMESTAMPTZ AS maximum_cutoff_time,
    -- We must not ignore ids which are above maximum_cutoff_time
    -- but below the highest id which is below maximum_cutoff_time
      MIN(q.fact_loader_batch_id)
      FILTER (
      WHERE %s %s > %s::TIMESTAMPTZ)
      OVER() AS min_missed_id
    $$,
      fact_table_id,
      queue_table_dep_id,
      (CASE WHEN fact_table_dep_id IS NULL THEN 'NULL'::TEXT ELSE fact_table_dep_id::TEXT END),
      (CASE WHEN fact_table_dep_queue_table_dep_id IS NULL THEN 'NULL'::TEXT ELSE fact_table_dep_queue_table_dep_id::TEXT END),
      'q.'||quote_ident(queue_table_id_field),
      quote_literal(maximum_cutoff_time),
      'q.'||quote_ident(queue_table_timestamp),
      CASE WHEN queue_table_tz IS NULL THEN '' ELSE 'AT TIME ZONE '||quote_literal(queue_table_tz) END,
      quote_literal(maximum_cutoff_time)
    )
      AS inner_shared_select_columns,
    $$
      fact_table_id,
      queue_table_dep_id,
      fact_table_dep_id,
      fact_table_dep_queue_table_dep_id,
      queue_table_id_field,
      fact_loader_batch_id,
      maximum_cutoff_time,
      min_missed_id
    $$
      AS outer_shared_select_columns,
    CASE WHEN queue_table_tz IS NULL THEN '' ELSE 'AT TIME ZONE '||quote_literal(queue_table_tz) END
      AS changed_at_tz_correction
  FROM fact_loader.queue_deps_all c
  WHERE c.fact_table_id = p_fact_table_id
)

, non_recursive_sql AS (
  SELECT
  /****
  Separate select list for:
    - raw queue_ids from queue tables
    - gathered data from joining queue_ids to source tables to get actual keys to update in fact tables
   */
  -- gathering all queue_ids from queue tables
    queue_table_dep_id,
    outer_shared_select_columns,
    format($$
      %s,
      %s %s AS changed_at,
      %s AS queue_table_id
    $$,
      inner_shared_select_columns,
      'q.'||quote_ident(queue_table_timestamp),
      changed_at_tz_correction,
      queue_table_id
    )
      AS inner_metadata_select_columns,
    format($$
      %s,
      queue_table_id
    $$,
      outer_shared_select_columns
    )
      AS outer_metadata_select_columns,

  -- gathering actual keys to update in fact tables by joining from queue_ids to source tables
    format($$
      %s,
      %s AS operation,
      %s %s AS changed_at,
      %s::REGPROC AS insert_merge_proid,
      %s::REGPROC AS update_merge_proid,
      %s::REGPROC AS delete_merge_proid
    $$,
      inner_shared_select_columns,
      'q.'||quote_ident(queue_table_op),
      'q.'||quote_ident(queue_table_timestamp),
      changed_at_tz_correction,
      CASE WHEN insert_merge_proid IS NULL THEN 'NULL' ELSE quote_literal(insert_merge_proid) END,
      CASE WHEN update_merge_proid IS NULL THEN 'NULL' ELSE quote_literal(update_merge_proid) END,
      CASE WHEN delete_merge_proid IS NULL THEN 'NULL' ELSE quote_literal(delete_merge_proid) END
    )
      AS inner_data_select_columns,
    format($$
      %s,
      operation,
      changed_at,
      insert_merge_proid,
      update_merge_proid,
      delete_merge_proid,
      key,
      source_change_date
    $$,
      outer_shared_select_columns
    )
      AS outer_data_select_columns,

  -- This is simply the queue table aliased as q
    format('%s q', queue_table_relid::TEXT) AS queue_table_aliased,

  -- This is the SQL to join from the queue table to the base table
    format($$
      INNER JOIN %s b
        ON q.%s::%s = b.%s
    $$,
      queue_of_base_table_relid::TEXT,
      quote_ident(queue_table_key),
      queue_of_base_table_key_type,
      quote_ident(queue_of_base_table_key))
           AS base_join_sql,

  -- This is a WHERE statement to be added to ALL gathering of new queue_ids to process.
  -- There is a further filter based on the window min_missed_id after this subquery
    format($$ %s
        $$,
        CASE
          WHEN last_cutoff_id IS NOT NULL
          THEN 'q.fact_loader_batch_id > '||last_cutoff_id
          ELSE
            'TRUE'
        END)
           AS inner_global_where_sql,
    format($$
        -- changed_at is guaranteed now to be in timestamptz - any time zone casting is only in subquery
        changed_at < %s
        AND (min_missed_id IS NULL OR (fact_loader_batch_id < min_missed_id))
        $$,
        quote_literal(c.maximum_cutoff_time)
        )
           AS outer_global_where_sql,
    format($$
      AND q.%s = 'I'
      $$,
        queue_table_op)
           AS where_for_insert_sql,
    format($$
      AND (q.%s = 'U' AND %s)
      $$,
        queue_table_op,
        CASE
          WHEN relevant_change_columns IS NULL
            THEN 'TRUE'
          ELSE
            format($$q.%s ?| '{%s}'$$, queue_table_change, array_to_string(relevant_change_columns,','))
        END)
           AS where_for_update_sql,
    format($$
      AND q.%s = 'D'
      $$,
        queue_table_op)
           AS where_for_delete_sql
  FROM field_vars c
)

, insert_sql_builder_final AS
(SELECT DISTINCT ON (queue_table_dep_id)
  *
FROM insert_sql_builder
ORDER BY queue_table_dep_id, level DESC
)

, update_sql_builder_final AS
(SELECT DISTINCT ON (queue_table_dep_id)
  *
FROM update_sql_builder
ORDER BY queue_table_dep_id, level DESC
)

, delete_sql_builder_final AS
(SELECT DISTINCT ON (queue_table_dep_id)
  *
FROM delete_sql_builder
ORDER BY queue_table_dep_id, level DESC
)

, all_queues_sql AS (
SELECT
  format($$
  SELECT %s
  FROM (
  SELECT %s
  FROM %s
  %s
  WHERE %s ) sub
  WHERE %s
  $$,
    nrs.outer_data_select_columns,
    nrs.inner_data_select_columns||isbf.key_select_column||isbf.source_change_date_select,
    nrs.queue_table_aliased||nrs.base_join_sql,
    isbf.key_retrieval_sql,
    nrs.inner_global_where_sql||nrs.where_for_insert_sql,
    nrs.outer_global_where_sql) AS queue_insert_sql,
  format($$
  SELECT %s
  FROM (
  SELECT %s
  FROM %s
  %s
  WHERE %s ) sub
  WHERE %s
  $$,
    nrs.outer_data_select_columns,
    nrs.inner_data_select_columns||usbf.key_select_column||usbf.source_change_date_select,
    nrs.queue_table_aliased||nrs.base_join_sql,
    usbf.key_retrieval_sql,
    nrs.inner_global_where_sql||nrs.where_for_update_sql,
    nrs.outer_global_where_sql) AS queue_update_sql,
  format($$
  SELECT %s
  FROM (
  SELECT %s
  FROM %s
  %s
  WHERE %s ) sub
  WHERE %s
  $$,
    nrs.outer_data_select_columns,
    nrs.inner_data_select_columns||dsbf.delete_key_select_column||usbf.source_change_date_select,
    nrs.queue_table_aliased,
    dsbf.delete_key_retrieval_sql,
    nrs.inner_global_where_sql||nrs.where_for_delete_sql,
    nrs.outer_global_where_sql) AS queue_delete_sql,
  format($$
  SELECT %s
  FROM (
  SELECT %s
  FROM %s
  WHERE %s ) sub
  WHERE %s
  $$,
    nrs.outer_metadata_select_columns,
    nrs.inner_metadata_select_columns,
    nrs.queue_table_aliased,
    nrs.inner_global_where_sql,
    nrs.outer_global_where_sql) AS queue_ids_sql
FROM non_recursive_sql nrs
INNER JOIN insert_sql_builder_final isbf ON isbf.queue_table_dep_id = nrs.queue_table_dep_id
INNER JOIN update_sql_builder_final usbf ON usbf.queue_table_dep_id = nrs.queue_table_dep_id
INNER JOIN delete_sql_builder_final dsbf ON dsbf.queue_table_dep_id = nrs.queue_table_dep_id
)

, final_queue_sql AS
(SELECT string_agg(
  /****
  This first UNION is to union together INSERT, UPDATE, and DELETE events for a single queue table
   */
  format($$
  %s
  UNION ALL
  %s
  UNION ALL
  %s
  $$,
    queue_insert_sql,
    queue_update_sql,
    queue_delete_sql)
  /****
  This second UNION as the second arg of string_agg is the union together ALL queue tables for this fact table
   */
  , E'\nUNION ALL\n') AS event_sql,
  string_agg(queue_ids_sql, E'\nUNION ALL\n') AS raw_queued_changes_sql_out
FROM all_queues_sql)

, final_outputs AS (
SELECT raw_queued_changes_sql_out,
$$
WITH all_changes AS (
($$||event_sql||$$)
ORDER BY changed_at)

, base_execution_groups AS
(SELECT fact_table_id,
    queue_table_dep_id,
    queue_table_id_field,
    operation,
    changed_at,
    source_change_date,
    insert_merge_proid,
    update_merge_proid,
    delete_merge_proid,
    maximum_cutoff_time,
    key,
    CASE WHEN operation = 'I' THEN insert_merge_proid
    WHEN operation = 'U' THEN update_merge_proid
    WHEN operation = 'D' THEN delete_merge_proid
        END AS proid,
  RANK() OVER (
    PARTITION BY
      CASE
        WHEN operation = 'I' THEN insert_merge_proid
        WHEN operation = 'U' THEN update_merge_proid
        WHEN operation = 'D' THEN delete_merge_proid
      END
  ) AS execution_group
  FROM all_changes
  WHERE key IS NOT NULL)

SELECT fact_table_id, proid, key, source_change_date
FROM base_execution_groups beg
WHERE proid IS NOT NULL
GROUP BY execution_group, fact_table_id, proid, key, source_change_date
/****
This ordering is particularly important for date-range history tables
where order of inserts is critical and usually expected to follow a pattern
***/
ORDER BY execution_group, MIN(changed_at), MIN(queue_table_id_field);
$$ AS gathered_queued_changes_sql_out
  ,

$$
DROP TABLE IF EXISTS process_queue;
CREATE TEMP TABLE process_queue
(process_queue_id serial,
 fact_table_id int,
 proid regproc,
 key_value text,
 source_change_date date);

INSERT INTO process_queue
(fact_table_id, proid, key_value, source_change_date)
$$ AS process_queue_snippet,

$$
WITH all_ids AS
($$||raw_queued_changes_sql_out||$$)

, new_metadata AS
(SELECT MAX(fact_loader_batch_id) AS last_cutoff_id,
  maximum_cutoff_time,
  queue_table_dep_id
FROM all_ids
--Exclude dependent fact tables from updates directly to queue_table_deps
WHERE fact_table_dep_id IS NULL
GROUP BY queue_table_dep_id, maximum_cutoff_time)

/****
The dependent fact table uses the same queue_table_id_field as last_cutoff
We are going to update fact_table_deps metadata instead of queue_table_deps
****/
, new_metadata_fact_dep AS
(SELECT MAX(fact_loader_batch_id) AS last_cutoff_id,
  maximum_cutoff_time,
  fact_table_dep_queue_table_dep_id
FROM all_ids
--Include dependent fact tables only
WHERE fact_table_dep_id IS NOT NULL
GROUP BY fact_table_dep_queue_table_dep_id, maximum_cutoff_time)

, update_key AS (
SELECT qdwr.queue_table_dep_id,
  --Cutoff the id to that newly found, otherwise default to last value
  COALESCE(mu.last_cutoff_id, qdwr.last_cutoff_id) AS last_cutoff_id,
  --This cutoff time must always be the same for all queue tables for given fact table.
  --Even if there are no new records, we move this forward to wherever the stream is at
  qdwr.maximum_cutoff_time AS last_cutoff_source_time
FROM fact_loader.queue_deps_all qdwr
LEFT JOIN new_metadata mu ON mu.queue_table_dep_id = qdwr.queue_table_dep_id
WHERE qdwr.fact_table_id = $$||p_fact_table_id||$$
--Exclude dependent fact tables from updates directly to queue_table_deps
  AND qdwr.fact_table_dep_id IS NULL
)

/****
This SQL also nearly matches that for the queue_table_deps but would be a little ugly to try to DRY up
****/
, update_key_fact_dep AS (
SELECT qdwr.fact_table_dep_queue_table_dep_id,
  qdwr.fact_table_id,
  COALESCE(mu.last_cutoff_id, qdwr.last_cutoff_id) AS last_cutoff_id,
  qdwr.maximum_cutoff_time AS last_cutoff_source_time
FROM fact_loader.queue_deps_all qdwr
LEFT JOIN new_metadata_fact_dep mu ON mu.fact_table_dep_queue_table_dep_id = qdwr.fact_table_dep_queue_table_dep_id
WHERE qdwr.fact_table_id = $$||p_fact_table_id||$$
--Include dependent fact tables only
  AND qdwr.fact_table_dep_id IS NOT NULL
)

, updated_queue_table_deps AS (
UPDATE fact_loader.queue_table_deps qtd
SET last_cutoff_id = uk.last_cutoff_id,
  last_cutoff_source_time = uk.last_cutoff_source_time
FROM update_key uk
WHERE qtd.queue_table_dep_id = uk.queue_table_dep_id
RETURNING qtd.*)

, updated_fact_table_deps AS (
UPDATE fact_loader.fact_table_dep_queue_table_deps ftd
SET last_cutoff_id = uk.last_cutoff_id,
  last_cutoff_source_time = uk.last_cutoff_source_time
FROM update_key_fact_dep uk
WHERE ftd.fact_table_dep_queue_table_dep_id = uk.fact_table_dep_queue_table_dep_id
RETURNING uk.*)

UPDATE fact_loader.fact_tables ft
SET last_refresh_source_cutoff = uqtd.last_cutoff_source_time,
  last_refresh_attempted_at = now(),
  last_refresh_succeeded = TRUE
FROM
(SELECT fact_table_id, last_cutoff_source_time
FROM updated_queue_table_deps
--Must use UNION to get only distinct values
UNION
SELECT fact_table_id, last_cutoff_source_time
FROM updated_fact_table_deps) uqtd
WHERE uqtd.fact_table_id = ft.fact_table_id;
$$ AS metadata_update_sql_out
FROM final_queue_sql)

SELECT raw_queued_changes_sql_out,
 gathered_queued_changes_sql_out
  ,
 format($$
  %s
  %s$$, process_queue_snippet, gathered_queued_changes_sql_out) AS process_queue_sql_out,
 metadata_update_sql_out
FROM final_outputs;

$BODY$
LANGUAGE SQL;


