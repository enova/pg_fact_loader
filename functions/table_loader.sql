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