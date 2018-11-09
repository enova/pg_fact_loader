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