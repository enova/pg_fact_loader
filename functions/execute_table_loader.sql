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