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