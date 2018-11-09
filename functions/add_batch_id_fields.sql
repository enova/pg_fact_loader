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
