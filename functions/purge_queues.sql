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

