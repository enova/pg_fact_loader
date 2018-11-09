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
