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