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