/***
This function exists mostly to easily mock out for testing purposes.
 */
CREATE FUNCTION fact_loader.logical_subscription()
RETURNS TABLE (subid OID, subpublications text[], subconninfo text, dbname text, driver fact_loader.driver)
AS $BODY$
BEGIN

IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pglogical') THEN

  RETURN QUERY EXECUTE $$
  SELECT sub_origin_if AS subid, sub_replication_sets AS subpublications, null::text AS subconninfo, null::text AS dbname, 'pglogical'::fact_loader.driver AS driver
  FROM pglogical.subscription
  UNION ALL
  SELECT oid, subpublications, subconninfo, (regexp_matches(subconninfo, 'dbname=(.*?)(?=\s|$)'))[1] AS dbname, 'native'::fact_loader.driver AS driver
  FROM fact_loader.subscription();
  $$;
ELSE
  RETURN QUERY
  SELECT oid, subpublications, subconninfo, (regexp_matches(subconninfo, 'dbname=(.*?)(?=\s|$)'))[1] AS dbname, 'native'::fact_loader.driver AS driver
  FROM fact_loader.subscription();

END IF;

END;
$BODY$
LANGUAGE plpgsql;
