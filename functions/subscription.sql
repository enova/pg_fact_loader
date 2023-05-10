/***
This function exists mostly to easily mock out for testing purposes.
 */
CREATE FUNCTION fact_loader.subscription()
RETURNS TABLE (oid OID, subpublications text[], subconninfo text)
AS $BODY$
BEGIN

RETURN QUERY
SELECT s.oid, s.subpublications, s.subconninfo FROM pg_subscription s;

END;
$BODY$
LANGUAGE plpgsql;
