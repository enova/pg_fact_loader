/***
This function exists mostly to easily mock out for testing purposes.
 */
CREATE FUNCTION fact_loader.subscription_rel()
RETURNS TABLE (srsubid OID, srrelid OID) 
AS $BODY$
BEGIN

RETURN QUERY
SELECT sr.srsubid, sr.srrelid FROM pg_subscription_rel sr;

END;
$BODY$
LANGUAGE plpgsql;
