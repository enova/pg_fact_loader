CREATE OR REPLACE FUNCTION fact_loader.launch_workers(number_to_launch int)
RETURNS INT[] AS
$BODY$
DECLARE
  v_pids INT[];
BEGIN

FOR i IN 1..number_to_launch
LOOP
  v_pids = array_append(v_pids, fact_loader.launch_worker());
/*
It's not strictly required to not launch all workers simultaneously, but it's
also a little more invasive to do that, probably requiring more advisory lock skips.
Better to just sleep 1 second between launches.
 */
PERFORM pg_sleep(1);
END LOOP;

RETURN v_pids;

END;
$BODY$
LANGUAGE plpgsql;
