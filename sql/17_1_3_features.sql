SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

--Launch 2 workers and only display for test the number of workers launched
SELECT array_length(fact_loader.launch_workers(2), 1);

--Should not work because it's too soon
SELECT number_terminated,
  number_still_live,
  array_length(pids_still_live, 1) AS pids_still_live_ct
FROM fact_loader.safely_terminate_workers();

--Should work because the processes should have been idle now for at least 5 seconds
SELECT pg_sleep(6);
SELECT number_terminated,
  number_still_live,
  array_length(pids_still_live, 1) AS pids_still_live_ct
FROM fact_loader.safely_terminate_workers();


INSERT INTO test.orders (order_id, customer_id, order_date, total)
VALUES ((SELECT MAX(order_id)+1 FROM test.orders) ,5, '2018-07-27', 2500.00);

SELECT pglogical_ticker.tick();

-- For some reason queue_table_id seems indeterminate so don't show it
DO $$
BEGIN
    IF NOT (SELECT COUNT(1) FROM fact_loader.raw_queued_changes(1)) = 24
    OR NOT (SELECT COUNT(1) FROM fact_loader.gathered_queued_changes(1)) = 1 THEN
        RAISE EXCEPTION '%', 'No worky';
    END IF;
END$$;

--Count could be different if we are doing FROMVERSION=1.2 or lower but should be at least 50 (actually should be 66 for 1.2 and 76 for 1.3)
SELECT COUNT(1) > 50 AS got_enough_logs FROM fact_loader.fact_table_refresh_logs;

--Test the auto-pruning
BEGIN;
UPDATE fact_loader.fact_table_refresh_logs SET refresh_attempted_at = refresh_attempted_at - interval '1 year' WHERE messages IS NULL;
INSERT INTO fact_loader.fact_table_refresh_logs (fact_table_refresh_log_id)
VALUES (1000);
SELECT COUNT(1) FROM fact_loader.fact_table_refresh_logs;
ROLLBACK; 

--Test support for extension without deps
BEGIN;
DROP EXTENSION pg_fact_loader CASCADE;
DROP EXTENSION pglogical_ticker CASCADE;
DROP EXTENSION pglogical CASCADE;
CREATE EXTENSION pg_fact_loader;
DROP EXTENSION pg_fact_loader;
CREATE EXTENSION pglogical;
CREATE EXTENSION pglogical_ticker;
CREATE EXTENSION pg_fact_loader VERSION '1.2';
ALTER EXTENSION pg_fact_loader UPDATE;
DROP EXTENSION pglogical_ticker;
DROP EXTENSION pglogical CASCADE;
ROLLBACK;
