SET client_min_messages TO warning;
--This is for testing functionality of timezone-specific timestamps
SET TIMEZONE TO 'America/Chicago';

SELECT COUNT(1) FROM test_audit_raw.customers_audit;

--We call this explicitly, because the worker will take the default add_interval of 1 hour, thus
--won't see any actual purging in the test suite.
SELECT fact_loader.purge_queues('0 seconds'::INTERVAL);

SELECT COUNT(1) FROM test_audit_raw.customers_audit;