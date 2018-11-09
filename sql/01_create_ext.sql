-- Allow running regression suite with upgrade paths
\set v `echo ${FROMVERSION:-1.5}`
SET client_min_messages TO warning;
CREATE EXTENSION pglogical;
CREATE EXTENSION pglogical_ticker;
CREATE EXTENSION pg_fact_loader VERSION :'v';
