# pg_fact_loader
Build fact tables with Postgres using replicated tables and a queue

[Overview](#overview)
- [High Level Description](#high_level)
- [Features](#features)
- [A Full Example](#full_example)
- [Installation](#installation)

[Setup and Deployment](#setup)
- [Configuration](#config)
- [Function Performance Considerations](#performance)
- [Deployment](#deployment)
- [Backfills](#backfills)

[Administration](#admin)
- [Manually Executing Jobs](#manual)
- [Troubleshooting Errors and Issues](#troubleshoot)

[Technical Documentation](#tech)
- [Workflow](#workflow)

# <a name="overview"></a>Overview

## <a name="high_level"></a>High Level Description
This extension is for building fact tables using queues that contain all
write events (inserts, updates, and deletes) as the driver.

By default, we assume that fact tables are built in a logical replica, not an OLTP master,
which is why we have logic within the codebase that checks for replication stream delay (but it is
possible to run this whole system locally without any deps on logical replication).

There are several essential steps to having a working setup where fact tables will be automatically
built for you as new data comes in that is queued for processing:

1. Replicate all source tables that will be used in the definition of how a fact table is built
2. Ensure audit star is installed on the OLTP system for all replicated source tables.  This change
  log will be the basis for building the fact tables. (see below for *later* adding these to replication) 
3. Create a fact table structure where the data will go
4. Create a Postgres function that takes a single key id field (like customer_id) as an argument,
  and returns 1 row of the fact table as a result.
5. Figure out which source tables are used in your fact table function.
6. Add the audit star table *structures* that you need to your reporting system.
  Only create these columns based on the OLTP structure:
  `$audit_id_field, changed_at, operation, change, primary_key, before_change`
7. Build your configuration that tells `pg_fact_loader` both when and how to use the audit tables
  to update your fact tables.  Leave the configuration disabled (this is the default).
8. Add the audit star tables to replication to start populating audit changes on the reporting system.
9. As soon as possible, backfill the entire fact table by running your Postgres function across every
  row of the table i.e. where `customers_fact_merge(int)` is your fact function which populates
  `customers_fact`, and the `customers` table contains the full set of customers, and hence will
  populate fact table data for every customer: `SELECT customers_fact_merge(customer_id) FROM customers;`.

10. Enable the configuration for your fact table.
11. Schedule the fact_loader.worker() function to run to start continuously processing changes


## <a name="full_example"></a>A Full Example

```sql
--Optional - if using this system on a logical replica
CREATE EXTENSION pglogical;
CREATE EXTENSION pglogical_ticker;

--Required
CREATE EXTENSION pg_fact_loader;
```

For now, please refer to the test suite in the `./sql` folder contain abundant examples of
how this configuration can be setup.

## <a name="installation"></a>Installation

The functionality of this requires postgres version 9.5+ and a working install
of pglogical and pglogical_ticker (or it can be used locally only without pglogical).

DEB available on official PGDG repository as postgresql-${PGSQL_VERSION}-pg-fact-loader see
installation instruction on https://wiki.postgresql.org/wiki/Apt

Or to build from source:
```
make
make install
make installcheck # run regression suite
```

Assuming you have pglogical and pglogical_ticker, then the extension can be
deployed as any postgres extension:
```sql
CREATE EXTENSION pg_fact_loader;
```

# <a name="setup"></a>Setup and Deployment

## <a name="config"></a>Configuration

### General Configuration Workflow
The general workflow of seeding the configuration to drive the refresh of a fact table is as follows:
1. Seed the fact table itself in `fact_tables`
2. Seed any queue tables which have not already been seeded in `queue_tables`
3. Run the function `fact_loader.add_batch_id_fields()` to add `fact_loader_batch_id` to each queue table.
**NOTE** that this function sets DEFAULT intentionally upon addition.  Users are assumed to only use this function
in a scenario when we are not adding a queue table which is massive.  In any case this will take a long lock
with lots of data.
4. Tie together fact tables and queue tables in `queue_table_deps`, along with functions to execute on each DML event
5. Explain how to get the key field values to pass into functions, which may include joining to other tables, in `key_retrieval_sequences`

There are two additional steps if your fact table depends on *other* fact tables:
1. Tie together parent-child fact table dependencies in `fact_table_deps`, along with "default" functions to execute on each DML event
2. Specify any unique function execution requirements in `fact_table_dep_queue_table_deps`   

**You can see a full example of all cases of configs in the regression suite, which lies in the ** `./sql` directory,
specifically the `schema` and `seeds` tests.

Once things are configured, the following queries can help to get a bird's eye view:

Less Detail:
```sql
SELECT fact_table_id,
  fact_table_relid,
  queue_table_relid,
  queue_of_base_table_relid,
  relevant_change_columns,
  insert_merge_proid,
  update_merge_proid,
  delete_merge_proid,
  level,
  return_columns,
  join_to_relation,
  join_to_column,
  return_columns_from_join,
  join_return_is_fact_key
FROM fact_loader.queue_deps_all_with_retrieval
WHERE fact_table_relid = 'test_fact.customers_fact'::REGCLASS
ORDER BY queue_table_relid::TEXT, queue_table_dep_id, level;
```

More Detail:
```sql
SELECT *
FROM fact_loader.queue_deps_all_with_retrieval
WHERE fact_table_relid = 'test_fact.customers_fact'::REGCLASS
ORDER BY queue_table_relid::TEXT, queue_table_dep_id, level;
```

**Note that each run of a job is logged in fact_loader_refresh_logs, which are pruned after 90 days.

**Note that if using temp tables, it is recommended that you use ON COMMIT DROP option, even though the worker
itself drops TEMP schema after each run.

### Configuring a Daily Scheduled Job
Although it is assumed most tables are driven by queues, which is encouraged, we provide the ability to run a
daily scheduled script instead.

This is much, much simpler to configure, but that is because you lose many of the enormous performance benefits
of a queue-based table.

You simply must configure `fact_tables` ONLY, including the provided fields for `daily_schedule`:
  - `use_daily_schedule` - must be marked `true`
  - `daily_scheduled_time` - the time of day *after which* to run the job (the system will attempt to run until midnight)
  - `daily_scheduled_tz` - the timezone your time is in.  This is critical to know when to allow a daily refresh from the
    standpoint of the business logic you require for a timezone-based date.
  - `daily_scheduled_proid` - the function to execute.  Currently it takes no arguments.  It is assumed to contain all the
    logic necessary to add any new daily entries.  See the unit tests in `sql/16_1_2_features.sql` for an example.

We support a simple set of chained jobs here as well.  That is, the first job is scheduled, and another job
can kick off after the first one finishes, and so on (chains of dependencies are supported).  The fields relevant are:
  - `use_daily_schedule` - must be marked `true` for dependent jobs
  - `depends_on_base_daily_job_id` - **first** job in chain which is actually the only one with a scheduled time
  - `depends_on_parent_daily_job_id` - Immediate parent which must complete before this job will run

Note that if a scheduled job fails and you re-enable it, it will try to run it again if it is still within the 
proper time range and has not yet succeeded the same day. 

### Detailed Configuration Explanations (Generated from table/column comments)
There are a number of config tables that drive pg_fact_loader loads:

`fact_tables`: Each fact table to be built via pg_fact_loader, which also drives the worker.  These are also referred to as "jobs".
  - `fact_table_id`: Unique identifier for the fact table or job - also referred to as job_id
  - `fact_table_relid`: The oid of the fact table itself regclass type to accept only valid relations.
  - `fact_table_agg_proid`: NOT REQUIRED.  The aggregate function definition for the fact table.
  This can be used when passed to create_table_loader_function to auto-create a merge function.
  It can also be a reference for dq checks because it indicates what function returns
  the correct results for a fact table as it should appear now.
  - `enabled`: Indicates whether or not the job is enabled.  The worker will skip this table unless marked TRUE.
  - `priority`: Determines the order in which the job runs (in combination with other sorting factors)
  - `force_worker_priority`: If marked TRUE, this fact table will be prioritized in execution order above all other factors.
  - `last_refresh_source_cutoff`: The data cutoff time of the last refresh - only records older than this have been updated.
  - `last_refresh_attempted_at`: The last time the worker ran on this fact table.  The oldest will be prioritized first, ahead of priority.
  - `last_refresh_succeeded`: Whether or not the last run of the job succeeded.  NULL if it has never been run.
  - `row_created_at`: Timestamp of when this row was first created.
  - `row_updated_at`: Timestamp of when this row was last updated (this is updated via trigger).
  - `use_daily_schedule`: If TRUE, this job is scheduled to run daily instead of using queue tables according to other daily column configuration.  Also must be marked TRUE for dependent jobs.
  - `daily_scheduled_time`: The time of day *after which* to run the job (the system will attempt to run until midnight). If you have a chain of daily scheduled jobs, only the base job has time filled in.
  - `daily_scheduled_tz`: The timezone your time is in.  This is critical to know when to allow a daily refresh from the standpoint of the business logic you require for a timezone-based date.
  - `daily_scheduled_proid`: The single function oid to execute at the scheduled time.  No arguments supported. It is assumed to contain all the
  logic necessary to add any new daily entries, if applicable.  See the unit tests in sql/16_1_2_features.sql for examples.
  - `depends_on_base_daily_job_id`: For jobs that depend on other daily scheduled jobs only. This is the fact_table_id of the FIRST job in a chain which is actually the only one with a scheduled_time.
  - `depends_on_parent_daily_job_id`: For jobs that depend on other daily scheduled jobs only. Immediate parent which must complete before this job will run.
  - `daily_scheduled_deps`: OPTIONAL for daily scheduled jobs.  The only purpose of this column is to consider if we should wait to run a scheduled job because dependent tables are out of date.  This is a regclass array of tables that this scheduled job depends on, which will only be considered if they are either listed in fact_loader.queue_tables or fact_loader.fact_tables.  If the former, replication delay will be considered (if table is not local).  If the latter, last_refresh_source_cutoff will be considered.  Works in combination with daily_scheduled_dep_delay_tolerance which says how much time delay is tolerated.  Job will FAIL if the time delay constraint is not met for all tables - this is intended to be configured as a rare occurrence and thus we want to raise an alarm about it.
  - `daily_scheduled_dep_delay_tolerance`: OPTIONAL for daily scheduled jobs.  Amount of time interval allowed that dependent tables can be out of date before running this job.  For example, if 10 minutes, then if ANY of the dependent tables are more than 10 minutes out of date, this job will FAIL if the time delay constraint is not met for all tables - this is intended to be configured as a rare occurrence and thus we want to raise an alarm about it.
  - `pre_execute_hook_sql`: OPTIONAL - custom sql to execute within the `load.sql` function, after the `process_queue` has been loaded, but prior to the actual load of the fact table using the `process_queue`.  This feature was originally written due to the need to index the process_queue in certain unique circumstances, prior to actual execution over the `process_queue`.

`queue_tables`: Each queue table along with the base table to which it belongs.
  - `queue_table_id`: Unique identifier for queue tables.
  - `queue_table_relid`: The oid of the queue table itself regclass type to accept only valid relations.
  - `queue_of_base_table_relid`: The oid of the base table for which the queue table contains an audited log of changes.  regclass type to accept only valid relations.
  - `pglogical_node_if_id`: Optional - If NULL, we assume this is a local queue table and we need not synchronize time
  for potential replication delay.  For use with tables that are replicated via pglogical.
  This is the pglogical.node_interface of the table.  This also requires pglogical_ticker
  and is used to synchronize time and ensure we don't continue to move forward
  in time when replication is delayed for this queue table.
  - `queue_table_tz`: **NOTE CAREFULLY** - If this is NULL, it assumes that changed_at in the queue
  tables is stored in TIMESTAMPTZ.  If it IS set, it assumes you are telling it that changed_at is
  of TIMESTAMP data type which is stored in the provided time zone of queue_table_tz.
  - `row_created_at`: Timestamp of when this row was first created.
  - `row_updated_at`: Timestamp of when this row was last updated (this is updated via trigger).
  - `purge`: Default is true because we prune queue tables as data is no longer needed. Can be set to false and no pruning will happen on this table.

`queue_table_deps`: Ties together which fact tables depend on which queue tables, along with holding
information on the last cutoff ids for each queue table.  **NOTE** that anything that exists in
queue_table_dep is assumed to be require its queue data not to be pruned even if the fact_tables
job is disabled.  That means that even if a job is disabled, you will not lose data, but you will also
have your queue tables building up in size until you either enable (successfully) or drop the job.
The regression suite in ./sql and ./expected has abundant examples of different configurations.
  - `queue_table_dep_id`: Unique identifier.
  - `fact_table_id`: Fact table to tie together with a queue table it depends on.
  - `queue_table_id`: Queue table to tie together with a fact table that needs its changes.
  - `relevant_change_columns`: Optional. For UPDATE changes to data, you can specify to only consider changes
  to these columns as sufficient to update the fact table.
  If NULL, all columns will be considered as potentially changing the fact table data.
  - `last_cutoff_id`: The last fact_loader_batch_id of the queue table that was processed for this queue table - fact table pair.
  After this job runs, records that have this id and lower are eligible to be pruned,
  assuming no other fact tables also depend on those same records.
  The next time the job runs, only records after this id are considered.
  - `last_cutoff_source_time`: The source data change time of the last queue table record that was processed for this
  queue table - fact table pair.  This helps pg_fact_loader synchronize time across
  multiple queue tables and only pull changes that are early enough, and not purge
  records that are later than these cutoff times.  THIS DOES NOT DETERMINE filter conditions
  for the starting point at which to pull new records as does last_cutoff_id - it is only
  used as an ending-point barrier.

  - `insert_merge_proid`: Function oid to execute on insert events - accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - fact table pair
  is configured in key_retrieval_sequences. NULL to ignore insert events.
  - `update_merge_proid`: Function oid to execute on update events - accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - fact table pair
  is configured in key_retrieval_sequences. NULL to ignore update events.
  - `delete_merge_proid`: Function oid to execute on delete events - accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - fact table pair
  is configured in key_retrieval_sequences. NULL to ignore delete events.
  - `row_created_at`: Timestamp of when this row was first created.
  - `row_updated_at`: Timestamp of when this row was last updated (this is updated via trigger).

`key_retrieval_sequences`: How to go from a change in the queue table itself to retrieve the key
that needs to be updated in the fact table.  That key specifically will be passed
to the insert/update/delete merge_proids configured in queue_table_deps.  When multiple joins
are required to get there, you will have more than one key_retrieval_sequence for a
single queue_table_dep.  You can also optionally have a different key_retrieval_sequence
if your insert/update/delete merge_proids don't all accept the exact same field as an arg.
NOTE - The regression suite in ./sql and ./expected has abundant examples of different configurations.
  - `key_retrieval_sequence_id`: Unique identifier.
  - `queue_table_dep_id`: Which fact table - queue table record this is for (queue_table_deps)
  - `filter_scope`: NULL or one of I, U, D.  Optional and likely rare.  By default, this key_retrieval_sequence
  will tell pg_fact_loader how to get the key for all events - insert, update, delete.
  But if your insert/update/delete merge_proids don't all accept the exact same field as an arg,
  you will have to tell it a different way to retrieve the different I, U, D events on separate rows.
  The regression suite has examples of this.
  - `level`: Default 1. When there are multiple joins required to retrieve a key,
  this indicates the order in which to perform the joins.  It will start at level 1,
  then the return_columns_from_join field will be used to join to the join_to_relation - join_to_column
  for the level 2 record, and so on.
  - `return_columns`: What field to return from the base table (if this is level 1), or (if this level 2+)
  this should be the same as the return_columns_from_join from the previous level.
  - `is_fact_key`: Only true if the base table itself contains the key. If return_columns contains the keys to pass into the functions without any additional join, TRUE.  Otherwise, FALSE if you need to join to get more information.
  - `join_to_relation`: Join from the base table (or if this is level 2+, the join_to_relation from the previous level) to this table to get the key or to do yet a further join.
  - `join_to_column`: Join to this column of join_to_relation.
  - `return_columns_from_join`: Return these columns from join_to_relation.
  - `join_return_is_fact_key`: If return_columns_from_join are your fact keys, true.  Otherwise false, and that means you need another level to get your key.
  - `pass_queue_table_change_date_at_tz`: If this is set to a time zone, then the changed_at field will be cast to this time zone and then cast to a date,
  for the purpose of creating a date-range based fact table.
  For casting queue_table_timestamp to a date, we first ensure we have it as timestamptz (objective UTC time).
  Then, we cast it to the timezone of interest on which the date should be based.
  For example, 02:00:00 UTC time on 2018-05-02 is actually 2018-05-01 in America/Chicago time.
  Thus, any date-based fact table must decide in what time zone to consider the date.

`fact_table_deps`: For queue-based fact tables that depend on other fact table changes ONLY. Add those dependencies here.
  - `fact_table_dep_id`: Unique identifier.
  - `parent_id`: The parent fact_table_id that the child depends on.
  - `child_id`: The child fact_table_id that will run only after the parent is updated.
  - `default_insert_merge_proid`: Default function to use for insert events to update child tables.
  This may need to be modified for each individual inherited fact_table_dep_queue_table_deps
  if that generalization isn't possible. See the regression suite in ./sql and ./expected for examples.
  - `default_update_merge_proid`: Default function to use for update events to update child tables.
  This may need to be modified for each individual inherited fact_table_dep_queue_table_deps
  if that generalization isn't possible. See the regression suite in ./sql and ./expected for examples.
  - `default_delete_merge_proid`: Default function to use for delete events to update child tables.
  This may need to be modified for each individual inherited fact_table_dep_queue_table_deps
  if that generalization isn't possible. See the regression suite in ./sql and ./expected for examples.
  - `row_created_at`: Timestamp of when this row was first created.
  - `row_updated_at`: Timestamp of when this row was last updated (this is updated via trigger).

`fact_table_dep_queue_table_deps`: Data in this table is by default auto-generated by refresh_fact_table_dep_queue_table_deps() only for queue-based fact tables that depend on other fact table changes.
Each row represents a parent's queue_table_dep, updates of which will trickle down to this dependent fact table.  Even though the default proids
from fact_table_deps are used initially, they may not be appropriate as generalized across all of these queue_table_deps.
The proids may need to be overridden for individual fact_table_dep_queue_table_deps if that generalization isn't possible.
See the regression suite in ./sql and ./expected for examples of this.
  - `fact_table_dep_queue_table_dep_id`: Unique identifier
  - `fact_table_dep_id`: fact_table_dep for this specific dependency.
  - `queue_table_dep_id`: Inherited queue_table_dep that this dependent fact table depends on.
  - `last_cutoff_id`: This is unique and maintained separately from last_cutoff_id in queue_table_deps,
  as it refers to the last_cutoff_id for this dependent fact table.  It is the last fact_loader_batch_id of
  the queue table that was processed for this queue table - dependent fact table pair.
  After this job runs, records that have this id and lower are eligible to be pruned,
  assuming no other fact tables also depend on those same records.
  The next time the job runs, only records after this id are considered.
  - `last_cutoff_source_time`: This is unique and maintained separately from last_cutoff_source_time in queue_table_deps,
  as it refers to the last_cutoff_source_time for this dependent fact table.  It is the source data
  change time of the last queue table record that was processed for this queue table - dependent fact table pair.
  This helps pg_fact_loader synchronize time across multiple queue tables and only pull changes
  that are early enough, and not purge records that are later than these cutoff times.  It will also
  never go past its parent(s) in time.  THIS DOES NOT DETERMINE filter conditions
  for the starting point at which to pull new records as does last_cutoff_id - it is only
  used as an ending-point barrier.
  - `insert_merge_proid`: Initially populated by default_insert_merge_proid from fact_table_deps, but can be
  overridden if a different proid is required. This is the function oid to execute on
  INSERT events *for this dependent fact table* - it accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - dependent fact table pair
  is configured in key_retrieval_sequences *for the parent(s)*. NULL to ignore insert events.
  See the regression suite in ./sql and ./expected for examples of this.
  - `update_merge_proid`: Initially populated by default_update_merge_proid from fact_table_deps, but can be
  overridden if a different proid is required. This is the function oid to execute on
  UPDATE events *for this dependent fact table* - it accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - dependent fact table pair
  is configured in key_retrieval_sequences *for the parent(s)*. NULL to ignore insert events.
  See the regression suite in ./sql and ./expected for examples of this.
  - `delete_merge_proid`: Initially populated by default_delete_merge_proid from fact_table_deps, but can be
  overridden if a different proid is required. This is the function oid to execute on
  DELETE events *for this dependent fact table* - it accepts a single value as its arg
  which is typically the key that has changed and needs to be updated.
  The way to retrieve this key for this queue table - dependent fact table pair
  is configured in key_retrieval_sequences *for the parent(s)*. NULL to ignore insert events.
  See the regression suite in ./sql and ./expected for examples of this.
  - `row_created_at`: Timestamp of when this row was first created.
  - `row_updated_at`: Timestamp of when this row was last updated (this is updated via trigger).

`process_queue`: Populated from gathering all unique queued changes for each fact table, then used to update fact tables.
Redundant changes are already aggregated out before insert to this table.
This table will never have data at the end of a transaction so can be unlogged.
  - `process_queue_id`: Unique identifier for each row to be executed.  The order of this field is essential as it
  is used to execute the proids over the keys in the exact order of changes from queue tables.
  - `fact_table_id`: Identifies which fact table these records belong to.  Strictly speaking this may not even
  be required because no other transaction is ever visible to the pg_fact_loader session loading this
  table, so you could only ever see your own records.  But it is sensible to keep this unique separation
  of data in the same table.
  - `proid`: The function proid that will be executed with key_value passed
  as the argument, and (if applicable), source_change_date as the second
  argument.  See fact_loader.key_retrieval_sequences.pass_queue_table_change_date_at_tz
  for more information on source_change_date.
  - `key_value`: The key that changed which will be passed to the function proid in
  order to update the fact table.  This is text data type because this
  key could be essentially any data type.  Casting is handled in the logic
  of execute_queue() which executes from this table.
  - `row_created_at`: Timestamp of when this row was first created.
  - `row_updated_at`: Timestamp of when this row was last updated (this is updated via trigger).
  - `source_change_date`: Only used with key_retrieval_sequences.pass_queue_table_change_date_at_tz.
  Will be populated by the changed_at timestamp of a queue table cast to date
  according to that configuration if it exists.

`debug_process_queue`: A mirror of process_queue for debugging only (unlogged) - only populated with log_min_duration set to DEBUG.

`queue_deps_all`: A view which gathers all fact table data in order to process queued changes and update it, including nested dependencies.

`queue_deps_all_with_retrieval`: The master view which builds on queue_deps_all to include key_retrieval_sequences.  This is the main view used by sql_builder(int) to gather all queued changes.

`fact_table_refresh_logs`: Used to log both job run times and exceptions.
  - `fact_table_refresh_log_id`: Unique identifier,
  - `fact_table_id`: Fact table that created the log.
  - `refresh_attempted_at`: The time of the attempt (transaction begin time), which can be correlated to fact_table.last_refresh_attempted_at (see also unresolved_failures).
  - `messages`: Only for failures - Error message content in JSON format - including message, message detail, context, and hint.
  - `refresh_finished_at`: The transaction commit time of the attempt, which can be used with refresh_attempted_at to get actual run time.

`unresolved_failures`: Will only show fact table and error messages for a job that just failed and has not been re-enabled since last failure.  Useful for monitoring.

**NOTE** - to generate this markdown from the database, use:
```sql
SELECT
    CASE WHEN d.objsubid = 0
    THEN format(E'\n`%s`: %s', c.relname, description)
    ELSE format('  - `%s`: %s', a.attname, d.description)
    END AS markdown
FROM pg_description d
INNER JOIN pg_class c ON c.oid = d.objoid AND d.classoid = (SELECT oid FROM pg_class WHERE relname = 'pg_class')
INNER JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid
WHERE n.nspname = 'fact_loader'
ORDER BY
CASE
    WHEN c.relname = 'fact_tables' THEN 1
    WHEN c.relname = 'queue_tables' THEN 2
    WHEN c.relname = 'queue_table_deps' THEN 3
    WHEN c.relname = 'key_retrieval_sequences' THEN 4
    WHEN c.relname = 'fact_table_deps' THEN 5
    WHEN c.relname = 'fact_table_dep_queue_table_deps' THEN 6
    WHEN c.relname = 'process_queue' THEN 7
    WHEN c.relname = 'debug_process_queue' THEN 8
    WHEN c.relname = 'queue_deps_all' THEN 9
    WHEN c.relname = 'queue_deps_all_with_retrieval' THEN 10
    WHEN c.relname = 'fact_table_refresh_logs' THEN 11
    WHEN c.relname = 'unresolved_failures' THEN 12
END, d.objsubid;
```

## <a name="performance"></a>Function Performance Considerations
You will notice the configuration APIs expect you to provide functions for `INSERT`, `UPDATE`, and `DELETE`
events which will execute on a PER-ID basis.  There are several reasons for this and if you consider performacne
up front, you will have a very optimized fact_loader system.
- Having a single id-based function allows for query plan caching if the functions are written in `plpgsql`.
  That means even a complex function with 20 joins may take 5 seconds to plan, but execute in 5ms.  After several
  executions, the plan will be cached and you will effectively have a consistent 5ms execution time for every
  execution of your function.
- It allows for simplicity of design and optimization.  It is very easy to see holes in your query plan and missing
  indexes in a query that is executing an aggregation on a single id.
  
In general, you should try to target a **sub-10ms execution time** for your functions.  Such a demand may be
relaxed for keys that are much more infrequently updated, or made more stringent for extremely high frequency
of changes.

## <a name="enable"></a>Backfilling and Enabling a Fact Table Job

Once you enable your fact table to be maintained by pg_fact_loader, all changes moving forward
will be maintained by the system according to your configuration and the new queue data coming in.
However, you will most likely need to initially populate the fact table as a starting point.

Here is the typical process then to enable a job, once your configuration is in place:
1. Ensure the fact_loader job is disabled (this is the default)
2. Truncate the fact table
3. Backfill in batches by running your configured `merge` function over the entire set of data. For example:
  `SELECT customers_fact_merge(customer_id) FROM customers;`
4. Enable the fact_loader job.
5. Run worker function in whatever scheduled way desired (i.e. crontab).

If you need to at any point in the future do another backfill on the table, this is the same set of step
to follow.  **However**, it will be better in production to not `TRUNCATE` the fact table, but rather to use
small batches to refresh the whole table while still allowing concurrent access.  This will also avoid overloading
any replication stream going out of your system. 

To **enable** a fact_table in the `fact_tables` for it to be considered by the worker for refresh,
simply run an update, i.e.
```sql
UPDATE fact_loader.fact_tables SET enabled = TRUE WHERE fact_table_relid = 'test_fact.customers_fact';
```

Concurrency is handled by locking fact_tables rows for update, which can be
seen in the wrapping `worker()` function.  Adding more workers means you will have smaller deltas, and
more up to date fact tables.  For example you can schedule 5 calls to `worker()` to kick off from cron every minute.


# <a name="admin"></a>Administration

## <a name="manual"></a>Manually Executing Jobs
If for some reason you need to manually execute a job in a concurrency-safe way that is integrated
into `pg_fact_loader`, you can run this function:
```sql
SELECT fact_loader.try_load(fact_table_id);
```

The function will return `TRUE` if it ran successfully.  It will return `FALSE` either if the job
errored out (see below [Troubleshooting](#troubleshoot)), or if the job is already being run and
has a lock on it.


## <a name="troubleshoot"></a>Troubleshooting Errors and Issues
If a job fails, it will be automatically disabled, and you can view the errors by running:
```sql
SELECT * FROM fact_loader.unresolved_failures 
```

The server logs may also have more details.

By default, only `DEBUG` level messages are printed to the server logs with the SQL generated by `sql_builder`.  This
can be very useful for debugging should a question arise.

You can peek at queued changes for any fact table by querying `SELECT * FROM gathered_queued_changes(fact_table_id);`.
This can also be used for data quality checks - you can verify that all records for a fact table
match expected output of your function definitions by comparing while excluding any gathered_queued_changes with this
function which should not be compared. 

Furthermore, be aware that enabling `DEBUG` level logging will add process queue records to the unlogged table
`debug_process_queue` to allow peeking at changes that are incoming.  You can also do something similar to this
by viewing the `gathered_queued_changes` function. 

For even further debugging, try running the sql generated by the `fact_loader.sql_builder(fact_table_id)`
function by hand, passing it the id of the failed job.  You can attempt to run this manually in a transaction.
If that still does not give you enough information, you can attempt to run `fact_loader.execute_queue(fact_table_id)`,
again still in a transaction that you may roll back.

Once you have fixed whatever issues a job may have, you will need to re-enable it to get it running again.

# <a name="tech"></a>Technical Documentation

## <a name="new_releases"></a>New Releases
There are some helper scripts to assist in adding a new version of pg_fact_loader, mainly `pg_fact_loader-sql-maker.sh`.

1. To add a new version, open this file, change the `last_version` and `new_version` to the correct new values.
2. Remove everything after `create_update_file_with_header` in the script.  The next few lines are custom files that were
changed with a particular release, which are added to the new version's SQL script.  Whatever functions or views you modify,
or if you have a schema change in the `schema/` directory, you will want to add these files using the provided function,
i.e. `add_file views/prioritized_jobs.sql $update_file` will add the SQL for views/prioritized_jobs.sql to the new extension
script.  You only need to add files that you modify with a release.
3. When all is prepared, run the script.  It should create new files for you for the new extension version, including an update
script from the previous version to the new version.
4. Update the Makefile to include these new SQL files.
5. Update the first script in both `sql/` and `expected/` directories, which refer to the most recent version as a
default.  Update it to the new version.
6. Update the pg_fact_loader.control file with the latest version.

To test your extension for all postgres versions, including testing extension upgrade paths, see and run the script `test_all_versions.sh`.

## <a name="workflow"></a>Workflow

The function `fact_loader.worker()` drives everything in the fact table loads.

It selects the next fact table based on several conditions, puts a lock on it,
then goes to refresh the fact table.

Here is the basic workflow and explanation of involved functions:

**High Level:**
`fact_loader.worker()` (chooses which fact table to work on, locks it and proceeds with load)
For this a single fact_table_id, the following is the workflow:
  - `fact_loader.load(fact_table_id)`
      - `fact_loader.sql_builder(p_fact_table_id)` returns `insert_to_process_queue_sql` and `metadata_update_sql`
      - The SQL to load the process_queue (`insert_to_process_queue_sql`) is executed.  If it is NULL, `SELECT 'No queue data' AS result` is executed instead.
      - `fact_loader.execute_queue(p_fact_table_id)` builds SQL `v_execute_sql` which executes the load across the process_queue in the correct order, again
      based on `(insert|update\delete)_merge_proid` in configs
      - Execute `metadata_update_sql` to update `last_cutoff_id` and `last_cutoff_source_time` for all relevant queues
  - `fact_loader.purge_queues();` - purge any queue data no longer needed across all configs, whether disabled or enabled 
      
**Detail Level:**
`fact_loader.worker()` (chooses which fact table to work on, locks it and proceeds with load)
For this a single fact_table_id, the following is the workflow:
  - `fact_loader.load(fact_table_id)`
      - `fact_loader.sql_builder(p_fact_table_id)` returns `insert_to_process_queue_sql` and `metadata_update_sql`
          - Retrieve all configuration information from `fact_loader.queue_deps_all_with_retrieval` for `I`, `U`, and `D` events
          - Recursively build the SQL to join from all queue tables in configuration to base tables (The recursive portion ONLY applies to cases requiring more than one `level` of joins in `key_retrieval_sequences`)
          - The `DELETE` case is unique in that joins have to factor in the likely possibility that the data no longer exists
          to join to, and thus one may have to join instead to the audit tables.
          - We `UNION ALL` together every event
          - We also `UNION ALL` together every query generated for every queue table of a given fact table
      - The SQL to load the process_queue (`insert_to_process_queue_sql`) is executed.  If it is NULL, `SELECT 'No queue data' AS result` is executed instead.
- The SQL to load the process_queue (`insert_to_process_queue_sql`) is executed.  If it is NULL, `SELECT 'No queue data' AS result` is executed instead.
      - `fact_loader.execute_queue(p_fact_table_id)` builds SQL `v_execute_sql` which executes the load across the process_queue in the correct order, again
      based on `(insert|update\delete)_merge_proid` in configs
      - Execute `metadata_update_sql` to update `last_cutoff_id` and `last_cutoff_source_time` for all relevant queues
  - `fact_loader.purge_queues();` - purge any queue data no longer needed across all configs, whether disabled or enabled
