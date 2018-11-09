CREATE OR REPLACE FUNCTION fact_loader.refresh_fact_table_dep_queue_table_deps()
RETURNS VOID AS
$BODY$
BEGIN
/****
This function will be used to refresh the fact_table_dep_queue_table_deps table.
The purpose of this table is to easily figure out queue data for fact tables that depend on other fact tables.
This will be run with every call of load().
This may not be the most efficient method, but it is certainly reliable and fast.
 */

/****
Recursively find all fact table deps including nested ones (fact tables that depend on other fact tables)
to build the fact_table_dep_queue_table_deps table.
 */
WITH RECURSIVE all_fact_table_deps AS (
  SELECT
    qtd.queue_table_dep_id
    , ftd.fact_table_dep_id
    , parent_id                                 AS parent_fact_table_id
    , child_id                                  AS fact_table_id
    , qtd.queue_table_id
    , qt.queue_table_relid
    , ftp.fact_table_relid AS parent_fact_table
    , ftc.fact_table_relid AS child_fact_table
    , ftd.default_insert_merge_proid
    , ftd.default_update_merge_proid
    , ftd.default_delete_merge_proid
  FROM fact_loader.queue_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt ON qtd.queue_table_id = qt.queue_table_id
  INNER JOIN fact_loader.fact_table_deps ftd ON ftd.parent_id = qtd.fact_table_id
  INNER JOIN fact_loader.fact_tables ftp USING (fact_table_id)
  INNER JOIN fact_loader.fact_tables ftc ON ftc.fact_table_id = ftd.child_id
  UNION ALL
  SELECT
    qtd.queue_table_dep_id
    , ftd.fact_table_dep_id
    , parent_id                                 AS parent_fact_table_id
    , child_id                                  AS fact_table_id
    , qtd.queue_table_id
    , qt.queue_table_relid
    , ftp.fact_table_relid AS parent_fact_table
    , ft.fact_table_relid AS child_fact_table
    , ftd.default_insert_merge_proid
    , ftd.default_update_merge_proid
    , ftd.default_delete_merge_proid
  FROM all_fact_table_deps qtd
  INNER JOIN fact_loader.queue_tables qt ON qtd.queue_table_id = qt.queue_table_id
  INNER JOIN fact_loader.fact_table_deps ftd ON ftd.parent_id = qtd.fact_table_id
  INNER JOIN fact_loader.fact_tables ftp ON ftp.fact_table_id = ftd.parent_id
  INNER JOIN fact_loader.fact_tables ft ON ft.fact_table_id = ftd.child_id
)

/****
Remove fact_table_dep_queue_table_deps that no longer exist if applicable
 */
, removed AS (
  DELETE FROM fact_loader.fact_table_dep_queue_table_deps ftdqc
  WHERE NOT EXISTS(SELECT 1
                   FROM all_fact_table_deps aftd
                   WHERE aftd.fact_table_dep_id = ftdqc.fact_table_dep_id
                    AND aftd.queue_table_dep_id = ftdqc.queue_table_dep_id)
)

/****
Add any new keys or ignore if they already exist
Add not exists because we think allowing all records to insert and conflict could be cause
of serialization errors in repeatable read isolation.
 */
INSERT INTO fact_loader.fact_table_dep_queue_table_deps
(fact_table_dep_id, queue_table_dep_id, insert_merge_proid, update_merge_proid, delete_merge_proid)
SELECT fact_table_dep_id, queue_table_dep_id, default_insert_merge_proid, default_update_merge_proid, default_delete_merge_proid
FROM all_fact_table_deps new
WHERE NOT EXISTS
    (SELECT 1
    FROM fact_loader.fact_table_dep_queue_table_deps existing
    WHERE existing.fact_table_dep_id = new.fact_table_dep_id
      AND existing.queue_table_dep_id = new.queue_table_dep_id)
ON CONFLICT (fact_table_dep_id, queue_table_dep_id)
DO NOTHING;

END;
$BODY$
LANGUAGE plpgsql;
