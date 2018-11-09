CREATE OR REPLACE VIEW fact_loader.unresolved_failures AS
SELECT ft.fact_table_id,
  fact_table_relid,
  refresh_attempted_at,
  messages
FROM fact_loader.fact_tables ft
INNER JOIN fact_loader.fact_table_refresh_logs ftrl
  ON ft.fact_table_id = ftrl.fact_table_id
  AND ft.last_refresh_attempted_at = ftrl.refresh_attempted_at
WHERE NOT enabled
  AND NOT last_refresh_succeeded;