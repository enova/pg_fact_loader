CREATE OR REPLACE FUNCTION fact_loader.fact_table_refresh_logs_pruner() RETURNS trigger
LANGUAGE plpgsql
AS $$

declare
        step int := 1000;
        -- step should equal the firing frequency in trigger definition
        overdrive int := 2;
        -- overdrive times step = max rows (see below)

        max_rows int := step * overdrive;
        rows int;

begin
        delete from fact_loader.fact_table_refresh_logs
        where fact_table_refresh_log_id in (
                select fact_table_refresh_log_id
                from fact_loader.fact_table_refresh_logs
                where refresh_attempted_at < now() - '90 days'::interval
                -- do not do the literal interval value above as a declare parameter
                order by fact_table_refresh_log_id
                limit max_rows
                for update skip locked
        );

        get diagnostics rows = row_count;
        return null;
end
$$;

CREATE TRIGGER fact_table_refresh_logs_pruner
AFTER INSERT ON fact_loader.fact_table_refresh_logs
FOR EACH ROW WHEN ((new.fact_table_refresh_log_id % 1000::bigint) = 0)
EXECUTE PROCEDURE fact_loader.fact_table_refresh_logs_pruner();
