-- orchestration/ddl/dq_check_results.sql
-- Run once to create the DQ results table in your wiki_videogame_ingestion catalog

CREATE TABLE IF NOT EXISTS wiki_videogame_ingestion.bronze.dq_check_results (
    check_run_id      STRING      NOT NULL,  -- UUID per pipeline run, so you can group all checks from one run together
    layer              STRING      NOT NULL,  -- e.g. 'bronze' — lets you reuse this same table for silver/gold checks later
    table_name         STRING      NOT NULL,  -- fully qualified table being checked
    check_name         STRING      NOT NULL,  -- e.g. 'duplicate_check', 'coverage_check', 'schema_check'
    check_status        STRING      NOT NULL,  -- 'PASS' | 'FAIL' | 'WARN' — WARN is for things like your partial-coverage case, not a hard failure
    expected_value     STRING,                -- nullable — not every check has a single expected value
    actual_value       STRING,
    details            STRING,                -- free-text: which articles/dates were affected, etc.
    checked_at         TIMESTAMP   NOT NULL
)
USING DELTA;
