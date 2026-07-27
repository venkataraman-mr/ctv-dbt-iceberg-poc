-- Watermark control table (dbt/macros/watermark.sql). One row per named process. A SINGLE table
-- shared by BOTH watermark styles, mirroring the legacy Databricks silver.watermark_control:
--   * version-based  -> uses last_commit_version (append-only sources; e.g. staging->raw)
--   * timestamp-based -> uses start_timestamp / end_timestamp (MERGE/delete-written sources;
--                        e.g. the creative sync-back in Piece 4)
-- Legacy columns (watermark_name, start_timestamp, end_timestamp, last_commit_version,
-- transaction_status, created_timestamp, updated_timestamp) match common/common_functions.py.
--
-- current_commit_version is an ADDITION to the legacy schema (kept for the later pieces): the
-- version macros pin the source's end snapshot here + mark 'InProgress' at begin, then promote it
-- into last_commit_version + 'SUCCEEDED' at finish. This two-phase commit gives a crash-safe,
-- exact-end watermark (no "source advanced mid-run" re-read window).
--
-- USED by Piece 1: digital_raw_occurrence reads new staging inserts via system.table_changes driven
-- by the version watermark 'BIS_CTV_US_INGESTION_STG_TO_RAW_OCC'. Seed that row (last_commit_version
-- = NULL) before the first run so the initial full load runs and then advances the watermark.
CREATE TABLE IF NOT EXISTS iceberg.silver.watermark_control (
    watermark_name          VARCHAR,
    start_timestamp         TIMESTAMP(6),
    end_timestamp           TIMESTAMP(6),
    last_commit_version     BIGINT,
    current_commit_version  BIGINT,
    transaction_status      VARCHAR,
    created_timestamp       TIMESTAMP(6),
    updated_timestamp       TIMESTAMP(6)
)
WITH (
    format = 'PARQUET'
);

-- Seed one row per process before its first run (mirrors the legacy migration seed). Examples:
--   version-based (append-only source):
--     INSERT INTO iceberg.silver.watermark_control
--       (watermark_name, start_timestamp, end_timestamp, last_commit_version,
--        current_commit_version, transaction_status, created_timestamp, updated_timestamp)
--     VALUES ('BIS_CTV_US_INGESTION_STG_TO_RAW_OCC', NULL, NULL, NULL, NULL,
--             'SUCCEEDED', current_timestamp, current_timestamp);
--   timestamp-based (MERGE/delete source):
--     INSERT INTO iceberg.silver.watermark_control
--       (watermark_name, start_timestamp, end_timestamp, last_commit_version,
--        current_commit_version, transaction_status, created_timestamp, updated_timestamp)
--     VALUES ('DIGITAL_RAW_CREATIVES_TO_PSQL', current_timestamp, current_timestamp, NULL, NULL,
--             'SUCCEEDED', current_timestamp, current_timestamp);
