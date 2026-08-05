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
    start_timestamp         TIMESTAMP(6) WITH TIME ZONE,
    end_timestamp           TIMESTAMP(6) WITH TIME ZONE,
    last_commit_version     BIGINT,
    current_commit_version  BIGINT,
    transaction_status      VARCHAR,
    created_timestamp       TIMESTAMP(6) WITH TIME ZONE,
    updated_timestamp       TIMESTAMP(6) WITH TIME ZONE
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

-- ---------------------------------------------------------------------------------------------------
-- Piece 3 watermark seeds. Run ONCE before the first Job A / Job B run (skip a row if it already
-- exists). NULL last_commit_version => first run does the one-time full read, then advances the
-- watermark. All three are VERSION-based (Job B was consolidated onto version watermarks — the
-- timestamp macros stay for later pieces), reading bronze.digital_raw_occurrence independently.
--   Job A            = DIGITAL_RAW_OCC_TO_CRTV_STAGING             (creative staging + first-seen seed)
--   Job B first-seen = DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE   (first-seen earliest-occurrence update)
--   Job B summary    = DIGITAL_RAW_OCC_SUMMARY_PSQL               (occurrence summary + park/release buffer)
-- ---------------------------------------------------------------------------------------------------
INSERT INTO iceberg.silver.watermark_control
    (watermark_name, start_timestamp, end_timestamp, last_commit_version,
     current_commit_version, transaction_status, created_timestamp, updated_timestamp)
VALUES ('DIGITAL_RAW_OCC_TO_CRTV_STAGING', NULL, NULL, NULL, NULL,
        'SUCCEEDED', current_timestamp, current_timestamp);

INSERT INTO iceberg.silver.watermark_control
    (watermark_name, start_timestamp, end_timestamp, last_commit_version,
     current_commit_version, transaction_status, created_timestamp, updated_timestamp)
VALUES ('DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE', NULL, NULL, NULL, NULL,
        'SUCCEEDED', current_timestamp, current_timestamp);

INSERT INTO iceberg.silver.watermark_control
    (watermark_name, start_timestamp, end_timestamp, last_commit_version,
     current_commit_version, transaction_status, created_timestamp, updated_timestamp)
VALUES ('DIGITAL_RAW_OCC_SUMMARY_PSQL', NULL, NULL, NULL, NULL,
        'SUCCEEDED', current_timestamp, current_timestamp);
