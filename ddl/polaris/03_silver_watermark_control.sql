-- Watermark control table on POLARIS (dbt_polaris/macros/watermark.sql). One row per named process.
-- A SINGLE table shared by BOTH watermark styles, mirroring legacy silver.watermark_control:
--   * version-based   -> last_commit_version (append-only sources; e.g. staging->raw)
--   * timestamp-based -> start_timestamp / end_timestamp (MERGE/delete sources; Piece 4 sync-back)
--
-- Written by Trino/dbt (the watermark macros UPDATE/INSERT here via run_query), NOT PyIceberg — so it
-- is format_version = 3 per the v3-everywhere rule. No VARIANT columns.
--
-- CONCURRENCY (unchanged from Nessie): PARTITION BY watermark_name so each process's row is its own
-- partition/data file (avoids ICEBERG_COMMIT_ERROR "Found conflicting files" when Piece 1 / Job A /
-- both Job B sub-pipelines commit concurrently). max_commit_retry = 20 as belt-and-suspenders.
-- NOTE: verify concurrent row-level UPDATEs behave the same on v3 as v2 (v3 delete-file format) — it
-- passed the catalog feature tests; confirm at pipeline scale (runbook §8).
CREATE TABLE IF NOT EXISTS polaris.silver.watermark_control (
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
    format = 'PARQUET',
    format_version = 3,
    partitioning = ARRAY['watermark_name'],
    max_commit_retry = 20
);

-- Step 2 (ingestion) seed: the version watermark for staging->raw. NULL last_commit_version => the
-- first run does the one-time full read, then advances the watermark. (Piece 3/4/5 watermark seeds are
-- added in their own step DDLs, not here.)
INSERT INTO polaris.silver.watermark_control
    (watermark_name, start_timestamp, end_timestamp, last_commit_version,
     current_commit_version, transaction_status, created_timestamp, updated_timestamp)
VALUES ('BIS_CTV_US_INGESTION_STG_TO_RAW_OCC', NULL, NULL, NULL, NULL,
        'SUCCEEDED', current_timestamp, current_timestamp);
