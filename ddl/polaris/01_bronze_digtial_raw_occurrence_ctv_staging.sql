-- Bronze CTV staging landing (append-only) on POLARIS. Written by the Python landing step
-- (ingestion_polaris/ctv_ingestion.py) via PyIceberg; pre-created here so the pipeline only appends.
-- Name mirrors the legacy Databricks table verbatim, including the historical "digtial" misspelling.
--
-- Columns mirror legacy (json_data VARIANT -> string, record_index, blob_name, source_filename,
-- created_timestamp) plus creative_url_hash: the precomputed exact Spark xxhash64(seed 42), added
-- because Trino's built-in xxhash64 (seed 0) cannot reproduce Spark's value in SQL.
-- created_timestamp is TIMESTAMP(6) WITH TIME ZONE (UTC instant), matching Databricks TIMESTAMP.
--
-- *** format_version = 2 (NOT v3) — deliberate. ***  This table is written by PyIceberg (the landing
-- step appends here), and PyIceberg 0.11.1 cannot write v3 at all (guard on v3 metadata serialization).
-- The staging landing has no VARIANT (json_data is raw text), so v2 is correct here. v3 applies to the
-- Trino/dbt-written pipeline tables (digital_raw_occurrence onward). See runbook §5 Build progress.
CREATE TABLE IF NOT EXISTS polaris.bronze.digtial_raw_occurrence_ctv_staging (
    json_data          VARCHAR,
    record_index       INTEGER,
    source_filename    VARCHAR,
    blob_name          VARCHAR,
    created_timestamp  TIMESTAMP(6) WITH TIME ZONE,
    creative_url_hash  BIGINT
)
WITH (
    format = 'PARQUET',
    format_version = 2
);
