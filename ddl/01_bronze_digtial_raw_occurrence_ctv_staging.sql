-- Bronze CTV staging landing (append-only). Written by the Python landing step
-- (ingestion/ctv_ingestion.py); pre-created here so the pipeline only appends.
-- Name mirrors the legacy Databricks table verbatim, including the historical "digtial" misspelling.
--
-- Columns mirror legacy (json_data VARIANT -> string, record_index, blob_name, source_filename,
-- created_timestamp) plus creative_url_hash: the precomputed exact Spark xxhash64(seed 42), added
-- because Trino's built-in xxhash64 (seed 0) cannot reproduce Spark's value in SQL.
-- created_timestamp is TIMESTAMP(6) WITH TIME ZONE (UTC instant), matching Databricks TIMESTAMP
-- semantics; the Arrow schema the landing appends must match these types exactly (tz-aware UTC).
CREATE TABLE IF NOT EXISTS iceberg.bronze.digtial_raw_occurrence_ctv_staging (
    json_data          VARCHAR,
    record_index       INTEGER,
    source_filename    VARCHAR,
    blob_name          VARCHAR,
    created_timestamp  TIMESTAMP(6) WITH TIME ZONE,
    creative_url_hash  BIGINT
)
WITH (
    format = 'PARQUET'
);
