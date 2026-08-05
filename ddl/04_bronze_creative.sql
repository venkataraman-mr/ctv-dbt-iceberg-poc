-- Generated from the Databricks bronze.py table DDL (read-only source of truth).
-- Spark->Trino Iceberg: STRING->VARCHAR, INT->INTEGER, FLOAT->REAL, VARIANT->VARCHAR,
-- TIMESTAMP->TIMESTAMP(6) WITH TIME ZONE (UTC, matches Databricks); GENERATED IDENTITY and
-- DEFAULT CURRENT_TIMESTAMP dropped (occurrence_id/creative_id come from Postgres sequences /
-- pipeline logic); CLUSTER BY -> partitioning(capture_month)+sorted_by. Delta TBLPROPERTIES
-- and COMMENTs omitted. Pre-created (structure-first); the Piece 3-5 dbt models write into them.

CREATE TABLE IF NOT EXISTS iceberg.bronze.creative_unique_urls (
    creative_id                            BIGINT,
    provider_creative_id                   BIGINT,
    provider_creative_url                  VARCHAR,
    creative_url_hash                      BIGINT,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    is_staged                              BOOLEAN,
    first_seen_media_id                    INTEGER,
    first_seen_provider_id                 INTEGER
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['creative_url_hash']
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.creative_autochaff (
    creative_id                            BIGINT,
    legacy_creative_id                     BIGINT,
    country_iso_2_code                     VARCHAR,
    provider_code                          VARCHAR,
    source_channel                         VARCHAR,
    provider_creative_id                   BIGINT,
    capture_month                          INTEGER,
    capture_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    creative_type                          VARCHAR,
    mime_type_id                           INTEGER,
    media_id                               INTEGER,
    media_property_id                      INTEGER,
    publisher_domain                       VARCHAR,
    creative_width                         INTEGER,
    creative_height                        INTEGER,
    creative_duration                      INTEGER,
    creative_url                           VARCHAR,
    creative_url_hash                      BIGINT,
    creative_machine_learning_payload      VARCHAR,
    creative_url_override                  VARCHAR,
    creative_payload                       VARCHAR,
    record_status                          VARCHAR,
    first_seen_metadata                    VARCHAR,
    suggested_vx0_product_id               BIGINT,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    updated_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['creative_url_hash']
);

-- Piece 3 Job B (occurrence summary) park/release buffer. capture_timestamp added for the summary
-- flow (Databricks MRVXVC-11059 "use capture timestamp column" for first_run/last_run); it must exist
-- for the UNION with the CDF read and the first_run/last_run population. On an already-created table,
-- run:  ALTER TABLE iceberg.bronze.missing_digital_occurrence_for_summary
--         ADD COLUMN capture_timestamp TIMESTAMP(6) WITH TIME ZONE;
CREATE TABLE IF NOT EXISTS iceberg.bronze.missing_digital_occurrence_for_summary (
    provider_occurrence_id                 VARCHAR,
    creative_url_hash                      BIGINT,
    provider_code                          VARCHAR,
    country_iso_2_code                     VARCHAR,
    source_channel                         VARCHAR,
    provider_dma_city_name                 VARCHAR,
    publisher_id                           BIGINT,
    capture_date                           DATE,
    capture_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET'
);

