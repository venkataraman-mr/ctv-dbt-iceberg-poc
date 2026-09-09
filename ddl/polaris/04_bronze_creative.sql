-- Persistent bronze creative tables on POLARIS (Piece 3). Pre-created (structure-first); the Piece 3
-- dbt models write into them. Types match the source Databricks bronze.py table_ddl.
--
-- v3 everywhere (format_version = 3). NONE of these has a VARIANT column, so `sorted_by` is retained
-- (the sorted_by+variant incompatibility only bites tables that have a variant column — §ddl/polaris/02).
--   - creative_unique_urls: url->creative_id registry + is_staged flag. No VARIANT. sorted_by kept.
--   - creative_autochaff: EMPTY for CTV (autochaff scoring runs only for BIS digital display). It is NOT
--     in the current source table_ddl; its payload columns are kept VARCHAR (as in ddl/nessie/04). If a
--     future media populates it and those are VARIANT in source, switch them to variant AND drop sorted_by.
--   - missing_digital_occurrence_for_summary: Job B occ-summary park/release buffer (incl. capture_timestamp).

CREATE TABLE IF NOT EXISTS polaris.bronze.creative_unique_urls (
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
    format_version = 3,
    sorted_by = ARRAY['creative_url_hash']   -- OK: no variant column on this table
);

CREATE TABLE IF NOT EXISTS polaris.bronze.creative_autochaff (
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
    creative_machine_learning_payload      VARCHAR,   -- see header note (empty for CTV; not in source table_ddl)
    creative_url_override                  VARCHAR,
    creative_payload                       VARCHAR,    -- see header note
    record_status                          VARCHAR,
    first_seen_metadata                    VARCHAR,    -- see header note
    suggested_vx0_product_id               BIGINT,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    updated_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    format_version = 3,
    sorted_by = ARRAY['creative_url_hash']   -- OK: no variant column (payloads kept VARCHAR; empty for CTV)
);

CREATE TABLE IF NOT EXISTS polaris.bronze.missing_digital_occurrence_for_summary (
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
    format = 'PARQUET',
    format_version = 3
);
