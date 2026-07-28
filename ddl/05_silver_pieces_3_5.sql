-- Generated from the Databricks silver.py table DDL (read-only source of truth).
-- Spark->Trino Iceberg: STRING->VARCHAR, INT->INTEGER, FLOAT->REAL, VARIANT->VARCHAR,
-- TIMESTAMP->TIMESTAMP(6) WITH TIME ZONE (UTC, matches Databricks); GENERATED IDENTITY and
-- DEFAULT CURRENT_TIMESTAMP dropped (occurrence_id/creative_id come from Postgres sequences /
-- pipeline logic); CLUSTER BY -> partitioning(capture_month)+sorted_by. Delta TBLPROPERTIES
-- and COMMENTs omitted. Pre-created (structure-first); the Piece 3-5 dbt models write into them.

CREATE TABLE IF NOT EXISTS iceberg.silver.creative_dedupe_map (
    creative_dedupe_map_id                 BIGINT,
    country_iso_2_code                     VARCHAR,
    child_creative_id                      BIGINT,
    child_creative_url_hash                BIGINT,
    child_provider_code                    VARCHAR,
    child_creative_type                    VARCHAR,
    child_creative_subtype                 VARCHAR,
    parent_creative_id                     BIGINT,
    parent_creative_url_hash               BIGINT,
    parent_creative_provider_code          VARCHAR,
    parent_creative_type                   VARCHAR,
    parent_creative_subtype                VARCHAR,
    match_type                             VARCHAR,
    revision_type                          VARCHAR,
    is_auto_mapped                         BOOLEAN,
    video_score                            REAL,
    audio_score                            REAL,
    json_response                          VARCHAR,
    created_by_user_id                     INTEGER,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    updated_by_user_id                     INTEGER,
    updated_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['child_creative_id']
);

CREATE TABLE IF NOT EXISTS iceberg.silver.digital_staging_occurrence (
    provider_occurrence_id                 VARCHAR,
    country_iso_2_code                     VARCHAR,
    provider_code                          VARCHAR,
    source_channel                         VARCHAR,
    provider_creative_id                   BIGINT,
    provider_creative_url_hash             BIGINT,
    capture_date                           DATE,
    capture_month                          INTEGER,
    capture_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    media_property_id                      INTEGER,
    purchase_method_id                     SMALLINT,
    daisy_chain                            VARCHAR,
    provider_campaign_id                   BIGINT,
    provider_campaign_name                 VARCHAR,
    provider_campaign_product_id           BIGINT,
    provider_campaign_product_name         VARCHAR,
    provider_campaign_advertiser_id        BIGINT,
    provider_campaign_advertiser_name      VARCHAR,
    provider_campaign_landing_page         VARCHAR,
    provider_campaign_landing_page_domain  VARCHAR,
    ad_insertion_point                     VARCHAR,
    job_log_key                            VARCHAR,
    provider_raw_json                      VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['capture_month'],
    sorted_by = ARRAY['provider_occurrence_id']
);

CREATE TABLE IF NOT EXISTS iceberg.silver.component_coding_translation_hold (
    component_coding_id                    BIGINT
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['component_coding_id']
);

CREATE TABLE IF NOT EXISTS iceberg.silver.creative_mapping_translation_hold (
    creative_id                            BIGINT
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['creative_id']
);

CREATE TABLE IF NOT EXISTS iceberg.silver.creative_product_translation_resync_log (
    creative_id                            BIGINT,
    legacy_creative_id                     BIGINT,
    primary_product_id                     INTEGER,
    vx1_product_id                         INTEGER,
    vx2_product_id                         INTEGER,
    mr_company_id                          INTEGER,
    secondary_products                     VARCHAR,
    vx1_secondary_products                 VARCHAR,
    vx2_secondary_products                 VARCHAR,
    mr_secondary_company_ids               VARCHAR,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['creative_id']
);

CREATE TABLE IF NOT EXISTS iceberg.silver.gold_creative_change_log (
    creative_id                            BIGINT,
    creative_url_hash                      BIGINT,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    psql_updated_timestamp                 TIMESTAMP(6) WITH TIME ZONE,
    json_log                               VARCHAR
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['creative_id', 'created_timestamp']
);

