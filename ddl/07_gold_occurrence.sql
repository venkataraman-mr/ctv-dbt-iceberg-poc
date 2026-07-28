-- Generated from the Databricks gold.py table DDL (read-only source of truth).
-- Spark->Trino Iceberg: STRING->VARCHAR, INT->INTEGER, FLOAT->REAL, VARIANT->VARCHAR,
-- TIMESTAMP->TIMESTAMP(6) WITH TIME ZONE (UTC, matches Databricks); GENERATED IDENTITY and
-- DEFAULT CURRENT_TIMESTAMP dropped (occurrence_id/creative_id come from Postgres sequences /
-- pipeline logic); CLUSTER BY -> partitioning(capture_month)+sorted_by. Delta TBLPROPERTIES
-- and COMMENTs omitted. Pre-created (structure-first); the Piece 3-5 dbt models write into them.

CREATE TABLE IF NOT EXISTS iceberg.gold.digital_gold_occurrence (
    occurrence_id                          BIGINT,
    country_iso_2_code                     VARCHAR,
    provider_code                          VARCHAR,
    source_channel_id                      INTEGER,
    creative_id                            BIGINT,
    provider_occurrence_id                 VARCHAR,
    provider_parent_creative_url_hash      BIGINT,
    provider_original_creative_id          BIGINT,
    provider_original_creative_url_hash    BIGINT,
    capture_date                           DATE,
    capture_month                          INTEGER,
    capture_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    updated_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    media_property_id                      INTEGER,
    market_id                              SMALLINT,
    purchase_method_id                     SMALLINT,
    deployment_chain_id                    BIGINT,
    mediator_chain                         VARCHAR,
    origin_channel_id                      SMALLINT,
    prelim_impressions                     BIGINT,
    prelim_spend                           DOUBLE,
    final_impressions                      BIGINT,
    final_spend                            DOUBLE,
    delete_flag                            BOOLEAN,
    is_house_ad                            BOOLEAN,
    historical_creative_id                 BIGINT,
    provider_campaign_id                   BIGINT,
    provider_campaign_name                 VARCHAR,
    provider_campaign_product_id           BIGINT,
    provider_campaign_product_name         VARCHAR,
    provider_campaign_advertiser_id        BIGINT,
    provider_campaign_advertiser_name      VARCHAR,
    provider_campaign_landing_page         VARCHAR,
    provider_campaign_landing_page_domain  VARCHAR,
    provider_raw_json                      VARCHAR,
    ad_insertion_point                     VARCHAR,
    job_log_key                            VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['capture_month'],
    sorted_by = ARRAY['occurrence_id']
);

