-- Gold digital occurrence table on POLARIS (Piece 5 target). Pre-created (structure-first) so the Piece-4
-- sync-back models that READ it (crtv_occid_update, crtv_lastseen_update -- both Piece-5-gated) resolve and
-- no-op until Step 6 populates it. The Step-6 raw->gold occurrence flow writes into it. Types match the
-- source Databricks gold.py table_ddl EXACTLY.
--
-- Spark->Trino/Iceberg mapping: STRING->VARCHAR, INT->INTEGER, BIGINT->BIGINT, DOUBLE->DOUBLE, DATE->DATE,
-- TIMESTAMP->TIMESTAMP(6) WITH TIME ZONE, BOOLEAN->BOOLEAN. GENERATED IDENTITY / DEFAULT CURRENT_TIMESTAMP
-- dropped (occurrence_id comes from a Postgres sequence / pipeline logic).
--
-- *** v3 + VARIANT (the hard requirement). ***  format_version = 3. The Databricks source has ONE VARIANT
-- column here -- provider_raw_json -- kept as real `variant` (Nessie stored it VARCHAR). dbt writes it via
-- CAST(... AS variant); consumers read it with CAST(col AS json) / col['key'] (never json_parse/json_query).
--
-- *** NO sorted_by (the ONE rule). ***  This table has a VARIANT column, so it CANNOT use sorted_by (Trino
-- sort-on-write rejects `variant`). The legacy CLUSTER BY (occurrence_id, capture_month) becomes
-- partitioning by capture_month only (perf-only clustering loss; partition pruning intact).
--
-- *** SMALLINT -> INTEGER. ***  market_id, purchase_method_id, origin_channel_id (Iceberg has no 16-bit int;
-- the Polaris/Iceberg REST connector rejects smallint).

CREATE TABLE IF NOT EXISTS polaris.gold.digital_gold_occurrence (
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
    market_id                              INTEGER,   -- source SMALLINT -> INTEGER
    purchase_method_id                     INTEGER,   -- source SMALLINT -> INTEGER
    deployment_chain_id                    BIGINT,
    mediator_chain                         VARCHAR,
    origin_channel_id                      INTEGER,   -- source SMALLINT -> INTEGER
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
    provider_raw_json                      VARIANT,   -- source VARIANT
    ad_insertion_point                     VARCHAR,
    job_log_key                            VARCHAR
)
WITH (
    format = 'PARQUET',
    format_version = 3,
    partitioning = ARRAY['capture_month']
    -- NO sorted_by: has VARIANT column provider_raw_json (legacy CLUSTER BY was occurrence_id, capture_month).
);
