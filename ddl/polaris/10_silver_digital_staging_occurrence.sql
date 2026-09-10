-- Piece 5 (gold occurrence flow) SILVER hold buffer on POLARIS. The occurrence "park/release" staging:
-- Half A (digital_occ_gold) MERGEs held occurrences in and releases resolved ones. Pre-created
-- (structure-first). Types match the source Databricks silver.py table_ddl EXACTLY.
--
-- *** v3 + VARIANT (the hard requirement). ***  format_version = 3. The Databricks source has TWO VARIANT
-- columns here -- daisy_chain and provider_raw_json -- kept as real `variant` (Nessie stored them VARCHAR).
-- dbt writes them via CAST(... AS variant); consumers read with CAST(col AS json) / col['key'].
--
-- *** NO sorted_by (the ONE rule). ***  Has variant columns -> cannot use sorted_by. Legacy CLUSTER BY
-- (provider_occurrence_id) dropped; partitioning by capture_month kept.
--
-- *** SMALLINT -> INTEGER. ***  purchase_method_id.

CREATE TABLE IF NOT EXISTS polaris.silver.digital_staging_occurrence (
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
    purchase_method_id                     INTEGER,   -- source SMALLINT -> INTEGER
    daisy_chain                            VARIANT,   -- source VARIANT
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
    provider_raw_json                      VARIANT    -- source VARIANT
)
WITH (
    format = 'PARQUET',
    format_version = 3,
    partitioning = ARRAY['capture_month']
    -- NO sorted_by: has VARIANT columns daisy_chain / provider_raw_json (legacy CLUSTER BY was provider_occurrence_id).
);
