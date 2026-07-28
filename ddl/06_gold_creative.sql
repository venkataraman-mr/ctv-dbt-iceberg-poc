-- Generated from the Databricks gold.py table DDL (read-only source of truth).
-- Spark->Trino Iceberg: STRING->VARCHAR, INT->INTEGER, FLOAT->REAL, VARIANT->VARCHAR,
-- TIMESTAMP->TIMESTAMP(6) WITH TIME ZONE (UTC, matches Databricks); GENERATED IDENTITY and
-- DEFAULT CURRENT_TIMESTAMP dropped (occurrence_id/creative_id come from Postgres sequences /
-- pipeline logic); CLUSTER BY -> partitioning(capture_month)+sorted_by. Delta TBLPROPERTIES
-- and COMMENTs omitted. Pre-created (structure-first); the Piece 3-5 dbt models write into them.

CREATE TABLE IF NOT EXISTS iceberg.gold.component_coding (
    component_coding_id                    INTEGER,
    creative_id                            BIGINT,
    legacy_creative_id                     BIGINT,
    component_template_id                  SMALLINT,
    component_template_name                VARCHAR,
    sequence                               SMALLINT,
    share                                  SMALLINT,
    attribute_response                     VARCHAR,
    attribute_response_vx2                 VARCHAR,
    is_logically_deleted                   BOOLEAN,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    modified_timestamp                     TIMESTAMP(6) WITH TIME ZONE,
    creative_path                          VARCHAR,
    page_no                                SMALLINT,
    height                                 REAL,
    width                                  REAL,
    area                                   REAL,
    x_offset                               REAL,
    y_offset                               REAL,
    status                                 VARCHAR,
    modified_by                            INTEGER,
    order_number                           INTEGER
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['creative_id']
);

CREATE TABLE IF NOT EXISTS iceberg.gold.creative (
    creative_id                            BIGINT,
    country_iso_2_code                     VARCHAR,
    provider_code                          VARCHAR,
    source_channel                         VARCHAR,
    provider_creative_id                   BIGINT,
    creative_url_hash                      BIGINT,
    creative_type                          VARCHAR,
    creative_mime_type                     VARCHAR,
    creative_width                         INTEGER,
    creative_height                        INTEGER,
    creative_duration                      INTEGER,
    creative_duration_bucket               VARCHAR,
    creative_tier_id                       INTEGER,
    primary_language_code                  VARCHAR,
    primary_product_id                     INTEGER,
    vx1_product_id                         INTEGER,
    vx2_product_id                         INTEGER,
    mr_company_id                          INTEGER,
    secondary_products                     VARCHAR,
    vx1_secondary_products                 VARCHAR,
    vx2_secondary_products                 VARCHAR,
    mr_secondary_company_ids               VARCHAR,
    classification_type                    VARCHAR,
    classified_by_user_id                  INTEGER,
    classification_comments                VARCHAR,
    classified_timestamp                   TIMESTAMP(6) WITH TIME ZONE,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    updated_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    classification_process_step            VARCHAR,
    asset_source_server_id                 INTEGER,
    creative_title                         VARCHAR,
    creative_headline                      VARCHAR,
    attribution_first_audio                VARCHAR,
    attribution_lead_text                  VARCHAR,
    attribution_visual                     VARCHAR,
    attribution_summary                    VARCHAR,
    attribution_other_details              VARCHAR,
    attribution_description                VARCHAR,
    attribution_hashtag                    VARCHAR,
    attribution_competitor                 VARCHAR,
    attribution_celebrity                  VARCHAR,
    attribution_slogan_tagline             VARCHAR,
    attribution_revision_description       VARCHAR,
    attribution_comments                   VARCHAR,
    attribution_creative_tags              VARCHAR,
    custom_attributes                      VARCHAR,
    attribution_timestamp                  TIMESTAMP(6) WITH TIME ZONE,
    attribution_by_user_id                 INTEGER,
    attribution_status                     VARCHAR,
    is_sponsored_video                     BOOLEAN,
    is_component_eligible                  BOOLEAN,
    component_entry_status                 VARCHAR,
    component_entry_by_user_id             INTEGER,
    component_entry_timestamp              TIMESTAMP(6) WITH TIME ZONE,
    additional_multi_product_flag          BOOLEAN,
    additional_coop_product_flag           BOOLEAN,
    send_to_adscope_unattributed           BOOLEAN,
    first_seen_media                       VARCHAR,
    first_seen_provider_occurrence_id      VARCHAR,
    first_seen_occurrence_id               BIGINT,
    first_seen_occurrence_timestamp        TIMESTAMP(6) WITH TIME ZONE,
    first_seen_provider_code               VARCHAR,
    first_seen_media_property_id           INTEGER,
    first_seen_media_property_name         VARCHAR,
    first_seen_media_category_id           INTEGER,
    first_seen_media_category_code         VARCHAR,
    first_seen_provider_creative_link_url  VARCHAR,
    first_seen_provider_publisher_id       BIGINT,
    first_seen_provider_publisher_domain   VARCHAR,
    first_seen_provider_campaign_id        BIGINT,
    first_seen_provider_campaign_name      VARCHAR,
    first_seen_provider_advertiser_id      BIGINT,
    first_seen_provider_advertiser_name    VARCHAR,
    first_seen_provider_product_id         BIGINT,
    first_seen_provider_product_name       VARCHAR,
    first_seen_market_id                   INTEGER,
    first_seen_market_name                 VARCHAR,
    first_seen_daypart_id                  INTEGER,
    first_seen_daypart_name                VARCHAR,
    first_seen_affiliate_id                INTEGER,
    first_seen_affiliate_name              VARCHAR,
    due_timestamp                          TIMESTAMP(6) WITH TIME ZONE,
    last_seen_timestamp                    TIMESTAMP(6) WITH TIME ZONE,
    attribution_competitor_vx2             VARCHAR,
    occurrence_description                 VARCHAR,
    historical_creative_md5                VARCHAR,
    legacy_creative_id                     BIGINT,
    creative_payload                       VARCHAR,
    machine_learning_payload               VARCHAR,
    print_los_id                           BIGINT,
    print_ad_type_id                       BIGINT,
    print_ad_nli                           REAL,
    print_ad_equ                           REAL,
    print_ad_col_inch                      REAL,
    print_ad_weighted_col_inch             REAL,
    print_ad_cost                          REAL,
    print_ad_size                          REAL,
    print_null_cost_comments               VARCHAR,
    print_recalculate_cost                 BOOLEAN,
    is_resegment                           BOOLEAN,
    print_matching_ads                     VARCHAR,
    print_ad_images                        VARCHAR,
    product_mapping_status                 VARCHAR,
    keywords                               VARCHAR,
    is_reclassified                        BOOLEAN,
    print_no_cost                          BOOLEAN
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['creative_id', 'creative_url_hash']
);

CREATE TABLE IF NOT EXISTS iceberg.gold.creative_first_seen (
    creative_id                            BIGINT,
    creative_url_hash                      BIGINT,
    provider_creative_id                   BIGINT,
    country_iso_2_code                     VARCHAR,
    media_id                               VARCHAR,
    occurrence_id                          VARCHAR,
    occurrence_timestamp                   TIMESTAMP(6) WITH TIME ZONE,
    occurrence_timestamp_local             TIMESTAMP(6) WITH TIME ZONE,
    provider_id                            SMALLINT,
    media_property_id                      INTEGER,
    media_property_name                    VARCHAR,
    media_category_id                      INTEGER,
    media_category_code                    VARCHAR,
    provider_creative_link_url             VARCHAR,
    provider_publisher_id                  BIGINT,
    provider_publisher_domain              VARCHAR,
    provider_campaign_id                   BIGINT,
    provider_campaign_name                 VARCHAR,
    provider_advertiser_id                 BIGINT,
    provider_advertiser_name               VARCHAR,
    provider_product_id                    BIGINT,
    provider_product_name                  VARCHAR,
    due_timestamp                          TIMESTAMP(6) WITH TIME ZONE,
    market_id                              SMALLINT,
    market_name                            VARCHAR,
    daypart_id                             SMALLINT,
    daypart_name                           VARCHAR,
    affiliate_id                           INTEGER,
    affiliate_name                         VARCHAR,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    updated_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    edition_name                           VARCHAR,
    section_name                           VARCHAR,
    edition_id                             INTEGER,
    section_id                             INTEGER
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['creative_id', 'creative_url_hash']
);

CREATE TABLE IF NOT EXISTS iceberg.gold.digital_deployment_chain (
    deployment_chain_id                    BIGINT,
    daisy_chain                            VARCHAR,
    daisy_chain_transformed_1              VARCHAR,
    daisy_chain_transformed_2              VARCHAR,
    purchase_method_id                     SMALLINT,
    purchase_method_daisy_chain_2_md5_hashcode VARCHAR,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['deployment_chain_id']
);

CREATE TABLE IF NOT EXISTS iceberg.gold.digital_deployment_chain_mediator (
    mediator_id                            BIGINT,
    mediator_name                          VARCHAR,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    updated_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['mediator_id']
);

CREATE TABLE IF NOT EXISTS iceberg.gold.digital_deployment_chain_role (
    role_id                                BIGINT,
    role_name                              VARCHAR,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    updated_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['role_id']
);

CREATE TABLE IF NOT EXISTS iceberg.gold.digital_spend_availability (
    submedia_id                            INTEGER,
    cost_state                             INTEGER,
    availability_date                      DATE,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['submedia_id', 'cost_state']
);

