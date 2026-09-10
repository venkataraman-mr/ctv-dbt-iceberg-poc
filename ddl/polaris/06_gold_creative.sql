-- Gold creative-family tables on POLARIS (Piece 4 / creative sync-back target). Pre-created
-- (structure-first); the Piece 4 dbt sync-back models (dbt_polaris/models/creatives/*) MERGE/write
-- into them. Types match the source Databricks gold.py table_ddl EXACTLY.
--
-- Spark->Trino/Iceberg mapping: STRING->VARCHAR, INT->INTEGER, BIGINT->BIGINT, FLOAT->REAL,
-- DOUBLE->DOUBLE, DATE->DATE, TIMESTAMP->TIMESTAMP(6) WITH TIME ZONE, BOOLEAN->BOOLEAN.
-- GENERATED IDENTITY and DEFAULT CURRENT_TIMESTAMP dropped (ids come from Postgres sequences /
-- pipeline logic). Delta TBLPROPERTIES and COMMENTs omitted. CLUSTER BY -> sorted_by (non-variant
-- tables only; see below).
--
-- *** v3 + VARIANT (the hard requirement). ***  Every table is format_version = 3. The Databricks
-- source has these VARIANT columns; we keep them as real `variant` (Nessie stored them as VARCHAR —
-- that workaround is retired on Polaris). dbt writes them via CAST(... AS variant); consumers read
-- with CAST(col AS json) or col['key'] (NEVER json_parse/json_query — both reject variant):
--   * gold.component_coding       : attribute_response, attribute_response_vx2
--   * gold.creative               : secondary_products, vx1_secondary_products, vx2_secondary_products,
--                                    mr_secondary_company_ids, attribution_competitor, attribution_celebrity,
--                                    custom_attributes, attribution_competitor_vx2, creative_payload,
--                                    machine_learning_payload, print_matching_ads, print_ad_images
--   * gold.digital_deployment_chain : daisy_chain
--
-- *** NO sorted_by on the three VARIANT tables (Polaris/Trino limitation). ***  Trino's sort-on-write
-- serializes every column (incl. variant) through its legacy Hive-type mapping, which rejects
-- `variant` ("Unsupported Hive type: variant"). So component_coding / creative /
-- digital_deployment_chain omit sorted_by (perf-only clustering loss; correctness unaffected).
-- The non-variant tables (creative_first_seen, mediator, role, spend_availability) keep sorted_by.
--
-- *** SMALLINT -> INTEGER. ***  Iceberg has no 16-bit int and the Polaris/Iceberg REST connector
-- rejects smallint ("Type not supported for Iceberg: smallint"). Affects component_coding
-- (component_template_id, sequence, share, page_no), digital_deployment_chain (purchase_method_id),
-- creative_first_seen (provider_id, market_id, daypart_id).

CREATE TABLE IF NOT EXISTS polaris.gold.component_coding (
    component_coding_id                    INTEGER,
    creative_id                            BIGINT,
    legacy_creative_id                     BIGINT,
    component_template_id                  INTEGER,   -- source SMALLINT -> INTEGER
    component_template_name                VARCHAR,
    sequence                               INTEGER,   -- source SMALLINT -> INTEGER
    share                                  INTEGER,   -- source SMALLINT -> INTEGER
    attribute_response                     VARIANT,   -- source VARIANT
    attribute_response_vx2                 VARIANT,   -- source VARIANT
    is_logically_deleted                   BOOLEAN,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    modified_timestamp                     TIMESTAMP(6) WITH TIME ZONE,
    creative_path                          VARCHAR,
    page_no                                INTEGER,   -- source SMALLINT -> INTEGER
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
    format_version = 3
    -- NO sorted_by: has VARIANT columns (was ARRAY['creative_id']).
);

CREATE TABLE IF NOT EXISTS polaris.gold.creative (
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
    secondary_products                     VARIANT,   -- source VARIANT
    vx1_secondary_products                 VARIANT,   -- source VARIANT
    vx2_secondary_products                 VARIANT,   -- source VARIANT
    mr_secondary_company_ids               VARIANT,   -- source VARIANT
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
    attribution_competitor                 VARIANT,   -- source VARIANT
    attribution_celebrity                  VARIANT,   -- source VARIANT
    attribution_slogan_tagline             VARCHAR,
    attribution_revision_description       VARCHAR,
    attribution_comments                   VARCHAR,
    attribution_creative_tags              VARCHAR,
    custom_attributes                      VARIANT,   -- source VARIANT
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
    first_seen_provider_campaign_landing_page VARCHAR,   -- Piece 4: written by the creative sync
    first_seen_market_id                   INTEGER,
    first_seen_market_name                 VARCHAR,
    first_seen_daypart_id                  INTEGER,
    first_seen_daypart_name                VARCHAR,
    first_seen_affiliate_id                INTEGER,
    first_seen_affiliate_name              VARCHAR,
    due_timestamp                          TIMESTAMP(6) WITH TIME ZONE,
    last_seen_timestamp                    TIMESTAMP(6) WITH TIME ZONE,
    attribution_competitor_vx2             VARIANT,   -- source VARIANT
    occurrence_description                 VARCHAR,
    historical_creative_md5                VARCHAR,
    legacy_creative_id                     BIGINT,
    creative_payload                       VARIANT,   -- source VARIANT
    machine_learning_payload               VARIANT,   -- source VARIANT
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
    print_matching_ads                     VARIANT,   -- source VARIANT
    print_ad_images                        VARIANT,   -- source VARIANT
    product_mapping_status                 VARCHAR,
    keywords                               VARCHAR,
    is_reclassified                        BOOLEAN,
    print_no_cost                          BOOLEAN
)
WITH (
    format = 'PARQUET',
    format_version = 3
    -- NO sorted_by: has VARIANT columns (was ARRAY['creative_id','creative_url_hash']).
);

CREATE TABLE IF NOT EXISTS polaris.gold.creative_first_seen (
    creative_id                            BIGINT,
    creative_url_hash                      BIGINT,
    provider_creative_id                   BIGINT,
    country_iso_2_code                     VARCHAR,
    media_id                               VARCHAR,
    occurrence_id                          VARCHAR,
    occurrence_timestamp                   TIMESTAMP(6) WITH TIME ZONE,
    occurrence_timestamp_local             TIMESTAMP(6) WITH TIME ZONE,
    provider_id                            INTEGER,   -- source SMALLINT -> INTEGER
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
    market_id                              INTEGER,   -- source SMALLINT -> INTEGER
    market_name                            VARCHAR,
    daypart_id                             INTEGER,   -- source SMALLINT -> INTEGER
    daypart_name                           VARCHAR,
    affiliate_id                           INTEGER,
    affiliate_name                         VARCHAR,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    updated_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    edition_name                           VARCHAR,
    section_name                           VARCHAR,
    edition_id                             INTEGER,
    section_id                             INTEGER,
    provider_campaign_landing_page         VARCHAR   -- Piece 4: written by the first-seen sync
)
WITH (
    format = 'PARQUET',
    format_version = 3,
    sorted_by = ARRAY['creative_id', 'creative_url_hash']   -- OK: no variant column
);

CREATE TABLE IF NOT EXISTS polaris.gold.digital_deployment_chain (
    deployment_chain_id                    BIGINT,
    daisy_chain                            VARIANT,   -- source VARIANT
    daisy_chain_transformed_1              VARCHAR,
    daisy_chain_transformed_2              VARCHAR,
    purchase_method_id                     INTEGER,   -- source SMALLINT -> INTEGER
    purchase_method_daisy_chain_2_md5_hashcode VARCHAR,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    format_version = 3
    -- NO sorted_by: has VARIANT column daisy_chain (was ARRAY['deployment_chain_id']).
);

CREATE TABLE IF NOT EXISTS polaris.gold.digital_deployment_chain_mediator (
    mediator_id                            BIGINT,
    mediator_name                          VARCHAR,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    updated_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    format_version = 3,
    sorted_by = ARRAY['mediator_id']   -- OK: no variant column
);

CREATE TABLE IF NOT EXISTS polaris.gold.digital_deployment_chain_role (
    role_id                                BIGINT,
    role_name                              VARCHAR,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    updated_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    format_version = 3,
    sorted_by = ARRAY['role_id']   -- OK: no variant column
);

CREATE TABLE IF NOT EXISTS polaris.gold.digital_spend_availability (
    submedia_id                            INTEGER,
    cost_state                             INTEGER,
    availability_date                      DATE,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    format_version = 3,
    sorted_by = ARRAY['submedia_id', 'cost_state']   -- OK: no variant column
);

-- NOTE: tables already created on the VM need the retrofit (Iceberg ADD COLUMN is metadata-only):
--   ALTER TABLE polaris.gold.creative_first_seen ADD COLUMN provider_campaign_landing_page VARCHAR;
--   ALTER TABLE polaris.gold.creative ADD COLUMN first_seen_provider_campaign_landing_page VARCHAR;
