-- Piece 4 (creative sync-back) SILVER support tables on POLARIS. Pre-created (structure-first); the
-- Piece 4 dbt models write into them via MERGE/INSERT/DELETE. Types match the source Databricks
-- silver.py table_ddl EXACTLY.
--
-- Spark->Trino/Iceberg mapping: STRING->VARCHAR, INT->INTEGER, BIGINT->BIGINT, FLOAT->REAL,
-- TIMESTAMP->TIMESTAMP(6) WITH TIME ZONE.
--
-- *** v3 + VARIANT. ***  Every table is format_version = 3. The Databricks source has these VARIANT
-- columns (Nessie stored them as VARCHAR; retired on Polaris). Writers CAST(... AS variant); the ONE
-- rule applies -- any table with a variant column OMITS sorted_by:
--   * silver.creative_dedupe_map                    : json_response
--   * silver.creative_product_translation_resync_log: secondary_products, vx1_secondary_products,
--                                                     vx2_secondary_products, mr_secondary_company_ids
--   * silver.gold_creative_change_log               : json_log
-- The two translation-hold tables carry only an id column (no variant) -> sorted_by retained.
--
-- (silver.digital_staging_occurrence is a Piece-5 table -- created with the raw->gold occurrence flow,
--  not here.)

CREATE TABLE IF NOT EXISTS polaris.silver.creative_dedupe_map (
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
    json_response                          VARIANT,   -- source VARIANT
    created_by_user_id                     INTEGER,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    updated_by_user_id                     INTEGER,
    updated_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    format_version = 3
    -- NO sorted_by: has VARIANT column json_response (was ARRAY['child_creative_id']).
);

CREATE TABLE IF NOT EXISTS polaris.silver.component_coding_translation_hold (
    component_coding_id                    BIGINT
)
WITH (
    format = 'PARQUET',
    format_version = 3,
    sorted_by = ARRAY['component_coding_id']   -- OK: no variant column
);

CREATE TABLE IF NOT EXISTS polaris.silver.creative_mapping_translation_hold (
    creative_id                            BIGINT
)
WITH (
    format = 'PARQUET',
    format_version = 3,
    sorted_by = ARRAY['creative_id']   -- OK: no variant column
);

CREATE TABLE IF NOT EXISTS polaris.silver.creative_product_translation_resync_log (
    creative_id                            BIGINT,
    legacy_creative_id                     BIGINT,
    primary_product_id                     INTEGER,
    vx1_product_id                         INTEGER,
    vx2_product_id                         INTEGER,
    mr_company_id                          INTEGER,
    secondary_products                     VARIANT,   -- source VARIANT
    vx1_secondary_products                 VARIANT,   -- source VARIANT
    vx2_secondary_products                 VARIANT,   -- source VARIANT
    mr_secondary_company_ids               VARIANT,   -- source VARIANT
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    format_version = 3
    -- NO sorted_by: has VARIANT columns (was ARRAY['creative_id']).
);

CREATE TABLE IF NOT EXISTS polaris.silver.gold_creative_change_log (
    creative_id                            BIGINT,
    creative_url_hash                      BIGINT,
    created_timestamp                      TIMESTAMP(6) WITH TIME ZONE,
    psql_updated_timestamp                 TIMESTAMP(6) WITH TIME ZONE,
    json_log                               VARIANT    -- source VARIANT
)
WITH (
    format = 'PARQUET',
    format_version = 3
    -- NO sorted_by: has VARIANT column json_log (was ARRAY['creative_id','created_timestamp']).
);
