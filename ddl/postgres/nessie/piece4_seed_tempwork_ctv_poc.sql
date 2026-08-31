-- =====================================================================================
-- Piece 4 PREREQUISITE — seed production creative data into tempwork *_ctv_poc clones.
-- Run ONCE on prod Postgres via a SQL client (psql is not on the VM). Idempotent.
--
-- WHY: Piece 4 (the creative sync-back) runs the unchanged proc
-- creatives.sp_dbx_creative_get_changes_for_databricks against CLONE tables instead of prod.
-- That proc reads a family of creatives.* tables; this script clones the ones that carry
-- per-creative data and seeds them from production so the PoC has realistic input. Real
-- creatives.* / ml_results.* stay UNTOUCHED (we only READ them here).
--
-- SCOPE (what we clone + seed here — the "creative-keyed" tables):
--   tempwork.creative_ctv_poc                                (clone of creatives.creative)
--   tempwork.creative_product_ctv_poc                        (clone of creatives.creative_product)
--   tempwork.creative_celebrity_ctv_poc                      (clone of creatives.creative_celebrity)
--   tempwork.creative_competitor_ctv_poc                     (clone of creatives.creative_competitor)
--   tempwork.creative_dedupe_map_ctv_poc                     (clone of creatives.creative_dedupe_map)
--   tempwork.creative_classification_engine_holding_ctv_poc  (clone of creatives.creative_classification_engine_holding)
--   tempwork.creative_ai_classification_staging_vx0_ctv_poc  (clone of ml_results.creative_ai_classification_staging_vx0)
--   tempwork.component_coding_ctv_poc                        (clone of creatives.component_coding; Piece 4 task 4)
--   tempwork.watermark_control_ctv_poc                       (clone of config.watermark_control; 2 seed rows)
--   tempwork.sp_seed_load_footprint_ctv_poc()                (internal: (re)load one anchor set)
--   tempwork.sp_seed_creative_clones_ctv_poc(p_mode)         (entry point: Mode 1 + Mode 2)
--
-- ALREADY EXISTS (Piece 3; NOT recreated here):
--   tempwork.creative_staging_ctv_poc            (Piece 3 Job A; NOT seeded — Mode 1 driver + reserved-id
--                                                 authority. Parents reference prod creatives.creative_staging
--                                                 directly; we never write clone staging, so the watermark is safe.)
--   tempwork.creative_first_seen_ctv_poc         (Job B owns rows for OUR creatives; seed ADDS rows only for
--                                                 EXTERNAL parents (prod id, url_hash not in clone staging).)
--   tempwork.creative_occurrence_summary_ctv_poc (same: seed ADDS EXTERNAL-parent rows only; never touches
--                                                 rows for our creatives.)
--
-- OUT OF SCOPE (read from prod at Piece 4 runtime; NOT cloned): reference.*, config.* (except the
--   watermark table), productcentral.*, and creatives.creative_archive (parents live in creative,
--   since CTV prod starts Jan 2025 and ML looks back 6 months).
--
-- ID MODEL (hybrid). clone_creative_id for EVERY creative we load is resolved by matching its
--   creative_url_hash against creative_staging_ctv_poc (the reserved-id authority):
--   * IN staging  -> our creative: use the RESERVED PoC id (>= 26,000,000,000), unchanged. Covers
--     this-run anchors, prior-run creatives, AND a dedup parent that is itself one of our creatives.
--   * NOT in staging -> external parent: keep the EXACT prod creative_id (< 26,000,000,000).
--   dedupe_map carries clone ids on BOTH child and parent; every other table carries full prod-seeded
--   data under the resolved (reserved-or-prod) id. first_seen / occ_summary are only seeded for
--   EXTERNAL parents — rows for our creatives there are Job-B-owned and left untouched.
--
-- WRITE STRATEGY (one proc serves both new-insert and update cases):
--   * MERGE / upsert (ON CONFLICT (creative_id)): creative, holding, staging_vx0 — one row per creative.
--   * DELETE-in-scope + INSERT: product / celebrity / competitor / dedupe_map (multi-row per creative,
--     so prod removals propagate) and the EXTERNAL-parent first_seen / occ_summary rows.
--   NOTE: holding/staging_vx0 use upsert, so a prod-side REMOVAL (e.g. a QA release deleting a holding
--   row) will not retract the clone row within seeding — accepted for the PoC (see docs/pipeline/ctv_creative_seed.md).
--
-- NO indexes / partitioning / triggers / foreign keys on clones (matches the Piece 3 pattern).
-- Constraints kept: primary key, unique, check. holding gets a PRIMARY KEY on creative_id (added for the
-- upsert; prod has none). serial surrogates are demoted to plain ints and COPIED verbatim from prod (not
-- regenerated) — except creative_occurrence_summary_ctv_poc.summary_row_id (a Job-B bigserial) which
-- auto-generates so external-parent rows coexist with Job B's rows.
-- =====================================================================================

CREATE SCHEMA IF NOT EXISTS tempwork;

-- =====================================================================================
-- 1. CLONE TABLES
-- =====================================================================================

-- ---------------------------------------------------------------------------------------
-- creatives.creative  (partitioning + composite PK dropped -> plain table, PK creative_id;
-- creative_url_hash is unique per creative, so creative_id is unique. FKs/indexes/triggers dropped.)
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tempwork.creative_ctv_poc (
    creative_id                    int8         NOT NULL,
    country_iso_2_code             varchar(2)   NOT NULL,
    legacy_creative_id             int8         NULL,
    provider_id                    int2         NULL,
    source_channel_id              int2         NULL,
    provider_creative_id           int8         NULL,
    creative_url_hash              int8         NOT NULL,
    title                          varchar(200) NULL,
    headline                       varchar(255) NULL,
    creative_type_id               int2         NULL,
    creative_mime_type_id          int2         NULL,
    creative_width                 int4         NULL,
    creative_height                int4         NULL,
    creative_duration              int4         NULL,
    occurrence_description         varchar      NULL,
    creative_tier_id               int2         NULL,
    primary_language_code          varchar(5)   NULL,
    has_additional_multi_product   bool         NULL,
    has_additional_coop_product    bool         NULL,
    classification_type_id         int2         NULL,
    classified_by_user_id          int4         NULL,
    classified_timestamp           timestamp    NULL,
    classification_comments        varchar      NULL,
    classification_process_step_id int2         NULL,
    created_timestamp              timestamp    NOT NULL,
    updated_timestamp              timestamp    NOT NULL,
    attribution_first_audio        varchar(500) NULL,
    attribution_lead_text          varchar(200) NULL,
    attribution_visual             varchar(200) NULL,
    attribution_summary            varchar(300) NULL,
    attribution_other_details      varchar(200) NULL,
    attribution_description        varchar(2000) NULL,
    attribution_hashtag            varchar(200) NULL,
    attribution_slogan_tagline_id  int4         NULL,
    attribution_revision_description varchar(500) NULL,
    attribution_comments           varchar(500) NULL,
    attribution_creative_tags      varchar(500) NULL,
    custom_attributes              jsonb        NULL,
    attribution_timestamp          timestamp    NULL,
    attribution_by_user_id         int4         NULL,
    attribution_status_id          int2         NULL,
    is_component_eligible          bool         NULL,
    component_entry_status_id      int2         NULL,
    component_entry_by_user_id     int4         NULL,
    component_entry_timestamp      timestamp    NULL,
    send_to_adscope_unattributed   bool         NULL,
    creative_payload               jsonb        NULL,
    machine_learning_payload       jsonb        NULL,
    priority_status                int2         NULL,
    split_request_option           varchar(50)  NULL,
    split_request_comment          varchar(500) NULL,
    split_request_timestamp        timestamp    NULL,
    is_sponsored_video             bool         NULL,
    asset_source_server_id         int2         NULL,
    is_moved_for_class             bool         DEFAULT false NULL,
    is_moved_for_attr              bool         DEFAULT false NULL,
    is_moved_for_attr_qa           bool         DEFAULT false NULL,
    is_moved_for_class_qa          bool         DEFAULT false NULL,
    is_moved_for_component_coding  bool         DEFAULT false NULL,
    has_secondary_product          bool         NULL,
    media_id                       int2         NULL,
    is_classified                  bool         DEFAULT false NULL,
    is_attributed                  bool         DEFAULT false NULL,
    is_compcoded                   bool         DEFAULT false NULL,
    is_mlevaluated                 bool         DEFAULT false NULL,
    print_los_id                   int4         NULL,
    print_ad_type_id               int4         NULL,
    print_ad_nli                   float8       NULL,
    print_ad_equ                   float8       NULL,
    print_ad_col_inch              float8       NULL,
    print_ad_weighted_col_inch     float8       NULL,
    print_ad_cost                  float8       NULL,
    print_ad_size                  float8       NULL,
    print_null_cost_comments       varchar      NULL,
    print_recalculate_cost         bool         NULL,
    is_resegment                   bool         NULL,
    product_mapping_status         varchar      NULL,
    keywords                       varchar[]    NULL,
    is_reclassified                bool         NULL,
    print_no_cost                  bool         NULL,
    print_matching_ads             jsonb        NULL,
    print_ad_images                jsonb        NULL,
    print_ad_type_desc_id          int2         NULL,
    is_qaverified_for_class        bool         DEFAULT false NULL,
    is_qaverified_for_attr         bool         DEFAULT false NULL,
    is_trimmed_video               bool         NULL,
    ml_response_by_user            jsonb        NULL,
    occurrence_timestamp           timestamp    NULL,
    market_id                      int2         NULL,
    is_qafiltered_for_class        bool         DEFAULT false NULL,
    is_qafiltered_for_attr         bool         DEFAULT false NULL,
    reattribution_status_id        int2         NULL,
    transcription_edited           varchar(2000) NULL,
    reclassification_status_id     int2         NULL,
    CONSTRAINT creative_ctv_poc_pkey PRIMARY KEY (creative_id)
);

-- ---------------------------------------------------------------------------------------
-- creatives.creative_product  (serial4 -> plain int4, value copied; enums kept)
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tempwork.creative_product_ctv_poc (
    creative_product_id int4    NOT NULL,
    creative_id         int8    NOT NULL,
    product_id          int4    NOT NULL,
    is_primary          bool    NOT NULL,
    map_type            creatives."product_map_type_enum"     NULL,
    map_sub_type        creatives."product_map_sub_type_enum" NULL,
    is_dominant         bool    NULL,
    sort_order          int2    NOT NULL,
    provider_id         int4    DEFAULT 0 NULL,
    media_id            int4    DEFAULT 0 NULL,
    CONSTRAINT creative_product_ctv_poc_pkey PRIMARY KEY (creative_product_id),
    CONSTRAINT creative_product_ctv_poc_creative_id_product_id_key UNIQUE (creative_id, product_id)
);

-- ---------------------------------------------------------------------------------------
-- creatives.creative_celebrity  (serial4 -> plain int4, value copied)
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tempwork.creative_celebrity_ctv_poc (
    creative_celebrity_id int4      NOT NULL,
    creative_id           int8      NOT NULL,
    celebrity_id          int4      NOT NULL,
    created_by_user_id    int4      NOT NULL,
    created_timestamp     timestamp NOT NULL,
    updated_by_user_id    int4      NOT NULL,
    updated_timestamp     timestamp NOT NULL,
    CONSTRAINT creative_celebrity_ctv_poc_pkey PRIMARY KEY (creative_celebrity_id)
);

-- ---------------------------------------------------------------------------------------
-- creatives.creative_competitor  (serial4 -> plain int4, value copied)
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tempwork.creative_competitor_ctv_poc (
    creative_competitor_id int4      NOT NULL,
    creative_id            int8      NOT NULL,
    competitor_id          int4      NOT NULL,
    created_by_user_id     int4      NOT NULL,
    created_timestamp      timestamp NOT NULL,
    updated_by_user_id     int4      NOT NULL,
    updated_timestamp      timestamp NOT NULL,
    CONSTRAINT creative_competitor_ctv_poc_pkey PRIMARY KEY (creative_competitor_id)
);

-- ---------------------------------------------------------------------------------------
-- creatives.creative_dedupe_map  (serial4 -> plain int4, value copied; match_type FK dropped)
-- UNIQUE(child_creative_id) preserved -> a child has exactly one parent.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tempwork.creative_dedupe_map_ctv_poc (
    creative_dedupe_map_id        int4        NOT NULL,
    country_iso_2_code            varchar(2)  NOT NULL,
    child_creative_id             int8        NOT NULL,
    child_creative_url_hash       int8        NOT NULL,
    child_provider_code           varchar(20) NOT NULL,
    child_creative_type           varchar(20) NOT NULL,
    child_creative_subtype        varchar(20) NULL,
    parent_creative_id            int8        NOT NULL,
    parent_creative_url_hash      int8        NOT NULL,
    parent_creative_provider_code varchar(20) NOT NULL,
    parent_creative_type          varchar(20) NOT NULL,
    parent_creative_subtype       varchar(20) NULL,
    match_type_id                 int2        NOT NULL,
    revision_type                 bpchar(2)   NULL,
    is_auto_mapped                bool        NOT NULL,
    video_score                   float8      NULL,
    audio_score                   float8      NULL,
    json_response                 jsonb       NULL,
    created_by_user_id            int4        NOT NULL,
    created_timestamp             timestamp   NOT NULL,
    updated_by_user_id            int4        NOT NULL,
    updated_timestamp             timestamp   NOT NULL,
    CONSTRAINT creative_dedupe_map_ctv_poc_pkey PRIMARY KEY (creative_dedupe_map_id),
    CONSTRAINT crtv_child_crtv_url_hash_ctv_poc_ukey UNIQUE (child_creative_id)
);

-- ---------------------------------------------------------------------------------------
-- creatives.creative_classification_engine_holding  (prod has no PK; we ADD PK(creative_id)
-- so the seed can upsert via ON CONFLICT. One holding row per creative.)
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tempwork.creative_classification_engine_holding_ctv_poc (
    creative_id int8      NOT NULL,
    inserted_at timestamp NOT NULL,
    CONSTRAINT creative_classification_engine_holding_ctv_poc_pkey PRIMARY KEY (creative_id)
);

-- ---------------------------------------------------------------------------------------
-- ml_results.creative_ai_classification_staging_vx0  (PK creative_id kept)
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tempwork.creative_ai_classification_staging_vx0_ctv_poc (
    creative_id                        int8        NOT NULL,
    creative_url                       varchar     NOT NULL,
    provider_code                      varchar(20) NOT NULL,
    creative_type                      varchar(25) NOT NULL,
    creative_source_type               varchar(25) NOT NULL,
    capture_timestamp                  timestamp   NOT NULL,
    product_id                         int8        NULL,
    confidence_score                   float8      NULL,
    sent_timestamp                     timestamp   NOT NULL,
    received_timestamp                 timestamp   NULL,
    status_id                          int2        NOT NULL,
    error_description                  varchar     NULL,
    updated_timestamp                  timestamp   NOT NULL,
    ai_brand_name                      varchar     NULL,
    ai_advertiser_name                 varchar     NULL,
    ai_product_name                    varchar     NULL,
    ai_reasoning                       varchar     NULL,
    ai_rule_id                         varchar     NULL,
    ai_rule_justification              varchar     NULL,
    ai_advertisement_domain            varchar     NULL,
    ai_product_type                    varchar     NULL,
    ai_parent_n                        varchar     NULL,
    ai_subsidiary_n                    varchar     NULL,
    ai_industrygroup_class_n           varchar     NULL,
    ai_microcategory_class_n           varchar     NULL,
    ai_microcategory_class_code        varchar     NULL,
    ai_product_descriptor              varchar     NULL,
    ai_ocr                             varchar     NULL,
    ai_audio_transcript                varchar     NULL,
    ai_product_brand_display_name      varchar     NULL,
    ai_product_advertiser_display_name varchar     NULL,
    ai_product_parent_display_name     varchar     NULL,
    ai_product_subsidiary_display_name varchar     NULL,
    tax_product_name                   varchar     NULL,
    tax_brand_name                     varchar     NULL,
    tax_advertiser_name                varchar     NULL,
    tax_parent_name                    varchar     NULL,
    tax_product_type                   varchar     NULL,
    tax_product_descriptor             varchar     NULL,
    tax_subsidiary_n                   varchar     NULL,
    tax_industrygroup_class_n          varchar     NULL,
    tax_microcategory_class_n          varchar     NULL,
    tax_microcategory_class_code       varchar     NULL,
    tax_product_brand_display_name     varchar     NULL,
    tax_product_advertiser_display_name varchar    NULL,
    tax_product_parent_display_name    varchar     NULL,
    tax_product_subsidiary_display_name varchar    NULL,
    ce_response_json                   jsonb       NULL,
    CONSTRAINT classification_staging_ctv_poc_pkey PRIMARY KEY (creative_id)
);

-- ---------------------------------------------------------------------------------------
-- creatives.component_coding  (Piece 4 task 4 — component-coding sync). serial4 -> plain int4
-- (value copied); template FK + indexes dropped; attribute_response kept as prod `json`.
-- Keyed by creative_id (multi-row per creative). For CTV this is typically empty (component
-- coding is print/mattress-oriented) but cloned + seeded so the sync runs unchanged.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tempwork.component_coding_ctv_poc (
    component_coding_id   int4         NOT NULL,
    creative_id           int8         NULL,
    component_template_id int2         NOT NULL,
    "sequence"            int2         NULL,
    "share"               int2         NULL,
    attribute_response    json         NULL,
    is_logically_deleted  bool         NULL,
    created               timestamp    NULL,
    modified              timestamp    NULL,
    creative_path         varchar(255) NULL,
    page_no               int2         NULL,
    height                float4       NULL,
    width                 float4       NULL,
    area                  float4       NULL,
    x_offset              float4       NULL,
    y_offset              float4       NULL,
    status                varchar      NULL,
    modified_by           int4         NULL,
    order_number          int4         NULL,
    CONSTRAINT component_coding_ctv_poc_pkey PRIMARY KEY (component_coding_id)
);

-- ---------------------------------------------------------------------------------------
-- config.watermark_control clone + two seed rows (high-water stored in table_tx_end).
--   CTV_POC_SEED_NEW    -> Mode 1 mark on tempwork.creative_staging_ctv_poc.updated_timestamp
--   CTV_POC_SEED_UPDATE -> Mode 2 mark on creatives.creative.updated_timestamp
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tempwork.watermark_control_ctv_poc (
    target_schema  varchar   NULL,
    watermark_name varchar   NOT NULL,
    table_tx_start timestamp NULL,
    table_tx_end   timestamp NULL,
    tx_status      varchar   NULL,
    tx_message     varchar   NULL,
    tx_datetime    timestamp NULL,
    CONSTRAINT watermark_control_ctv_poc_pkey PRIMARY KEY (watermark_name)
);

INSERT INTO tempwork.watermark_control_ctv_poc
    (target_schema, watermark_name, table_tx_start, table_tx_end, tx_status, tx_message, tx_datetime)
VALUES
    ('tempwork', 'CTV_POC_SEED_NEW',    NULL, '1900-01-01 00:00:00'::timestamp, 'INIT', 'seed mode1 new inserts (clone staging)', clock_timestamp() AT TIME ZONE 'UTC'),
    ('tempwork', 'CTV_POC_SEED_UPDATE', NULL, '1900-01-01 00:00:00'::timestamp, 'INIT', 'seed mode2 creative updates (prod creative)', clock_timestamp() AT TIME ZONE 'UTC')
ON CONFLICT (watermark_name) DO NOTHING;


-- =====================================================================================
-- 2. FOOTPRINT LOADER (internal)
--    Given a session TEMP table  _seed_anchor(creative_url_hash int8, reserved_id int8)
--    -- the anchor creatives to (re)seed -- this proc loads the full clone footprint for those
--    anchors and their one-hop dedup parents, in PRODUCTION insertion order. clone_creative_id is
--    resolved against creative_staging_ctv_poc (reserved id if the url_hash is ours, else prod id).
--    MERGE/upsert for one-row-per-creative tables (creative, staging_vx0, holding); DELETE-in-scope +
--    INSERT for multi-row tables (product/celebrity/competitor/dedupe_map) and the EXTERNAL-parent
--    first_seen/occ_summary rows. Both Mode 1 and Mode 2 build _seed_anchor then CALL this.
-- =====================================================================================
CREATE OR REPLACE PROCEDURE tempwork.sp_seed_load_footprint_ctv_poc()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- -----------------------------------------------------------------------------------
    -- 2.1  Id map = this-run anchors + their one-hop dedup parents. clone_creative_id is resolved
    --      by matching creative_url_hash against clone staging (the reserved-id authority):
    --      ours -> reserved id (unchanged); external parent -> prod id. is_ours flags which.
    --      IF NOT EXISTS + TRUNCATE keeps a stable OID across calls so plpgsql cached plans hold.
    -- -----------------------------------------------------------------------------------
    CREATE TEMP TABLE IF NOT EXISTS _seed_idmap (
        prod_creative_id  int8 NOT NULL,
        clone_creative_id int8 NOT NULL,
        creative_url_hash int8 NOT NULL,
        is_ours           bool NOT NULL
    );
    TRUNCATE _seed_idmap;

    -- anchors (this run): always ours -> reserved id from clone staging. Anchors whose url_hash is
    -- not yet in creatives.creative are dropped by the inner join (Mode 2 catches them later).
    INSERT INTO _seed_idmap (prod_creative_id, clone_creative_id, creative_url_hash, is_ours)
    SELECT pc.creative_id, a.reserved_id, a.creative_url_hash, true
    FROM _seed_anchor a
    JOIN creatives.creative pc ON pc.creative_url_hash = a.creative_url_hash;

    -- one-hop parents of those anchors. A parent that is itself one of our creatives (url_hash in
    -- clone staging) takes its RESERVED id and is loaded WITHOUT changing that id; a genuinely
    -- external parent keeps its prod id. Parents already mapped (this-run anchors) and archive-only
    -- parents are excluded. Two-level graph => no recursion.
    INSERT INTO _seed_idmap (prod_creative_id, clone_creative_id, creative_url_hash, is_ours)
    SELECT DISTINCT dm.parent_creative_id,
           COALESCE(s.creative_id, dm.parent_creative_id),
           dm.parent_creative_url_hash,
           (s.creative_id IS NOT NULL)
    FROM creatives.creative_dedupe_map dm
    JOIN _seed_idmap a ON a.prod_creative_id = dm.child_creative_id
    LEFT JOIN tempwork.creative_staging_ctv_poc s ON s.creative_url_hash = dm.parent_creative_url_hash
    WHERE dm.parent_creative_id NOT IN (SELECT prod_creative_id FROM _seed_idmap)
      AND EXISTS (SELECT 1 FROM creatives.creative pc WHERE pc.creative_id = dm.parent_creative_id);

    -- ===================================================================================
    -- Load in PRODUCTION insertion order:
    --   first_seen -> dedup -> occ_summary -> staging_vx0 -> creative -> product/celebrity/competitor
    --   -> holding.  (creative_staging is NOT seeded: it is the Mode 1 driver / reserved-id authority;
    --   parents reference prod creatives.creative_staging directly.)
    -- ===================================================================================

    -- 2.2  first_seen  [EXTERNAL parents only; our creatives' rows are Job-B-owned]  (delete + insert)
    DELETE FROM tempwork.creative_first_seen_ctv_poc
      WHERE creative_id IN (SELECT clone_creative_id FROM _seed_idmap WHERE is_ours = false);
    INSERT INTO tempwork.creative_first_seen_ctv_poc (
        creative_id, creative_url_hash, provider_creative_id, country_iso_2_code, media_id,
        occurrence_id, occurrence_timestamp, occurrence_timestamp_local, provider_id, media_property_id,
        media_property_name, media_category_id, media_category_code, provider_creative_link_url,
        provider_publisher_id, provider_publisher_domain, provider_campaign_id, provider_campaign_name,
        provider_advertiser_id, provider_advertiser_name, provider_product_id, provider_product_name,
        due_timestamp, market_id, market_name, daypart_id, daypart_name, affiliate_id, affiliate_name,
        created_timestamp, updated_timestamp, edition_id, edition_name, section_id, section_name,
        provider_campaign_landing_page
    )
    SELECT
        fs.creative_id, fs.creative_url_hash, fs.provider_creative_id, fs.country_iso_2_code, fs.media_id,
        fs.occurrence_id, fs.occurrence_timestamp, fs.occurrence_timestamp_local, fs.provider_id, fs.media_property_id,
        fs.media_property_name, fs.media_category_id, fs.media_category_code, fs.provider_creative_link_url,
        fs.provider_publisher_id, fs.provider_publisher_domain, fs.provider_campaign_id, fs.provider_campaign_name,
        fs.provider_advertiser_id, fs.provider_advertiser_name, fs.provider_product_id, fs.provider_product_name,
        fs.due_timestamp, fs.market_id, fs.market_name, fs.daypart_id, fs.daypart_name, fs.affiliate_id, fs.affiliate_name,
        fs.created_timestamp, clock_timestamp() at time zone 'utc' , fs.edition_id, fs.edition_name, fs.section_id, fs.section_name,
        fs.provider_campaign_landing_page
    FROM creatives.creative_first_seen fs
    JOIN _seed_idmap m ON m.prod_creative_id = fs.creative_id AND m.is_ours = false;

    -- 2.3  dedupe_map  [child = one of our CTV clone creatives; child->reserved, parent->reserved if ours else prod]  (delete + insert)
    --   Only rows whose CHILD is one of our CTV clone creatives are loaded (child-side joins are INNER),
    --   so non-CTV children of a CTV parent are excluded. Descriptor fields:
    --     child_provider_code / child_creative_type  <- clone staging  (INNER; always present)
    --     child_creative_subtype                     <- reference.media.display_n via clone first_seen.media_id (INNER)
    --     parent provider_code / creative_type       <- clone staging if parent is ours, else prod creative_staging
    --     parent_creative_subtype                    <- reference.media.display_n via clone (ours) / prod (external) first_seen
    --     parent_creative_url_hash / parent_creative_id  <- clone if parent is ours, else prod (COALESCE)
    --   Parent descriptors COALESCE down to the prod dedupe_map denormalized value as a NOT-NULL-safe fallback.
    DELETE FROM tempwork.creative_dedupe_map_ctv_poc
      WHERE child_creative_id IN (SELECT clone_creative_id FROM _seed_idmap);
    INSERT INTO tempwork.creative_dedupe_map_ctv_poc (
        creative_dedupe_map_id, country_iso_2_code, child_creative_id, child_creative_url_hash,
        child_provider_code, child_creative_type, child_creative_subtype, parent_creative_id,
        parent_creative_url_hash, parent_creative_provider_code, parent_creative_type,
        parent_creative_subtype, match_type_id, revision_type, is_auto_mapped, video_score,
        audio_score, json_response, created_by_user_id, created_timestamp, updated_by_user_id,
        updated_timestamp
    )
    SELECT
        dm.creative_dedupe_map_id, dm.country_iso_2_code, mc.clone_creative_id, dm.child_creative_url_hash,
        css_c.provider_code,
        css_c.creative_type,
        med_c.display_n,
        COALESCE(mp.clone_creative_id, dm.parent_creative_id),
        COALESCE(css_p.creative_url_hash, dm.parent_creative_url_hash),
        COALESCE(css_p.provider_code, pps.provider_code, dm.parent_creative_provider_code),
        COALESCE(css_p.creative_type, pps.creative_type, dm.parent_creative_type),
        COALESCE(med_pc.display_n, med_pp.display_n, dm.parent_creative_subtype),
        dm.match_type_id, dm.revision_type, dm.is_auto_mapped, dm.video_score, dm.audio_score,
        dm.json_response, dm.created_by_user_id, dm.created_timestamp, dm.updated_by_user_id, clock_timestamp() at time zone 'utc'
    FROM creatives.creative_dedupe_map dm
    JOIN _seed_idmap mc ON mc.prod_creative_id = dm.child_creative_id
    LEFT JOIN _seed_idmap mp ON mp.prod_creative_id = dm.parent_creative_id
    -- child descriptors from OUR clone. INNER joins restrict dedupe_map to OUR CTV children only:
    -- a child absent from clone staging/first_seen is not our CTV data (e.g. a non-CTV sibling of a
    -- CTV parent) and is excluded, rather than kept with prod-fallback values.
    JOIN tempwork.creative_staging_ctv_poc    css_c ON css_c.creative_id = mc.clone_creative_id
    JOIN tempwork.creative_first_seen_ctv_poc  cfs_c ON cfs_c.creative_id = mc.clone_creative_id
    JOIN reference.media                        med_c ON med_c.media_id   = cfs_c.media_id
    -- parent descriptors from clone when the parent is one of ours ...
    LEFT JOIN tempwork.creative_staging_ctv_poc    css_p ON css_p.creative_id = mp.clone_creative_id
    LEFT JOIN tempwork.creative_first_seen_ctv_poc  cfs_p ON cfs_p.creative_id = mp.clone_creative_id
    LEFT JOIN reference.media                        med_pc ON med_pc.media_id = cfs_p.media_id
    -- ... or from PROD staging/first_seen when the parent is external
    LEFT JOIN creatives.creative_staging            pps  ON pps.creative_id   = dm.parent_creative_id
    LEFT JOIN creatives.creative_first_seen         ppfs ON ppfs.creative_id  = dm.parent_creative_id
    LEFT JOIN reference.media                        med_pp ON med_pp.media_id = ppfs.media_id;

    -- 2.4  occ_summary  [EXTERNAL parents only]  (delete + insert; summary_row_id auto-generates)
    DELETE FROM tempwork.creative_occurrence_summary_ctv_poc
      WHERE creative_id IN (SELECT clone_creative_id FROM _seed_idmap WHERE is_ours = false);
    INSERT INTO tempwork.creative_occurrence_summary_ctv_poc (
        creative_id, creative_url_hash, country_iso_2_code, media_id, market_id, media_property_id,
        occurrence_count, first_run, last_run, seven_days_occ_count
    )
    SELECT
        os.creative_id, os.creative_url_hash, os.country_iso_2_code, os.media_id, os.market_id, os.media_property_id,
        os.occurrence_count, os.first_run, os.last_run, os.seven_days_occ_count
    FROM creatives.creative_occurrence_summary os
    JOIN _seed_idmap m ON m.prod_creative_id = os.creative_id AND m.is_ours = false
    -- external parents only, and only those that are a related parent in the clone dedupe_map
    WHERE m.clone_creative_id IN (SELECT d.parent_creative_id FROM tempwork.creative_dedupe_map_ctv_poc d JOIN _seed_idmap ci ON ci.clone_creative_id = d.child_creative_id);

    -- 2.5  staging_vx0  [all in-scope creatives; remap creative_id]  (MERGE / upsert on creative_id)
    INSERT INTO tempwork.creative_ai_classification_staging_vx0_ctv_poc (
        creative_id, creative_url, provider_code, creative_type, creative_source_type, capture_timestamp,
        product_id, confidence_score, sent_timestamp, received_timestamp, status_id, error_description,
        updated_timestamp, ai_brand_name, ai_advertiser_name, ai_product_name, ai_reasoning, ai_rule_id,
        ai_rule_justification, ai_advertisement_domain, ai_product_type, ai_parent_n, ai_subsidiary_n,
        ai_industrygroup_class_n, ai_microcategory_class_n, ai_microcategory_class_code, ai_product_descriptor,
        ai_ocr, ai_audio_transcript, ai_product_brand_display_name, ai_product_advertiser_display_name,
        ai_product_parent_display_name, ai_product_subsidiary_display_name, tax_product_name, tax_brand_name,
        tax_advertiser_name, tax_parent_name, tax_product_type, tax_product_descriptor, tax_subsidiary_n,
        tax_industrygroup_class_n, tax_microcategory_class_n, tax_microcategory_class_code,
        tax_product_brand_display_name, tax_product_advertiser_display_name, tax_product_parent_display_name,
        tax_product_subsidiary_display_name, ce_response_json
    )
    SELECT
        m.clone_creative_id, v.creative_url,
        COALESCE(css.provider_code, v.provider_code),   -- our creatives: from clone staging; external parents: prod vx0
        COALESCE(css.creative_type, v.creative_type),   -- our creatives: from clone staging; external parents: prod vx0
        v.creative_source_type, v.capture_timestamp,    -- creative_source_type: prod vx0 (no clone equivalent)
        v.product_id, v.confidence_score, v.sent_timestamp, v.received_timestamp, v.status_id, v.error_description,
        v.updated_timestamp, v.ai_brand_name, v.ai_advertiser_name, v.ai_product_name, v.ai_reasoning, v.ai_rule_id,
        v.ai_rule_justification, v.ai_advertisement_domain, v.ai_product_type, v.ai_parent_n, v.ai_subsidiary_n,
        v.ai_industrygroup_class_n, v.ai_microcategory_class_n, v.ai_microcategory_class_code, v.ai_product_descriptor,
        v.ai_ocr, v.ai_audio_transcript, v.ai_product_brand_display_name, v.ai_product_advertiser_display_name,
        v.ai_product_parent_display_name, v.ai_product_subsidiary_display_name, v.tax_product_name, v.tax_brand_name,
        v.tax_advertiser_name, v.tax_parent_name, v.tax_product_type, v.tax_product_descriptor, v.tax_subsidiary_n,
        v.tax_industrygroup_class_n, v.tax_microcategory_class_n, v.tax_microcategory_class_code,
        v.tax_product_brand_display_name, v.tax_product_advertiser_display_name, v.tax_product_parent_display_name,
        v.tax_product_subsidiary_display_name, v.ce_response_json
    FROM ml_results.creative_ai_classification_staging_vx0 v
    JOIN _seed_idmap m ON m.prod_creative_id = v.creative_id
    LEFT JOIN tempwork.creative_staging_ctv_poc css ON css.creative_id = m.clone_creative_id  -- our creatives only
    -- Restrict to OUR CTV creatives + their related parents only. A creative qualifies if it is ours
    -- (is_ours = in clone staging) OR it is recorded as a parent in the clone dedupe_map. This excludes
    -- non-CTV creatives that are merely children of our CTV parents. (dedupe_map is loaded earlier, at 2.3.)
    WHERE m.is_ours
       OR m.clone_creative_id IN (SELECT d.parent_creative_id FROM tempwork.creative_dedupe_map_ctv_poc d JOIN _seed_idmap ci ON ci.clone_creative_id = d.child_creative_id)
    ON CONFLICT (creative_id) DO UPDATE SET
        creative_url = EXCLUDED.creative_url, provider_code = EXCLUDED.provider_code, creative_type = EXCLUDED.creative_type,
        creative_source_type = EXCLUDED.creative_source_type, capture_timestamp = EXCLUDED.capture_timestamp, product_id = EXCLUDED.product_id,
        confidence_score = EXCLUDED.confidence_score, sent_timestamp = EXCLUDED.sent_timestamp, received_timestamp = EXCLUDED.received_timestamp,
        status_id = EXCLUDED.status_id, error_description = EXCLUDED.error_description, updated_timestamp = EXCLUDED.updated_timestamp,
        ai_brand_name = EXCLUDED.ai_brand_name, ai_advertiser_name = EXCLUDED.ai_advertiser_name, ai_product_name = EXCLUDED.ai_product_name,
        ai_reasoning = EXCLUDED.ai_reasoning, ai_rule_id = EXCLUDED.ai_rule_id, ai_rule_justification = EXCLUDED.ai_rule_justification,
        ai_advertisement_domain = EXCLUDED.ai_advertisement_domain, ai_product_type = EXCLUDED.ai_product_type, ai_parent_n = EXCLUDED.ai_parent_n,
        ai_subsidiary_n = EXCLUDED.ai_subsidiary_n, ai_industrygroup_class_n = EXCLUDED.ai_industrygroup_class_n, ai_microcategory_class_n = EXCLUDED.ai_microcategory_class_n,
        ai_microcategory_class_code = EXCLUDED.ai_microcategory_class_code, ai_product_descriptor = EXCLUDED.ai_product_descriptor, ai_ocr = EXCLUDED.ai_ocr,
        ai_audio_transcript = EXCLUDED.ai_audio_transcript, ai_product_brand_display_name = EXCLUDED.ai_product_brand_display_name, ai_product_advertiser_display_name = EXCLUDED.ai_product_advertiser_display_name,
        ai_product_parent_display_name = EXCLUDED.ai_product_parent_display_name, ai_product_subsidiary_display_name = EXCLUDED.ai_product_subsidiary_display_name, tax_product_name = EXCLUDED.tax_product_name,
        tax_brand_name = EXCLUDED.tax_brand_name, tax_advertiser_name = EXCLUDED.tax_advertiser_name, tax_parent_name = EXCLUDED.tax_parent_name,
        tax_product_type = EXCLUDED.tax_product_type, tax_product_descriptor = EXCLUDED.tax_product_descriptor, tax_subsidiary_n = EXCLUDED.tax_subsidiary_n,
        tax_industrygroup_class_n = EXCLUDED.tax_industrygroup_class_n, tax_microcategory_class_n = EXCLUDED.tax_microcategory_class_n, tax_microcategory_class_code = EXCLUDED.tax_microcategory_class_code,
        tax_product_brand_display_name = EXCLUDED.tax_product_brand_display_name, tax_product_advertiser_display_name = EXCLUDED.tax_product_advertiser_display_name, tax_product_parent_display_name = EXCLUDED.tax_product_parent_display_name,
        tax_product_subsidiary_display_name = EXCLUDED.tax_product_subsidiary_display_name, ce_response_json = EXCLUDED.ce_response_json;

    -- 2.6  creative  [all in-scope creatives; remap creative_id]  (MERGE / upsert on creative_id)
    INSERT INTO tempwork.creative_ctv_poc (
        creative_id, country_iso_2_code, legacy_creative_id, provider_id, source_channel_id,
        provider_creative_id, creative_url_hash, title, headline, creative_type_id,
        creative_mime_type_id, creative_width, creative_height, creative_duration, occurrence_description,
        creative_tier_id, primary_language_code, has_additional_multi_product, has_additional_coop_product,
        classification_type_id, classified_by_user_id, classified_timestamp, classification_comments,
        classification_process_step_id, created_timestamp, updated_timestamp, attribution_first_audio,
        attribution_lead_text, attribution_visual, attribution_summary, attribution_other_details,
        attribution_description, attribution_hashtag, attribution_slogan_tagline_id,
        attribution_revision_description, attribution_comments, attribution_creative_tags, custom_attributes,
        attribution_timestamp, attribution_by_user_id, attribution_status_id, is_component_eligible,
        component_entry_status_id, component_entry_by_user_id, component_entry_timestamp,
        send_to_adscope_unattributed, creative_payload, machine_learning_payload, priority_status,
        split_request_option, split_request_comment, split_request_timestamp, is_sponsored_video,
        asset_source_server_id, is_moved_for_class, is_moved_for_attr, is_moved_for_attr_qa,
        is_moved_for_class_qa, is_moved_for_component_coding, has_secondary_product, media_id,
        is_classified, is_attributed, is_compcoded, is_mlevaluated, print_los_id, print_ad_type_id,
        print_ad_nli, print_ad_equ, print_ad_col_inch, print_ad_weighted_col_inch, print_ad_cost,
        print_ad_size, print_null_cost_comments, print_recalculate_cost, is_resegment,
        product_mapping_status, keywords, is_reclassified, print_no_cost, print_matching_ads,
        print_ad_images, print_ad_type_desc_id, is_qaverified_for_class, is_qaverified_for_attr,
        is_trimmed_video, ml_response_by_user, occurrence_timestamp, market_id, is_qafiltered_for_class,
        is_qafiltered_for_attr, reattribution_status_id, transcription_edited, reclassification_status_id
    )
    SELECT
        m.clone_creative_id, pc.country_iso_2_code, pc.legacy_creative_id,
        COALESCE(cfs.provider_id::int2, pc.provider_id),               -- ours: clone first_seen; else prod
        COALESCE(rsc.source_channel_id, pc.source_channel_id),        -- ours: reference.source_channel.short_desc = clone staging.source_channel; else prod
        COALESCE(css.provider_creative_id, pc.provider_creative_id),  -- ours: clone staging; else prod
        pc.creative_url_hash, pc.title, pc.headline,
        COALESCE(rct.creative_type_id, pc.creative_type_id),          -- ours: reference.creative_type.creative_type = clone staging.creative_type; else prod
        COALESCE(css.mime_type_id, pc.creative_mime_type_id),         -- ours: clone staging; else prod
        COALESCE(css.creative_width, pc.creative_width),              -- ours: clone staging; else prod
        COALESCE(css.creative_height, pc.creative_height),            -- ours: clone staging; else prod
        COALESCE(css.creative_duration, pc.creative_duration),        -- ours: clone staging; else prod
        pc.occurrence_description,                                    -- prod (no clone source; seeded from prod)
        pc.creative_tier_id, pc.primary_language_code, pc.has_additional_multi_product, pc.has_additional_coop_product,
        pc.classification_type_id, pc.classified_by_user_id, pc.classified_timestamp, pc.classification_comments,
        pc.classification_process_step_id, pc.created_timestamp, clock_timestamp() at time zone 'utc', pc.attribution_first_audio,
        pc.attribution_lead_text, pc.attribution_visual, pc.attribution_summary, pc.attribution_other_details,
        pc.attribution_description, pc.attribution_hashtag, pc.attribution_slogan_tagline_id,
        pc.attribution_revision_description, pc.attribution_comments, pc.attribution_creative_tags, pc.custom_attributes,
        pc.attribution_timestamp, pc.attribution_by_user_id, pc.attribution_status_id, pc.is_component_eligible,
        pc.component_entry_status_id, pc.component_entry_by_user_id, pc.component_entry_timestamp,
        pc.send_to_adscope_unattributed, pc.creative_payload, pc.machine_learning_payload, pc.priority_status,
        pc.split_request_option, pc.split_request_comment, pc.split_request_timestamp, pc.is_sponsored_video,
        pc.asset_source_server_id, pc.is_moved_for_class, pc.is_moved_for_attr, pc.is_moved_for_attr_qa,
        pc.is_moved_for_class_qa, pc.is_moved_for_component_coding, pc.has_secondary_product,
        COALESCE(cfs.media_id::int2, pc.media_id),                    -- ours: clone first_seen; else prod
        pc.is_classified, pc.is_attributed, pc.is_compcoded, pc.is_mlevaluated, pc.print_los_id, pc.print_ad_type_id,
        pc.print_ad_nli, pc.print_ad_equ, pc.print_ad_col_inch, pc.print_ad_weighted_col_inch, pc.print_ad_cost,
        pc.print_ad_size, pc.print_null_cost_comments, pc.print_recalculate_cost, pc.is_resegment,
        pc.product_mapping_status, pc.keywords, pc.is_reclassified, pc.print_no_cost, pc.print_matching_ads,
        pc.print_ad_images, pc.print_ad_type_desc_id, pc.is_qaverified_for_class, pc.is_qaverified_for_attr,
        pc.is_trimmed_video, pc.ml_response_by_user,
        COALESCE(cfs.occurrence_timestamp, pc.occurrence_timestamp),  -- ours: clone first_seen; else prod
        COALESCE(cfs.market_id::int2, pc.market_id),                  -- ours: clone first_seen; else prod
        pc.is_qafiltered_for_class,
        pc.is_qafiltered_for_attr, pc.reattribution_status_id, pc.transcription_edited, pc.reclassification_status_id
    FROM creatives.creative pc
    JOIN _seed_idmap m ON m.prod_creative_id = pc.creative_id
    LEFT JOIN tempwork.creative_staging_ctv_poc    css ON css.creative_id = m.clone_creative_id  -- our creatives only
    LEFT JOIN tempwork.creative_first_seen_ctv_poc  cfs ON cfs.creative_id = m.clone_creative_id  -- our creatives only
    LEFT JOIN reference.creative_type               rct ON rct.creative_type = css.creative_type  -- name -> creative_type_id (our creatives)
    LEFT JOIN reference.source_channel              rsc ON rsc.short_desc    = css.source_channel -- short_desc -> source_channel_id (our creatives)
    -- OUR CTV creatives + their related parents only (see 2.5); excludes non-CTV children of CTV parents.
    WHERE m.is_ours
       OR m.clone_creative_id IN (SELECT d.parent_creative_id FROM tempwork.creative_dedupe_map_ctv_poc d JOIN _seed_idmap ci ON ci.clone_creative_id = d.child_creative_id)
    ON CONFLICT (creative_id) DO UPDATE SET
        country_iso_2_code = EXCLUDED.country_iso_2_code, legacy_creative_id = EXCLUDED.legacy_creative_id, provider_id = EXCLUDED.provider_id,
        source_channel_id = EXCLUDED.source_channel_id, provider_creative_id = EXCLUDED.provider_creative_id, creative_url_hash = EXCLUDED.creative_url_hash,
        title = EXCLUDED.title, headline = EXCLUDED.headline, creative_type_id = EXCLUDED.creative_type_id,
        creative_mime_type_id = EXCLUDED.creative_mime_type_id, creative_width = EXCLUDED.creative_width, creative_height = EXCLUDED.creative_height,
        creative_duration = EXCLUDED.creative_duration, occurrence_description = EXCLUDED.occurrence_description, creative_tier_id = EXCLUDED.creative_tier_id,
        primary_language_code = EXCLUDED.primary_language_code, has_additional_multi_product = EXCLUDED.has_additional_multi_product, has_additional_coop_product = EXCLUDED.has_additional_coop_product,
        classification_type_id = EXCLUDED.classification_type_id, classified_by_user_id = EXCLUDED.classified_by_user_id, classified_timestamp = EXCLUDED.classified_timestamp,
        classification_comments = EXCLUDED.classification_comments, classification_process_step_id = EXCLUDED.classification_process_step_id, created_timestamp = EXCLUDED.created_timestamp,
        updated_timestamp = EXCLUDED.updated_timestamp, attribution_first_audio = EXCLUDED.attribution_first_audio, attribution_lead_text = EXCLUDED.attribution_lead_text,
        attribution_visual = EXCLUDED.attribution_visual, attribution_summary = EXCLUDED.attribution_summary, attribution_other_details = EXCLUDED.attribution_other_details,
        attribution_description = EXCLUDED.attribution_description, attribution_hashtag = EXCLUDED.attribution_hashtag, attribution_slogan_tagline_id = EXCLUDED.attribution_slogan_tagline_id,
        attribution_revision_description = EXCLUDED.attribution_revision_description, attribution_comments = EXCLUDED.attribution_comments, attribution_creative_tags = EXCLUDED.attribution_creative_tags,
        custom_attributes = EXCLUDED.custom_attributes, attribution_timestamp = EXCLUDED.attribution_timestamp, attribution_by_user_id = EXCLUDED.attribution_by_user_id,
        attribution_status_id = EXCLUDED.attribution_status_id, is_component_eligible = EXCLUDED.is_component_eligible, component_entry_status_id = EXCLUDED.component_entry_status_id,
        component_entry_by_user_id = EXCLUDED.component_entry_by_user_id, component_entry_timestamp = EXCLUDED.component_entry_timestamp, send_to_adscope_unattributed = EXCLUDED.send_to_adscope_unattributed,
        creative_payload = EXCLUDED.creative_payload, machine_learning_payload = EXCLUDED.machine_learning_payload, priority_status = EXCLUDED.priority_status,
        split_request_option = EXCLUDED.split_request_option, split_request_comment = EXCLUDED.split_request_comment, split_request_timestamp = EXCLUDED.split_request_timestamp,
        is_sponsored_video = EXCLUDED.is_sponsored_video, asset_source_server_id = EXCLUDED.asset_source_server_id, is_moved_for_class = EXCLUDED.is_moved_for_class,
        is_moved_for_attr = EXCLUDED.is_moved_for_attr, is_moved_for_attr_qa = EXCLUDED.is_moved_for_attr_qa, is_moved_for_class_qa = EXCLUDED.is_moved_for_class_qa,
        is_moved_for_component_coding = EXCLUDED.is_moved_for_component_coding, has_secondary_product = EXCLUDED.has_secondary_product, media_id = EXCLUDED.media_id,
        is_classified = EXCLUDED.is_classified, is_attributed = EXCLUDED.is_attributed, is_compcoded = EXCLUDED.is_compcoded,
        is_mlevaluated = EXCLUDED.is_mlevaluated, print_los_id = EXCLUDED.print_los_id, print_ad_type_id = EXCLUDED.print_ad_type_id,
        print_ad_nli = EXCLUDED.print_ad_nli, print_ad_equ = EXCLUDED.print_ad_equ, print_ad_col_inch = EXCLUDED.print_ad_col_inch,
        print_ad_weighted_col_inch = EXCLUDED.print_ad_weighted_col_inch, print_ad_cost = EXCLUDED.print_ad_cost, print_ad_size = EXCLUDED.print_ad_size,
        print_null_cost_comments = EXCLUDED.print_null_cost_comments, print_recalculate_cost = EXCLUDED.print_recalculate_cost, is_resegment = EXCLUDED.is_resegment,
        product_mapping_status = EXCLUDED.product_mapping_status, keywords = EXCLUDED.keywords, is_reclassified = EXCLUDED.is_reclassified,
        print_no_cost = EXCLUDED.print_no_cost, print_matching_ads = EXCLUDED.print_matching_ads, print_ad_images = EXCLUDED.print_ad_images,
        print_ad_type_desc_id = EXCLUDED.print_ad_type_desc_id, is_qaverified_for_class = EXCLUDED.is_qaverified_for_class, is_qaverified_for_attr = EXCLUDED.is_qaverified_for_attr,
        is_trimmed_video = EXCLUDED.is_trimmed_video, ml_response_by_user = EXCLUDED.ml_response_by_user, occurrence_timestamp = EXCLUDED.occurrence_timestamp,
        market_id = EXCLUDED.market_id, is_qafiltered_for_class = EXCLUDED.is_qafiltered_for_class, is_qafiltered_for_attr = EXCLUDED.is_qafiltered_for_attr,
        reattribution_status_id = EXCLUDED.reattribution_status_id, transcription_edited = EXCLUDED.transcription_edited, reclassification_status_id = EXCLUDED.reclassification_status_id;

    -- 2.7  creative_product / celebrity / competitor  [all in-scope; remap creative_id]  (delete + insert)
    DELETE FROM tempwork.creative_product_ctv_poc
      WHERE creative_id IN (SELECT clone_creative_id FROM _seed_idmap);
    INSERT INTO tempwork.creative_product_ctv_poc
        (creative_product_id, creative_id, product_id, is_primary, map_type, map_sub_type,
         is_dominant, sort_order, provider_id, media_id)
    SELECT cp.creative_product_id, m.clone_creative_id, cp.product_id, cp.is_primary, cp.map_type,
           cp.map_sub_type, cp.is_dominant, cp.sort_order, cp.provider_id, cp.media_id
    FROM creatives.creative_product cp
    JOIN _seed_idmap m ON m.prod_creative_id = cp.creative_id
    WHERE m.is_ours
       OR m.clone_creative_id IN (SELECT d.parent_creative_id FROM tempwork.creative_dedupe_map_ctv_poc d JOIN _seed_idmap ci ON ci.clone_creative_id = d.child_creative_id);

    DELETE FROM tempwork.creative_celebrity_ctv_poc
      WHERE creative_id IN (SELECT clone_creative_id FROM _seed_idmap);
    INSERT INTO tempwork.creative_celebrity_ctv_poc
        (creative_celebrity_id, creative_id, celebrity_id, created_by_user_id, created_timestamp,
         updated_by_user_id, updated_timestamp)
    SELECT cc.creative_celebrity_id, m.clone_creative_id, cc.celebrity_id, cc.created_by_user_id,
           cc.created_timestamp, cc.updated_by_user_id, clock_timestamp() at time zone 'utc'
    FROM creatives.creative_celebrity cc
    JOIN _seed_idmap m ON m.prod_creative_id = cc.creative_id
    WHERE m.is_ours
       OR m.clone_creative_id IN (SELECT d.parent_creative_id FROM tempwork.creative_dedupe_map_ctv_poc d JOIN _seed_idmap ci ON ci.clone_creative_id = d.child_creative_id);

    DELETE FROM tempwork.creative_competitor_ctv_poc
      WHERE creative_id IN (SELECT clone_creative_id FROM _seed_idmap);
    INSERT INTO tempwork.creative_competitor_ctv_poc
        (creative_competitor_id, creative_id, competitor_id, created_by_user_id, created_timestamp,
         updated_by_user_id, updated_timestamp)
    SELECT ck.creative_competitor_id, m.clone_creative_id, ck.competitor_id, ck.created_by_user_id,
           ck.created_timestamp, ck.updated_by_user_id, clock_timestamp() at time zone 'utc'
    FROM creatives.creative_competitor ck
    JOIN _seed_idmap m ON m.prod_creative_id = ck.creative_id
    WHERE m.is_ours
       OR m.clone_creative_id IN (SELECT d.parent_creative_id FROM tempwork.creative_dedupe_map_ctv_poc d JOIN _seed_idmap ci ON ci.clone_creative_id = d.child_creative_id);

    -- 2.7b  component_coding  [all in-scope; remap creative_id; multi-row per creative]  (delete + insert)
    DELETE FROM tempwork.component_coding_ctv_poc
      WHERE creative_id IN (SELECT clone_creative_id FROM _seed_idmap);
    INSERT INTO tempwork.component_coding_ctv_poc
        (component_coding_id, creative_id, component_template_id, "sequence", "share",
         attribute_response, is_logically_deleted, created, modified, creative_path,
         page_no, height, width, area, x_offset, y_offset, status, modified_by, order_number)
    SELECT cco.component_coding_id, m.clone_creative_id, cco.component_template_id, cco."sequence", cco."share",
           cco.attribute_response, cco.is_logically_deleted, cco.created, clock_timestamp() at time zone 'utc', cco.creative_path,
           cco.page_no, cco.height, cco.width, cco.area, cco.x_offset, cco.y_offset, cco.status, cco.modified_by, cco.order_number
    FROM creatives.component_coding cco
    JOIN _seed_idmap m ON m.prod_creative_id = cco.creative_id
    WHERE m.is_ours
       OR m.clone_creative_id IN (SELECT d.parent_creative_id FROM tempwork.creative_dedupe_map_ctv_poc d JOIN _seed_idmap ci ON ci.clone_creative_id = d.child_creative_id);

    -- 2.8  holding  [all in-scope; source deduped to one row per creative]  (MERGE / upsert on creative_id)
    INSERT INTO tempwork.creative_classification_engine_holding_ctv_poc (creative_id, inserted_at)
    SELECT DISTINCT ON (m.clone_creative_id) m.clone_creative_id, h.inserted_at
    FROM creatives.creative_classification_engine_holding h
    JOIN _seed_idmap m ON m.prod_creative_id = h.creative_id
    WHERE m.is_ours
       OR m.clone_creative_id IN (SELECT d.parent_creative_id FROM tempwork.creative_dedupe_map_ctv_poc d JOIN _seed_idmap ci ON ci.clone_creative_id = d.child_creative_id)
    ORDER BY m.clone_creative_id, h.inserted_at DESC
    ON CONFLICT (creative_id) DO UPDATE SET inserted_at = EXCLUDED.inserted_at;

    TRUNCATE _seed_idmap;  -- leave the (empty) temp table in place; keeps OID stable for the next call
END;
$procedure$
;


-- =====================================================================================
-- 3. ENTRY POINT  sp_seed_creative_clones_ctv_poc(p_mode)
--    p_mode = 'NEW'  -> Mode 1 only    (new inserts, driven by clone staging.updated_timestamp)
--             'UPDATE'-> Mode 2 only    (creative updates, driven by prod creative.updated_timestamp)
--             'ALL'   -> Mode 1 then Mode 2  (default)
--    Run adhoc:  CALL tempwork.sp_seed_creative_clones_ctv_poc('ALL');
-- =====================================================================================
CREATE OR REPLACE PROCEDURE tempwork.sp_seed_creative_clones_ctv_poc(IN p_mode character varying DEFAULT 'ALL')
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_mode     varchar := upper(coalesce(p_mode, 'ALL'));
    v_last     timestamp;
    v_now      timestamp;
    v_rows     bigint;
BEGIN
    IF v_mode NOT IN ('ALL', 'NEW', 'UPDATE') THEN
        RAISE EXCEPTION 'p_mode must be one of ALL, NEW, UPDATE (got %)', p_mode;
    END IF;

    -- =================================================================================
    -- MODE 1 — NEW INSERTS.  Anchor = creatives newly inserted into clone staging since the
    -- last mark. Take their url_hashes, look up current prod state, load the footprint.
    -- =================================================================================
    IF v_mode IN ('ALL', 'NEW') THEN
        SELECT coalesce(table_tx_end, '1900-01-01'::timestamp) INTO v_last
        FROM tempwork.watermark_control_ctv_poc WHERE watermark_name = 'CTV_POC_SEED_NEW';

        SELECT max(updated_timestamp) INTO v_now FROM tempwork.creative_staging_ctv_poc;

        IF v_now IS NOT NULL AND v_now > v_last THEN
            CREATE TEMP TABLE IF NOT EXISTS _seed_anchor (creative_url_hash int8 NOT NULL, reserved_id int8 NOT NULL);
            TRUNCATE _seed_anchor;

            INSERT INTO _seed_anchor (creative_url_hash, reserved_id)
            SELECT s.creative_url_hash, s.creative_id
            FROM tempwork.creative_staging_ctv_poc s
            WHERE s.updated_timestamp > v_last;

            GET DIAGNOSTICS v_rows = ROW_COUNT;
            RAISE NOTICE 'MODE 1 (new inserts): % candidate anchor(s) since %', v_rows, v_last;

            CALL tempwork.sp_seed_load_footprint_ctv_poc();

            UPDATE tempwork.watermark_control_ctv_poc
            SET table_tx_start = v_last, table_tx_end = v_now, tx_status = 'OK',
                tx_message = format('mode1 new inserts: %s anchor(s)', v_rows), tx_datetime = clock_timestamp() AT TIME ZONE 'UTC'
            WHERE watermark_name = 'CTV_POC_SEED_NEW';
        ELSE
            RAISE NOTICE 'MODE 1 (new inserts): nothing new in clone staging since %', v_last;
        END IF;
    END IF;

    -- =================================================================================
    -- MODE 2 — CREATIVE UPDATES.  Grab prod creatives changed since the last mark and rebuild the
    -- affected footprints. Two ways a change touches us:
    --   (a) one of OUR creatives (a child, in clone staging) changed  -> rebuild it;
    --   (b) a PARENT of one of our clone children changed (a pure parent-attribute change does NOT
    --       bump the child's updated_timestamp, so it would otherwise be missed) -> rebuild that
    --       child, which re-pulls its dedupe_map row (refreshing the denormalized parent_* fields)
    --       and re-pulls the parent creative itself via the loader's one-hop closure.
    -- The anchor set is the UNION of (a) and (b); the existing loader handles both uniformly.
    -- =================================================================================
    IF v_mode IN ('ALL', 'UPDATE') THEN
        SELECT coalesce(table_tx_end, '1900-01-01'::timestamp) INTO v_last
        FROM tempwork.watermark_control_ctv_poc WHERE watermark_name = 'CTV_POC_SEED_UPDATE';

        SELECT max(updated_timestamp) INTO v_now FROM creatives.creative;

        IF v_now IS NOT NULL AND v_now > v_last THEN
            CREATE TEMP TABLE IF NOT EXISTS _seed_anchor (creative_url_hash int8 NOT NULL, reserved_id int8 NOT NULL);
            TRUNCATE _seed_anchor;

            INSERT INTO _seed_anchor (creative_url_hash, reserved_id)
            -- (a) our creatives (children) whose own prod row changed
            SELECT s.creative_url_hash, s.creative_id
            FROM tempwork.creative_staging_ctv_poc s
            JOIN creatives.creative pc ON pc.creative_url_hash = s.creative_url_hash
            WHERE pc.updated_timestamp > v_last
            UNION
            -- (b) our children whose PARENT changed in prod (parent may be ours or external). The
            --     clone dedupe_map already records child_creative_id (reserved) + parent_creative_url_hash.
            SELECT dm.child_creative_url_hash, dm.child_creative_id
            FROM tempwork.creative_dedupe_map_ctv_poc dm
            JOIN creatives.creative pc ON pc.creative_url_hash = dm.parent_creative_url_hash
            WHERE pc.updated_timestamp > v_last;

            GET DIAGNOSTICS v_rows = ROW_COUNT;
            RAISE NOTICE 'MODE 2 (updates): % affected anchor(s) (changed children + children of changed parents) since %', v_rows, v_last;

            IF v_rows > 0 THEN
                CALL tempwork.sp_seed_load_footprint_ctv_poc();
            END IF;

            UPDATE tempwork.watermark_control_ctv_poc
            SET table_tx_start = v_last, table_tx_end = v_now, tx_status = 'OK',
                tx_message = format('mode2 updates: %s matched anchor(s)', v_rows), tx_datetime = clock_timestamp() AT TIME ZONE 'UTC'
            WHERE watermark_name = 'CTV_POC_SEED_UPDATE';
        ELSE
            RAISE NOTICE 'MODE 2 (updates): no prod creative changes since %', v_last;
        END IF;
    END IF;
END;
$procedure$
;

