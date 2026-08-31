-- =====================================================================================
-- Piece 3 — Postgres bootstrap for the CTV PoC creative push (run ONCE on prod Postgres).
--
-- Everything lives in the tempwork schema with a _ctv_poc suffix so the PoC never touches
-- the real creatives.* tables/procs. Clone tables start EMPTY (no seed from prod). Triggers
-- are intentionally NOT recreated (we only prove Iceberg -> Postgres data movement, not the
-- PG-side downstream). Idempotent: safe to re-run (CREATE ... IF NOT EXISTS / CREATE OR REPLACE).
--
-- Objects created:
--   tempwork.creative_id_seq_ctv_poc                                (sequence, start 26,000,000,000)
--   tempwork.creative_id_block_ctv_poc + sp_reserve_creative_ids_ctv_poc (Job A id-block reservation)
--   tempwork.creative_staging_ctv_poc                               (clone of creatives.creative_staging)
--   tempwork.creative_first_seen_ctv_poc                            (clone of creatives.creative_first_seen)
--   tempwork.creative_occurrence_summary_ctv_poc                    (clone of creatives.creative_occurrence_summary; Job B)
--   tempwork.sp_dbx_digital_insert_crtv_staging_first_seen_ctv_poc  (Job A insert proc, retargeted)
--   tempwork.sp_dbx_digital_update_raw_occ_to_crtv_first_seen_ctv_poc (Job B first-seen update proc, retargeted)
--   tempwork.sp_dbx_digital_upsert_to_crtv_occ_summary_ctv_poc      (Job B occurrence-summary upsert proc, retargeted)
--
-- Reads (unchanged, prod, READ-ONLY): reference.data_provider  (provider_code -> provider_id).
--
-- Temp tables written each run by the dbt/Trino flow (NOT created here — dbt drops+creates them):
--   tempwork.tmp_digital_raw_occ_to_crtv_staging_ctv_poc
--   tempwork.tmp_digital_raw_occ_to_crtv_firstseen_ctv_poc
-- =====================================================================================

CREATE SCHEMA IF NOT EXISTS tempwork;

-- ---------------------------------------------------------------------------------------
-- Sequence — stands in for the Universal Creative ID API. Job A reserves a contiguous block
-- per run. Start at 26,000,000,000 to guarantee no collision with existing prod creative_id.
-- ---------------------------------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS tempwork.creative_id_seq_ctv_poc
    AS bigint
    START WITH 26000000000
    INCREMENT BY 1
    MINVALUE 26000000000
    NO MAXVALUE
    CACHE 1
    NO CYCLE;

-- ---------------------------------------------------------------------------------------
-- Creative-id block reservation (stands in for the Universal Creative ID API, which returned a
-- [min, max] block). sp_reserve_creative_ids_ctv_poc(n) atomically pops n values off the sequence
-- and records the reserved block; dbt reads the newest block row back (Trino's writable system.execute
-- can't return rows, and its row-returning system.query is read-only, so the block table is the
-- "API response log" the reader picks up). One row per Job A run.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tempwork.creative_id_block_ctv_poc (
    block_id     bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reserved_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    n            integer     NOT NULL,
    block_start  bigint      NOT NULL,
    block_end    bigint      NOT NULL
);

CREATE OR REPLACE PROCEDURE tempwork.sp_reserve_creative_ids_ctv_poc(IN p_n integer)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start bigint;
    v_end   bigint;
BEGIN
    IF p_n IS NULL OR p_n <= 0 THEN
        RETURN;
    END IF;
    -- pop n contiguous values off the sequence; capture the block bounds
    SELECT min(v), max(v) INTO v_start, v_end
    FROM (SELECT nextval('tempwork.creative_id_seq_ctv_poc') AS v
          FROM generate_series(1, p_n)) s;
    INSERT INTO tempwork.creative_id_block_ctv_poc (n, block_start, block_end)
    VALUES (p_n, v_start, v_end);
END;
$procedure$
;

-- ---------------------------------------------------------------------------------------
-- Clone: creative_staging  (columns verbatim; PK + unique(creative_url_hash) only, no triggers).
-- The unique(creative_url_hash) is REQUIRED for the proc's ON CONFLICT (creative_url_hash).
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tempwork.creative_staging_ctv_poc (
    creative_id                        int8        NOT NULL,
    legacy_creative_id                 int8        NULL,
    country_iso_2_code                 varchar(2)  NOT NULL,
    provider_code                      varchar(20) NOT NULL,
    source_channel                     varchar(25) NULL,
    provider_creative_id               int8        NOT NULL,
    capture_month                      int4        NOT NULL,
    capture_timestamp                  timestamp   NOT NULL,
    creative_type                      varchar(25) NOT NULL,
    mime_type_id                       int2        NOT NULL,
    media_id                           int2        NOT NULL,
    media_property_id                  int4        NOT NULL,
    publisher_domain                   varchar     NULL,
    creative_width                     int4        NULL,
    creative_height                    int4        NULL,
    creative_duration                  int4        NULL,
    creative_url                       varchar     NOT NULL,
    creative_url_hash                  int8        NOT NULL,
    creative_machine_learning_payload  jsonb       NULL,
    creative_url_override              varchar     NULL,
    creative_payload                   jsonb       NULL,
    record_status                      bpchar(1)   NULL,
    first_seen_metadata                jsonb       NULL,
    suggested_vx0_product_id           int4        NULL,
    created_timestamp                  timestamp   DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_timestamp                  timestamp   DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT creative_staging_ctv_poc_pkey PRIMARY KEY (creative_id),
    CONSTRAINT creative_staging_ctv_poc_creative_url_hash_key UNIQUE (creative_url_hash)
);

-- ---------------------------------------------------------------------------------------
-- Clone: creative_first_seen  (columns verbatim; PK + unique(creative_url_hash) only, no triggers).
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tempwork.creative_first_seen_ctv_poc (
    creative_id                     int8      NOT NULL,
    creative_url_hash               int8      NULL,
    provider_creative_id            int8      NULL,
    country_iso_2_code              text      NULL,
    media_id                        int4      NULL,
    occurrence_id                   text      NULL,
    occurrence_timestamp            timestamp NULL,
    occurrence_timestamp_local      timestamp NULL,
    provider_id                     int4      NULL,
    media_property_id               int4      NULL,
    media_property_name             text      NULL,
    media_category_id               int4      NULL,
    media_category_code             text      NULL,
    provider_creative_link_url      text      NULL,
    provider_publisher_id           int4      NULL,
    provider_publisher_domain       text      NULL,
    provider_campaign_id            int4      NULL,
    provider_campaign_name          text      NULL,
    provider_advertiser_id          int4      NULL,
    provider_advertiser_name        text      NULL,
    provider_product_id             int4      NULL,
    provider_product_name           text      NULL,
    due_timestamp                   timestamp NULL,
    market_id                       int4      NULL,
    market_name                     text      NULL,
    daypart_id                      int4      NULL,
    daypart_name                    text      NULL,
    affiliate_id                    int4      NULL,
    affiliate_name                  text      NULL,
    created_timestamp               timestamp NULL,
    updated_timestamp               timestamp NULL,
    edition_id                      int4      NULL,
    edition_name                    text      NULL,
    section_id                      int4      NULL,
    section_name                    text      NULL,
    provider_campaign_landing_page  varchar   NULL,
    CONSTRAINT creative_first_seen_ctv_poc_pkey PRIMARY KEY (creative_id),
    CONSTRAINT creative_first_seen_ctv_poc_creative_url_hash_key UNIQUE (creative_url_hash)
);

-- ---------------------------------------------------------------------------------------
-- Clone proc (Job A): insert new creatives into creative_staging + seed creative_first_seen.
-- Body verbatim from creatives.sp_dbx_digital_insert_crtv_staging_first_seen, retargeted to the
-- tempwork _ctv_poc clones. reference.data_provider join kept (read-only prod lookup).
-- Example: CALL tempwork.sp_dbx_digital_insert_crtv_staging_first_seen_ctv_poc('tempwork.tmp_digital_raw_occ_to_crtv_staging_ctv_poc');
-- ---------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE tempwork.sp_dbx_digital_insert_crtv_staging_first_seen_ctv_poc(IN p_tmp_tbl_name character varying)
 LANGUAGE plpgsql
AS $procedure$
BEGIN

        EXECUTE format('INSERT INTO tempwork.creative_staging_ctv_poc(
        creative_id,
        legacy_creative_id,
        country_iso_2_code,
        provider_code,
        source_channel,
        provider_creative_id,
        capture_month,
        capture_timestamp,
        creative_type,
        mime_type_id,
        media_id,
        media_property_id,
        publisher_domain,
        creative_width,
        creative_height,
        creative_duration,
        creative_url,
        creative_url_hash,
        creative_machine_learning_payload,
        creative_url_override,
        creative_payload,
        record_status,
        first_seen_metadata,
        suggested_vx0_product_id,
        created_timestamp,
		updated_timestamp
        )
SELECT
        creative_id,
        legacy_creative_id,
        country_iso_2_code,
        provider_code,
        source_channel,
        provider_creative_id,
        capture_month,
        capture_timestamp,
        creative_type,
        mime_type_id,
        media_id,
        media_property_id,
        publisher_domain,
        creative_width,
        creative_height,
        creative_duration,
        creative_url,
        creative_url_hash,
        cast(creative_machine_learning_response as jsonb) as creative_machine_learning_payload,
        creative_url_override,
        cast(creative_payload as jsonb) as creative_payload,
        record_status,
        cast(replace(first_seen_metadata,''\u0000'','''') as jsonb) as first_seen_metadata,
		suggested_vx0_product_id,
        clock_timestamp() AT TIME ZONE ''UTC'',
        clock_timestamp() AT TIME ZONE ''UTC''

FROM %s
ON CONFLICT (creative_url_hash)
DO NOTHING',p_tmp_tbl_name);


-- Inserting to creative firstseen into creative_first_seen table

EXECUTE format('INSERT INTO tempwork.creative_first_seen_ctv_poc(
	        creative_id,
			creative_url_hash,
			provider_creative_id,
			country_iso_2_code,
			media_id,
			occurrence_id,
			occurrence_timestamp,
			provider_id,
			media_property_id,
			media_property_name,
			media_category_id,
			media_category_code,
			provider_creative_link_url,
			provider_publisher_id,
			provider_publisher_domain,
			provider_campaign_id,
			provider_campaign_name,
			provider_advertiser_id,
			provider_advertiser_name,
			provider_product_id,
			provider_product_name,
			due_timestamp,
			market_id,
			market_name,
			daypart_id,
			daypart_name,
			affiliate_id,
			affiliate_name,
			created_timestamp,
			updated_timestamp,
			provider_campaign_landing_page
	        )

		select
				cs.creative_id,
				cs.creative_url_hash,
				cs.provider_creative_id,
				cs.country_iso_2_code,
				cs.media_id,
				cs.first_seen_metadata ->> ''occurrence_id'' occurrence_id,
				(cs.first_seen_metadata ->> ''occurrence_timestamp'' ):: timestamp,
				dp.data_provider_id,
				cs.media_property_id :: INTEGER,
				cs.first_seen_metadata ->> ''media_property_name'' media_property_name,
				(cs.first_seen_metadata ->> ''media_category_id'')::INTEGER ,
				cs.first_seen_metadata ->> ''media_category_code'' media_category_code,
				cs.first_seen_metadata ->> ''provider_creative_link_url'' provider_creative_link_url,
				(cs.first_seen_metadata ->> ''provider_publisher_id'') :: INTEGER,
				cs.first_seen_metadata ->> ''provider_publisher_domain'' provider_publisher_domain,
				(cs.first_seen_metadata ->> ''provider_campaign_id'')::INTEGER,
				cs.first_seen_metadata ->> ''provider_campaign_name'' provider_campaign_name,
				(cs.first_seen_metadata ->> ''provider_advertiser_id'') :: INTEGER ,
				cs.first_seen_metadata ->> ''provider_advertiser_name'' provider_advertiser_name,
				(cs.first_seen_metadata ->> ''provider_product_id'')::INTEGER,
				cs.first_seen_metadata ->> ''provider_product_name'' provider_product_name,
				(cs.first_seen_metadata ->> ''due_timestamp'')::TIMESTAMP,
				(cs.first_seen_metadata ->> ''market_id'')::INTEGER,
				cs.first_seen_metadata ->> ''market_name'' market_name,
				(cs.first_seen_metadata ->> ''daypart_id'')::INTEGER,
				cs.first_seen_metadata ->> ''daypart_name'' daypart_name,
				(cs.first_seen_metadata ->> ''affiliate_id'') :: INTEGER ,
				cs.first_seen_metadata ->> ''affiliate_name'' affiliate_name,
				case when created_timestamp :: date  = ''2025-05-26'' then created_timestamp else clock_timestamp() AT TIME ZONE ''UTC'' end as created_timestamp,
				clock_timestamp() AT TIME ZONE ''UTC'',
				cs.first_seen_metadata ->> ''provider_campaign_landing_page'' provider_campaign_landing_page
				from(
						select
							creative_id,
							creative_url_hash,
							provider_code,
							provider_creative_id,
							country_iso_2_code,
							media_id,
							media_property_id,
							created_timestamp,
							updated_timestamp,
							cast(replace(first_seen_metadata,''\u0000'','''') as jsonb) as first_seen_metadata
							from %s ) cs
				join reference.data_provider dp
				on cs.provider_code = dp.data_provider_code
				ON CONFLICT (creative_url_hash)
				DO NOTHING;',p_tmp_tbl_name);
END
;$procedure$
;

-- ---------------------------------------------------------------------------------------
-- Clone proc (Job B): update first-seen only when the incoming occurrence is EARLIER.
-- Body verbatim from creatives.sp_dbx_digital_update_raw_occ_to_crtv_first_seen, retargeted.
-- Example: CALL tempwork.sp_dbx_digital_update_raw_occ_to_crtv_first_seen_ctv_poc('tempwork.tmp_digital_raw_occ_to_crtv_firstseen_ctv_poc');
-- ---------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE tempwork.sp_dbx_digital_update_raw_occ_to_crtv_first_seen_ctv_poc(IN p_tmp_tbl_name character varying)
 LANGUAGE plpgsql
AS $procedure$
	BEGIN

		EXECUTE FORMAT (
    'UPDATE tempwork.creative_first_seen_ctv_poc AS trg
    SET provider_creative_id = tmp.provider_creative_id,
	media_id = tmp.media_id,
	occurrence_id =  tmp.occurrence_id,
	occurrence_timestamp = tmp.occurrence_timestamp,
	provider_id = tmp.provider_id,
	media_property_id = tmp.media_property_id,
	media_property_name = tmp.media_property_name,
	media_category_id = tmp.media_category_id,
	media_category_code = tmp.media_category_code,
	provider_creative_link_url = tmp.provider_creative_link_url,
	provider_publisher_id = tmp.provider_publisher_id,
	provider_publisher_domain = tmp.provider_publisher_domain,
	provider_campaign_id = tmp.provider_campaign_id,
	provider_campaign_name = tmp.provider_campaign_name,
	provider_advertiser_id = tmp.provider_advertiser_id,
	provider_advertiser_name = tmp.provider_advertiser_name,
	provider_product_id = tmp.provider_product_id,
	provider_product_name = tmp.provider_product_name,
	due_timestamp = tmp.due_timestamp,
	market_id = tmp.market_id,
	market_name = tmp.market_name,
	updated_timestamp = tmp.updated_timestamp,
	provider_campaign_landing_page = tmp.provider_campaign_landing_page
    FROM %s AS tmp
    WHERE trg.creative_url_hash = tmp.creative_url_hash
    AND trg.occurrence_timestamp > tmp.occurrence_timestamp;',p_tmp_tbl_name);
	RAISE NOTICE 'Records updated in tempwork.creative_first_seen_ctv_poc matching names';
END;
$procedure$
;

-- =====================================================================================
-- Piece 3 Job B (occurrence summary) — clone of creatives.creative_occurrence_summary + its
-- upsert proc, retargeted to tempwork *_ctv_poc. Real creatives.* untouched. Clone starts empty.
-- The upsert proc accumulates per-(creative,hash,country,media,market,property) occurrence_count +
-- first_run/last_run + a 7-day count. Its internal scratch tables are retargeted into tempwork.
-- =====================================================================================

CREATE TABLE IF NOT EXISTS tempwork.creative_occurrence_summary_ctv_poc (
    summary_row_id        bigserial   NOT NULL,
    creative_id           int8        NOT NULL,
    creative_url_hash     int8        NOT NULL,
    country_iso_2_code    varchar(2)  NOT NULL,
    media_id              int2        NOT NULL,
    market_id             int2        NOT NULL,
    media_property_id     int4        NOT NULL,
    occurrence_count      int4        NOT NULL,
    first_run             timestamp   NOT NULL,
    last_run              timestamp   NOT NULL,
    seven_days_occ_count  int4        DEFAULT 0 NOT NULL,
    CONSTRAINT creative_occurrence_summary_ctv_poc_pkey PRIMARY KEY (summary_row_id),
    CONSTRAINT creative_occurrence_summary_ctv_poc_ukey
        UNIQUE (creative_id, creative_url_hash, media_id, market_id, media_property_id)
);

-- Body verbatim from creatives.sp_dbx_digital_upsert_to_crtv_occ_summary, retargeted.
-- Example: CALL tempwork.sp_dbx_digital_upsert_to_crtv_occ_summary_ctv_poc('tempwork.tmp_raw_occ_for_crtv_occ_summary_ctv_poc');
CREATE OR REPLACE PROCEDURE tempwork.sp_dbx_digital_upsert_to_crtv_occ_summary_ctv_poc(IN p_tmp_tbl_name character varying)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
        DROP TABLE IF EXISTS tempwork.tmp_raw_crtv_occ_summary_local_psql_ctv_poc;

        EXECUTE FORMAT ('CREATE TABLE tempwork.tmp_raw_crtv_occ_summary_local_psql_ctv_poc AS
        SELECT
                creative_id,
                creative_url_hash,
                country_iso_2_code,
                market_id,
                media_id,
                media_property_id,
                sum(occ_cnt) as occurrence_count,
                min(min_capture_timestamp) as min_capture_timestamp,
                max(max_capture_timestamp) as max_capture_timestamp
        FROM %s
        GROUP BY
        creative_id,
        creative_url_hash,
        country_iso_2_code,
        market_id,
        media_id,
        media_property_id', p_tmp_tbl_name);

        MERGE INTO tempwork.creative_occurrence_summary_ctv_poc AS T
        USING tempwork.tmp_raw_crtv_occ_summary_local_psql_ctv_poc as S
        ON  T.creative_id = S.creative_id
                AND T.creative_url_hash = S.creative_url_hash
                AND T.country_iso_2_code = S.country_iso_2_code
                AND T.media_id = S.media_id
                AND T.market_id = S.market_id
                AND coalesce(T.media_property_id,-1) = coalesce(S.media_property_id,-1)
        WHEN MATCHED THEN UPDATE SET
                occurrence_count = T.occurrence_count + S.occurrence_count,
                first_run = CASE WHEN T.first_run > S.min_capture_timestamp THEN S.min_capture_timestamp else T.first_run END,
                last_run = CASE WHEN T.last_run < S.max_capture_timestamp THEN S.max_capture_timestamp else T.last_run END
        WHEN NOT MATCHED THEN
                INSERT (creative_id, creative_url_hash, country_iso_2_code, media_id, market_id, media_property_id,
                        occurrence_count, first_run, last_run)
                VALUES (S.creative_id, S.creative_url_hash, S.country_iso_2_code, S.media_id,
                        S.market_id, S.media_property_id, S.occurrence_count, S.min_capture_timestamp, S.max_capture_timestamp);

        --==================================Populate seven days occurrence summary================================--

        DROP TABLE IF EXISTS tempwork.tmp_seven_days_raw_crtv_occ_summary_local_psql_ctv_poc;

        EXECUTE FORMAT ('CREATE TABLE tempwork.tmp_seven_days_raw_crtv_occ_summary_local_psql_ctv_poc AS
        SELECT
                source.creative_id,
                source.creative_url_hash,
                source.country_iso_2_code,
                source.market_id,
                source.media_id,
                source.media_property_id,
                sum(occ_cnt) as occurrence_count
        FROM %s source
        INNER JOIN tempwork.creative_occurrence_summary_ctv_poc occ_summary
        ON source.creative_id = occ_summary.creative_id
                AND source.creative_url_hash = occ_summary.creative_url_hash
                AND source.country_iso_2_code = occ_summary.country_iso_2_code
                AND source.media_id = occ_summary.media_id
                AND source.market_id = occ_summary.market_id
                AND coalesce(source.media_property_id,-1) = coalesce(occ_summary.media_property_id,-1)
        WHERE source.capture_date::date - occ_summary.first_run::date < 7
        GROUP BY
        source.creative_id,
        source.creative_url_hash,
        source.country_iso_2_code,
        source.market_id,
        source.media_id,
        source.media_property_id', p_tmp_tbl_name);

        MERGE INTO tempwork.creative_occurrence_summary_ctv_poc AS T
        USING tempwork.tmp_seven_days_raw_crtv_occ_summary_local_psql_ctv_poc as S
        ON  T.creative_id = S.creative_id
                AND T.creative_url_hash = S.creative_url_hash
                AND T.country_iso_2_code = S.country_iso_2_code
                AND T.media_id = S.media_id
                AND T.market_id = S.market_id
                AND coalesce(T.media_property_id,-1) = coalesce(S.media_property_id,-1)
        WHEN MATCHED THEN UPDATE SET
        seven_days_occ_count = T.seven_days_occ_count + S.occurrence_count;
END;
$procedure$
;
