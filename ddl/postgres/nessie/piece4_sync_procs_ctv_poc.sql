-- =====================================================================================
-- Piece 4 sync-back — CLONED stored procs (run ONCE on prod Postgres via a SQL client).
-- Bodies are VERBATIM from prod, retargeted to the tempwork *_ctv_poc clones (created by
-- ddl/postgres/nessie/piece3_* and piece4_seed_*). jobwork.* scratch -> tempwork *_ctv_poc; reference.*/
-- config.*/"template".* left as prod READS. Real creatives.*/ml_results.* are NOT read here.
--
-- Outputs the dbt/Trino sync reads:
--   tempwork.creative_forsync_tmp_ctv_poc         (from sp_dbx_creative_get_changes_for_databricks_ctv_poc)
--   tempwork.component_coding_forsync_tmp_ctv_poc (from sp_dbx_component_get_changes_for_databricks_ctv_poc)
-- Runtime scratch created by the procs: crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc.
-- Inputs the dbt flow writes before CALL: creatives_advert_hold_tmp_ctv_poc / component_hold_creative_tmp_ctv_poc.
--
-- ARCHIVE PARKED: the creative proc still references real creatives.creative_archive (read-only). The
-- dbt caller passes ca_flag_timestamp far in the FUTURE ('9999-12-31'), so the archive branches return
-- ZERO rows and no real-archive data enters the clone flow. Revisit archive at end of PoC (see
-- docs/pipeline/ctv_creative_sync_plan.md §7).
--
-- Requires tempwork_admin_role membership (event trigger reassigns new tempwork tables).
-- =====================================================================================

-- DROP PROCEDURE tempwork.sp_dbx_creative_get_changes_for_databricks_ctv_poc(text);

CREATE OR REPLACE PROCEDURE tempwork.sp_dbx_creative_get_changes_for_databricks_ctv_poc(IN timestamp_string text)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    c_flag_timestamp  timestamp without time zone;
    ca_flag_timestamp timestamp without time zone;

BEGIN
-- Split the input string by comma and cast each to timestamp
    c_flag_timestamp := split_part(timestamp_string, ',', 1)::timestamp;
    ca_flag_timestamp := split_part(timestamp_string, ',', 2)::timestamp;

-- =============================================================================
-- Stored Procedure  Name: creatives.creative_get_changes_for_databricks
-- Author: Akash Sharma
-- Date Created: 2024-11-11
-- Last Modified By:2026-06-24 Alay Makwana
--
-- Description:
-- This Stored Procedure is used to truncate and create tempwork.creative_forsync_tmp_ctv_poc table
-- And based on last updated timestamp of previous batch pouplate all the records that helps in Sync with mrdpp_dev.gold.creative table
-- Also joins the records pushed by databricks which had missing product's reverse_translation i.e tempwork.creatives_advert_hold_tmp_ctv_poc
--
-- Parameters:
-- c_flag_timestamp = updated timestamp to read the changes from creative table.
-- ca_flag_timestamp = updated timestamp to read the changes from creative_archive table.
--
-- Return:
-- Create a table tempwork.creative_forsync_tmp_ctv_poc
--
-- Dependencies:
-- Dependent tables : tempwork.creative_forsync_tmp_ctv_poc, tempwork.creatives_advert_hold_tmp_ctv_poc
--
-- Example Usage:
-- CALL tempwork.sp_dbx_creative_get_changes_for_databricks_ctv_poc(:timestamp_string);
--
-- Revision History:
-- 2025-03-05 - Akash Sharma - Added documentation comments.
-- 2025-03-27 - Bhaskara Kadi -- Appeding has_no_audio field to creative payload.
-- 2025-07-31 - Akash Sharma --  Added code to extract updated creatives from creative_archive Table.
-- 2025-09-18 - Akash Sharma --  Added first_seen_provider_campaign_landing_page column.
-- 2026-02-24 - Patrick Kibe -- Add machine_learning_payload column to sync to databricks
-- 2026-03-13 - Patrick Kibe -- Add holding logic for BIS Social creatives that need to be classified using classification engine
-- 2026-06-12 - Patrick Kibe -- Add logic for creatives to bypass holding if there is no AdClarity URL which would make them able to go to CE
-- 2026-06-24 - Vipul Dholetar -- Prevent duplicate records send to databricks
-- 2026-06-24 - Alay Makwana -- Add logic for TV Creative holding Mechanisim MRVXVC-16002 + existing logic modified
-- =============================================================================
perform 1
FROM information_schema.tables
WHERE table_schema = 'tempwork' and
    table_name = 'creative_forsync_tmp_ctv_poc';
if FOUND THEN
    EXECUTE format('truncate tempwork.creative_forsync_tmp_ctv_poc');
ELSE
    CREATE TABLE if not exists tempwork.creative_forsync_tmp_ctv_poc (
  creative_id int8 NOT NULL,
  country_iso_2_code varchar(2) NULL,
  provider_id varchar(20) NULL,
  source_channel_id varchar(20) NULL,
  provider_creative_id int8 NULL,
  creative_url_hash int8 NULL,
  creative_type varchar(20) NULL,
  creative_mime_type varchar(50) NULL,
  creative_width int4 NULL,
  creative_height int4 NULL,
  creative_duration int4 NULL,
  creative_duration_bucket text NULL,
  creative_tier_id int2 NULL,
  primary_language_code text NULL,
  primary_product_id int8 NULL,
  secondary_products jsonb NULL,
  mr_company_id int8 NULL,
  classification_type varchar(20) NULL,
  classified_by_user_id int4 NULL,
  classification_comments text NULL,
  classified_timestamp timestamp NULL,
  created_timestamp timestamp NULL,
  updated_timestamp timestamp NULL,
  classification_process_step varchar(100) NULL,
  asset_source_server_id int4 NULL,
  creative_title varchar NULL,
  creative_headline varchar NULL,
  attribution_first_audio varchar NULL,
  attribution_lead_text varchar NULL,
  attribution_visual varchar NULL,
  attribution_summary varchar NULL,
  attribution_other_details varchar NULL,
  attribution_description varchar(2000) null,
  attribution_hashtag varchar NULL,
  attribution_competitor jsonb NULL,
  attribution_celebrity jsonb NULL,
  attribution_slogan_tagline varchar NULL,
  attribution_revision_description varchar NULL,
  attribution_comments varchar NULL,
  attribution_creative_tags varchar NULL,
  custom_attribute jsonb NULL,
  attribution_timestamp timestamp NULL,
  attribution_by_user_id int4 NULL,
  attribution_status varchar(100) NULL,
  is_sponsored_video bool NULL,
  is_component_eligible bool NULL,
  component_entry_status varchar(100) NULL,
  component_entry_by_user_id int4 NULL,
  component_entry_timestamp timestamp NULL,
  has_additional_multi_product bool NULL,
  has_additional_coop_product bool NULL,
  send_to_adscope_unattributed bool NULL,
  first_seen_media text NULL,
  first_seen_provider_occurrence_id text null,
  first_seen_occurrence_id int8 NULL,
  first_seen_occurrence_timestamp timestamp NULL,
  first_seen_provider_code text NULL,
  first_seen_media_property_id int4 NULL,
  first_seen_media_property_name varchar(100) NULL,
  first_seen_media_category_id int4 NULL,
  first_seen_media_category_code varchar(100) NULL,
  first_seen_provider_creative_link_url text NULL,
  first_seen_provider_publisher_id int8 NULL,
  first_seen_provider_publisher_domain varchar NULL,
  first_seen_provider_campaign_id int8 NULL,
  first_seen_provider_campaign_name text NULL,
  first_seen_provider_advertiser_id int8 NULL,
  first_seen_provider_advertiser_name text NULL,
  first_seen_provider_product_id int8 NULL,
  first_seen_provider_product_name text NULL,
  first_seen_provider_campaign_landing_page text NULL,
  first_seen_market_id int2 NULL,
  first_seen_market_name varchar(50) NULL,
  first_seen_daypart_id int2 NULL,
  first_seen_daypart_name varchar(50) NULL,
  first_seen_affiliate_id int4 NULL,
  first_seen_affiliate_name varchar(50) NULL,
  due_timestamp timestamp NULL,
  last_seen_timestamp timestamptz NULL,
  occurrence_description text NULL,
  historical_creative_md5 text NULL,
  legacy_creative_id int8 NULL,
  creative_payload jsonb NULL,
  machine_learning_payload jsonb NULL,
  print_los_id int4 NULL,
  print_ad_type_id int4 NULL,
  print_ad_nli float8 NULL,
  print_ad_equ float8 NULL,
  print_ad_col_inch float8 NULL,
  print_ad_weighted_col_inch float8 NULL,
  print_ad_cost float8 NULL,
  print_ad_size float8 NULL,
  print_null_cost_comments varchar NULL,
  print_recalculate_cost bool NULL,
  is_resegment bool NULL,
  print_matching_ads jsonb NULL,
  print_ad_images jsonb NULL,
  product_mapping_status varchar NULL,
  keywords varchar NULL,
  is_reclassified bool NULL,
  print_no_cost bool NULL,
  is_archive bool NULL
);
END if;

	--Create new table that will be the holding table for creatives that are still waiting from classification engine
	--This table will never be truncated and will only have records that should not move forward
	CREATE TABLE IF NOT EXISTS tempwork.creative_classification_engine_holding_ctv_poc (creative_id INT8 NOT NULL, inserted_at TIMESTAMP NOT NULL);

	--Create index on the holding table
	CREATE INDEX IF NOT EXISTS creative_classification_engine_holding_creative_idx_ctv_poc ON ONLY tempwork.creative_classification_engine_holding_ctv_poc USING btree (creative_id);


	--Drop temp table of creatives with holding flag
	DROP TABLE IF EXISTS tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc;


--Get all creatives with updated_timestamp and determine hold flag
CREATE TABLE tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc AS (
    WITH crtvs AS (
        --Get newly updated creatives
        SELECT *
        FROM tempwork.creative_ctv_poc c
        WHERE c.updated_timestamp >= c_flag_timestamp
          AND c.classification_type_id IS NOT NULL
          AND c.provider_id NOT IN ('14', '15')

        UNION

        --Get creatives in holding for reevaluation
        SELECT *
        FROM tempwork.creative_ctv_poc c
        WHERE EXISTS (
            SELECT 1
            FROM tempwork.creative_classification_engine_holding_ctv_poc holding
            WHERE holding.creative_id = c.creative_id
        )
    )
    SELECT
        c.*, --We need to select all values from creative table to avoid out of sync issues

        CASE
            -- =====================================================================
            -- PRE-CHECKS: Common release conditions across all providers
            -- Checked first so we exit early without entering provider-specific logic
            -- =====================================================================

            --Creative was created before the holding start date → release
            WHEN c.created_timestamp < cef.start_date THEN FALSE

			WHEN c.provider_id in (2,19) THEN FALSE --BIS, TROI

            --No usable adclarity_url then release
			WHEN c.provider_id in (16,18) -- Social, CTV
				AND COALESCE(c.creative_payload ->> 'adclarity_url', '') = '' THEN FALSE

			--If child set to false and have parent/child check later overwrite this flag
			WHEN cdm.child_creative_id IS NOT NULL THEN FALSE

            --Non-advert type, already attributed, manually classified, QA verified, or human user → release
            WHEN c.classification_type_id IN (2, 3, 4)                 --(BadAd, NonAd, SplitRequest)
                OR c.attribution_status_id IN (2, 3, 4)                --(NonAd, BadAd, Attributed)
                OR (c.classification_type_id = 1 					   -- Advert
                    AND (c.classification_process_step_id = 3 		   -- Manual classification
						OR c.is_qaverified_for_class = TRUE            --QA verified
                        OR c.classified_by_user_id > 0))               --Classified by  human user
            THEN FALSE

			-- =====================Advert Records only================================

            WHEN c.provider_id IN (11, 13, 18) THEN TRUE --TV / PlayOn / CTV

            -- =====================================================================
            -- BLOCK 1a: Social (provider 16) — specific media_property_id
            -- cef joins on real mp_id from creative_first_seen for this provider
            -- =====================================================================
            WHEN c.provider_id = 16
            THEN
                CASE
                    --AI Auto classified (step=5) → release (CE already processed this creative)
                    WHEN c.classification_process_step_id = 5 THEN FALSE

                    --CE staging entry failed or errored → release
                    WHEN cacs.status_id IN (2, 3) THEN FALSE

                    --CE responded with success AND above confidence threshold → hold (reclassification update is coming)
                    WHEN cacs.status_id = 1 AND cacs.confidence_score >= cef.score_threshold THEN TRUE

                    --CE responded with success AND below confidence threshold → release (no reclassification)
                    WHEN cacs.status_id = 1 AND cacs.confidence_score < cef.score_threshold THEN FALSE

                    --No staging entry yet AND still within timeout window → hold (waiting for CE to pick up)
                    WHEN cacs.creative_id IS NULL
                        AND c.classified_timestamp + cef.timeout_threshold::INTERVAL > NOW() AT TIME ZONE 'UTC' THEN TRUE

                    --No staging entry AND timeout exceeded → release (CE never picked it up)
                    WHEN cacs.creative_id IS NULL
                        AND c.classified_timestamp + cef.timeout_threshold::INTERVAL <= NOW() AT TIME ZONE 'UTC' THEN FALSE

                    --Staging sent, no response AND send-timeout exceeded → release (CE unresponsive)
                    WHEN cacs.creative_id IS NOT NULL AND cacs.received_timestamp IS NULL
                        AND NOW() AT TIME ZONE 'UTC' > cacs.sent_timestamp + cef.timeout_threshold::INTERVAL THEN FALSE

                    --Fallback: staging entry exists but no response yet and within timeout → hold
                    ELSE TRUE
                END

            --No config match or qualifying conditions not met → release
            ELSE FALSE

        END AS hold_flag

    FROM crtvs c

    --Resolve media_property_id for Block 1 (Social/Digital)
    LEFT JOIN tempwork.creative_first_seen_ctv_poc cfs
        ON cfs.creative_id = c.creative_id
        AND cfs.provider_id = c.provider_id

    --Single config join covering both specific mp_id (Block 1) and wildcard -1 (Block 2)
    LEFT JOIN config.classification_engine_filters cef
        ON cef.data_provider_id = c.provider_id
        AND cef.is_active = TRUE
        AND cef.filter_column = 'all'
        AND (
            cef.media_property_id = -1                        --Block 2: wildcard providers
            OR cef.media_property_id = cfs.media_property_id  --Block 1: specific mp_id match
        )

    --AI staging results — used in Block 1 CE pipeline only
    LEFT JOIN tempwork.creative_ai_classification_staging_vx0_ctv_poc cacs
        ON cacs.creative_id = c.creative_id

	LEFT JOIN tempwork.creative_dedupe_map_ctv_poc cdm ON cdm.child_creative_id = c.creative_id
);


RAISE NOTICE 'Got list of creatives';

--Create Indexes on creative_id table with holding flags
CREATE INDEX crtvs_with_hold_flag_creative_idx_ctv_poc ON ONLY tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc USING btree (creative_id);
CREATE INDEX crtvs_with_hold_flag_hold_flag_idx_ctv_poc ON ONLY tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc USING btree (hold_flag);
CREATE INDEX crtvs_with_hold_flag_mime_type_idx_ctv_poc ON ONLY tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc USING btree (creative_mime_type_id);
CREATE INDEX crtvs_with_hold_flag_slogan_tagline_idx_ctv_poc ON ONLY tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc USING btree (attribution_slogan_tagline_id);
CREATE INDEX crtvs_with_hold_flag_url_hash_idx_ctv_poc ON ONLY tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc USING btree (creative_url_hash);
CREATE INDEX crtvs_with_hold_flag_crtv_type_id_idx_ctv_poc ON ONLY tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc USING btree (creative_type_id);
CREATE INDEX crtvs_with_hold_flag_att_status_idx_ctv_poc ON ONLY tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc USING btree (attribution_status_id);
CREATE INDEX crtvs_with_hold_flag_comp_entry_idx_ctv_poc ON ONLY tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc USING btree (component_entry_status_id);
CREATE INDEX crtvs_with_hold_flag_process_step_idx_ctv_poc ON ONLY tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc USING btree (classification_process_step_id);
CREATE INDEX crtvs_with_hold_flag_media_id_idx_ctv_poc ON ONLY tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc USING btree (media_id);
CREATE INDEX crtvs_with_hold_flag_class_type_idx_ctv_poc ON ONLY tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc USING btree (classification_type_id);

--If parent has hold_flag = TRUE then child must get updated to be the same
MERGE INTO tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc TARGET
USING (
  --Get Parents with hold flag = TRUE status and child ids
  SELECT parent_creative_id, child_creative_id
  FROM tempwork.creative_dedupe_map_ctv_poc cdm
  WHERE EXISTS (
    SELECT 1
    FROM tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc hold
    WHERE hold.creative_id = cdm.parent_creative_id
      AND hold.hold_flag = TRUE
  )
) SOURCE ON SOURCE.child_creative_id = TARGET.creative_id
WHEN MATCHED THEN UPDATE SET hold_flag = TRUE;

RAISE NOTICE 'starting insert';

INSERT INTO tempwork.creative_forsync_tmp_ctv_poc (
 with cte_new_creative as --cte to get all the new updated creatives
  (
    SELECT
    c.creative_id,
    c.country_iso_2_code,
    c.provider_id,
    c.source_channel_id,
    c.provider_creative_id,
    c.creative_url_hash,
    c.creative_mime_type_id,
    c.creative_type_id ,
    c.creative_width,
    c.creative_height,
    c.creative_duration,
    c.creative_tier_id,
    c.primary_language_code,
    c.classification_type_id,
    c.classified_by_user_id,
    c.classification_comments,
    c.classified_timestamp,
    c.created_timestamp,
    c.updated_timestamp,
    c.classification_process_step_id ,
    c.asset_source_server_id,
    c.title ,
    c.headline ,
    c.attribution_first_audio,
    c.attribution_lead_text,
    c.attribution_visual,
    c.attribution_summary,
    c.attribution_other_details,
    c.attribution_description,
    c.attribution_hashtag,
    c.attribution_slogan_tagline_id ,
    c.attribution_revision_description,
    c.attribution_comments,
    c.attribution_creative_tags,
    c.custom_attributes,
    c.attribution_timestamp,
    c.attribution_by_user_id,
    c.attribution_status_id,
    c.is_sponsored_video,
    c.is_component_eligible,
    c.component_entry_status_id,
    c.component_entry_by_user_id,
    c.component_entry_timestamp,
    c.has_additional_multi_product,
    c.has_additional_coop_product,
    c.send_to_adscope_unattributed,
    c.media_id,
    c.occurrence_description,
    c.legacy_creative_id,
    c.machine_learning_payload,
    c.transcription_edited,
    c.creative_payload ,
    c.print_los_id,
    c.print_ad_type_id,
    c.print_ad_nli,
    c.print_ad_equ,
    c.print_ad_col_inch,
    c.print_ad_weighted_col_inch,
    c.print_ad_cost,
    c.print_ad_size,
    c.print_null_cost_comments,
    c.print_recalculate_cost,
    c.is_resegment,
    c.print_matching_ads,
    c.print_ad_images,
    c.product_mapping_status,
    c.keywords,
    c.is_reclassified,
    c.print_no_cost,
    False as is_archive

    --Now selecting from jobwork table of creatives with holding flag
    FROM tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc c
    WHERE c.hold_flag = FALSE

   UNION
    select
    ca.creative_id,
    ca.country_iso_2_code,
    ca.provider_id,
    ca.source_channel_id,
    ca.provider_creative_id,
    ca.creative_url_hash,
    ca.creative_mime_type_id,
    ca.creative_type_id ,
    ca.creative_width,
    ca.creative_height,
    ca.creative_duration,
    ca.creative_tier_id,
    ca.primary_language_code,
    ca.classification_type_id,
    ca.classified_by_user_id,
    ca.classification_comments,
    ca.classified_timestamp,
    ca.created_timestamp,
    ca.updated_timestamp,
    ca.classification_process_step_id ,
    ca.asset_source_server_id,
    ca.title ,
    ca.headline ,
    ca.attribution_first_audio,
    ca.attribution_lead_text,
    ca.attribution_visual,
    ca.attribution_summary,
    ca.attribution_other_details,
    ca.attribution_description,
    ca.attribution_hashtag,
    ca.attribution_slogan_tagline_id ,
    ca.attribution_revision_description,
    ca.attribution_comments,
    ca.attribution_creative_tags,
    ca.custom_attributes,
    ca.attribution_timestamp,
    ca.attribution_by_user_id,
    ca.attribution_status_id,
    ca.is_sponsored_video,
    ca.is_component_eligible,
    ca.component_entry_status_id,
    ca.component_entry_by_user_id,
    ca.component_entry_timestamp,
    ca.has_additional_multi_product,
    ca.has_additional_coop_product,
    ca.send_to_adscope_unattributed,
    ca.media_id,
    ca.occurrence_description,
    ca.legacy_creative_id,
    ca.machine_learning_payload,
    null as transcription_edited,
    ca.creative_payload ,
    ca.print_los_id,
    ca.print_ad_type_id,
    ca.print_ad_nli,
    ca.print_ad_equ,
    ca.print_ad_col_inch,
    ca.print_ad_weighted_col_inch,
    ca.print_ad_cost,
    ca.print_ad_size,
    ca.print_null_cost_comments,
    ca.print_recalculate_cost,
    ca.is_resegment,
    ca.print_matching_ads,
    ca.print_ad_images,
    ca.product_mapping_status,
    ca.keywords,
    ca.is_reclassified,
    ca.print_no_cost,
    True as is_archive
    FROM
    creatives.creative_archive ca
    WHERE ca.updated_timestamp > ca_flag_timestamp
    AND ca.classification_type_id is not null
    AND ca.provider_id NOT IN ('4','14', '15')
      ),
  cte_all_creatives as --cte to get the creatives that we pushed from databricks
  (
    SELECT
    c.creative_id,
    c.country_iso_2_code,
    c.provider_id,
    c.source_channel_id,
    c.provider_creative_id,
    c.creative_url_hash,
    c.creative_mime_type_id,
    c.creative_type_id ,
    c.creative_width,
    c.creative_height,
    c.creative_duration,
    c.creative_tier_id,
    c.primary_language_code,
    c.classification_type_id,
    c.classified_by_user_id,
    c.classification_comments,
    c.classified_timestamp,
    c.created_timestamp,
    c.updated_timestamp,
    c.classification_process_step_id ,
    c.asset_source_server_id,
    c.title ,
    c.headline ,
    c.attribution_first_audio,
    c.attribution_lead_text,
    c.attribution_visual,
    c.attribution_summary,
    c.attribution_other_details,
    c.attribution_description,
    c.attribution_hashtag,
    c.attribution_slogan_tagline_id ,
    c.attribution_revision_description,
    c.attribution_comments,
    c.attribution_creative_tags,
    c.custom_attributes,
    c.attribution_timestamp,
    c.attribution_by_user_id,
    c.attribution_status_id,
    c.is_sponsored_video,
    c.is_component_eligible,
    c.component_entry_status_id,
    c.component_entry_by_user_id,
    c.component_entry_timestamp,
    c.has_additional_multi_product,
    c.has_additional_coop_product,
    c.send_to_adscope_unattributed,
    c.media_id,
    c.occurrence_description,
    c.legacy_creative_id,
    c.machine_learning_payload,
    c.transcription_edited,
    c.creative_payload ,
    c.print_los_id,
    c.print_ad_type_id,
    c.print_ad_nli,
    c.print_ad_equ,
    c.print_ad_col_inch,
    c.print_ad_weighted_col_inch,
    c.print_ad_cost,
    c.print_ad_size,
    c.print_null_cost_comments,
    c.print_recalculate_cost,
    c.is_resegment,
    c.print_matching_ads,
    c.print_ad_images,
    c.product_mapping_status,
    c.keywords,
    c.is_reclassified,
    c.print_no_cost,
    c.is_archive
    FROM
      cte_new_creative c

    UNION
    SELECT
    c2.creative_id,
    c2.country_iso_2_code,
    c2.provider_id,
    c2.source_channel_id,
    c2.provider_creative_id,
    c2.creative_url_hash,
    c2.creative_mime_type_id,
    c2.creative_type_id ,
    c2.creative_width,
    c2.creative_height,
    c2.creative_duration,
    c2.creative_tier_id,
    c2.primary_language_code,
    c2.classification_type_id,
    c2.classified_by_user_id,
    c2.classification_comments,
    c2.classified_timestamp,
    c2.created_timestamp,
    c2.updated_timestamp,
    c2.classification_process_step_id ,
    c2.asset_source_server_id,
    c2.title ,
    c2.headline ,
    c2.attribution_first_audio,
    c2.attribution_lead_text,
    c2.attribution_visual,
    c2.attribution_summary,
    c2.attribution_other_details,
    c2.attribution_description,
    c2.attribution_hashtag,
    c2.attribution_slogan_tagline_id ,
    c2.attribution_revision_description,
    c2.attribution_comments,
    c2.attribution_creative_tags,
    c2.custom_attributes,
    c2.attribution_timestamp,
    c2.attribution_by_user_id,
    c2.attribution_status_id,
    c2.is_sponsored_video,
    c2.is_component_eligible,
    c2.component_entry_status_id,
    c2.component_entry_by_user_id,
    c2.component_entry_timestamp,
    c2.has_additional_multi_product,
    c2.has_additional_coop_product,
    c2.send_to_adscope_unattributed,
    c2.media_id,
    c2.occurrence_description,
    c2.legacy_creative_id,
    c2.machine_learning_payload,
    c2.transcription_edited,
    c2.creative_payload ,
    c2.print_los_id,
    c2.print_ad_type_id,
    c2.print_ad_nli,
    c2.print_ad_equ,
    c2.print_ad_col_inch,
    c2.print_ad_weighted_col_inch,
    c2.print_ad_cost,
    c2.print_ad_size,
    c2.print_null_cost_comments,
    c2.print_recalculate_cost,
    c2.is_resegment,
    c2.print_matching_ads,
    c2.print_ad_images,
    c2.product_mapping_status,
    c2.keywords,
    c2.is_reclassified,
    c2.print_no_cost,
    False as is_archive
    FROM
      tempwork.creative_ctv_poc c2
      INNER JOIN tempwork.creatives_advert_hold_tmp_ctv_poc s on s.creative_id = c2.creative_id
      WHERE c2.provider_id NOT IN ('4','14', '15')
			AND NOT (c2.updated_timestamp >= c_flag_timestamp
           	AND c2.classification_type_id IS NOT NULL
           	AND c2.provider_id NOT IN ('14', '15'))

      UNION

    SELECT
    ca.creative_id,
    ca.country_iso_2_code,
    ca.provider_id,
    ca.source_channel_id,
    ca.provider_creative_id,
    ca.creative_url_hash,
    ca.creative_mime_type_id,
    ca.creative_type_id ,
    ca.creative_width,
    ca.creative_height,
    ca.creative_duration,
    ca.creative_tier_id,
    ca.primary_language_code,
    ca.classification_type_id,
    ca.classified_by_user_id,
    ca.classification_comments,
    ca.classified_timestamp,
    ca.created_timestamp,
    ca.updated_timestamp,
    ca.classification_process_step_id ,
    ca.asset_source_server_id,
    ca.title ,
    ca.headline ,
    ca.attribution_first_audio,
    ca.attribution_lead_text,
    ca.attribution_visual,
    ca.attribution_summary,
    ca.attribution_other_details,
    ca.attribution_description,
    ca.attribution_hashtag,
    ca.attribution_slogan_tagline_id ,
    ca.attribution_revision_description,
    ca.attribution_comments,
    ca.attribution_creative_tags,
    ca.custom_attributes,
    ca.attribution_timestamp,
    ca.attribution_by_user_id,
    ca.attribution_status_id,
    ca.is_sponsored_video,
    ca.is_component_eligible,
    ca.component_entry_status_id,
    ca.component_entry_by_user_id,
    ca.component_entry_timestamp,
    ca.has_additional_multi_product,
    ca.has_additional_coop_product,
    ca.send_to_adscope_unattributed,
    ca.media_id,
    ca.occurrence_description,
    ca.legacy_creative_id,
    ca.machine_learning_payload,
    null as transcription_edited,
    ca.creative_payload ,
    ca.print_los_id,
    ca.print_ad_type_id,
    ca.print_ad_nli,
    ca.print_ad_equ,
    ca.print_ad_col_inch,
    ca.print_ad_weighted_col_inch,
    ca.print_ad_cost,
    ca.print_ad_size,
    ca.print_null_cost_comments,
    ca.print_recalculate_cost,
    ca.is_resegment,
    ca.print_matching_ads,
    ca.print_ad_images,
    ca.product_mapping_status,
    ca.keywords,
    ca.is_reclassified,
    ca.print_no_cost,
    True as is_archive
    FROM
      creatives.creative_archive ca
      INNER JOIN tempwork.creatives_advert_hold_tmp_ctv_poc s on s.creative_id = ca.creative_id
      WHERE ca.provider_id NOT IN ('14','4', '15')
      ),
  cust_attr as --cte for making custom_attributes schema appropriate to Databricks
  (
    SELECT
      creative_id,
      jsonb_agg(
        json_build_object(
          'fKey',
          arr -> 'fKey',
          'selectedOptValue',
          CASE WHEN arr -> 'gKey' is not null THEN arr -> 'selectedOptValue' ELSE arr -> 'respValue' end,
          'subfKey',
          arr -> 'subfKey',
          'respValue',
          CASE WHEN arr -> 'gKey' is not null THEN arr -> 'respValue' ELSE null end,
          'unit',
          CASE WHEN (arr ->> 'unit') != '' THEN arr -> 'unit' ELSE null end
        )
      ) custom_attribute
    FROM
      (
        SELECT
          creative_id,
          jsonb_array_elements(custom_attributes) arr
        FROM
          cte_all_creatives
      ) expl_attr
    group by
      creative_id
  ),
  cte_creative_attr as --cte for getting celebrity related data for creatives
  (
    SELECT
      c.*,
      array_agg(o."label") over (
        partition by c.creative_id, c.creative_url_hash,
        cl.celebrity_id
      ) as type,
      cl.celebrity_id,
      cl.first_name,
      cl.last_name
    FROM
      cte_all_creatives c
      LEFT JOIN tempwork.creative_celebrity_ctv_poc cc on cc.creative_id = c.creative_id
      LEFT JOIN reference.celebrity cl on cl.celebrity_id = cc.celebrity_id
      LEFT JOIN reference.celebrity_type_mapping ctm on ctm.celebrity_id = cl.celebrity_id
      LEFT JOIN config.option o on o.optionid = 19
      and o.indexid = ctm.celebrity_type_id
  ),
  cte_creative_last_run as --cte for getting last_Seen_Timestamp for creatives
  (
    SELECT
      c.creative_id,
      c.creative_url_hash,
      max(cs.last_run) as last_seen_timestamp
    FROM
      --cte_creative_attr c --MRVXVC-15333
	  cte_all_creatives c
      LEFT JOIN tempwork.creative_occurrence_summary_ctv_poc cs on c.creative_id = cs.creative_id
      and c.creative_url_hash = cs.creative_url_hash
      and c.media_id = cs.media_id
    GROUP BY
      c.creative_id,
      c.creative_url_hash,
      c.media_id
  )
  select distinct
    c.creative_id,
    c.country_iso_2_code,
    c.provider_id,
    c.source_channel_id,
    c.provider_creative_id,
    c.creative_url_hash,
    ct2.creative_type,
    smt.description as creative_mime_type,
    c.creative_width,
    c.creative_height,
    c.creative_duration,
    CASE WHEN c.creative_duration BETWEEN 1 AND 10 THEN '1-10 seconds'
	WHEN c.creative_duration BETWEEN 11 AND 15 THEN '11-15 seconds'
	WHEN c.creative_duration BETWEEN 16 AND 30 THEN '16-30 seconds'
	WHEN c.creative_duration BETWEEN 31 AND 45 THEN '31-45 seconds'
	WHEN c.creative_duration BETWEEN 46 AND 60 THEN '46-60 seconds'
	WHEN c.creative_duration > 60 THEN '> 60 seconds'
	ELSE NULL
	END AS creative_duration_bucket,
    c.creative_tier_id,
    c.primary_language_code,
    CASE WHEN cpm.is_primary = true THEN cpm.product_id END AS primary_product_id,
    CASE WHEN (
      cpm.is_primary = false
      or cpm.is_primary is null
    )
    and cpm.product_id is not null THEN jsonb_build_object(
      'sort_order', cpm.sort_order, 'type',
      cpm.map_type, 'subtype', cpm.map_sub_type,
      'product_id', cpm.product_id, 'is_dominant',
      cpm.is_dominant
    ) END AS secondary_products,
    null::int4 as mr_company_id,
    ct.classification_type,
    c.classified_by_user_id,
    c.classification_comments,
    c.classified_timestamp,
    c.created_timestamp,
    c.updated_timestamp,
    cps.classification_process_step,
    c.asset_source_server_id,
    c.title as creative_title,
    c.headline as creative_headline,
    c.attribution_first_audio,
    c.attribution_lead_text,
    c.attribution_visual,
    c.attribution_summary,
    c.attribution_other_details,
    c.attribution_description,
    c.attribution_hashtag,
    CASE WHEN cm.competitor_id is null THEN null else jsonb_build_object ('id', cm.competitor_id) end as attriubtion_competitor,
    jsonb_build_object (
      'id', c.celebrity_id, 'first_name',
      c.first_name, 'last_name', c.last_name,
      'type', case when c.type[1] is  null then null else c.type end
    ) as attriubtion_celebrity,
    st.slogan_tagline as attribution_slogan_tagline,
    c.attribution_revision_description,
    c.attribution_comments,
    c.attribution_creative_tags,
    ctr.custom_attribute,
    c.attribution_timestamp,
    c.attribution_by_user_id,
    as2.attribution_status,
    c.is_sponsored_video,
    c.is_component_eligible,
    ces.component_entry_status,
    c.component_entry_by_user_id,
    c.component_entry_timestamp,
    c.has_additional_multi_product,
    c.has_additional_coop_product,
    c.send_to_adscope_unattributed,
    m.display_n as first_seen_media,
    cfs.occurrence_id as first_seen_provider_occurrence_id,
    case when c.media_id =  3 then c.legacy_creative_id else null end as first_seen_occurrence_id,
    cfs.occurrence_timestamp as first_seen_occurrence_timestamp,
    dp.data_provider_code as first_seen_provider_code,
    cfs.media_property_id as first_seen_media_property_id,
    cfs.media_property_name as first_seen_media_property_name,
    cfs.media_category_id as first_seen_media_category_id,
    cfs.media_category_code as first_seen_media_category_code,
    cfs.provider_creative_link_url as first_seen_provider_creative_link_url,
    cfs.provider_publisher_id as first_seen_provider_publisher_id,
    cfs.provider_publisher_domain as first_seen_provider_publisher_domain,
    cfs.provider_campaign_id as first_seen_provider_campaign_id,
    cfs.provider_campaign_name as first_seen_provider_campaign_name,
    cfs.provider_advertiser_id as first_seen_provider_advertiser_id,
    cfs.provider_advertiser_name as first_seen_provider_advertiser_name,
    cfs.provider_product_id as first_seen_provider_product_id,
    cfs.provider_product_name as first_seen_provider_product_name,
	cfs.provider_campaign_landing_page as first_seen_provider_campaign_landing_page,
    cfs.market_id as first_seen_market_id,
    cfs.market_name as first_seen_market_name,
    cfs.daypart_id as first_seen_daypart_id,
    cfs.daypart_name as first_seen_daypart_name,
    cfs.affiliate_id as first_seen_affiliate_id,
    cfs.affiliate_name as first_seen_affiliate_name,
    cfs.due_timestamp,
    clr.last_seen_timestamp,
    c.occurrence_description,
    cast(null as text) as historical_creative_md5,
    c.legacy_creative_id,
   -- cast(coalesce(c.creative_payload, '{}') as jsonb) ||
   -- cast('{"has_no_audio": ' || COALESCE(((machine_learning_payload ->>'creative_details')::jsonb->>'has_no_audio')::boolean, 'FALSE')::TEXT || '}' as jsonb)
   -- AS creative_payload,

  	COALESCE(c.creative_payload, '{}'::jsonb)
  	|| cast('{"has_no_audio": ' || COALESCE(((machine_learning_payload ->>'creative_details')::jsonb->>'has_no_audio')::boolean, 'FALSE')::TEXT || '}' as jsonb)
  	|| jsonb_build_object(
       'transcription',
       COALESCE(
         NULLIF(c.transcription_edited, ''),
         NULLIF(machine_learning_payload -> 'creative_details' ->> 'transcription_en', ''),
         NULLIF(c.machine_learning_payload -> 'creative_details'->>'transcription_original', '')
       )
     ) AS creative_payload,
	c.machine_learning_payload,
    c.print_los_id,
    c.print_ad_type_id,
    c.print_ad_nli,
    c.print_ad_equ,
    c.print_ad_col_inch,
    c.print_ad_weighted_col_inch,
    c.print_ad_cost,
    c.print_ad_size,
    c.print_null_cost_comments,
    c.print_recalculate_cost,
    c.is_resegment,
    c.print_matching_ads,
    c.print_ad_images,
    c.product_mapping_status,
    c.keywords,
    c.is_reclassified,
    c.print_no_cost,
    c.is_archive
  FROM
    cte_creative_attr c
    LEFT JOIN tempwork.creative_product_ctv_poc cpm on c.creative_id = cpm.creative_id
    LEFT JOIN reference.standard_mime_type smt on smt.mime_type_id = c.creative_mime_type_id
    LEFT JOIN reference.slogan_tagline st on st.slogan_tagline_id = c.attribution_slogan_tagline_id
    LEFT JOIN tempwork.creative_first_seen_ctv_poc cfs on cfs.creative_id = c.creative_id
   	 AND cfs.creative_url_hash = c.creative_url_hash
	 --AND cfs.provider_id = c.provider_id --MRVXVC-15333
    LEFT JOIN tempwork.creative_competitor_ctv_poc cm on cm.creative_id = c.creative_id
--    LEFT JOIN mapping.provider_metadata_creative_map mp on mp.creative_url_hash = c.creative_url_hash --MRVXVC-15333
    LEFT JOIN cust_attr ctr on c.creative_id = ctr.creative_id
    LEFT JOIN cte_creative_last_run clr on clr.creative_id = c.creative_id
   	 AND clr.creative_url_hash = c.creative_url_hash
    LEFT JOIN reference.creative_type ct2 on ct2.creative_type_id = c.creative_type_id
    LEFT JOIN reference.attribution_status as2 on as2.attribution_status_id = c.attribution_status_id
    LEFT JOIN reference.component_entry_status ces on ces.component_entry_status_id = c.component_entry_status_id
    LEFT JOIN reference.classification_process_step cps on cps.classification_process_step_id = c.classification_process_step_id
    LEFT JOIN reference.media m on m.media_id = c.media_id
    LEFT JOIN reference.classification_type ct on c.classification_type_id = ct.classification_type_id
 	LEFT JOIN reference.data_provider dp   on cfs.provider_id = dp.data_provider_id
--	ORDER BY c.creative_id,c.updated_timestamp DESC

    );
END;
$procedure$
;


-- DROP PROCEDURE tempwork.sp_dbx_component_get_changes_for_databricks_ctv_poc(timestamp);

CREATE OR REPLACE PROCEDURE tempwork.sp_dbx_component_get_changes_for_databricks_ctv_poc(IN p_flag_timestamp_cc timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
begin
-- =============================================================================
-- Stored Procedure  Name: tempwork.sp_dbx_component_get_changes_for_databricks_ctv_poc
-- Author: Akash Sharma
-- Date Created: 2024-11-11
-- Last Modified By: Akash Sharma on 2025-03-05
--
-- Description:
-- This Stored Procedure is used to truncate and create tempwork.component_coding_forsync_tmp_ctv_poc table
-- And based on last updated timestamp of previous batch pouplate all the records that helps in Sync with mrdpp_dev.gold.component_coding table
-- Also joins the records pushed by databricks which had missing component's product's reverse_translation i.e tempwork.component_hold_creative_tmp_ctv_poc
--
-- Parameters:
-- p_flag_timestamp_cc =  updated timestamp to read the changes from component table
--
-- Return:
-- Create a table tempwork.component_coding_forsync_tmp_ctv_poc
--
-- Dependencies:
-- Dependent tables : tempwork.component_coding_forsync_tmp_ctv_poc, tempwork.component_hold_creative_tmp_ctv_poc
--
-- Example Usage:
-- CALL creatives creatives.component_get_changes_for_databricks(:p_flag_timestamp_cc);
--
-- Revision History:
-- 2024-11-11 - Akash Sharma - Added documentation comments.
-- =============================================================================
	perform 1
FROM information_schema.tables
WHERE table_schema = 'tempwork' and
	table_name = 'component_coding_forsync_tmp_ctv_poc';
IF FOUND THEN
  EXECUTE format('truncate tempwork.component_coding_forsync_tmp_ctv_poc');
ELSE
  CREATE TABLE if not exists tempwork.component_coding_forsync_tmp_ctv_poc (
  component_coding_id int8 not null,
  creative_id int8 NULL,
  legacy_creative_id int8 NULL,
  component_template_id int2 NULL,
  component_template_name VARCHAR(20) NULL,
  "sequence" int2 NULL,
  "share" int2 NULL,
  attribute_response jsonb null,
  is_logically_deleted BOOLEAN null,
  created_timestamp timestamp NULL,
  modified_timestamp timestamp null,
  creative_path varchar(255) NULL,
  page_no int2 NULL,
  height float4 NULL,
  width float4 NULL,
  area float4 NULL,
  x_offset float4 NULL,
  y_offset float4 NULL,
  status varchar NULL,
  modified_by int4 NULL,
  order_number int4 null
  );
end if;

	INSERT INTO tempwork.component_coding_forsync_tmp_ctv_poc
(
-- We capture all new and updated component coded creatives based on the last updated timestamp i.e p_flag_timestamp_cc.
WITH cte_comp_crtv as (
	 SELECT
      component_coding_id,
      creative_id,
      component_template_id,
      "sequence",
      "share",
      attribute_response :: jsonb,
      is_logically_deleted,
      created,
      modified,
      creative_path,
      page_no,
      height,
      width,
      area,
      x_offset,
      y_offset,
      status,
      modified_by,
      order_number
    FROM
      tempwork.component_coding_ctv_poc c
    WHERE
      modified > p_flag_timestamp_cc
    UNION
--- Union it with the Creatives we pushed in PSQL from Databricks  for which we didn't found the
--- Vx0 to Vx2 Product translation i.e present in Attribute_response.
    SELECT
      c2.component_coding_id,
      creative_id,
      component_template_id,
      "sequence",
      "share",
      attribute_response :: jsonb,
      is_logically_deleted,
      created,
      modified,
      creative_path,
      page_no,
      height,
      width,
      area,
      x_offset,
      y_offset,
      status,
      modified_by,
      order_number
    FROM
      tempwork.component_coding_ctv_poc c2
      INNER JOIN tempwork.component_hold_creative_tmp_ctv_poc t ON c2.component_coding_id = t.component_coding_id
  )
SELECT
cc.component_coding_id,
cc.creative_id,
c.legacy_creative_id,
cc.component_template_id,
ct.componenttemplatename as component_template_name,
cc."sequence",
cc."share",
cc.attribute_response,
cc.is_logically_deleted,
cc.created as created_timestamp,
cc.modified as modified_timestamp,
cc.creative_path,
cc.page_no,
cc.height,
cc.width,
cc.area,
cc.x_offset,
cc.y_offset,
cc.status,
cc.modified_by,
cc.order_number
FROM cte_comp_crtv cc
LEFT JOIN tempwork.creative_ctv_poc c
ON  cc.creative_id = c.creative_id
LEFT JOIN
"template".component_template ct
ON cc.component_template_id = ct.componenttemplateid
 );
END;$procedure$
;
