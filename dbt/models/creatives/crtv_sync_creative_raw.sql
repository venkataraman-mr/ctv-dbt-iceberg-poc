{#
  Piece 4 — TASK 1, STAGE 2a: schema-process / collapse (the "raw_processed_creatives" candidate).
  Reads the forsync (populated by stage 1's proc CALL) from Postgres and produces ONE row per
  (creative_id, creative_url_hash): per-product/competitor/celebrity forsync rows collapsed (array_agg
  for secondary_products / attribution_competitor / attribution_celebrity; max for primary_product_id),
  provider_code / source_channel resolved from the Iceberg reference dims. Faithful port of
  PsqlCrtvSync.schema_process (collect_set-over-window -> GROUP BY here). json cols carried as Trino json
  (stage 2b reverse-translates; stage 3 json_format()s into gold). Depends on stage 1 so the proc runs first.
  VALIDATE ON VM: row count == distinct creatives from stage 1 (~33,407); provider_code/source_channel resolved.
#}
-- depends_on: {{ ref('crtv_sync_creative_forsync') }}

{%- set fs = source('tempwork', 'creative_forsync_tmp_ctv_poc') -%}
{%- set dp_ref = source('km_preparation_db', 'data_provider') -%}
{%- set sc_ref = source('km_preparation_db', 'source_channel') -%}
{%- set status_flag_active = "'ACTIVE'" -%}

{{ config(materialized='table', schema='bronze', tags=['creatives','p4_sync'], views_enabled=false) }}

select
    c.creative_id,
    c.creative_url_hash,
    arbitrary(c.country_iso_2_code) as country_iso_2_code,
    max(dp.data_provider_code) as provider_code,
    max(sc.short_desc) as source_channel,
    arbitrary(c.provider_creative_id) as provider_creative_id,
    arbitrary(c.creative_type) as creative_type,
    arbitrary(c.creative_mime_type) as creative_mime_type,
    arbitrary(c.creative_width) as creative_width,
    arbitrary(c.creative_height) as creative_height,
    arbitrary(c.creative_duration) as creative_duration,
    arbitrary(c.creative_duration_bucket) as creative_duration_bucket,
    arbitrary(c.creative_tier_id) as creative_tier_id,
    arbitrary(c.primary_language_code) as primary_language_code,
    max(c.primary_product_id) as primary_product_id,
    array_agg(c.secondary_products) filter (where c.secondary_products is not null) as secondary_products,
    arbitrary(c.mr_company_id) as mr_company_id,
    arbitrary(c.classification_type) as classification_type,
    arbitrary(c.classified_by_user_id) as classified_by_user_id,
    arbitrary(c.classification_comments) as classification_comments,
    arbitrary(c.classified_timestamp) as classified_timestamp,
    arbitrary(c.created_timestamp) as created_timestamp,
    arbitrary(c.updated_timestamp) as updated_timestamp,
    arbitrary(c.classification_process_step) as classification_process_step,
    arbitrary(c.asset_source_server_id) as asset_source_server_id,
    arbitrary(c.creative_title) as creative_title,
    arbitrary(c.creative_headline) as creative_headline,
    arbitrary(c.attribution_first_audio) as attribution_first_audio,
    arbitrary(c.attribution_lead_text) as attribution_lead_text,
    arbitrary(c.attribution_visual) as attribution_visual,
    arbitrary(c.attribution_summary) as attribution_summary,
    arbitrary(c.attribution_other_details) as attribution_other_details,
    arbitrary(c.attribution_description) as attribution_description,
    arbitrary(c.attribution_hashtag) as attribution_hashtag,
    array_agg(c.attribution_competitor) filter (where c.attribution_competitor is not null) as attribution_competitor,
    array_agg(c.attribution_celebrity) filter (where c.attribution_celebrity is not null) as attribution_celebrity,
    arbitrary(c.attribution_slogan_tagline) as attribution_slogan_tagline,
    arbitrary(c.attribution_revision_description) as attribution_revision_description,
    arbitrary(c.attribution_comments) as attribution_comments,
    arbitrary(c.attribution_creative_tags) as attribution_creative_tags,
    arbitrary(c.custom_attribute) as custom_attributes,
    arbitrary(c.attribution_timestamp) as attribution_timestamp,
    arbitrary(c.attribution_by_user_id) as attribution_by_user_id,
    arbitrary(c.attribution_status) as attribution_status,
    arbitrary(c.is_sponsored_video) as is_sponsored_video,
    arbitrary(c.is_component_eligible) as is_component_eligible,
    arbitrary(c.component_entry_status) as component_entry_status,
    arbitrary(c.component_entry_by_user_id) as component_entry_by_user_id,
    arbitrary(c.component_entry_timestamp) as component_entry_timestamp,
    arbitrary(c.has_additional_multi_product) as additional_multi_product_flag,
    arbitrary(c.has_additional_coop_product) as additional_coop_product_flag,
    arbitrary(c.send_to_adscope_unattributed) as send_to_adscope_unattributed,
    arbitrary(c.first_seen_media) as first_seen_media,
    arbitrary(c.first_seen_provider_occurrence_id) as first_seen_provider_occurrence_id,
    arbitrary(c.first_seen_occurrence_id) as first_seen_occurrence_id,
    arbitrary(c.first_seen_occurrence_timestamp) as first_seen_occurrence_timestamp,
    arbitrary(c.first_seen_provider_code) as first_seen_provider_code,
    arbitrary(c.first_seen_media_property_id) as first_seen_media_property_id,
    arbitrary(c.first_seen_media_property_name) as first_seen_media_property_name,
    arbitrary(c.first_seen_media_category_id) as first_seen_media_category_id,
    arbitrary(c.first_seen_media_category_code) as first_seen_media_category_code,
    arbitrary(c.first_seen_provider_creative_link_url) as first_seen_provider_creative_link_url,
    arbitrary(c.first_seen_provider_publisher_id) as first_seen_provider_publisher_id,
    arbitrary(c.first_seen_provider_publisher_domain) as first_seen_provider_publisher_domain,
    arbitrary(c.first_seen_provider_campaign_id) as first_seen_provider_campaign_id,
    arbitrary(c.first_seen_provider_campaign_name) as first_seen_provider_campaign_name,
    arbitrary(c.first_seen_provider_advertiser_id) as first_seen_provider_advertiser_id,
    arbitrary(c.first_seen_provider_advertiser_name) as first_seen_provider_advertiser_name,
    arbitrary(c.first_seen_provider_product_id) as first_seen_provider_product_id,
    arbitrary(c.first_seen_provider_product_name) as first_seen_provider_product_name,
    arbitrary(c.first_seen_provider_campaign_landing_page) as first_seen_provider_campaign_landing_page,
    arbitrary(c.first_seen_market_id) as first_seen_market_id,
    arbitrary(c.first_seen_market_name) as first_seen_market_name,
    arbitrary(c.first_seen_daypart_id) as first_seen_daypart_id,
    arbitrary(c.first_seen_daypart_name) as first_seen_daypart_name,
    arbitrary(c.first_seen_affiliate_id) as first_seen_affiliate_id,
    arbitrary(c.first_seen_affiliate_name) as first_seen_affiliate_name,
    arbitrary(c.due_timestamp) as due_timestamp,
    arbitrary(c.last_seen_timestamp) as last_seen_timestamp,
    arbitrary(c.occurrence_description) as occurrence_description,
    arbitrary(c.historical_creative_md5) as historical_creative_md5,
    arbitrary(c.legacy_creative_id) as legacy_creative_id,
    arbitrary(c.creative_payload) as creative_payload,
    arbitrary(c.machine_learning_payload) as machine_learning_payload,
    arbitrary(c.print_los_id) as print_los_id,
    arbitrary(c.print_ad_type_id) as print_ad_type_id,
    arbitrary(c.print_ad_nli) as print_ad_nli,
    arbitrary(c.print_ad_equ) as print_ad_equ,
    arbitrary(c.print_ad_col_inch) as print_ad_col_inch,
    arbitrary(c.print_ad_weighted_col_inch) as print_ad_weighted_col_inch,
    arbitrary(c.print_ad_cost) as print_ad_cost,
    arbitrary(c.print_ad_size) as print_ad_size,
    arbitrary(c.print_null_cost_comments) as print_null_cost_comments,
    arbitrary(c.print_recalculate_cost) as print_recalculate_cost,
    arbitrary(c.is_resegment) as is_resegment,
    arbitrary(c.print_matching_ads) as print_matching_ads,
    arbitrary(c.print_ad_images) as print_ad_images,
    arbitrary(c.product_mapping_status) as product_mapping_status,
    arbitrary(c.keywords) as keywords,
    arbitrary(c.is_reclassified) as is_reclassified,
    arbitrary(c.print_no_cost) as print_no_cost,
    arbitrary(c.is_archive) as is_archive
from {{ fs }} c
left join {{ dp_ref }} dp
    on cast(c.provider_id as integer) = dp.data_provider_id
   and dp.record_status_flag = {{ status_flag_active }}
left join {{ sc_ref }} sc
    on cast(c.source_channel_id as integer) = sc.source_channel_id
   and sc.record_status_flag = {{ status_flag_active }}
group by c.creative_id, c.creative_url_hash
