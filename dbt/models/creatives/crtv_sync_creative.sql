{#
  Piece 4 -- TASK 1, STAGE 3 (final): gold upsert + change log + hold loops + watermark advance.
  Port of PsqlCrtvSync.persist_gold_creative / persist_to_log / persist_translation_hold_creative +
  the Step-7 CE-holding merge-back + update_watermark, reading the reverse-translated candidate
  crtv_sync_creative_revxlate (stage 2b).

  Post-hooks (in order), all gated on the reverse-translation holding_flag:
    1. gold MERGE  -> iceberg.gold.creative (107 cols; VARIANT->VARCHAR json strings passed through;
       exact Databricks UPDATE-SET subset so immutable/identity cols are preserved on match;
       updated_timestamp := current_timestamp (MRVXVC-14938); first_seen_occurrence_id keeps the
       existing value when the incoming is null). Only holding_flag=false rows (source is pre-filtered).
    2. change log  -> iceberg.silver.gold_creative_change_log (json_object over the Databricks
       json_log_columns; nested json cols re-parsed so they embed, not escape; absent-on-null).
    3. translation hold MERGE -> iceberg.silver.creative_mapping_translation_hold: park held
       (holding_flag=true) / release resolved (holding_flag=false). Keyed by creative_id.
    4. CE-holding merge-back (Postgres, via pg_call/system.execute): add held / remove processed in
       tempwork.creative_classification_engine_holding_ctv_poc from the proc scratch
       crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc (its own proc-computed hold_flag).
    5. watermark advance CTV_SYNC_CREATIVE -> max(updated_timestamp) of NON-HELD rows only
       (Databricks get_max_timestamp anti-joins the translation hold), so held creatives are re-read
       next run (also re-included via the stage-1 advert-hold pushback).

  DAG: depends on crtv_sync_creative_revxlate (stage 2b), which chains back through raw/forsync so the
  proc + transforms run first. Body = a 1-row run summary (merged / held / new watermark).
  VALIDATE ON VM: gold row delta, change-log append, translation-hold contents, CE-holding delta,
  and that watermark_control.CTV_SYNC_CREATIVE advanced to the non-held max.
#}

-- depends_on: {{ ref('crtv_sync_creative_revxlate') }}

{%- set gold_merge_sql %}
merge into iceberg.gold.creative t
    using (
    select
      cast(creative_id as bigint) as creative_id,
      cast(country_iso_2_code as varchar) as country_iso_2_code,
      cast(provider_code as varchar) as provider_code,
      cast(source_channel as varchar) as source_channel,
      cast(provider_creative_id as bigint) as provider_creative_id,
      cast(creative_url_hash as bigint) as creative_url_hash,
      cast(creative_type as varchar) as creative_type,
      cast(creative_mime_type as varchar) as creative_mime_type,
      cast(creative_width as integer) as creative_width,
      cast(creative_height as integer) as creative_height,
      cast(creative_duration as integer) as creative_duration,
      cast(creative_duration_bucket as varchar) as creative_duration_bucket,
      cast(creative_tier_id as integer) as creative_tier_id,
      cast(primary_language_code as varchar) as primary_language_code,
      cast(primary_product_id as integer) as primary_product_id,
      cast(vx1_product_id as integer) as vx1_product_id,
      cast(vx2_product_id as integer) as vx2_product_id,
      cast(mr_company_id as integer) as mr_company_id,
      cast(secondary_products as varchar) as secondary_products,
      cast(vx1_secondary_products as varchar) as vx1_secondary_products,
      cast(vx2_secondary_products as varchar) as vx2_secondary_products,
      cast(null as varchar) as mr_secondary_company_ids,
      cast(classification_type as varchar) as classification_type,
      cast(classified_by_user_id as integer) as classified_by_user_id,
      cast(classification_comments as varchar) as classification_comments,
      cast(classified_timestamp as timestamp(6) with time zone) as classified_timestamp,
      cast(created_timestamp as timestamp(6) with time zone) as created_timestamp,
      cast(current_timestamp as timestamp(6) with time zone) as updated_timestamp,
      cast(classification_process_step as varchar) as classification_process_step,
      cast(asset_source_server_id as integer) as asset_source_server_id,
      cast(creative_title as varchar) as creative_title,
      cast(creative_headline as varchar) as creative_headline,
      cast(attribution_first_audio as varchar) as attribution_first_audio,
      cast(attribution_lead_text as varchar) as attribution_lead_text,
      cast(attribution_visual as varchar) as attribution_visual,
      cast(attribution_summary as varchar) as attribution_summary,
      cast(attribution_other_details as varchar) as attribution_other_details,
      cast(attribution_description as varchar) as attribution_description,
      cast(attribution_hashtag as varchar) as attribution_hashtag,
      cast(attribution_competitor as varchar) as attribution_competitor,
      case when json_extract_scalar(attribution_celebrity, '$[0].id') is null then null else cast(attribution_celebrity as varchar) end as attribution_celebrity,
      cast(attribution_slogan_tagline as varchar) as attribution_slogan_tagline,
      cast(attribution_revision_description as varchar) as attribution_revision_description,
      cast(attribution_comments as varchar) as attribution_comments,
      cast(attribution_creative_tags as varchar) as attribution_creative_tags,
      cast(custom_attributes as varchar) as custom_attributes,
      cast(attribution_timestamp as timestamp(6) with time zone) as attribution_timestamp,
      cast(attribution_by_user_id as integer) as attribution_by_user_id,
      cast(attribution_status as varchar) as attribution_status,
      cast(is_sponsored_video as boolean) as is_sponsored_video,
      cast(is_component_eligible as boolean) as is_component_eligible,
      cast(component_entry_status as varchar) as component_entry_status,
      cast(component_entry_by_user_id as integer) as component_entry_by_user_id,
      cast(component_entry_timestamp as timestamp(6) with time zone) as component_entry_timestamp,
      cast(additional_multi_product_flag as boolean) as additional_multi_product_flag,
      cast(additional_coop_product_flag as boolean) as additional_coop_product_flag,
      cast(send_to_adscope_unattributed as boolean) as send_to_adscope_unattributed,
      cast(first_seen_media as varchar) as first_seen_media,
      cast(first_seen_provider_occurrence_id as varchar) as first_seen_provider_occurrence_id,
      cast(first_seen_occurrence_id as bigint) as first_seen_occurrence_id,
      cast(first_seen_occurrence_timestamp as timestamp(6) with time zone) as first_seen_occurrence_timestamp,
      cast(first_seen_provider_code as varchar) as first_seen_provider_code,
      cast(first_seen_media_property_id as integer) as first_seen_media_property_id,
      cast(first_seen_media_property_name as varchar) as first_seen_media_property_name,
      cast(first_seen_media_category_id as integer) as first_seen_media_category_id,
      cast(first_seen_media_category_code as varchar) as first_seen_media_category_code,
      cast(first_seen_provider_creative_link_url as varchar) as first_seen_provider_creative_link_url,
      cast(first_seen_provider_publisher_id as bigint) as first_seen_provider_publisher_id,
      cast(first_seen_provider_publisher_domain as varchar) as first_seen_provider_publisher_domain,
      cast(first_seen_provider_campaign_id as bigint) as first_seen_provider_campaign_id,
      cast(first_seen_provider_campaign_name as varchar) as first_seen_provider_campaign_name,
      cast(first_seen_provider_advertiser_id as bigint) as first_seen_provider_advertiser_id,
      cast(first_seen_provider_advertiser_name as varchar) as first_seen_provider_advertiser_name,
      cast(first_seen_provider_product_id as bigint) as first_seen_provider_product_id,
      cast(first_seen_provider_product_name as varchar) as first_seen_provider_product_name,
      cast(first_seen_provider_campaign_landing_page as varchar) as first_seen_provider_campaign_landing_page,
      cast(first_seen_market_id as integer) as first_seen_market_id,
      cast(first_seen_market_name as varchar) as first_seen_market_name,
      cast(first_seen_daypart_id as integer) as first_seen_daypart_id,
      cast(first_seen_daypart_name as varchar) as first_seen_daypart_name,
      cast(first_seen_affiliate_id as integer) as first_seen_affiliate_id,
      cast(first_seen_affiliate_name as varchar) as first_seen_affiliate_name,
      cast(due_timestamp as timestamp(6) with time zone) as due_timestamp,
      cast(last_seen_timestamp as timestamp(6) with time zone) as last_seen_timestamp,
      cast(attribution_competitor_vx2 as varchar) as attribution_competitor_vx2,
      cast(occurrence_description as varchar) as occurrence_description,
      cast(historical_creative_md5 as varchar) as historical_creative_md5,
      cast(legacy_creative_id as bigint) as legacy_creative_id,
      cast(creative_payload as varchar) as creative_payload,
      cast(machine_learning_payload as varchar) as machine_learning_payload,
      cast(print_los_id as bigint) as print_los_id,
      cast(print_ad_type_id as bigint) as print_ad_type_id,
      cast(print_ad_nli as real) as print_ad_nli,
      cast(print_ad_equ as real) as print_ad_equ,
      cast(print_ad_col_inch as real) as print_ad_col_inch,
      cast(print_ad_weighted_col_inch as real) as print_ad_weighted_col_inch,
      cast(print_ad_cost as real) as print_ad_cost,
      cast(print_ad_size as real) as print_ad_size,
      cast(print_null_cost_comments as varchar) as print_null_cost_comments,
      cast(print_recalculate_cost as boolean) as print_recalculate_cost,
      cast(is_resegment as boolean) as is_resegment,
      cast(print_matching_ads as varchar) as print_matching_ads,
      cast(print_ad_images as varchar) as print_ad_images,
      cast(product_mapping_status as varchar) as product_mapping_status,
      cast(keywords as varchar) as keywords,
      cast(is_reclassified as boolean) as is_reclassified,
      cast(print_no_cost as boolean) as print_no_cost
    from {{ ref('crtv_sync_creative_revxlate') }}
    where holding_flag = false
    ) s
    on t.creative_id = s.creative_id and t.creative_url_hash = s.creative_url_hash
    when matched then update set
      t.creative_duration = s.creative_duration,
      t.creative_duration_bucket = s.creative_duration_bucket,
      t.creative_tier_id = s.creative_tier_id,
      t.primary_product_id = s.primary_product_id,
      t.vx1_product_id = s.vx1_product_id,
      t.vx2_product_id = s.vx2_product_id,
      t.mr_company_id = s.mr_company_id,
      t.secondary_products = s.secondary_products,
      t.vx1_secondary_products = s.vx1_secondary_products,
      t.vx2_secondary_products = s.vx2_secondary_products,
      t.mr_secondary_company_ids = s.mr_secondary_company_ids,
      t.primary_language_code = s.primary_language_code,
      t.classification_type = s.classification_type,
      t.classified_by_user_id = s.classified_by_user_id,
      t.classification_comments = s.classification_comments,
      t.classified_timestamp = s.classified_timestamp,
      t.updated_timestamp = s.updated_timestamp,
      t.classification_process_step = s.classification_process_step,
      t.creative_title = s.creative_title,
      t.creative_headline = s.creative_headline,
      t.attribution_first_audio = s.attribution_first_audio,
      t.attribution_lead_text = s.attribution_lead_text,
      t.attribution_visual = s.attribution_visual,
      t.attribution_summary = s.attribution_summary,
      t.attribution_other_details = s.attribution_other_details,
      t.attribution_description = s.attribution_description,
      t.attribution_hashtag = s.attribution_hashtag,
      t.attribution_competitor = s.attribution_competitor,
      t.attribution_celebrity = s.attribution_celebrity,
      t.attribution_slogan_tagline = s.attribution_slogan_tagline,
      t.attribution_revision_description = s.attribution_revision_description,
      t.attribution_comments = s.attribution_comments,
      t.attribution_creative_tags = s.attribution_creative_tags,
      t.custom_attributes = s.custom_attributes,
      t.attribution_timestamp = s.attribution_timestamp,
      t.attribution_by_user_id = s.attribution_by_user_id,
      t.attribution_status = s.attribution_status,
      t.is_sponsored_video = s.is_sponsored_video,
      t.is_component_eligible = s.is_component_eligible,
      t.component_entry_status = s.component_entry_status,
      t.component_entry_by_user_id = s.component_entry_by_user_id,
      t.component_entry_timestamp = s.component_entry_timestamp,
      t.additional_multi_product_flag = s.additional_multi_product_flag,
      t.additional_coop_product_flag = s.additional_coop_product_flag,
      t.send_to_adscope_unattributed = s.send_to_adscope_unattributed,
      t.first_seen_media = s.first_seen_media,
      t.first_seen_provider_occurrence_id = s.first_seen_provider_occurrence_id,
      t.first_seen_occurrence_id = case when s.first_seen_occurrence_id is not null then s.first_seen_occurrence_id else t.first_seen_occurrence_id end,
      t.first_seen_occurrence_timestamp = s.first_seen_occurrence_timestamp,
      t.first_seen_provider_code = s.first_seen_provider_code,
      t.first_seen_media_property_id = s.first_seen_media_property_id,
      t.first_seen_media_property_name = s.first_seen_media_property_name,
      t.first_seen_media_category_id = s.first_seen_media_category_id,
      t.first_seen_media_category_code = s.first_seen_media_category_code,
      t.first_seen_provider_creative_link_url = s.first_seen_provider_creative_link_url,
      t.first_seen_provider_publisher_id = s.first_seen_provider_publisher_id,
      t.first_seen_provider_publisher_domain = s.first_seen_provider_publisher_domain,
      t.first_seen_provider_campaign_id = s.first_seen_provider_campaign_id,
      t.first_seen_provider_campaign_name = s.first_seen_provider_campaign_name,
      t.first_seen_provider_advertiser_id = s.first_seen_provider_advertiser_id,
      t.first_seen_provider_advertiser_name = s.first_seen_provider_advertiser_name,
      t.first_seen_provider_product_id = s.first_seen_provider_product_id,
      t.first_seen_provider_product_name = s.first_seen_provider_product_name,
      t.first_seen_provider_campaign_landing_page = s.first_seen_provider_campaign_landing_page,
      t.first_seen_market_id = s.first_seen_market_id,
      t.first_seen_market_name = s.first_seen_market_name,
      t.first_seen_daypart_id = s.first_seen_daypart_id,
      t.first_seen_daypart_name = s.first_seen_daypart_name,
      t.first_seen_affiliate_id = s.first_seen_affiliate_id,
      t.first_seen_affiliate_name = s.first_seen_affiliate_name,
      t.due_timestamp = s.due_timestamp,
      t.last_seen_timestamp = s.last_seen_timestamp,
      t.print_ad_nli = s.print_ad_nli,
      t.print_ad_equ = s.print_ad_equ,
      t.print_ad_cost = s.print_ad_cost,
      t.print_ad_size = s.print_ad_size,
      t.print_null_cost_comments = s.print_null_cost_comments,
      t.print_los_id = s.print_los_id,
      t.print_ad_type_id = s.print_ad_type_id,
      t.print_ad_col_inch = s.print_ad_col_inch,
      t.print_ad_weighted_col_inch = s.print_ad_weighted_col_inch,
      t.print_recalculate_cost = s.print_recalculate_cost,
      t.print_matching_ads = s.print_matching_ads,
      t.print_ad_images = s.print_ad_images,
      t.is_resegment = s.is_resegment,
      t.product_mapping_status = s.product_mapping_status,
      t.is_reclassified = s.is_reclassified,
      t.print_no_cost = s.print_no_cost,
      t.attribution_competitor_vx2 = s.attribution_competitor_vx2,
      t.creative_payload = s.creative_payload,
      t.machine_learning_payload = s.machine_learning_payload
    when not matched then insert (creative_id, country_iso_2_code, provider_code, source_channel, provider_creative_id, creative_url_hash, creative_type, creative_mime_type, creative_width, creative_height, creative_duration, creative_duration_bucket, creative_tier_id, primary_language_code, primary_product_id, vx1_product_id, vx2_product_id, mr_company_id, secondary_products, vx1_secondary_products, vx2_secondary_products, mr_secondary_company_ids, classification_type, classified_by_user_id, classification_comments, classified_timestamp, created_timestamp, updated_timestamp, classification_process_step, asset_source_server_id, creative_title, creative_headline, attribution_first_audio, attribution_lead_text, attribution_visual, attribution_summary, attribution_other_details, attribution_description, attribution_hashtag, attribution_competitor, attribution_celebrity, attribution_slogan_tagline, attribution_revision_description, attribution_comments, attribution_creative_tags, custom_attributes, attribution_timestamp, attribution_by_user_id, attribution_status, is_sponsored_video, is_component_eligible, component_entry_status, component_entry_by_user_id, component_entry_timestamp, additional_multi_product_flag, additional_coop_product_flag, send_to_adscope_unattributed, first_seen_media, first_seen_provider_occurrence_id, first_seen_occurrence_id, first_seen_occurrence_timestamp, first_seen_provider_code, first_seen_media_property_id, first_seen_media_property_name, first_seen_media_category_id, first_seen_media_category_code, first_seen_provider_creative_link_url, first_seen_provider_publisher_id, first_seen_provider_publisher_domain, first_seen_provider_campaign_id, first_seen_provider_campaign_name, first_seen_provider_advertiser_id, first_seen_provider_advertiser_name, first_seen_provider_product_id, first_seen_provider_product_name, first_seen_provider_campaign_landing_page, first_seen_market_id, first_seen_market_name, first_seen_daypart_id, first_seen_daypart_name, first_seen_affiliate_id, first_seen_affiliate_name, due_timestamp, last_seen_timestamp, attribution_competitor_vx2, occurrence_description, historical_creative_md5, legacy_creative_id, creative_payload, machine_learning_payload, print_los_id, print_ad_type_id, print_ad_nli, print_ad_equ, print_ad_col_inch, print_ad_weighted_col_inch, print_ad_cost, print_ad_size, print_null_cost_comments, print_recalculate_cost, is_resegment, print_matching_ads, print_ad_images, product_mapping_status, keywords, is_reclassified, print_no_cost)
    values (s.creative_id, s.country_iso_2_code, s.provider_code, s.source_channel, s.provider_creative_id, s.creative_url_hash, s.creative_type, s.creative_mime_type, s.creative_width, s.creative_height, s.creative_duration, s.creative_duration_bucket, s.creative_tier_id, s.primary_language_code, s.primary_product_id, s.vx1_product_id, s.vx2_product_id, s.mr_company_id, s.secondary_products, s.vx1_secondary_products, s.vx2_secondary_products, s.mr_secondary_company_ids, s.classification_type, s.classified_by_user_id, s.classification_comments, s.classified_timestamp, s.created_timestamp, s.updated_timestamp, s.classification_process_step, s.asset_source_server_id, s.creative_title, s.creative_headline, s.attribution_first_audio, s.attribution_lead_text, s.attribution_visual, s.attribution_summary, s.attribution_other_details, s.attribution_description, s.attribution_hashtag, s.attribution_competitor, s.attribution_celebrity, s.attribution_slogan_tagline, s.attribution_revision_description, s.attribution_comments, s.attribution_creative_tags, s.custom_attributes, s.attribution_timestamp, s.attribution_by_user_id, s.attribution_status, s.is_sponsored_video, s.is_component_eligible, s.component_entry_status, s.component_entry_by_user_id, s.component_entry_timestamp, s.additional_multi_product_flag, s.additional_coop_product_flag, s.send_to_adscope_unattributed, s.first_seen_media, s.first_seen_provider_occurrence_id, s.first_seen_occurrence_id, s.first_seen_occurrence_timestamp, s.first_seen_provider_code, s.first_seen_media_property_id, s.first_seen_media_property_name, s.first_seen_media_category_id, s.first_seen_media_category_code, s.first_seen_provider_creative_link_url, s.first_seen_provider_publisher_id, s.first_seen_provider_publisher_domain, s.first_seen_provider_campaign_id, s.first_seen_provider_campaign_name, s.first_seen_provider_advertiser_id, s.first_seen_provider_advertiser_name, s.first_seen_provider_product_id, s.first_seen_provider_product_name, s.first_seen_provider_campaign_landing_page, s.first_seen_market_id, s.first_seen_market_name, s.first_seen_daypart_id, s.first_seen_daypart_name, s.first_seen_affiliate_id, s.first_seen_affiliate_name, s.due_timestamp, s.last_seen_timestamp, s.attribution_competitor_vx2, s.occurrence_description, s.historical_creative_md5, s.legacy_creative_id, s.creative_payload, s.machine_learning_payload, s.print_los_id, s.print_ad_type_id, s.print_ad_nli, s.print_ad_equ, s.print_ad_col_inch, s.print_ad_weighted_col_inch, s.print_ad_cost, s.print_ad_size, s.print_null_cost_comments, s.print_recalculate_cost, s.is_resegment, s.print_matching_ads, s.print_ad_images, s.product_mapping_status, s.keywords, s.is_reclassified, s.print_no_cost)
{%- endset %}
{%- set change_log_sql %}
insert into iceberg.silver.gold_creative_change_log
      (creative_id, creative_url_hash, created_timestamp, psql_updated_timestamp, json_log)
    select creative_id, creative_url_hash,
      cast(current_timestamp as timestamp(6) with time zone) as created_timestamp,
      updated_timestamp as psql_updated_timestamp,
      json_object(
        key 'creative_duration' value creative_duration,
        key 'creative_duration_bucket' value creative_duration_bucket,
        key 'creative_tier_id' value creative_tier_id,
        key 'primary_language_code' value primary_language_code,
        key 'primary_product_id' value primary_product_id,
        key 'vx1_product_id' value vx1_product_id,
        key 'vx2_product_id' value vx2_product_id,
        key 'secondary_products' value json_parse(secondary_products),
        key 'vx1_secondary_products' value json_parse(vx1_secondary_products),
        key 'vx2_secondary_products' value json_parse(vx2_secondary_products),
        key 'classification_type' value classification_type,
        key 'classified_by_user_id' value classified_by_user_id,
        key 'classification_comments' value classification_comments,
        key 'classified_timestamp' value cast(classified_timestamp as varchar),
        key 'classification_process_step' value classification_process_step,
        key 'creative_title' value creative_title,
        key 'creative_headline' value creative_headline,
        key 'attribution_first_audio' value attribution_first_audio,
        key 'attribution_lead_text' value attribution_lead_text,
        key 'attribution_visual' value attribution_visual,
        key 'attribution_summary' value attribution_summary,
        key 'attribution_other_details' value attribution_other_details,
        key 'attribution_description' value attribution_description,
        key 'attribution_hashtag' value attribution_hashtag,
        key 'attribution_competitor' value json_parse(attribution_competitor),
        key 'attribution_celebrity' value json_parse(attribution_celebrity),
        key 'attribution_slogan_tagline' value attribution_slogan_tagline,
        key 'attribution_revision_description' value attribution_revision_description,
        key 'attribution_comments' value attribution_comments,
        key 'attribution_creative_tags' value attribution_creative_tags,
        key 'custom_attributes' value json_parse(custom_attributes),
        key 'attribution_timestamp' value cast(attribution_timestamp as varchar),
        key 'attribution_by_user_id' value attribution_by_user_id,
        key 'attribution_status' value attribution_status,
        key 'is_component_eligible' value is_component_eligible,
        key 'component_entry_status' value component_entry_status,
        key 'component_entry_by_user_id' value component_entry_by_user_id,
        key 'component_entry_timestamp' value cast(component_entry_timestamp as varchar),
        key 'additional_multi_product_flag' value additional_multi_product_flag,
        key 'additional_coop_product_flag' value additional_coop_product_flag,
        key 'send_to_adscope_unattributed' value send_to_adscope_unattributed,
        key 'first_seen_media' value first_seen_media,
        key 'first_seen_provider_occurrence_id' value first_seen_provider_occurrence_id,
        key 'first_seen_occurrence_id' value first_seen_occurrence_id,
        key 'first_seen_occurrence_timestamp' value cast(first_seen_occurrence_timestamp as varchar),
        key 'first_seen_provider_code' value first_seen_provider_code,
        key 'first_seen_media_property_id' value first_seen_media_property_id,
        key 'first_seen_media_property_name' value first_seen_media_property_name,
        key 'first_seen_media_category_id' value first_seen_media_category_id,
        key 'first_seen_media_category_code' value first_seen_media_category_code,
        key 'first_seen_provider_creative_link_url' value first_seen_provider_creative_link_url,
        key 'first_seen_provider_publisher_id' value first_seen_provider_publisher_id,
        key 'first_seen_provider_publisher_domain' value first_seen_provider_publisher_domain,
        key 'first_seen_provider_campaign_id' value first_seen_provider_campaign_id,
        key 'first_seen_provider_campaign_name' value first_seen_provider_campaign_name,
        key 'first_seen_provider_advertiser_id' value first_seen_provider_advertiser_id,
        key 'first_seen_provider_advertiser_name' value first_seen_provider_advertiser_name,
        key 'first_seen_provider_product_id' value first_seen_provider_product_id,
        key 'first_seen_provider_product_name' value first_seen_provider_product_name,
        key 'first_seen_market_id' value first_seen_market_id,
        key 'first_seen_market_name' value first_seen_market_name,
        key 'first_seen_daypart_id' value first_seen_daypart_id,
        key 'first_seen_daypart_name' value first_seen_daypart_name,
        key 'first_seen_affiliate_id' value first_seen_affiliate_id,
        key 'first_seen_affiliate_name' value first_seen_affiliate_name,
        key 'due_timestamp' value cast(due_timestamp as varchar),
        key 'last_seen_timestamp' value cast(last_seen_timestamp as varchar),
        key 'attribution_competitor_vx2' value json_parse(attribution_competitor_vx2),
        key 'occurrence_description' value occurrence_description,
        key 'legacy_creative_id' value legacy_creative_id,
        key 'creative_payload' value json_parse(creative_payload),
        key 'machine_learning_payload' value json_parse(machine_learning_payload),
        key 'is_resegment' value is_resegment,
        key 'product_mapping_status' value product_mapping_status,
        key 'is_reclassified' value is_reclassified
        absent on null
      ) as json_log
    from {{ ref('crtv_sync_creative_revxlate') }}
    where holding_flag = false
{%- endset %}
{%- set translation_hold_sql %}
merge into iceberg.silver.creative_mapping_translation_hold t
    using (select distinct creative_id, holding_flag from {{ ref('crtv_sync_creative_revxlate') }}) s
    on t.creative_id = s.creative_id
    when matched and s.holding_flag = false then delete
    when not matched and s.holding_flag = true then insert (creative_id) values (s.creative_id)
{%- endset %}
{%- set ce_hold_pg_sql %}merge into tempwork.creative_classification_engine_holding_ctv_poc t using tempwork.crtv_sync_to_db_updated_crtvs_with_holding_flag_ctv_poc s on s.creative_id = t.creative_id when matched and s.hold_flag = false then delete when not matched and s.hold_flag = true then insert (creative_id, inserted_at) values (s.creative_id, clock_timestamp() at time zone 'UTC'){%- endset %}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'p4_sync'],
    views_enabled=false,
    post_hook=[
      gold_merge_sql,
      change_log_sql,
      translation_hold_sql,
      pg_call(ce_hold_pg_sql),
      "{{ watermark_ts_finish_from_relation('CTV_SYNC_CREATIVE', ref('crtv_sync_creative_revxlate'), 'updated_timestamp', 'holding_flag = false') }}"
    ]
) }}

select
    count_if(not holding_flag)                                          as merged_to_gold,
    count_if(holding_flag)                                              as held,
    cast(max(if(not holding_flag, updated_timestamp)) as timestamp(6) with time zone) as new_watermark_ts
from {{ ref('crtv_sync_creative_revxlate') }}
