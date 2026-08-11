{#
  Piece 5 -- HALF A, STAGE 5 (writer): persist Not-Hold occurrences to gold + park/release the staging buffer +
  advance the version watermark. Port of persist_to_gold_occ + persist_to_intermediate_staging + watermark set.

  Body = the gold candidate: Not-Hold rows (from digital_occ_classified) + prelim spend (property avg -> media avg
  fallback) + a reserved occurrence_id. occurrence_id (IDENTITY in prod) is reserved from the Postgres sequence
  tempwork.occurrence_id_seq_ctv_poc (START 75,000,000,000) via reserve_occurrence_ids(n); assigned
  block_start + row_number()-1. (Slight over-reservation vs prod's per-insert identity -- the gold MERGE only
  INSERTs NOT-MATCHED rows -- unused ids just leave gaps; fine.)

  Post-hooks:
    1. MERGE gold.digital_gold_occurrence WHEN NOT MATCHED INSERT (38 cols) on (country, capture_date, month, occ_id).
    2. MERGE silver.digital_staging_occurrence from ALL classified rows: release resolved holds
       (Not-Hold + '2-IntermediateStaging' -> DELETE); park new holds (Hold + '1-RawOccurrence' -> INSERT).
    3. watermark_version_finish('DIGITAL_RAW_OCC_TO_GOLD_OCC') -- promote the pinned end snapshot AFTER the gold write.

  PREREQ: ddl/postgres/piece5_occ_id_seq_ctv_poc.sql (the 75B sequence + reservation proc).
#}

-- depends_on: {{ ref('digital_occ_classified') }}

{%- set cls = 'iceberg.bronze.digital_occ_classified' -%}
{%- set self_rel = 'iceberg.bronze.' ~ this.identifier -%}
{%- set psp = source('spend', 'digital_dmi_prelim_spend_average_by_property') -%}
{%- set psm = source('spend', 'digital_dmi_prelim_spend_average_by_media') -%}

{%- set n_new = run_query("select count(*) as n from " ~ cls ~ " where occurrence_hold_flag = 'Not Hold'").rows[0]['n'] if execute else 0 -%}
{%- set id_base = reserve_occurrence_ids(n_new) -%}

{%- set gold_merge %}
merge into iceberg.gold.digital_gold_occurrence goc
using {{ self_rel }} spv
on goc.country_iso_2_code = spv.country_iso_2_code
   and goc.capture_date = spv.capture_date
   and goc.capture_month = spv.capture_month
   and goc.provider_occurrence_id = spv.provider_occurrence_id
when not matched then insert (
    occurrence_id, country_iso_2_code, provider_code, source_channel_id, creative_id, provider_occurrence_id,
    provider_parent_creative_url_hash, provider_original_creative_id, provider_original_creative_url_hash,
    capture_date, capture_month, capture_timestamp, created_timestamp, updated_timestamp, media_property_id,
    market_id, purchase_method_id, deployment_chain_id, mediator_chain, origin_channel_id, prelim_impressions,
    prelim_spend, final_impressions, final_spend, delete_flag, is_house_ad, historical_creative_id,
    provider_campaign_id, provider_campaign_name, provider_campaign_product_id, provider_campaign_product_name,
    provider_campaign_advertiser_id, provider_campaign_advertiser_name, provider_campaign_landing_page,
    provider_campaign_landing_page_domain, provider_raw_json, ad_insertion_point, job_log_key)
values (
    spv.occurrence_id, spv.country_iso_2_code, spv.provider_code, spv.source_channel_id, spv.creative_id, spv.provider_occurrence_id,
    spv.provider_parent_creative_url_hash, spv.provider_original_creative_id, spv.provider_original_creative_url_hash,
    spv.capture_date, spv.capture_month, spv.capture_timestamp, spv.created_timestamp, spv.updated_timestamp, spv.media_property_id,
    spv.market_id, spv.purchase_method_id, spv.deployment_chain_id, spv.mediator_chain, spv.origin_channel_id, spv.prelim_impressions,
    spv.prelim_spend, spv.final_impressions, spv.final_spend, spv.delete_flag, spv.is_house_ad, spv.historical_creative_id,
    spv.provider_campaign_id, spv.provider_campaign_name, spv.provider_campaign_product_id, spv.provider_campaign_product_name,
    spv.provider_campaign_advertiser_id, spv.provider_campaign_advertiser_name, spv.provider_campaign_landing_page,
    spv.provider_campaign_landing_page_domain, spv.provider_raw_json, spv.ad_insertion_point, spv.job_log_key)
{%- endset %}

{%- set staging_merge %}
merge into iceberg.silver.digital_staging_occurrence target
using {{ cls }} source
on target.provider_occurrence_id = source.provider_occurrence_id
   and target.country_iso_2_code = source.country_iso_2_code
   and target.capture_date = source.capture_date
   and target.capture_month = source.capture_month
when matched and source.occurrence_hold_flag = 'Not Hold' and source.source_flag = '2-IntermediateStaging' then delete
when not matched and source.occurrence_hold_flag = 'Hold' and source.source_flag = '1-RawOccurrence' then insert (
    provider_occurrence_id, country_iso_2_code, provider_code, source_channel, provider_creative_id, provider_creative_url_hash,
    capture_date, capture_month, capture_timestamp, created_timestamp, media_property_id, purchase_method_id, daisy_chain,
    provider_campaign_id, provider_campaign_name, provider_campaign_product_id, provider_campaign_product_name,
    provider_campaign_advertiser_id, provider_campaign_advertiser_name, provider_campaign_landing_page,
    provider_campaign_landing_page_domain, ad_insertion_point, job_log_key, provider_raw_json)
values (
    source.provider_occurrence_id, source.country_iso_2_code, source.provider_code, source.source_channel,
    source.provider_original_creative_id, source.provider_original_creative_url_hash,
    source.capture_date, source.capture_month, source.capture_timestamp, source.created_timestamp,
    source.media_property_id, source.purchase_method_id, source.daisy_chain,
    source.provider_campaign_id, source.provider_campaign_name, source.provider_campaign_product_id, source.provider_campaign_product_name,
    source.provider_campaign_advertiser_id, source.provider_campaign_advertiser_name, source.provider_campaign_landing_page,
    source.provider_campaign_landing_page_domain, source.ad_insertion_point, source.job_log_key, source.provider_raw_json)
{%- endset %}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['occurrences', 'DIGITAL_RAW_OCC_TO_GOLD_OCC'],
    views_enabled=false,
    on_table_exists='drop',
    post_hook=[
      gold_merge,
      staging_merge,
      "{{ watermark_version_finish('DIGITAL_RAW_OCC_TO_GOLD_OCC') }}"
    ]
) }}

with not_hold as (
    select * from {{ ref('digital_occ_classified') }}
    where occurrence_hold_flag = 'Not Hold'
),

with_spend as (
    select
        h.*,
        cast(coalesce(psp.average_impressions, psm.average_impressions) as bigint) as prelim_impressions,
        cast(coalesce(psp.average_spend, psm.average_spend) as double)             as prelim_spend
    from not_hold h
    left join {{ psp }} psp on psp.media_property_id = h.media_property_id
    left join {{ psm }} psm on psm.source_channel_id = h.source_channel_id
)

select
    {{ id_base }} + cast(row_number() over (order by capture_timestamp, provider_occurrence_id) as bigint) - 1 as occurrence_id,
    country_iso_2_code,
    provider_code,
    cast(source_channel_id as integer)                       as source_channel_id,
    creative_id,
    provider_occurrence_id,
    cast(provider_parent_creative_url_hash as bigint)        as provider_parent_creative_url_hash,
    cast(provider_original_creative_id as bigint)            as provider_original_creative_id,
    cast(provider_original_creative_url_hash as bigint)      as provider_original_creative_url_hash,
    capture_date,
    capture_month,
    capture_timestamp,
    created_timestamp,
    cast(current_timestamp as timestamp(6) with time zone)   as updated_timestamp,
    cast(media_property_id as integer)                       as media_property_id,
    cast(market_id as smallint)                              as market_id,
    cast(purchase_method_id as smallint)                     as purchase_method_id,
    cast(deployment_chain_id as bigint)                      as deployment_chain_id,
    mediator_chain,
    cast(origin_channel_id as smallint)                      as origin_channel_id,
    prelim_impressions,
    prelim_spend,
    cast(null as bigint)                                     as final_impressions,
    cast(null as double)                                     as final_spend,
    delete_flag,
    is_house_ad,
    cast(null as bigint)                                     as historical_creative_id,
    cast(provider_campaign_id as bigint)                     as provider_campaign_id,
    provider_campaign_name,
    cast(provider_campaign_product_id as bigint)             as provider_campaign_product_id,
    provider_campaign_product_name,
    cast(provider_campaign_advertiser_id as bigint)          as provider_campaign_advertiser_id,
    provider_campaign_advertiser_name,
    provider_campaign_landing_page,
    provider_campaign_landing_page_domain,
    provider_raw_json,
    ad_insertion_point,
    job_log_key
from with_spend
