{#
  Piece 5 -- HALF A, STAGE 4: the gate. Port of combine_raw_occ_stg_occ_adclassification's cte_occs_classification
  (final select) -> tmp_occ_dedup_result. Joins the enriched occurrences (digital_occ_combined) to gold.creative
  (twice: crtv on parent|original, crtv_child on original), origin, deployment_chain, product_flatten (ppf1/ppf2),
  and media_property_flatten_vx0_vw (is_house_ad), then computes occurrence_hold_flag + delete_flag + is_house_ad.

  Gate (occurrence_hold_flag): creative must exist in gold.creative as Advert with primary_product_id set; the BIS/
  BISSocial branches additionally require a deployment_chain_id (dc join is BIS/BISSocial only, so INERT for
  'AVOD BISCTV' -> CTV resolves on the two simple branches). delete_flag: BISSocial -> false; creative NonAd/BadAd
  -> true; else false. Filter: a '%video%' source_channel with a non-video creative mime type is dropped.
  deployment_chain_id = coalesce(dc.deployment_chain_id, -1). rnum=1 keeps one row per (country, occurrence_id).
#}

-- depends_on: {{ ref('digital_occ_combined') }}
-- depends_on: {{ ref('digital_occ_deploychain') }}

{%- set creative = 'iceberg.gold.creative' -%}
{%- set deploy   = 'iceberg.gold.digital_deployment_chain' -%}
{%- set origin   = source('km_preparation_db', 'origin') -%}
{%- set pflat    = source('productcentral', 'product_flatten') -%}
{%- set mpf_vw   = ref('media_property_flatten_vx0_vw') -%}

{%- set status_flag_active      = "'ACTIVE'" -%}
{%- set bc_active_status_id     = 92 -%}
{%- set source_bis_code         = "'BIS'" -%}
{%- set source_bis_social_code  = "'BISSocial'" -%}
{%- set classification_type_Advert = "'Advert'" -%}
{%- set classification_type_NonAd  = "'NonAd'" -%}
{%- set classification_type_BadAd  = "'BadAd'" -%}
{%- set job_log_key = "'" ~ invocation_id ~ "|OCC_FROM_RAW_TO_OCC_GOLD'" -%}

{{ config(materialized='table', schema='bronze', tags=['occurrences', 'p5_digital_raw_to_gold_occ'], views_enabled=false, on_table_exists='drop') }}

with cte_origin as (
    select origin_channel_id, lower(origin_channel_name) as origin_channel_name
    from {{ origin }} where record_status_flag = {{ status_flag_active }}
),

classified as (
    select
        mn.source_flag,
        mn.country_iso_2_code,
        mn.data_provider_id,
        mn.provider_code,
        crtv.creative_id,
        mn.parent_creative_url_hash as provider_parent_creative_url_hash,
        mn.creative_url_hash        as provider_original_creative_url_hash,
        mn.provider_creative_id     as provider_original_creative_id,
        mn.source_channel,
        mn.source_channel_id,
        mn.capture_date, mn.capture_month, mn.capture_timestamp,
        mn.media_property_id,
        mn.purchase_method_id,
        coalesce(dc.deployment_chain_id, -1) as deployment_chain_id,
        mn.mediator_chain,
        mn.provider_campaign_product_id, mn.provider_campaign_product_name,
        mn.provider_campaign_id, mn.provider_campaign_name,
        mn.provider_campaign_advertiser_id, mn.provider_campaign_advertiser_name,
        mn.provider_campaign_landing_page, mn.provider_campaign_landing_page_domain,
        org.origin_channel_id,
        case
            when mn.provider_code = {{ source_bis_social_code }} then false
            when coalesce(crtv.classification_type, '') not in ({{ classification_type_NonAd }}, {{ classification_type_BadAd }}) then false
            else true
        end as delete_flag,
        case
            when coalesce(mn.purchase_method_daisy_chain_col2_md5_hashcode, '') != ''
                 and crtv.creative_url_hash = mn.parent_creative_url_hash
                 and crtv_child.creative_url_hash is not null
                 and coalesce(crtv.primary_product_id, -1) != -1
                 and coalesce(dc.deployment_chain_id, -1) != -1
                then 'Not Hold'
            when coalesce(mn.purchase_method_daisy_chain_col2_md5_hashcode, '') != ''
                 and crtv.creative_url_hash is not null and crtv.creative_url_hash = mn.creative_url_hash
                 and coalesce(crtv.primary_product_id, -1) != -1
                 and coalesce(dc.deployment_chain_id, -1) != -1
                then 'Not Hold'
            when crtv.creative_url_hash = mn.parent_creative_url_hash and crtv_child.creative_url_hash is not null
                 and coalesce(crtv.primary_product_id, -1) != -1
                then 'Not Hold'
            when crtv.creative_url_hash is not null and crtv.creative_url_hash = mn.creative_url_hash
                 and coalesce(crtv.primary_product_id, -1) != -1
                then 'Not Hold'
            else 'Hold'
        end as occurrence_hold_flag,
        mn.market_id,
        mn.ad_insertion_point,
        mn.purchase_method_daisy_chain_col2_md5_hashcode,
        mn.provider_occurrence_id,
        cast(current_timestamp as timestamp(6) with time zone) as created_timestamp,
        mn.daisy_chain,
        {{ job_log_key }} as job_log_key,
        mn.provider_raw_json,
        case
            when mpf.vx0_parent_company_id in (coalesce(ppf2.parent_id, ppf1.parent_id), coalesce(ppf2.subsidiary_id, ppf1.subsidiary_id))
            then true else false
        end as is_house_ad,
        row_number() over (partition by mn.country_iso_2_code, mn.provider_occurrence_id
                           order by mn.capture_timestamp desc) as rnum
    from {{ ref('digital_occ_combined') }} mn
    left join {{ creative }} crtv
        on crtv.creative_url_hash = coalesce(mn.parent_creative_url_hash, mn.creative_url_hash)
       and crtv.classification_type = {{ classification_type_Advert }}
    left join {{ creative }} crtv_child
        on crtv_child.creative_url_hash = mn.creative_url_hash
       and crtv_child.classification_type = {{ classification_type_Advert }}
    left join cte_origin org
        on mn.source_channel = org.origin_channel_name
    left join {{ deploy }} dc
        on mn.purchase_method_daisy_chain_col2_md5_hashcode = dc.purchase_method_daisy_chain_2_md5_hashcode
       and mn.provider_code in ({{ source_bis_code }}, {{ source_bis_social_code }})
    left join {{ pflat }} ppf1 on ppf1.product_id = crtv.primary_product_id
    left join {{ pflat }} ppf2 on ppf2.product_id = ppf1.current_product_id
    left join {{ mpf_vw }} mpf
        on mpf.legacy_media_property_id = mn.media_property_id
       and mpf.source_channel_id = mn.source_channel_id
       and mpf.media_property_status_id = {{ bc_active_status_id }}
    where 1 = case
                when mn.source_channel like '%video%' and crtv.creative_mime_type not like '%video%' then 0
                else 1
              end
)

select * from classified where rnum = 1
