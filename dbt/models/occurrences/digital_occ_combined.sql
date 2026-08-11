{#
  Piece 5 -- HALF A, STAGE 3: combine new raw + the hold buffer, dedup, and enrich (media_property / market /
  source_channel / dedupe-map parent + the occurrence's own daisy md5). Port of the first ~2/3 of
  combine_raw_occ_stg_occ_adclassification (through cte_add_market_id_name_occ). The gate is stage 4.

  UNION '1-RawOccurrence' (digital_occ_raw_cdf) + '2-IntermediateStaging' (silver.digital_staging_occurrence, the
  held buffer -- empty on the first run). Dedup by (country, provider_occurrence_id) keeping source_flag desc
  (prefer the staged copy) then capture_timestamp. provider_raw_json: BISSocial -> the social struct; else the
  region struct (both as JSON via json_object). Per occurrence, transform its daisy array IN PLACE (sorted by
  index) -> col1/col2 + md5 (the deployment-chain key the gate joins). Reuses the validated Piece-3
  media_property/market/source_channel CTEs (crtv_staging_candidate).
#}

-- depends_on: {{ ref('digital_occ_raw_cdf') }}

{%- set raw = ref('digital_occ_raw_cdf') -%}
{%- set staging = 'iceberg.silver.digital_staging_occurrence' -%}
{%- set dedupe  = 'iceberg.silver.creative_dedupe_map' -%}

{%- set status_flag_active     = "'ACTIVE'" -%}
{%- set bc_active_status_id    = 92 -%}
{%- set match_type_map         = "'Map'" -%}
{%- set source_bis_code        = "'BIS'" -%}
{%- set source_bis_social_code = "'BISSocial'" -%}
{%- set source_bis_ctv_code    = "'AVOD BISCTV'" -%}
{%- set source_playon_code     = "'PlayOn'" -%}

{{ config(materialized='table', schema='bronze', tags=['occurrences', 'p5_digital_raw_to_gold_occ'], views_enabled=false, on_table_exists='drop') }}

with raw_side as (
    select
        '1-RawOccurrence' as source_flag,
        creative_url_hash, country_iso_2_code, provider_code, source_channel, provider_occurrence_id,
        provider_creative_id, capture_date, capture_month, capture_timestamp,
        cast(null as integer) as media_property_id,
        purchase_method_id, provider_campaign_product_id, provider_campaign_product_name,
        provider_campaign_id, provider_campaign_name, provider_campaign_advertiser_id, provider_campaign_advertiser_name,
        provider_campaign_landing_page,
        coalesce(regexp_extract(lower(coalesce(regexp_replace(provider_campaign_landing_page, '[\n\r]+', ''), ' ')),
                                '^(?:https?://)?(?:[@\n]+@)?(?:www.)?([^:/\n?]+)', 1), '') as provider_campaign_landing_page_domain,
        false as delete_flag,
        ad_insertion_point,
        case when provider_code = {{ source_bis_social_code }} then
            json_object(
                'region_city_id'    value coalesce(cast(region_city_id as varchar), 'NA'),
                'region_city_name'  value coalesce(region_city_name, 'NA'),
                'region_country_code' value coalesce(region_country_code, 'NA'),
                'region_dma_id'     value coalesce(cast(region_dma_id as varchar), 'NA'),
                'region_dma_name'   value coalesce(region_dma_name, 'NA'),
                'region_state_id'   value coalesce(cast(region_state_id as varchar), 'NA'),
                'region_state_name' value coalesce(region_state_name, 'NA'),
                'MAIN_ADVERTISER_CATEGORY'   value coalesce(json_extract_scalar(try(json_parse(raw_json)), '$.mainAdvertiserCategory'), 'NA'),
                'SECOND_ADVERTISER_CATEGORY' value coalesce(json_extract_scalar(try(json_parse(raw_json)), '$.secondAdvertiserCategory'), 'NA'),
                'OCCURRENCE_SOCIAL_PAGE_LINK'      value coalesce(json_extract_scalar(try(json_parse(raw_json)), '$.occurrence.socialPageLink'), 'NA'),
                'OCCURRENCE_SOCIAL_PAGE_NAME'      value coalesce(json_extract_scalar(try(json_parse(raw_json)), '$.occurrence.socialPageName'), 'NA'),
                'OCCURRENCE_SOCIAL_CAMPAIGN_TEXT'  value coalesce(json_extract_scalar(try(json_parse(raw_json)), '$.occurrence.socialCampaignText'), 'NA'),
                'OCCURRENCE_CREATIVEURL' value coalesce(json_extract_scalar(try(json_parse(raw_json)), '$.occurrence.creativeUrl'), 'NA'),
                'FIRST_SEEN_DATE'   value coalesce(json_extract_scalar(try(json_parse(raw_json)), '$.occurrence.creative.firstSeenDate'), 'NA')
            )
        else
            json_object(
                'region_city_id'    value region_city_id,
                'region_city_name'  value region_city_name,
                'region_country_code' value region_country_code,
                'region_dma_id'     value region_dma_id,
                'region_dma_name'   value region_dma_name,
                'region_state_id'   value region_state_id,
                'region_state_name' value region_state_name
            )
        end as provider_raw_json,
        daisy_chain,
        publisher_id
    from {{ raw }}
),

staging_side as (
    select
        '2-IntermediateStaging' as source_flag,
        provider_creative_url_hash as creative_url_hash, country_iso_2_code, provider_code, source_channel, provider_occurrence_id,
        provider_creative_id, capture_date, capture_month, capture_timestamp,
        media_property_id,
        purchase_method_id, provider_campaign_product_id, provider_campaign_product_name,
        provider_campaign_id, provider_campaign_name, provider_campaign_advertiser_id, provider_campaign_advertiser_name,
        provider_campaign_landing_page, provider_campaign_landing_page_domain,
        false as delete_flag,
        ad_insertion_point,
        provider_raw_json,
        daisy_chain,
        cast(null as bigint) as publisher_id
    from {{ staging }}
),

combined as (
    select * from raw_side
    union all
    select * from staging_side
),

deduped as (
    select * from (
        select *,
            row_number() over (partition by country_iso_2_code, provider_occurrence_id
                               order by source_flag desc, capture_timestamp) as rnum
        from combined
    )
    where rnum = 1
),

-- per occurrence: transform its own daisy array in place (sorted by index) -> col1/col2 + deployment md5
occ_daisy as (
    select
        d.*,
        array_join(transform(array_sort(transform(
            cast(json_parse(d.daisy_chain) as array(json)),
            e -> cast(row(cast(json_extract_scalar(e,'$.index') as bigint),
                          cast(json_extract_scalar(e,'$.id') as bigint),
                          cast(json_extract_scalar(e,'$.roleId') as bigint)) as row(idx bigint, id bigint, role_id bigint)))),
            x -> concat(cast(x.id as varchar), '(', cast(x.role_id as varchar), ')')), ',') as occ_col1,
        array_join(transform(array_sort(transform(
            cast(json_parse(d.daisy_chain) as array(json)),
            e -> cast(row(cast(json_extract_scalar(e,'$.index') as bigint),
                          cast(json_extract_scalar(e,'$.id') as bigint),
                          cast(json_extract_scalar(e,'$.roleId') as bigint)) as row(idx bigint, id bigint, role_id bigint)))),
            x -> concat(cast(x.id as varchar), '.', cast(x.role_id as varchar))), '|') as occ_col2,
        case
            when d.provider_code in ({{ source_bis_code }}, {{ source_bis_social_code }}, {{ source_bis_ctv_code }})
                then coalesce(json_extract_scalar(d.provider_raw_json, '$.region_dma_name'),
                              json_extract_scalar(d.provider_raw_json, '$.region_city_name'))
            when d.provider_code in ({{ source_playon_code }})
                then concat(coalesce(json_extract_scalar(d.provider_raw_json, '$.region_city_name'), ''), ', ',
                            coalesce(json_extract_scalar(d.provider_raw_json, '$.region_state_name'), ''))
        end as provider_dma_city_name
    from deduped d
),

-- ===== reused media_property / source_channel / dedupe-map CTEs (Piece-3 crtv_staging_candidate) =====
cte_media as (
    select
        mpf.media_property_id, mpf.legacy_media_property_id, mpf.market_id,
        mpm.data_provider_id, mpm.source_channel_id, mpm.publisher_id, mpf.legacy_country_code
    from {{ source('km_preparation_gold_db', 'media_property_flatten') }} mpf
    inner join {{ source('km_preparation_db', 'media_property_data_provider_map') }} mpm
        on mpf.country_id = mpm.country_id
       and mpf.media_property_id = mpm.media_property_id
       and mpf.source_channel_id = mpm.source_channel_id
       and mpm.record_status_flag = {{ status_flag_active }}
       and mpf.media_property_status_id = {{ bc_active_status_id }}
),
cte_source_channel as (
    select lower(short_desc) as source_channel_name, source_channel_id
    from {{ source('km_preparation_db', 'source_channel') }}
    where record_status_flag = {{ status_flag_active }}
),
cte_dedup_map as (
    select * from {{ dedupe }}
    where match_type = {{ match_type_map }} and parent_creative_id is not null
)

select
    occ.source_flag,
    occ.creative_url_hash,
    cdm.parent_creative_url_hash,
    occ.country_iso_2_code, occ.provider_code, occ.source_channel, occ.provider_occurrence_id, occ.provider_creative_id,
    occ.capture_date, occ.capture_month, occ.capture_timestamp,
    occ.purchase_method_id,
    occ.provider_campaign_product_id, occ.provider_campaign_product_name,
    occ.provider_campaign_id, occ.provider_campaign_name,
    occ.provider_campaign_advertiser_id, occ.provider_campaign_advertiser_name,
    occ.provider_campaign_landing_page, occ.provider_campaign_landing_page_domain,
    occ.delete_flag, occ.ad_insertion_point,
    occ.occ_col1 as occurrence_daisy_chain_transformed_col1,
    occ.occ_col2 as occurrence_daisy_chain_transformed_col2,
    occ.provider_dma_city_name, occ.daisy_chain, occ.publisher_id, occ.provider_raw_json,
    dp.data_provider_id,
    sc.source_channel_id,
    case when occ.source_flag = '2-IntermediateStaging' then occ.media_property_id
         else mp.legacy_media_property_id end as media_property_id,
    case when gmm.market_id is not null then gmm.market_id
         when gmm.market_id is null and mp.market_id > 0 then mp.market_id
         else 302 end as market_id,
    occ.occ_col1 as mediator_chain,
    case when occ.occ_col2 is not null and occ.occ_col2 != ''
         then upper(to_hex(md5(to_utf8(concat(cast(occ.purchase_method_id as varchar), occ.occ_col2)))))
         else '' end as purchase_method_daisy_chain_col2_md5_hashcode
from occ_daisy occ
left join cte_dedup_map cdm on occ.creative_url_hash = cdm.child_creative_url_hash
inner join {{ source('km_preparation_db', 'data_provider') }} dp
    on dp.data_provider_code = occ.provider_code and dp.record_status_flag = {{ status_flag_active }}
inner join cte_source_channel sc on sc.source_channel_name = occ.source_channel
left join cte_media mp
    on mp.legacy_country_code = occ.country_iso_2_code
   and mp.data_provider_id = dp.data_provider_id
   and mp.source_channel_id = sc.source_channel_id
   and mp.publisher_id = occ.publisher_id
left join {{ source('reference', 'provider_global_market_map') }} gmm
    on cast(gmm.provider_dma_city_name as varchar) = cast(occ.provider_dma_city_name as varchar)
