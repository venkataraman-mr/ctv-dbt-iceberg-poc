{#
  Job B — first-seen update (DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE). FAITHFUL 1:1 transliteration of
  the Databricks RawocctoDigitalCrtvFirstseen (get_new_raw_occurrence_data_cdf +
  map_media_market_details_to_occurrence + update_occurrence_firstseen_psql).

  Reads new digital_raw_occurrence inserts (VERSION watermark — Job B consolidated onto version
  watermarks), joins creative_unique_urls for the creative_id + the media/market dims, and produces the
  occurrence-level first-seen payload. The push (post-hooks) writes a Postgres temp table then CALLs the
  cloned update proc, which pulls each creative_first_seen row back to its EARLIEST occurrence
  (UPDATE ... WHERE trg.occurrence_timestamp > tmp.occurrence_timestamp). Job A seeds creative_first_seen;
  Job B only updates existing rows.

  Spark->Trino swaps only: table_changes signature (+ version begin/first-run branch), ANTI JOIN n/a,
  INTERVAL 4 HOUR -> interval '4' hour, REPLACE(x, NUL char, '') -> replace(x, chr(0), '') (Spark NUL char
  is the NUL char), current_timestamp() -> current_timestamp. Scratch model (tagged CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY), dropped at
  end of a successful run.
#}

{%- set wm_name = 'DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE' -%}
{%- set raw_rel = ref('digital_raw_occurrence') -%}
{%- set wm = watermark_version_begin(wm_name, raw_rel) -%}

{%- set self_rel = this.database ~ '.bronze.' ~ this.identifier -%}
{%- set uu = 'iceberg.bronze.creative_unique_urls' -%}
{%- set tmp_pg = 'tempwork.tmp_digital_raw_occ_to_crtv_firstseen_ctv_poc' -%}
{%- set tmp_pg_cat = 'postgres.' ~ tmp_pg -%}

{%- set country_code_US     = "'US'" -%}
{%- set status_flag_active  = "'ACTIVE'" -%}
{%- set bc_active_status_id = 92 -%}
{%- set source_bis_code        = "'BIS'" -%}
{%- set source_bis_social_code = "'BISSocial'" -%}
{%- set source_playon_code     = "'PlayOn'" -%}
{%- set source_bis_ctv_code    = "'AVOD BISCTV'" -%}
{%- set ctv_media_id     = 30 -%}
{%- set digital_media_id = 5 -%}

{#- ---- post-hooks: PG temp CTAS -> CALL update proc -> watermark finish ------------------------- -#}
{%- set h_pg_drop = 'drop table if exists ' ~ tmp_pg_cat -%}
{%- set h_pg_ctas -%}
create table {{ tmp_pg_cat }} as
select
    creative_id,
    creative_url_hash,
    provider_creative_id,
    country_iso_2_code,
    media_id,
    occurrence_id,
    cast(occurrence_timestamp as timestamp(6))       as occurrence_timestamp,
    cast(occurrence_timestamp_local as timestamp(6)) as occurrence_timestamp_local,
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
    provider_product_id,
    provider_product_name,
    provider_advertiser_name,
    cast(due_timestamp as timestamp(6))              as due_timestamp,
    market_id,
    market_name,
    daypart_id,
    daypart_name,
    affiliate_id,
    affiliate_name,
    cast(created_timestamp as timestamp(6))          as created_timestamp,
    cast(updated_timestamp as timestamp(6))          as updated_timestamp,
    provider_campaign_landing_page
from {{ self_rel }}
{%- endset -%}
{%- set h_call = pg_call("call tempwork.sp_dbx_digital_update_raw_occ_to_crtv_first_seen_ctv_poc('" ~ tmp_pg ~ "')") -%}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY'],
    views_enabled=false,
    post_hook=[h_pg_drop, h_pg_ctas, h_call,
               "{{ watermark_version_finish('" ~ wm_name ~ "') }}"]
) }}

with

-- get_new_raw_occurrence_data_cdf (version)
cdf_source as (
{%- if wm.start_version is none %}
    select * from {{ raw_rel }}{% if wm.end_version is not none %} for version as of {{ wm.end_version }}{% endif %}
{%- elif wm.start_version == wm.end_version %}
    select * from {{ raw_rel }} where false
{%- else %}
    select *
    from table(system.table_changes(
        schema_name       => 'bronze',
        table_name        => 'digital_raw_occurrence',
        start_snapshot_id => {{ wm.start_version }},
        end_snapshot_id   => {{ wm.end_version }}))
    where _change_type = 'insert'
{%- endif %}
),

raw_changes as (
    select
        *,
        row_number() over (partition by raw_occ.creative_url_hash
                           order by raw_occ.capture_timestamp asc) as rnum
    from cdf_source raw_occ
    where country_iso_2_code = {{ country_code_US }}
      and creative_url is not null
      and coalesce(retransmit, false) = false
),

tmp_raw_occ_for_firstseen as (
    select * from raw_changes where rnum = 1
),

cte_add_creative_id_to_occ_data as (
    select
        crtv.creative_id,
        occ_fs.country_iso_2_code,
        occ_fs.provider_code,
        occ_fs.source_channel,
        occ_fs.provider_occurrence_id,
        occ_fs.provider_creative_id,
        occ_fs.provider_source_id,
        occ_fs.capture_date,
        occ_fs.capture_month,
        occ_fs.capture_timestamp,
        occ_fs.created_timestamp,
        occ_fs.publisher_id,
        occ_fs.publisher_domain,
        occ_fs.creative_url_hash,
        occ_fs.provider_campaign_id,
        occ_fs.provider_campaign_product_id,
        occ_fs.provider_campaign_advertiser_id,
        occ_fs.provider_campaign_name,
        occ_fs.provider_campaign_product_name,
        occ_fs.provider_campaign_advertiser_name,
        occ_fs.occurrence_description,
        occ_fs.occurrence_link_url,
        occ_fs.provider_campaign_landing_page,
        case
            when provider_code in ({{ source_bis_code }}, {{ source_bis_social_code }}) then coalesce (occ_fs.region_dma_name, occ_fs.region_city_name)
            when provider_code in ({{ source_playon_code }}, {{ source_bis_ctv_code }}) then concat(coalesce(occ_fs.region_city_name, ''), ', ', coalesce(occ_fs.region_state_name, ''))
        end as provider_dma_city_name
        from
        tmp_raw_occ_for_firstseen occ_fs
        inner join {{ uu }} crtv on crtv.creative_url_hash = occ_fs.creative_url_hash
),

cte_media_property_data_provider_mapping as (
    select
        mpf.media_property_id,
        mpf.legacy_media_property_id,
        mpf.media_property_name,
        mpf.primary_media_category_id,
        mpf.primary_media_category_code,
        mpf.legacy_country_code,
        mpf.primary_language_code,
        mpf.domain_url,
        mpm.data_provider_id,
        mpm.source_channel_id,
        mpm.publisher_id,
        mpf.media_property_status_id,
        mpf.market_id,
        gbm.market_name
    from
    {{ source('km_preparation_gold_db', 'media_property_flatten') }} mpf
    inner join {{ source('km_preparation_db', 'media_property_data_provider_map') }} mpm
    on mpf.country_id = mpm.country_id
    and mpf.media_property_id = mpm.media_property_id
    and mpf.source_channel_id = mpm.source_channel_id
    and mpm.record_status_flag = {{ status_flag_active }}
    and mpf.media_property_status_id = {{ bc_active_status_id }}
    left join {{ source('reference', 'global_market') }} gbm on gbm.market_id = mpf.market_id
),

cte_source_channel as (
    select
        lower(short_desc) as source_channel_name,
        source_channel_id
    from {{ source('km_preparation_db', 'source_channel') }}
    where record_status_flag = {{ status_flag_active }}
),

cte_map_media_market_details_to_occ as (
    select
        row_number() over(
            partition by occ.creative_url_hash
            order by
            occ.capture_timestamp asc
        ) as rnum,
        occ.creative_id,
        occ.country_iso_2_code,
        occ.provider_creative_id,
        occ.provider_code,
        p.data_provider_id,
        occ.capture_timestamp,
        mpdpm.legacy_media_property_id as media_property_id,
        occ.creative_url_hash,
        occ.provider_occurrence_id,
        mpdpm.media_property_name,
        mpdpm.primary_media_category_id as media_category_id,
        mpdpm.primary_media_category_code as media_category_code,
        occ.occurrence_link_url as provider_creative_link_url,
        occ.publisher_id as provider_publisher_id,
        occ.publisher_domain as provider_publisher_domain,
        occ.provider_campaign_id,
        occ.provider_campaign_name,
        occ.provider_campaign_advertiser_id,
        occ.provider_campaign_advertiser_name,
        occ.provider_campaign_product_id,
        occ.provider_campaign_product_name,
        occ.occurrence_description,
        cast(occ.created_timestamp as timestamp) as created_timestamp,
        case
            when gmm.market_id is not null then gmm.market_id
            when gmm.market_id is null and mpdpm.market_id > 0 then mpdpm.market_id
            else 302
        end as market_id,
        case
            when gb.market_name is not null then gb.market_name
            when gb.market_name is null and mpdpm.market_id > 0 and mpdpm.market_name is not null then mpdpm.market_name
            else '~NOT SPECIFIED'
        end as market_name,
        occ.provider_campaign_landing_page
        from cte_add_creative_id_to_occ_data occ
        inner join {{ source('km_preparation_db', 'data_provider') }} p on p.data_provider_code = occ.provider_code
        and p.record_status_flag = {{ status_flag_active }}
        inner join cte_source_channel sc on sc.source_channel_name = occ.source_channel
        left join cte_media_property_data_provider_mapping mpdpm on mpdpm.legacy_country_code = occ.country_iso_2_code
        and mpdpm.data_provider_id = p.data_provider_id
        and mpdpm.source_channel_id = sc.source_channel_id
        and mpdpm.publisher_id = occ.publisher_id
        left join {{ source('reference', 'provider_global_market_map') }} gmm on gmm.provider_dma_city_name = occ.provider_dma_city_name
        left join {{ source('reference', 'global_market') }} gb on gb.market_id = gmm.market_id
)

select
    creative_id,
    creative_url_hash,
    provider_creative_id,
    country_iso_2_code,
    case when provider_code = {{ source_bis_ctv_code }} then {{ ctv_media_id }}
    else {{ digital_media_id }} end as media_id,
    provider_occurrence_id as occurrence_id,
    capture_timestamp as occurrence_timestamp,
    cast(null as timestamp) as occurrence_timestamp_local,
    data_provider_id as provider_id,
    media_property_id,
    media_property_name,
    media_category_id,
    media_category_code,
    replace(provider_creative_link_url, chr(0), '') as provider_creative_link_url,
    provider_publisher_id,
    provider_publisher_domain,
    provider_campaign_id,
    provider_campaign_name,
    provider_campaign_advertiser_id as provider_advertiser_id,
    provider_campaign_product_id as provider_product_id,
    provider_campaign_product_name as provider_product_name,
    provider_campaign_advertiser_name as provider_advertiser_name,
    (capture_timestamp + interval '4' hour) as due_timestamp,
    market_id,
    market_name,
    cast(null as integer) as daypart_id,
    cast(null as varchar) as daypart_name,
    cast(null as integer) as affiliate_id,
    cast(null as varchar) as affiliate_name,
    created_timestamp,
    current_timestamp as updated_timestamp,
    replace(provider_campaign_landing_page, chr(0), '') as provider_campaign_landing_page
    from cte_map_media_market_details_to_occ
    where
    rnum = 1
