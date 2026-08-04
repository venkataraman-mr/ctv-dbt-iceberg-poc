{#
  Job A candidate — DIGITAL_RAW_OCC_TO_CRTV_STAGING.
  FAITHFUL 1:1 transliteration of the Databricks DigitalRawocctoCrtvStaging SQL:
    get_new_raw_occurrence_data_cdf  -> cte_raw_changes / cte_occ_filter_duplicates / tmp_digital_raw_occurrence
    map_media_market_details_to_occurrence -> cte_raw_occ / cte_media_property_data_provider_mapping /
                                              cte_mime_type / cte_source_channel /
                                              cte_raw_occ_media_property_market_details
  Output = tmp_digital_raw_occ_media (SELECT * WHERE rnum = 1) — every column preserved in legacy order,
  including columns nothing downstream consumes (kept deliberately: do not prune/reorder the blueprint).

  ONLY the unavoidable Spark->Trino syntax swaps are applied, nothing else:
    * table_changes(tbl, v1, v2)  -> system.table_changes(schema,table,start_snap,end_snap) + the
      version-watermark first-run/no-change branches (engine needs a start snapshot; same as Piece 1).
    * ANTI JOIN                   -> LEFT JOIN ... WHERE key IS NULL (Trino has no ANTI JOIN keyword).
    * raw_json:occurrence:x       -> json_extract_scalar(try(json_parse(raw_json)), '$.occurrence.x').
    * RLIKE                       -> regexp_like ; Nvl -> coalesce ; Regexp_extract -> regexp_extract.
    * substring_index(...)        -> {{ substring_index() }} macro (Spark SUBSTRING_INDEX; no Trino builtin).
    * boolean('false')            -> false.
    * /*+ BROADCAST(..) */ hints  -> dropped (Spark optimizer hint; Trino uses its CBO).
#}

{%- set wm_name = 'DIGITAL_RAW_OCC_TO_CRTV_STAGING' -%}
{%- set raw_rel = ref('digital_raw_occurrence') -%}
{%- set wm = watermark_version_begin(wm_name, raw_rel) -%}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'job_a'],
    views_enabled=false
) }}

{#- legacy constants (common/constants.py), inlined verbatim as literals -#}
{%- set country_code_US               = "'US'" -%}
{%- set status_flag_active            = "'ACTIVE'" -%}
{%- set bc_active_status_id           = 92 -%}
{%- set source_bis_code               = "'BIS'" -%}
{%- set source_bis_social_code        = "'BISSocial'" -%}
{%- set source_playon_code            = "'PlayOn'" -%}
{%- set source_bis_ctv_code           = "'AVOD BISCTV'" -%}
{%- set not_attributed                = "'NA'" -%}
{%- set source_channel_desc_desktop_video       = "'desktop video'" -%}
{%- set source_channel_desc_mobile_video        = "'mobile video'" -%}
{%- set source_channel_desc_desktop_video_panel = "'desktop video-panel'" -%}
{%- set source_channel_desc_social_video        = "'social video'" -%}
{%- set source_channel_desc_social_display      = "'social display'" -%}
{%- set mime_type_evaliant_video      = "'evaliant/video'" -%}
{%- set mime_type_evaliant_richmedia  = "'evaliant/richmedia'" -%}
{%- set mime_type_application_json    = "'application/json'" -%}
{%- set mime_type_application_octet_stream = "'application/octet-stream'" -%}
{%- set mime_type_carousel            = "'carousel'" -%}
{%- set mime_type_image_jpeg          = "'image/jpeg'" -%}
{%- set mime_type_image_bmp           = "'image/bmp'" -%}
{%- set mime_type_image_gif           = "'image/gif'" -%}
{%- set mime_type_image_png           = "'image/png'" -%}
{%- set mime_type_image_tiff          = "'image/tiff'" -%}
{%- set image_ext_bm   = "'bm'" -%}
{%- set image_ext_bmp  = "'bmp'" -%}
{%- set image_ext_jpe  = "'jpe'" -%}
{%- set image_ext_jpg  = "'jpg'" -%}
{%- set image_ext_jpeg = "'jpeg'" -%}
{%- set image_ext_gif  = "'gif'" -%}
{%- set image_ext_png  = "'png'" -%}
{%- set image_ext_tiff = "'tiff'" -%}

with

-- ===== get_new_raw_occurrence_data_cdf =====
-- engine adaptation: choose the table_changes source (first-run full read / no-change / incremental)
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

cte_raw_changes as (
    select
        *,
        row_number() over (partition by raw_occ.creative_url_hash
                           order by raw_occ.capture_timestamp asc) as rnum
    from cdf_source raw_occ
    where country_iso_2_code = {{ country_code_US }}
      and creative_url is not null
      and coalesce(retransmit, false) = false
),

cte_occ_filter_duplicates as (
    select * from cte_raw_changes where rnum = 1
),

{#- ANTI JOIN creative_unique_urls (is_staged=true) ANTI JOIN creative_autochaff -> LEFT JOIN/IS NULL -#}
tmp_digital_raw_occurrence as (
    select occ.*
    from cte_occ_filter_duplicates occ
    left join {{ source('bronze', 'creative_unique_urls') }} cuu
           on occ.creative_url_hash = cuu.creative_url_hash and cuu.is_staged = true
    left join {{ source('bronze', 'creative_autochaff') }} cac
           on occ.creative_url_hash = cac.creative_url_hash
    where cuu.creative_url_hash is null
      and cac.creative_url_hash is null
),

{#- ===== map_media_market_details_to_occurrence ==================================================== -#}
cte_raw_occ as (
    select
        raw_occ.country_iso_2_code,
        raw_occ.provider_code,
        raw_occ.source_channel,
        raw_occ.provider_occurrence_id,
        raw_occ.provider_creative_id,
        raw_occ.provider_source_id,
        raw_occ.capture_date,
        raw_occ.capture_month,
        raw_occ.capture_timestamp,
        raw_occ.created_timestamp,
        raw_occ.region_dma_id,
        raw_occ.region_dma_name,
        raw_occ.region_country_code,
        raw_occ.region_city_id,
        raw_occ.region_city_name,
        raw_occ.region_state_id,
        raw_occ.region_state_name,
        raw_occ.creative_type,
        case
            when raw_occ.source_channel in ({{ source_channel_desc_desktop_video }},
                                            {{ source_channel_desc_mobile_video }},
                                            {{ source_channel_desc_desktop_video_panel }},
                                            {{ source_channel_desc_social_video }})
                                            then {{ mime_type_evaliant_video }}
            when raw_occ.source_channel = {{ source_channel_desc_social_display }}
            and raw_occ.creative_mime_type = {{ mime_type_carousel }} then {{ mime_type_evaliant_video }}
            when raw_occ.creative_mime_type = {{ mime_type_application_json }} then {{ mime_type_evaliant_richmedia }}
            when raw_occ.creative_mime_type = {{ mime_type_application_octet_stream }} then
            case when raw_occ.creative_url is null then {{ mime_type_image_jpeg }}
            else
              case {{ substring_index('raw_occ.creative_url', '.', -1) }}
                when {{ image_ext_bm }} then {{ mime_type_image_bmp }}
                when {{ image_ext_bmp }} then {{ mime_type_image_bmp }}
                when {{ image_ext_jpe }} then {{ mime_type_image_jpeg }}
                when {{ image_ext_jpg }} then {{ mime_type_image_jpeg }}
                when {{ image_ext_jpeg }} then {{ mime_type_image_jpeg }}
                when {{ image_ext_gif }} then {{ mime_type_image_gif }}
                when {{ image_ext_png }} then {{ mime_type_image_png }}
                when {{ image_ext_tiff }} then {{ mime_type_image_tiff }}
              else {{ mime_type_image_jpeg }}
              end
            end
          else
            case when raw_occ.creative_mime_type is not null then raw_occ.creative_mime_type
            else
              case {{ substring_index('raw_occ.creative_url', '.', -1) }}
                when {{ image_ext_bm }} then {{ mime_type_image_bmp }}
                when {{ image_ext_bmp }} then {{ mime_type_image_bmp }}
                when {{ image_ext_jpe }} then {{ mime_type_image_jpeg }}
                when {{ image_ext_jpg }} then {{ mime_type_image_jpeg }}
                when {{ image_ext_jpeg }} then {{ mime_type_image_jpeg }}
                when {{ image_ext_gif }} then {{ mime_type_image_gif }}
                when {{ image_ext_png }} then {{ mime_type_image_png }}
                when {{ image_ext_tiff }} then {{ mime_type_image_tiff }}
              else {{ mime_type_image_jpeg }}
              end
            end
          end as creative_mime_type,
        raw_occ.publisher_id,
        raw_occ.publisher_domain,
        raw_occ.creative_width,
        raw_occ.creative_height,
        raw_occ.creative_duration,
        raw_occ.creative_url,
        raw_occ.creative_url_hash,
        raw_occ.retransmit,
        raw_occ.provider_campaign_id,
        raw_occ.provider_campaign_product_id,
        raw_occ.provider_campaign_advertiser_id,
        raw_occ.provider_campaign_name,
        raw_occ.provider_campaign_product_name,
        raw_occ.provider_campaign_advertiser_name,
        case
            when provider_code = {{ source_bis_social_code }} then
            coalesce(raw_occ.provider_campaign_description, '')
            else raw_occ.provider_campaign_description
        end as provider_campaign_description,
        raw_occ.provider_campaign_landing_page,
        raw_occ.occurrence_description,
        raw_occ.occurrence_link_url,
        case
            when provider_code in ({{ source_bis_code }}, {{ source_bis_social_code }}) then coalesce (region_dma_name, region_city_name)
            when provider_code in ({{ source_playon_code }}, {{ source_bis_ctv_code }}) then concat(region_city_name, ', ', region_state_name)
            end as provider_dma_city_name,
        case
            when regexp_like(json_extract_scalar(try(json_parse(raw_json)), '$.occurrence.creativeUrl'),
                             '^(https?://)([\w.-]+)(:[0-9]+)?(/.*)?$')
            then json_extract_scalar(try(json_parse(raw_json)), '$.occurrence.creativeUrl')
            else null
         end as occurrence_creative_url,
        case
            when provider_code = {{ source_bis_social_code }} then
                {{ not_attributed }}
            else null
        end as attribution_status,
        case
            when provider_code = {{ source_bis_social_code }} then
            coalesce(json_extract_scalar(try(json_parse(raw_json)), '$.occurrence.socialCampaignText'), '')
            else null
        end as social_campaign_text
        from
        tmp_digital_raw_occurrence raw_occ
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

cte_mime_type as (
    select
        mime_type_id,
        lower(description) as mime_type_description
    from {{ source('km_preparation_db', 'standard_mime_type') }}
    where record_status_flag = {{ status_flag_active }}
),

cte_source_channel as (
    select
        lower(short_desc) as source_channel_name,
        source_channel_id
    from {{ source('km_preparation_db', 'source_channel') }}
    where record_status_flag = {{ status_flag_active }}
),

cte_raw_occ_media_property_market_details as (
    select
        row_number() over(
            partition by occ.creative_url_hash
            order by
            occ.capture_timestamp asc
        ) as rnum,
        occ.country_iso_2_code,
        occ.provider_code,
        occ.source_channel,
        occ.provider_creative_id,
        occ.capture_month,
        occ.capture_timestamp,
        occ.creative_type,
        mt.mime_type_id,
        mpdpm.legacy_media_property_id as media_property_id,
        occ.publisher_domain,
        occ.creative_width,
        occ.creative_height,
        occ.creative_duration,
        occ.creative_url,
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
        occ.attribution_status,
        occ.provider_campaign_description,
        occ.social_campaign_text,
        cast(occ.created_timestamp as timestamp) as created_timestamp,
        sc.source_channel_id,
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
        occ.occurrence_creative_url,
        coalesce(
           regexp_extract(lower(coalesce(regexp_replace(occ.occurrence_creative_url, '[\n\r]+', ''), ' ')),
                          '^(?:https?://)?(?:[@\n]+@)?(?:www.)?([^:/\n?]+)', 1), ''
           ) as occurrence_creative_url_domain,
        mpdpm.domain_url,
        occ.provider_campaign_landing_page

    from cte_raw_occ occ
    inner join {{ source('km_preparation_db', 'data_provider') }} p
        on p.data_provider_code = occ.provider_code
        and p.record_status_flag = {{ status_flag_active }}
    inner join cte_source_channel sc
        on sc.source_channel_name = occ.source_channel
    left join cte_media_property_data_provider_mapping mpdpm
        on mpdpm.legacy_country_code = occ.country_iso_2_code
        and mpdpm.data_provider_id = p.data_provider_id
        and mpdpm.source_channel_id = sc.source_channel_id
        and mpdpm.publisher_id = occ.publisher_id
    left join cte_mime_type mt
        on occ.creative_mime_type = mt.mime_type_description
    left join {{ source('reference', 'provider_global_market_map') }} gmm
        on gmm.provider_dma_city_name = occ.provider_dma_city_name
    left join {{ source('reference', 'global_market') }} gb
        on gb.market_id = gmm.market_id
)

select * from cte_raw_occ_media_property_market_details where rnum = 1
