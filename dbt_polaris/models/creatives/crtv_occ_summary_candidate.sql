{#
  Job B — occurrence summary candidate = TEMP_all_OCCURRENCE_MARKET_MEDIA_VIEW. FAITHFUL 1:1
  transliteration of CrtvOccSummary.process_occurrence_to_psql_and_staging (STEP-1 get_raw_occurrence_cdf
  + STEP-2 all_occ_media_market_df), minus the PrintCreativeOccurrenceSummary path.

  Reads new digital_raw_occurrence inserts (VERSION watermark DIGITAL_RAW_OCC_SUMMARY_PSQL), UNIONs the
  parked buffer bronze.missing_digital_occurrence_for_summary (occurrences whose creative wasn't staged
  yet), dedups latest per (provider_code, provider_occurrence_id), then joins the media/market dims +
  creative_unique_urls (to resolve creative_id) and anti-joins creative_autochaff. Output feeds:
    * crtv_occ_summary_final  — aggregate -> CALL the upsert proc (rows with creative_id + media_property_id)
    * the park/release MERGE  — DELETE resolved / INSERT unresolved back into the buffer (in _final's hooks)

  Spark->Trino: table_changes signature (+ version begin/first-run branch); SELECT * EXCEPT(row_num) ->
  explicit column list; ANTI JOIN -> LEFT JOIN ... IS NULL. Scratch model (tagged CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY).
#}

{%- set wm_name = 'DIGITAL_RAW_OCC_SUMMARY_PSQL' -%}
{%- set raw_rel = ref('digital_raw_occurrence') -%}
{%- set wm = watermark_version_begin(wm_name, raw_rel) -%}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY'],
    views_enabled=false
) }}

{%- set missing = source('bronze', 'missing_digital_occurrence_for_summary') -%}
{%- set country_code_US     = "'US'" -%}
{%- set status_flag_active  = "'ACTIVE'" -%}
{%- set bc_active_status_id = 92 -%}
{%- set source_bis_code        = "'BIS'" -%}
{%- set source_bis_social_code = "'BISSocial'" -%}
{%- set source_playon_code     = "'PlayOn'" -%}
{%- set source_bis_ctv_code    = "'AVOD BISCTV'" -%}
{%- set ctv_media_id     = 30 -%}
{%- set digital_media_id = 5 -%}

with

-- STEP-1 get_raw_occurrence_cdf (version)
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

temp_raw_digital_occurrence as (
    select
        provider_occurrence_id,
        creative_url_hash,
        provider_code,
        country_iso_2_code,
        source_channel,
        case
            when provider_code in ({{ source_bis_code }}, {{ source_bis_social_code }}) then coalesce(region_dma_name, region_city_name)
            when provider_code in ({{ source_playon_code }}, {{ source_bis_ctv_code }}) then concat(region_city_name, ', ', region_state_name)
        end as provider_dma_city_name,
        publisher_id,
        capture_date,
        capture_timestamp
    from cdf_source
),

{#- STEP-2 deduped_occurence_cte: UNION new CDF + parked buffer, latest per (provider_code, provider_occurrence_id) -#}
deduped_occurence_cte as (
    select
        provider_occurrence_id, creative_url_hash, provider_code, country_iso_2_code,
        source_channel, provider_dma_city_name, publisher_id, capture_date, capture_timestamp
    from (
        select
            u.*,
            row_number() over (partition by provider_code, provider_occurrence_id order by capture_date desc) as row_num
        from (
            select provider_occurrence_id, creative_url_hash, provider_code, country_iso_2_code,
                   source_channel, provider_dma_city_name, publisher_id, capture_date, capture_timestamp
            from temp_raw_digital_occurrence
            union
            select provider_occurrence_id, creative_url_hash, provider_code, country_iso_2_code,
                   source_channel, provider_dma_city_name, publisher_id, capture_date, capture_timestamp
            from {{ missing }}
        ) u
    ) rocc
    where rocc.row_num = 1
),

media_property_data_provider_mapping_cte as (
    select
        mpf.media_property_id,
        mpf.legacy_media_property_id,
        mpf.legacy_country_code,
        mpm.data_provider_id,
        mpf.market_id,
        mpm.source_channel_id,
        mpm.publisher_id
    from {{ source('km_preparation_gold_db', 'media_property_flatten') }} mpf
    inner join {{ source('km_preparation_db', 'media_property_data_provider_map') }} mpm
        on mpf.country_id = mpm.country_id
        and mpf.media_property_id = mpm.media_property_id
        and mpf.source_channel_id = mpm.source_channel_id
        and mpm.record_status_flag = {{ status_flag_active }}
        and mpf.media_property_status_id = {{ bc_active_status_id }}
)

select
    s.provider_occurrence_id,
    s.provider_code,
    cs.creative_id,
    s.creative_url_hash,
    s.country_iso_2_code,
    case
        when gmm.market_id is not null then gmm.market_id
        when gmm.market_id is null and mpdpm.market_id > 0 then mpdpm.market_id
        else 302
        end as market_id,
    case when s.provider_code = {{ source_bis_ctv_code }} then {{ ctv_media_id }} else
    me.media_id end as media_id,
    mpdpm.legacy_media_property_id as media_property_id,
    s.provider_dma_city_name,
    s.publisher_id,
    s.capture_date,
    s.capture_timestamp,
    s.source_channel
from deduped_occurence_cte s
inner join {{ source('km_preparation_db', 'data_provider') }} p
        on p.data_provider_code = s.provider_code
        and p.record_status_flag = {{ status_flag_active }}
left join {{ source('km_preparation_db', 'source_channel') }} sc
        on lower(sc.short_desc) = s.source_channel
        and sc.record_status_flag = {{ status_flag_active }}
left join {{ source('reference', 'provider_global_market_map') }} gmm
    on s.provider_dma_city_name = gmm.provider_dma_city_name
left join {{ source('bronze', 'creative_unique_urls') }} cs
    on cs.creative_url_hash = s.creative_url_hash
left join media_property_data_provider_mapping_cte mpdpm
        on mpdpm.legacy_country_code = s.country_iso_2_code
        and mpdpm.data_provider_id = p.data_provider_id
        and mpdpm.source_channel_id = sc.source_channel_id
        and mpdpm.publisher_id = s.publisher_id
left join {{ source('reference', 'media') }} me
    on me.media_id = {{ digital_media_id }}
left join {{ source('bronze', 'creative_autochaff') }} och
    on och.creative_url_hash = s.creative_url_hash
where och.creative_url_hash is null
