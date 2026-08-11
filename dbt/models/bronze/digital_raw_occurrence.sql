{#
  Piece 1 — CTV staging -> raw occurrence (dbt-trino port of StagingToRawOccurrenceBisCtvUS).

  Watermark-driven incremental read (mirrors the legacy version watermark + Delta table_changes):
  the VERSION watermark (last_commit_version in silver.watermark_control) tracks the last staging
  snapshot processed. Each run reads ONLY new inserts via Trino's system.table_changes between the
  last processed snapshot (exclusive) and the current latest snapshot (inclusive) — no full scan.
  The one exception is the very first run (watermark NULL): table_changes needs a start snapshot, so
  we read the full current snapshot once, then record the watermark; every run after is incremental.
  The watermark row 'BIS_CTV_US_INGESTION_STG_TO_RAW_OCC' must be seeded in watermark_control first
  (ddl/03), with last_commit_version = NULL for the initial full load.

  Fidelity notes (mirrors legacy exactly except where noted):
    - creative_url_hash is NOT recomputed here — it is the precomputed exact Spark xxhash64(seed 42)
      carried from the landing step (Trino's built-in xxhash64 uses seed 0 and can't match Spark).
    - Dedup order key is the STAGING load timestamp (staging_loaded_at), standing in for the legacy
      CDF _commit_timestamp: latest-landed row per occurrence id wins.
    - Idempotency is the legacy LEFT ANTI JOIN on (provider_occurrence_id, capture_month) against
      the target — kept alongside table_changes, exactly as legacy does.
    - raw_json stores the original occurrence JSON text (VARIANT->string). Legacy stored the
      schema-normalized re-serialization; keeping the raw text is a closer record of the source.
    - Requires the Trino session time zone = UTC so captureDate parses to UTC (matches legacy).
#}
{%- set wm_name = 'BIS_CTV_US_INGESTION_STG_TO_RAW_OCC' -%}
{%- set staging_src = source('bronze', 'digtial_raw_occurrence_ctv_staging') -%}
{%- set wm = watermark_version_begin(wm_name, staging_src) -%}

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='bronze',
    tags=['bronze', 'BIS_CTV_BZ2FILE_TO_RAW_OCC'],
    views_enabled=false,
    post_hook="{{ watermark_version_finish('" ~ wm_name ~ "') }}",
    properties={
      'partitioning': "ARRAY['capture_month']",
      'sorted_by': "ARRAY['provider_occurrence_id']"
    }
) }}

with staged as (
{%- if wm.start_version is none %}
    -- first run (no watermark): one-time full read, pinned at the end snapshot
    select
        json_parse(json_data) as j,
        json_data             as raw_json_text,
        creative_url_hash,
        created_timestamp     as staging_loaded_at
    from {{ staging_src }}
    {%- if wm.end_version is not none %} for version as of {{ wm.end_version }}{%- endif %}
{%- elif wm.start_version == wm.end_version %}
    -- no new staging snapshots since the last run
    select
        json_parse(json_data) as j,
        json_data             as raw_json_text,
        creative_url_hash,
        created_timestamp     as staging_loaded_at
    from {{ staging_src }}
    where false
{%- else %}
    -- incremental: only inserts added after the last processed snapshot (Delta table_changes analog)
    select
        json_parse(json_data) as j,
        json_data             as raw_json_text,
        creative_url_hash,
        created_timestamp     as staging_loaded_at
    from table(system.table_changes(
        schema_name       => 'bronze',
        table_name        => 'digtial_raw_occurrence_ctv_staging',
        start_snapshot_id => {{ wm.start_version }},
        end_snapshot_id   => {{ wm.end_version }}))
    where _change_type = 'insert'
{%- endif %}
),

typed as (
    select
        j,
        raw_json_text,
        creative_url_hash,
        staging_loaded_at,
        json_extract_scalar(j, '$.occurrence.id') as occurrence_id,
        -- captureDate: observed as '2026-07-01T05:41:39.000+0000'. Try ISO-8601, then the explicit
        -- millis+offset Joda pattern, then a plain 'yyyy-MM-dd HH:mm:ss'. All land in UTC.
        coalesce(
            try(cast(from_iso8601_timestamp(json_extract_scalar(j, '$.occurrence.captureDate')) as timestamp(6))),
            try(cast(parse_datetime(json_extract_scalar(j, '$.occurrence.captureDate'), 'yyyy-MM-dd''T''HH:mm:ss.SSSZ') as timestamp(6))),
            try(cast(json_extract_scalar(j, '$.occurrence.captureDate') as timestamp(6)))
        ) as capture_ts,
        try(cast(json_extract_scalar(j, '$.occurrence.creative.videoAttributes.duration') as integer)) as video_duration
    from staged
),

parsed as (
    select
        upper(json_extract_scalar(j, '$.occurrence.source.region.countryId'))              as country_iso_2_code,
        'AVOD BISCTV'                                                                       as provider_code,
        lower(json_extract_scalar(j, '$.channel'))                                          as source_channel,
        occurrence_id                                                                       as provider_occurrence_id,
        try(cast(json_extract_scalar(j, '$.occurrence.creative.id') as bigint))             as provider_creative_id,
        try(cast(json_extract_scalar(j, '$.occurrence.source.id') as bigint))               as provider_source_id,
        cast(capture_ts as date)                                                            as capture_date,
        cast(date_format(capture_ts, '%Y%m') as integer)                                    as capture_month,
        cast(capture_ts as timestamp(6) with time zone)                                     as capture_timestamp,
        cast(null as timestamp(6) with time zone)                                           as eventhub_enqueued_timestamp,
        cast(current_timestamp as timestamp(6) with time zone)                              as created_timestamp,
        try(cast(json_extract_scalar(j, '$.occurrence.source.region.dmaId') as integer))    as region_dma_id,
        json_extract_scalar(j, '$.occurrence.source.region.dmaName')                        as region_dma_name,
        json_extract_scalar(j, '$.occurrence.source.region.countryId')                      as region_country_code,
        try(cast(json_extract_scalar(j, '$.occurrence.source.region.cityId') as integer))   as region_city_id,
        json_extract_scalar(j, '$.occurrence.source.region.cityName')                       as region_city_name,
        try(cast(json_extract_scalar(j, '$.occurrence.source.region.stateId') as integer))  as region_state_id,
        json_extract_scalar(j, '$.occurrence.source.region.stateName')                      as region_state_name,
        json_extract_scalar(j, '$.occurrence.creative.type')                                as creative_type,
        json_extract_scalar(j, '$.occurrence.creative.mimeType')                            as creative_mime_type,
        try(cast(json_extract_scalar(j, '$.occurrence.source.publisher.id') as bigint))     as publisher_id,
        json_extract_scalar(j, '$.occurrence.source.publisher.domain')                      as publisher_domain,
        cast(null as integer)                                                               as creative_width,
        cast(null as integer)                                                               as creative_height,
        case when video_duration is null or video_duration = 0 then video_duration
             else cast(cast(video_duration as double) / 1000 as integer) end                as creative_duration,
        json_extract_scalar(j, '$.occurrence.creative.url')                                 as creative_url,
        creative_url_hash                                                                   as creative_url_hash,
        try(cast(json_extract_scalar(j, '$.retransmit') as boolean))                        as retransmit,
        try(cast(json_extract_scalar(j, '$.occurrence.campaign.id') as bigint))             as provider_campaign_id,
        try(cast(json_extract_scalar(j, '$.occurrence.campaign.product.id') as bigint))     as provider_campaign_product_id,
        try(cast(json_extract_scalar(j, '$.occurrence.campaign.advertiser.id') as bigint))  as provider_campaign_advertiser_id,
        json_extract_scalar(j, '$.occurrence.campaign.name')                                as provider_campaign_name,
        json_extract_scalar(j, '$.occurrence.campaign.product.name')                        as provider_campaign_product_name,
        json_extract_scalar(j, '$.occurrence.campaign.advertiser.name')                     as provider_campaign_advertiser_name,
        json_extract_scalar(j, '$.occurrence.campaign.description')                          as provider_campaign_description,
        json_extract_scalar(j, '$.occurrence.campaign.landingPage')                         as provider_campaign_landing_page,
        cast(null as varchar)                                                               as occurrence_description,
        cast(null as varchar)                                                               as occurrence_link_url,
        json_format(json_extract(j, '$.occurrence.unifiedChain'))                           as daisy_chain,
        try(cast(json_extract_scalar(j, '$.occurrence.purchaseMethod') as smallint))        as purchase_method_id,
        cast(null as varchar)                                                               as ad_insertion_point,
        raw_json_text                                                                       as raw_json,
        row_number() over (partition by occurrence_id order by staging_loaded_at desc)      as r_num
    from typed
)

select
    country_iso_2_code,
    provider_code,
    source_channel,
    provider_occurrence_id,
    provider_creative_id,
    provider_source_id,
    capture_date,
    capture_month,
    capture_timestamp,
    eventhub_enqueued_timestamp,
    created_timestamp,
    region_dma_id,
    region_dma_name,
    region_country_code,
    region_city_id,
    region_city_name,
    region_state_id,
    region_state_name,
    creative_type,
    creative_mime_type,
    publisher_id,
    publisher_domain,
    creative_width,
    creative_height,
    creative_duration,
    creative_url,
    creative_url_hash,
    retransmit,
    provider_campaign_id,
    provider_campaign_product_id,
    provider_campaign_advertiser_id,
    provider_campaign_name,
    provider_campaign_product_name,
    provider_campaign_advertiser_name,
    provider_campaign_description,
    provider_campaign_landing_page,
    occurrence_description,
    occurrence_link_url,
    daisy_chain,
    purchase_method_id,
    ad_insertion_point,
    raw_json
from parsed f
where r_num = 1
  and creative_type = 'video'
  and creative_mime_type = 'video/mp4'
  and publisher_id in (32734360, 33434701, 2945144, 2786484, 43838908, 38149324,
                       11019659, 29528950, 2947526, 29321129, 204389)
{% if is_incremental() %}
  and not exists (
      select 1
      from {{ this }} t
      where t.provider_occurrence_id = f.provider_occurrence_id
        and t.capture_month = f.capture_month
  )
{% endif %}
