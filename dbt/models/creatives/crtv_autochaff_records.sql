{#
  Job A auto-chaff persist record = persist_to_auto_chaff's cte_auto_chaff_data. FAITHFUL 1:1
  transliteration: tmp_digital_raw_occ_media (tom) INNER JOIN tmp_digital_occ_auto_chaff (tac, is_auto_chaff=true).
  Column list + order preserved exactly (creative_id LAST, as in legacy). crtv_staging_final's post_hook
  inserts these into iceberg.bronze.creative_autochaff (legacy: MERGE ... WHEN NOT MATCHED INSERT *),
  anti-joined on creative_url_hash. Empty for CTV.

  first_seen_metadata: legacy parse_json(to_json(struct(...))); here a JSON-object string
  (json_object_str) since the Iceberg column is VARCHAR. Struct is the AUTO-CHAFF variant (no
  attribution_status / social_campaign_text / provider_campaign_description). Timestamps pre-formatted
  as ::timestamp-castable strings. Spark->Trino: INTERVAL 4 HOUR -> interval '4' hour.
#}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'job_a'],
    views_enabled=false
) }}

{%- set source_bis_ctv_code = "'AVOD BISCTV'" -%}
{%- set ctv_media_id        = 30 -%}
{%- set digital_media_id    = 5 -%}
{%- set ts_fmt = "'%Y-%m-%d %H:%i:%s.%f'" -%}

select
    cast(null as bigint) as legacy_creative_id,
    tom.country_iso_2_code,
    tom.provider_code,
    tom.source_channel,
    tom.provider_creative_id,
    tom.capture_month,
    tom.capture_timestamp,
    tom.creative_type,
    tom.mime_type_id,
    case when tom.provider_code = {{ source_bis_ctv_code }} then {{ ctv_media_id }} else
    {{ digital_media_id }}  end as media_id,
    tom.media_property_id,
    tom.publisher_domain,
    tom.creative_width,
    tom.creative_height,
    tom.creative_duration,
    tom.creative_url,
    tom.creative_url_hash,
    cast(null as varchar) as creative_machine_learning_payload,
    cast(null as varchar) as creative_url_override,
    cast(null as varchar) as creative_payload,
    cast(null as varchar) as record_status,
    {{ json_object_str([
        ['occurrence_id',              'tom.provider_occurrence_id'],
        ['occurrence_timestamp',       "date_format(cast(tom.capture_timestamp as timestamp(6)), " ~ ts_fmt ~ ")"],
        ['provider_code',              'tom.provider_code'],
        ['media_property_id',          'tom.media_property_id'],
        ['media_property_name',        'tom.media_property_name'],
        ['media_category_id',          'tom.media_category_id'],
        ['media_category_code',        'tom.media_category_code'],
        ['provider_creative_link_url', 'tom.provider_creative_link_url'],
        ['provider_publisher_id',      'tom.provider_publisher_id'],
        ['provider_publisher_domain',  'tom.provider_publisher_domain'],
        ['provider_campaign_id',       'tom.provider_campaign_id'],
        ['provider_campaign_name',     'tom.provider_campaign_name'],
        ['provider_advertiser_id',     'tom.provider_campaign_advertiser_id'],
        ['provider_advertiser_name',   'tom.provider_campaign_advertiser_name'],
        ['provider_product_id',        'tom.provider_campaign_product_id'],
        ['provider_product_name',      'tom.provider_campaign_product_name'],
        ['due_timestamp',              "date_format(cast(tom.capture_timestamp + interval '4' hour as timestamp(6)), " ~ ts_fmt ~ ")"],
        ['market_id',                  'tom.market_id'],
        ['market_name',                'tom.market_name'],
        ['daypart_id',                 'cast(null as integer)'],
        ['daypart_name',               'cast(null as varchar)'],
        ['affiliate_id',               'cast(null as integer)'],
        ['affiliate_name',             'cast(null as varchar)'],
        ['occurrence_description',     'tom.occurrence_description'],
        ['provider_campaign_landing_page', 'tom.provider_campaign_landing_page'],
        ['adclarity_url',              'tom.occurrence_creative_url']
    ]) }} as first_seen_metadata,
    cast(null as integer) as suggested_vx0_product_id,
    current_timestamp as created_timestamp,
    current_timestamp as updated_timestamp,
    cast(null as bigint) as creative_id
from {{ ref('crtv_staging_candidate') }} tom
inner join {{ ref('crtv_autochaff') }} tac
    on tom.creative_url_hash = tac.creative_url_hash
    and tac.is_auto_chaff = true
