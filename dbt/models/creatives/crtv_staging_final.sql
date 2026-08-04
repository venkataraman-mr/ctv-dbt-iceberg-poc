{#
  Job A final — combine + push. FAITHFUL transliteration of get_new_creatives / generate_internal_id /
  get_previous_creratives / combine_creatives, plus the persist steps from process_to_crtv_staging.

  BODY = the combined staging record (legacy jobwork.tmp_digital_raw_occ_to_crtv_staging): the 26-column
  contract with first_seen_metadata, unioning:
    * new creatives      (get_new_creatives): exclude set ANTI JOIN creative_unique_urls, id assigned
                          from the reserved sequence block: base + row_number() over (order by
                          created_timestamp) - 1  (legacy row_number() + (min_id - 1), API -> sequence).
    * previous creatives (get_previous_creratives): exclude set JOIN creative_unique_urls is_staged=false,
                          reusing cuu.creative_id.
  Both share generate_internal_id's projection; only creative_id differs, so it's one projection with a
  CASE on is_new (equivalent, avoids duplicating the 26-col block).

  POST-HOOKS (ordered exactly as process_to_crtv_staging, watermark last so it only advances after the
  push succeeds):
    1 persist_to_auto_chaff        -> insert crtv_autochaff_records into bronze.creative_autochaff (anti-join)
    2 persist_to_unique_url        -> insert NEW rows into bronze.creative_unique_urls (is_staged=false)
    3 drop + 4 create PG temp      -> tempwork.tmp_digital_raw_occ_to_crtv_staging_ctv_poc (cross-catalog CTAS)
    5 insert_to_crtv_staging_...   -> pg_call the insert proc
    6 update_flag_creative_unique_url -> flip is_staged=true
    7 watermark_version_finish

  Spark->Trino: to_json(struct(..)) -> json_object_str ; INTERVAL 4 HOUR -> interval '4' hour ;
  MERGE WHEN NOT MATCHED -> INSERT ... WHERE NOT EXISTS ; jobwork Delta temp -> this Iceberg table.
#}

{%- set wm_name = 'DIGITAL_RAW_OCC_TO_CRTV_STAGING' -%}
{%- set uu = source('bronze', 'creative_unique_urls') -%}
{%- set autochaff_tbl = source('bronze', 'creative_autochaff') -%}
{%- set dp = source('km_preparation_db', 'data_provider') -%}
{%- set tmp_pg = 'tempwork.tmp_digital_raw_occ_to_crtv_staging_ctv_poc' -%}
{%- set tmp_pg_cat = 'postgres.' ~ tmp_pg -%}
{#- Relations used inside post_hooks MUST be plain literal strings, not source()/ref() Relation
    objects. post_hook strings are captured at parse and re-rendered at run; a Relation embedded via
    {% set %} degrades to the model's own relation (iceberg.silver.<model>) on that re-render, while a
    plain string survives intact. (self_rel is built from the stable this.database/this.identifier +
    the explicit 'bronze' schema, since this.schema would be the parse-time default 'silver'.)
    Ordering to crtv_autochaff_records — only referenced in a hook — is preserved via depends_on below. -#}
{%- set self_rel             = this.database ~ '.bronze.' ~ this.identifier -%}
{%- set rel_uu               = 'iceberg.bronze.creative_unique_urls' -%}
{%- set rel_autochaff        = 'iceberg.bronze.creative_autochaff' -%}
{%- set rel_autochaff_recs   = 'iceberg.bronze.crtv_autochaff_records' -%}
{%- set rel_data_provider    = 'iceberg.km_preparation_db.data_provider' -%}
-- depends_on: {{ ref('crtv_autochaff_records') }}
{%- set ts_fmt = "'%Y-%m-%d %H:%i:%s.%f'" -%}
{%- set source_bis_ctv_code = "'AVOD BISCTV'" -%}
{%- set source_bis_social_code = "'BISSocial'" -%}
{%- set record_status_O = "'O'" -%}
{%- set ctv_media_id = 30 -%}
{%- set digital_media_id = 5 -%}
{%- set status_flag_active = "'ACTIVE'" -%}

{#- reserve the creative_id block for NEW creatives (count = exclude set not in creative_unique_urls) -#}
{%- if execute -%}
  {%- set n_new_sql -%}
    select count(*) as n
    from {{ ref('crtv_staging_excluded') }} e
    left join {{ uu }} cuu on e.creative_url_hash = cuu.creative_url_hash
    where cuu.creative_url_hash is null
  {%- endset -%}
  {%- set n_new = run_query(n_new_sql).rows[0]['n'] -%}
  {%- set id_base = reserve_creative_ids(n_new) -%}
{%- else -%}
  {%- set id_base = 0 -%}
{%- endif -%}

{#- ---- POST-HOOKS (built as strings; ordered) --------------------------------------------------- -#}
{%- set h_autochaff -%}
insert into {{ rel_autochaff }} (
    creative_id, legacy_creative_id, country_iso_2_code, provider_code, source_channel,
    provider_creative_id, capture_month, capture_timestamp, creative_type, mime_type_id, media_id,
    media_property_id, publisher_domain, creative_width, creative_height, creative_duration,
    creative_url, creative_url_hash, creative_machine_learning_payload, creative_url_override,
    creative_payload, record_status, first_seen_metadata, suggested_vx0_product_id,
    created_timestamp, updated_timestamp)
select
    src.creative_id, src.legacy_creative_id, src.country_iso_2_code, src.provider_code, src.source_channel,
    src.provider_creative_id, src.capture_month, src.capture_timestamp, src.creative_type, src.mime_type_id, src.media_id,
    src.media_property_id, src.publisher_domain, src.creative_width, src.creative_height, src.creative_duration,
    src.creative_url, src.creative_url_hash, src.creative_machine_learning_payload, src.creative_url_override,
    src.creative_payload, src.record_status, src.first_seen_metadata, src.suggested_vx0_product_id,
    src.created_timestamp, src.updated_timestamp
from {{ rel_autochaff_recs }} src
where not exists (select 1 from {{ rel_autochaff }} t where t.creative_url_hash = src.creative_url_hash)
{%- endset -%}

{%- set h_uu_insert -%}
insert into {{ rel_uu }} (
    creative_id, provider_creative_id, provider_creative_url, creative_url_hash,
    created_timestamp, is_staged, first_seen_media_id, first_seen_provider_id)
select
    s.creative_id, s.provider_creative_id, s.creative_url, s.creative_url_hash,
    cast(current_timestamp as timestamp(6) with time zone), false, s.media_id, dp.data_provider_id
from {{ self_rel }} s
left join (select distinct data_provider_id, data_provider_code
           from {{ rel_data_provider }} where record_status_flag = {{ status_flag_active }}) dp
       on s.provider_code = dp.data_provider_code
where not exists (select 1 from {{ rel_uu }} u where u.creative_url_hash = s.creative_url_hash)
{%- endset -%}

{%- set h_pg_drop = 'drop table if exists ' ~ tmp_pg_cat -%}

{%- set h_pg_ctas -%}
create table {{ tmp_pg_cat }} as
select
    creative_id,
    legacy_creative_id,
    country_iso_2_code,
    provider_code,
    source_channel,
    provider_creative_id,
    capture_month,
    cast(capture_timestamp as timestamp(6)) as capture_timestamp,
    creative_type,
    mime_type_id,
    media_id,
    media_property_id,
    publisher_domain,
    creative_width,
    creative_height,
    creative_duration,
    creative_url,
    creative_url_hash,
    creative_machine_learning_response,
    creative_url_override,
    creative_payload,
    record_status,
    first_seen_metadata,
    suggested_vx0_product_id,
    cast(created_timestamp as timestamp(6)) as created_timestamp,
    cast(updated_timestamp as timestamp(6)) as updated_timestamp
from {{ self_rel }}
{%- endset -%}

{%- set h_call = pg_call("call tempwork.sp_dbx_digital_insert_crtv_staging_first_seen_ctv_poc('" ~ tmp_pg ~ "')") -%}

{%- set h_flip -%}
update {{ rel_uu }} set is_staged = true
where creative_url_hash in (select creative_url_hash from {{ self_rel }})
  and is_staged = false
{%- endset -%}

{#- watermark finish is passed as a RUN-TIME template string (rendered at run, execute=True), NOT a
    {% set %} capture: watermark_version_finish returns a no-op "select 1" when execute is False, so
    capturing it at parse would freeze the no-op. Same pattern Piece 1 uses. -#}
{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'job_a'],
    views_enabled=false,
    post_hook=[h_autochaff, h_uu_insert, h_pg_drop, h_pg_ctas, h_call, h_flip,
               "{{ watermark_version_finish('" ~ wm_name ~ "') }}"]
) }}

with excluded as (
    select * from {{ ref('crtv_staging_excluded') }}
),

{#- classify: not in creative_unique_urls -> new (get_new_creatives); present (is_staged=false, since
    is_staged=true was removed upstream) -> previous (get_previous_creratives), reuse cuu.creative_id -#}
enriched as (
    select
        e.*,
        cuu.creative_id                 as existing_creative_id,
        (cuu.creative_url_hash is null)  as is_new
    from excluded e
    left join {{ uu }} cuu on e.creative_url_hash = cuu.creative_url_hash
),

with_id as (
    select
        *,
        case when is_new
             then {{ id_base }} + row_number() over (partition by is_new order by created_timestamp) - 1
             else existing_creative_id
        end as creative_id
    from enriched
)

{#- generate_internal_id projection (26-col staging contract) -#}
select
    cast(null as bigint)                as legacy_creative_id,
    country_iso_2_code,
    provider_code,
    source_channel,
    provider_creative_id,
    capture_month,
    capture_timestamp,
    creative_type,
    mime_type_id,
    case when provider_code = {{ source_bis_ctv_code }} then {{ ctv_media_id }} else {{ digital_media_id }} end as media_id,
    media_property_id,
    publisher_domain,
    creative_width,
    creative_height,
    creative_duration,
    creative_url,
    creative_url_hash,
    case when provider_code = {{ source_bis_social_code }}
         then {{ json_object_str([['attribution_status', 'attribution_status']]) }}
         else cast(null as varchar) end as creative_machine_learning_response,
    cast(null as varchar)               as creative_url_override,
    cast(null as varchar)               as creative_payload,
    case when provider_code = {{ source_bis_social_code }} then {{ record_status_O }} else cast(null as varchar) end as record_status,
    {{ json_object_str([
        ['occurrence_id',              'provider_occurrence_id'],
        ['occurrence_timestamp',       "date_format(cast(capture_timestamp as timestamp(6)), " ~ ts_fmt ~ ")"],
        ['provider_code',              'provider_code'],
        ['media_property_id',          'media_property_id'],
        ['media_property_name',        'media_property_name'],
        ['media_category_id',          'media_category_id'],
        ['media_category_code',        'media_category_code'],
        ['provider_creative_link_url', 'provider_creative_link_url'],
        ['provider_publisher_id',      'provider_publisher_id'],
        ['provider_publisher_domain',  'provider_publisher_domain'],
        ['provider_campaign_id',       'provider_campaign_id'],
        ['provider_campaign_name',     'provider_campaign_name'],
        ['provider_advertiser_id',     'provider_campaign_advertiser_id'],
        ['provider_advertiser_name',   'provider_campaign_advertiser_name'],
        ['provider_product_id',        'provider_campaign_product_id'],
        ['provider_product_name',      'provider_campaign_product_name'],
        ['due_timestamp',              "date_format(cast(capture_timestamp + interval '4' hour as timestamp(6)), " ~ ts_fmt ~ ")"],
        ['market_id',                  'market_id'],
        ['market_name',                'market_name'],
        ['daypart_id',                 'cast(null as integer)'],
        ['daypart_name',               'cast(null as varchar)'],
        ['affiliate_id',               'cast(null as integer)'],
        ['affiliate_name',             'cast(null as varchar)'],
        ['occurrence_description',     'occurrence_description'],
        ['attribution_status',         'attribution_status'],
        ['provider_campaign_description', 'provider_campaign_description'],
        ['social_campaign_text',       'social_campaign_text'],
        ['provider_campaign_landing_page', 'provider_campaign_landing_page'],
        ['adclarity_url',              'occurrence_creative_url']
    ]) }}                               as first_seen_metadata,
    cast(null as integer)               as suggested_vx0_product_id,
    created_timestamp,
    cast(current_timestamp as timestamp(6) with time zone) as updated_timestamp,
    creative_id
from with_id
