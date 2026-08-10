{#
  Piece 4 -- TASK 5: first-seen-info update -> gold.creative.
  Port of UpdateCreativeFirstSeenInfo.update_creative_first_seen. Propagates the dedup FAMILY's earliest
  first-seen occurrence onto the PARENT creative's gold.creative row.

  Driven by CDF changes from two gold tables (Trino table_changes is append-only, so -> timestamp-column
  watermark scans on updated_timestamp):
    * CTV_FSINFO_FROM_FIRSTSEEN  <- gold.creative_first_seen.updated_timestamp
    * CTV_FSINFO_FROM_CREATIVE   <- gold.creative.updated_timestamp
  Union the two changed-creative sets, resolve each to its parent via silver.creative_dedupe_map
  (match_type='Map'; self if unmapped), expand to the whole family (parent + all children), pick the
  family's EARLIEST creative_first_seen row (row_number order by occurrence_timestamp nulls last = 1),
  enrich with data_provider_code (km_preparation_db.data_provider, ACTIVE) and an INNER JOIN to
  reference.media (a FILTER only -- Databricks leaves `first_seen_media = media_name` COMMENTED OUT, so
  media_name is fetched but never written; the join just gates rows to those with a known media_id).

  Post-hooks: (1) MERGE the candidate into gold.creative on creative_id = parent_creative_id, update-only,
  WHEN MATCHED AND first_seen_occurrence_timestamp <> occurrence_timestamp -> set the 23 first_seen_* fields
  + updated_timestamp. (2)/(3) advance both watermarks (watermark_ts_advance_from_source, post-MERGE).

  Fidelity notes (faithful to prod, flagged): the `<>` guard is NULL-unsafe -- a parent whose
  first_seen_occurrence_timestamp is NULL will NOT be updated (same as Databricks). No safety lag on these
  internal Iceberg watermarks (idempotent, `<>`-guarded, like dedup). The CTV_FSINFO_FROM_CREATIVE advance
  reads the post-MERGE max, so it sweeps past this run's own writes (intended -- see the macro comment).

  DAG: depends on tasks 1/2/3 (they populate gold.creative / gold.creative_first_seen /
  silver.creative_dedupe_map via their hooks). gold/silver tables are referenced as LITERAL relations (not
  ref/source) since they are hook-written, not dbt model outputs.
  VALIDATE ON VM: parents updated, both watermarks advanced off their base, family-earliest is correct.
#}

-- depends_on: {{ ref('crtv_sync_creative') }}
-- depends_on: {{ ref('crtv_sync_first_seen') }}
-- depends_on: {{ ref('crtv_sync_dedupe_map') }}

{%- set wm_fs   = 'CTV_FSINFO_FROM_FIRSTSEEN' -%}
{%- set wm_crtv = 'CTV_FSINFO_FROM_CREATIVE' -%}
{%- set fs_begin   = watermark_ts_begin(wm_fs) -%}
{%- set crtv_begin = watermark_ts_begin(wm_crtv) -%}
{%- set start_fs   = (fs_begin.start_ts   | string)[:19] if fs_begin.start_ts   is not none else '1900-01-01 00:00:00' -%}
{%- set start_crtv = (crtv_begin.start_ts | string)[:19] if crtv_begin.start_ts is not none else '1900-01-01 00:00:00' -%}

{%- set match_type_map = "'Map'" -%}          {#- constants.match_type_map -#}
{%- set status_active  = "'ACTIVE'" -%}       {#- constants.status_flag_active -#}
{%- set dp    = source('km_preparation_db', 'data_provider') -%}
{%- set media = source('reference', 'media') -%}
{%- set self_rel = 'iceberg.bronze.' ~ this.identifier -%}   {#- literal: ref()/this degrade to silver in a hook -#}

{%- set merge_sql %}
merge into iceberg.gold.creative m
using {{ self_rel }} s
on m.creative_id = s.parent_creative_id
when matched and m.first_seen_occurrence_timestamp <> s.occurrence_timestamp then update set
  first_seen_provider_occurrence_id = s.occurrence_id,
  first_seen_occurrence_timestamp = s.occurrence_timestamp,
  first_seen_provider_code = s.data_provider_code,
  first_seen_media_property_id = s.media_property_id,
  first_seen_media_property_name = s.media_property_name,
  first_seen_media_category_id = s.media_category_id,
  first_seen_media_category_code = s.media_category_code,
  first_seen_provider_creative_link_url = s.provider_creative_link_url,
  first_seen_provider_publisher_id = s.provider_publisher_id,
  first_seen_provider_publisher_domain = s.provider_publisher_domain,
  first_seen_provider_campaign_id = s.provider_campaign_id,
  first_seen_provider_campaign_name = s.provider_campaign_name,
  first_seen_provider_advertiser_id = s.provider_advertiser_id,
  first_seen_provider_advertiser_name = s.provider_advertiser_name,
  first_seen_provider_product_id = s.provider_product_id,
  first_seen_provider_product_name = s.provider_product_name,
  first_seen_provider_campaign_landing_page = s.provider_campaign_landing_page,
  first_seen_market_id = s.market_id,
  first_seen_market_name = s.market_name,
  first_seen_daypart_id = s.daypart_id,
  first_seen_daypart_name = s.daypart_name,
  first_seen_affiliate_id = s.affiliate_id,
  first_seen_affiliate_name = s.affiliate_name,
  updated_timestamp = cast(current_timestamp as timestamp(6) with time zone)
{%- endset %}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'p4_sync'],
    views_enabled=false,
    on_table_exists='drop',
    post_hook=[
      merge_sql,
      "{{ watermark_ts_advance_from_source('CTV_FSINFO_FROM_FIRSTSEEN', 'iceberg.gold.creative_first_seen', 'updated_timestamp') }}",
      "{{ watermark_ts_advance_from_source('CTV_FSINFO_FROM_CREATIVE', 'iceberg.gold.creative', 'updated_timestamp') }}"
    ]
) }}

with changed as (
    select creative_id
    from iceberg.gold.creative_first_seen
    where updated_timestamp > cast(timestamp '{{ start_fs }}' as timestamp(6) with time zone)
    union
    select creative_id
    from iceberg.gold.creative
    where updated_timestamp > cast(timestamp '{{ start_crtv }}' as timestamp(6) with time zone)
),

-- resolve each changed creative to its parent (self if unmapped)
resolved_parent as (
    select distinct coalesce(cdm.parent_creative_id, cc.creative_id) as parent_creative_id
    from changed cc
    left join iceberg.silver.creative_dedupe_map cdm
      on cdm.child_creative_id = cc.creative_id and cdm.match_type = {{ match_type_map }}
),

-- expand each parent to its whole family (children + the parent itself)
fetch_entire_family as (
    select r.parent_creative_id, cdm.child_creative_id
    from resolved_parent r
    join iceberg.silver.creative_dedupe_map cdm
      on cdm.parent_creative_id = r.parent_creative_id and cdm.match_type = {{ match_type_map }}
    union all
    select parent_creative_id, parent_creative_id as child_creative_id
    from resolved_parent
),

-- per parent, the family's earliest first-seen row
family_first_seen as (
    select e.parent_creative_id, s.*
    from fetch_entire_family e
    inner join iceberg.gold.creative_first_seen s
      on e.child_creative_id = s.creative_id
    qualify row_number() over (partition by e.parent_creative_id
                               order by s.occurrence_timestamp nulls last) = 1
)

select
    fs.parent_creative_id,
    fs.occurrence_id,
    fs.occurrence_timestamp,
    dp.data_provider_code,
    fs.media_property_id,
    fs.media_property_name,
    fs.media_category_id,
    fs.media_category_code,
    fs.provider_creative_link_url,
    fs.provider_publisher_id,
    fs.provider_publisher_domain,
    fs.provider_campaign_id,
    fs.provider_campaign_name,
    fs.provider_advertiser_id,
    fs.provider_advertiser_name,
    fs.provider_product_id,
    fs.provider_product_name,
    fs.provider_campaign_landing_page,
    cast(fs.market_id  as integer) as market_id,
    fs.market_name,
    cast(fs.daypart_id as integer) as daypart_id,
    fs.daypart_name,
    fs.affiliate_id,
    fs.affiliate_name
from family_first_seen fs
inner join {{ dp }} dp
  on dp.data_provider_id = cast(fs.provider_id as integer) and dp.record_status_flag = {{ status_active }}
inner join {{ media }} media
  on cast(fs.media_id as varchar) = cast(media.media_id as varchar)
