{#
  Piece 5 -- HALF B: react to creative changes -> update EXISTING gold occurrences.
  Port of get_new_creative_data_cdf + merge_statement_creative_match_unmatch_update + update_gold_occ_delete_flag.

  Databricks reads gold.creative via version table_changes; gold.creative is MERGE-written (delete files) so Trino
  version-CDF is invalid there -> TIMESTAMP watermark DIGITAL_CRTV_CHANGES_TO_GOLD_OCC on gold.creative.updated_timestamp
  (same adaptation as first-seen-info). Body = the changed creatives (deduped to the newest per
  creative_url_hash+country). Hold RELEASE is handled by Half A (it unions the staging buffer each run); Half B only
  UPDATES existing gold rows:
    1. re-parent (STEP-9): on a dedup mapping change, set gold occ.creative_id + provider_parent_creative_url_hash.
       MATCH branch (child now maps to a parent, Advert) UNION UNMATCH branch (creative no longer 'Map'-mapped).
    2. delete_flag (STEP-9.1): a creative that became NonAd/BadAd -> set its gold occurrences delete_flag = true.
    3. advance the watermark (gold.creative unmodified by these MERGEs -> exact changed-set max).

  PARKED (documented): update_house_ad_flag (STEP-10) -- a house-ad refresh gated on gold.digital_spend_availability
  (not populated in this PoC; also needs source_ic_code/source_EDO_code). Add later if spend-availability lands.
#}

-- depends_on: {{ ref('digital_occ_gold') }}

{%- set wm = 'DIGITAL_CRTV_CHANGES_TO_GOLD_OCC' -%}
{%- set begin = watermark_ts_begin(wm) -%}
{%- set start = (begin.start_ts | string)[:19] if begin.start_ts is not none else '1900-01-01 00:00:00' -%}
{%- set self_rel = 'iceberg.bronze.' ~ this.identifier -%}
{%- set gold_occ = 'iceberg.gold.digital_gold_occurrence' -%}
{%- set dedupe   = 'iceberg.silver.creative_dedupe_map' -%}

{%- set match_type_map = "'Map'" -%}
{%- set classification_type_Advert = "'Advert'" -%}
{%- set classification_type_NonAd  = "'NonAd'" -%}
{%- set classification_type_BadAd  = "'BadAd'" -%}

{%- set reparent_merge %}
merge into {{ gold_occ }} target
using (
    -- MATCH: changed Advert child now maps to a parent (dedup Map) whose id differs from the occurrence's creative_id
    select occ.occurrence_id,
           coalesce(cdm.parent_creative_id, cc.creative_id) as creative_id,
           cdm.parent_creative_url_hash
    from {{ self_rel }} cc
    inner join {{ dedupe }} cdm
        on cc.creative_url_hash = cdm.child_creative_url_hash and cdm.match_type = {{ match_type_map }}
    inner join {{ gold_occ }} occ
        on cc.creative_url_hash = occ.provider_original_creative_url_hash
    where cc.classification_type = {{ classification_type_Advert }}
      and cdm.parent_creative_id is not null
      and cdm.parent_creative_id != occ.creative_id

    union

    -- UNMATCH: changed creative no longer 'Map'-mapped; point the occurrence back at the creative itself
    select occd.occurrence_id,
           tcc.creative_id,
           cast(null as bigint) as parent_creative_url_hash
    from {{ self_rel }} tcc
    left join {{ dedupe }} tcdm
        on tcc.creative_url_hash = tcdm.child_creative_url_hash
    inner join {{ gold_occ }} occd
        on tcc.creative_url_hash = occd.provider_original_creative_url_hash
       and tcc.creative_id != occd.creative_id
    where coalesce(tcdm.match_type, '') != {{ match_type_map }}
) source
on target.occurrence_id = source.occurrence_id
when matched then update set
    creative_id = source.creative_id,
    provider_parent_creative_url_hash = source.parent_creative_url_hash,
    updated_timestamp = cast(current_timestamp as timestamp(6) with time zone)
{%- endset %}

{%- set delete_flag_merge %}
merge into {{ gold_occ }} target
using {{ self_rel }} source
on target.creative_id = source.creative_id
when matched and source.classification_type in ({{ classification_type_NonAd }}, {{ classification_type_BadAd }})
              and target.delete_flag = false
then update set
    delete_flag = true,
    updated_timestamp = cast(current_timestamp as timestamp(6) with time zone)
{%- endset %}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['occurrences', 'DIGITAL_RAW_OCC_TO_GOLD_OCC'],
    views_enabled=false,
    on_table_exists='drop',
    post_hook=[
      reparent_merge,
      delete_flag_merge,
      "{{ watermark_ts_advance_from_source('DIGITAL_CRTV_CHANGES_TO_GOLD_OCC', 'iceberg.gold.creative', 'updated_timestamp') }}"
    ]
) }}

select * from (
    select
        country_iso_2_code, creative_id, creative_url_hash, classification_type, provider_code,
        primary_product_id, first_seen_provider_occurrence_id, first_seen_occurrence_id,
        row_number() over (partition by creative_url_hash, country_iso_2_code
                           order by updated_timestamp desc) as rnum
    from iceberg.gold.creative
    where updated_timestamp > cast(timestamp '{{ start }}' as timestamp(6) with time zone)
)
where rnum = 1
