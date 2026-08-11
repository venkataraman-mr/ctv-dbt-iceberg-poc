{#
  Piece 4 — TASK 3a: dedup-map sync, UPSERT pass (watermark CTV_SYNC_DEDUP_UPSERT).
  Port of SyncCreativeDedupMapFromPsql (insert/update): read changed rows from the seeded clone
  tempwork.creative_dedupe_map_ctv_poc, resolve match_type_id -> match_type NAME via
  reference.creative_match_type (Iceberg, UC reference sync),
  (reference.creative_match_type is in Iceberg via the UC reference sync) and MERGE into
  iceberg.silver.creative_dedupe_map on (child_creative_id, child_creative_url_hash).

  Timestamp watermark (UTC), NO lag/buffer for dedup (prod uses `> start`, no buffer; the watermark
  advances to the exact max processed, so `> start` is no-miss), idempotent MERGE.
  Types cast to the silver target (ids BIGINT; scores REAL; json_response VARCHAR; naive Postgres
  timestamps -> timestamp(6) with time zone). Delete pass (removed children) is crtv_sync_dedupe_map_delete.

  Prereq (once): seed CTV_SYNC_DEDUP_UPSERT (ddl/08).
#}

{%- set wm_name = 'CTV_SYNC_DEDUP_UPSERT' -%}
{%- set src = source('tempwork', 'creative_dedupe_map_ctv_poc') -%}
{%- set mt  = source('reference', 'creative_match_type') -%}   {#- Iceberg (UC reference sync) -#}
{%- set tgt = 'iceberg.silver.creative_dedupe_map' -%}
{%- set self_rel = this.database ~ '.bronze.' ~ this.identifier -%}

{%- set wm = watermark_ts_begin(wm_name) -%}
{%- set start_ts = wm.start_ts -%}
{%- set start_ts_naive = (start_ts | string)[:19] if start_ts is not none else '1900-01-01 00:00:00' -%}

{%- set cols = [
  'creative_dedupe_map_id','country_iso_2_code','child_creative_id','child_creative_url_hash',
  'child_provider_code','child_creative_type','child_creative_subtype','parent_creative_id',
  'parent_creative_url_hash','parent_creative_provider_code','parent_creative_type','parent_creative_subtype',
  'match_type','revision_type','is_auto_mapped','video_score','audio_score','json_response',
  'created_by_user_id','created_timestamp','updated_by_user_id','updated_timestamp'
] -%}

{#- MERGE on (child_creative_id, child_creative_url_hash); SET = all cols except those two keys. -#}
{%- set merge_sql -%}
merge into {{ tgt }} m
using {{ self_rel }} s
  on m.child_creative_id = s.child_creative_id
 and m.child_creative_url_hash = s.child_creative_url_hash
when matched then update set
  {% for c in cols if c not in ['child_creative_id','child_creative_url_hash'] %}{{ c }} = s.{{ c }}{{ "," if not loop.last }}
  {% endfor %}
when not matched then insert ({{ cols | join(', ') }})
  values ({% for c in cols %}s.{{ c }}{{ ", " if not loop.last }}{% endfor %})
{%- endset -%}

{%- set wm_finish = "{{ watermark_ts_finish_from_relation('" ~ wm_name ~ "', '" ~ self_rel ~ "', 'updated_timestamp') }}" -%}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'SYNC_CREATIVES_TO_ICEBERG'],
    views_enabled=false,
    post_hook=[merge_sql, wm_finish]
) }}

select
    cast(cdm.creative_dedupe_map_id   as bigint)                     as creative_dedupe_map_id,
    cast(cdm.country_iso_2_code       as varchar)                    as country_iso_2_code,
    cast(cdm.child_creative_id        as bigint)                     as child_creative_id,
    cast(cdm.child_creative_url_hash  as bigint)                     as child_creative_url_hash,
    cast(cdm.child_provider_code      as varchar)                    as child_provider_code,
    cast(cdm.child_creative_type      as varchar)                    as child_creative_type,
    cast(cdm.child_creative_subtype   as varchar)                    as child_creative_subtype,
    cast(cdm.parent_creative_id       as bigint)                     as parent_creative_id,
    cast(cdm.parent_creative_url_hash as bigint)                     as parent_creative_url_hash,
    cast(cdm.parent_creative_provider_code as varchar)               as parent_creative_provider_code,
    cast(cdm.parent_creative_type     as varchar)                    as parent_creative_type,
    cast(cdm.parent_creative_subtype  as varchar)                    as parent_creative_subtype,
    cast(cmt.match_type               as varchar)                    as match_type,
    cast(cdm.revision_type            as varchar)                    as revision_type,
    cast(cdm.is_auto_mapped           as boolean)                    as is_auto_mapped,
    cast(cdm.video_score              as real)                       as video_score,
    cast(cdm.audio_score              as real)                       as audio_score,
    json_format(cdm.json_response)                                   as json_response,   -- jsonb -> Trino json -> VARCHAR text
    cast(cdm.created_by_user_id       as integer)                    as created_by_user_id,
    cast(cdm.created_timestamp        as timestamp(6) with time zone) as created_timestamp,
    cast(cdm.updated_by_user_id       as integer)                    as updated_by_user_id,
    cast(cdm.updated_timestamp        as timestamp(6) with time zone) as updated_timestamp
from {{ src }} cdm
left join {{ mt }} cmt on cmt.match_type_id = cdm.match_type_id
where cdm.updated_timestamp > timestamp '{{ start_ts_naive }}'
