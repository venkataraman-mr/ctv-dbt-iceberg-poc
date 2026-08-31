{#
  Piece 4 — TASK 3b: dedup-map sync, DELETE pass (watermark CTV_SYNC_DEDUP_DELETE).
  Port of SyncCreativeDedupMapFromPsql (delete): find our clone creatives updated since the watermark
  that are NO LONGER a child in the clone dedupe_map (their mapping was removed), and DELETE those
  rows from iceberg.silver.creative_dedupe_map by child_creative_id.

  Candidate = the removed-child creative_ids (+ updated_timestamp for the watermark). Post-hooks:
  (1) DELETE from silver by child_creative_id; (2) advance the watermark. Timestamp watermark (UTC),
  NO lag/buffer (dedup, `> start`). child_creative_id in silver is the reserved clone id, matching
  creative_ctv_poc.creative_id.

  Prereq (once): seed CTV_SYNC_DEDUP_DELETE (ddl/nessie/08).
#}

-- depends_on: {{ ref('crtv_sync_dedupe_map') }}
{#- Force this DELETE pass to run AFTER the upsert model (both write silver.creative_dedupe_map).
    No actual data dependency — just a DAG edge so dbt never runs them concurrently, even at threads>1. #}

{%- set wm_name = 'CTV_SYNC_DEDUP_DELETE' -%}
{%- set crtv = source('tempwork', 'creative_ctv_poc') -%}
{%- set dm   = source('tempwork', 'creative_dedupe_map_ctv_poc') -%}
{%- set tgt = 'iceberg.silver.creative_dedupe_map' -%}
{%- set self_rel = this.database ~ '.bronze.' ~ this.identifier -%}

{%- set wm = watermark_ts_begin(wm_name) -%}
{%- set start_ts = wm.start_ts -%}
{%- set start_ts_naive = (start_ts | string)[:19] if start_ts is not none else '1900-01-01 00:00:00' -%}

{%- set delete_sql -%}
delete from {{ tgt }}
where child_creative_id in (select creative_id from {{ self_rel }})
{%- endset -%}

{%- set wm_finish = "{{ watermark_ts_finish_from_relation('" ~ wm_name ~ "', '" ~ self_rel ~ "', 'updated_timestamp') }}" -%}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'SYNC_CREATIVES_TO_ICEBERG'],
    views_enabled=false,
    post_hook=[delete_sql, wm_finish]
) }}

select
    cast(c.creative_id       as bigint)                      as creative_id,
    cast(c.updated_timestamp as timestamp(6) with time zone) as updated_timestamp
from {{ crtv }} c
left join {{ dm }} d on d.child_creative_id = c.creative_id
where c.updated_timestamp > timestamp '{{ start_ts_naive }}'
  and d.child_creative_id is null
