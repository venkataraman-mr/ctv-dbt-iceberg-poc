{#
  Piece 4 -- TASK 6: last-seen-info update -> gold.creative.last_seen_timestamp.
  Port of UpdateCreativeLastSeenInfo.update_creative_last_seen. Sets each changed creative's last_seen_timestamp
  to its LATEST occurrence capture_timestamp.

  Databricks reads CDF from three occurrence tables (tv / digital / live_events) under three watermarks; for the
  CTV PoC only DIGITAL applies (tv + live_events dropped). Trino table_changes is append-only -> timestamp-column
  watermark scan on gold.digital_gold_occurrence.updated_timestamp (CTV_LAST_SEEN_DIGITAL).

  Flow: changed creatives = distinct creative_id from digital_gold_occurrence where updated_timestamp > wm ->
  fetch ALL their occurrences (delete_flag = false) -> pick the latest capture_timestamp (row_number desc,
  QUALIFY not supported on this Trino build) -> MERGE into gold.creative on creative_id, update-only,
  WHEN MATCHED AND last_seen_timestamp <> capture_timestamp. NO parent->family resolution (unlike first-seen-info):
  last-seen is per the occurrence's own creative_id. Advance CTV_LAST_SEEN_DIGITAL (source unmodified by the MERGE
  -> post-merge max == changed-set max).

  Fidelity notes: exact `<>` guard (NULL-unsafe -- a creative with NULL last_seen_timestamp won't update, as prod);
  no safety lag (internal Iceberg watermark, idempotent). PIECE-5-GATED: digital_gold_occurrence is empty until
  Piece 5 -> clean 0-update no-op now; validate after Piece 5. gold tables referenced as LITERAL relations.
#}

{#- Databricks DAG: occurrence-id → last-seen. -#}
-- depends_on: {{ ref('crtv_occid_update') }}

{%- set wm = 'CTV_LAST_SEEN_DIGITAL' -%}
{%- set begin = watermark_ts_begin(wm) -%}
{%- set start = (begin.start_ts | string)[:19] if begin.start_ts is not none else '1900-01-01 00:00:00' -%}
{%- set self_rel = 'iceberg.bronze.' ~ this.identifier -%}

{%- set merge_sql %}
merge into iceberg.gold.creative m
using {{ self_rel }} s
on m.creative_id = s.parent_creative_id
when matched and m.last_seen_timestamp <> s.capture_timestamp then update set
  last_seen_timestamp = s.capture_timestamp,
  updated_timestamp = cast(current_timestamp as timestamp(6) with time zone)
{%- endset %}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'p4_sync_creative_to_iceberg'],
    views_enabled=false,
    on_table_exists='drop',
    post_hook=[
      merge_sql,
      "{{ watermark_ts_advance_from_source('CTV_LAST_SEEN_DIGITAL', 'iceberg.gold.digital_gold_occurrence', 'updated_timestamp') }}"
    ]
) }}

with changed as (
    select distinct creative_id as parent_creative_id
    from iceberg.gold.digital_gold_occurrence
    where updated_timestamp > cast(timestamp '{{ start }}' as timestamp(6) with time zone)
),

-- all live occurrences for the changed creatives (digital only)
all_occ as (
    select c.parent_creative_id, o.capture_timestamp
    from changed c
    inner join iceberg.gold.digital_gold_occurrence o
      on c.parent_creative_id = o.creative_id and o.delete_flag = false
),

-- latest occurrence per creative (QUALIFY not supported on this Trino build -> subquery)
latest as (
    select * from (
        select parent_creative_id, capture_timestamp,
               row_number() over (partition by parent_creative_id
                                  order by capture_timestamp desc nulls last) as rn
        from all_occ
    )
    where rn = 1
)

select parent_creative_id, capture_timestamp
from latest
