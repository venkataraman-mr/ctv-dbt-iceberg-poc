{#
  Piece 4 -- TASK 4, STAGE 4 of 4 (writer): persist the component-coded creatives.
  Port of PsqlComponentSync.persist_gold_comp_coding + persist_translation_hold_comp_coding + watermark set.
  Reads the rebuilt candidate (comp_sync_revxlate) and, via post-hooks (all gated on holding_flag):
    1. MERGE holding_flag=false rows into gold.component_coding on (creative_id, component_coding_id) -- UPDATE the
       mutable cols / INSERT new (all 22 cols);
    2. translation-hold MERGE on component_coding_id: park held (holding_flag=true) / release resolved (false);
    3. advance CTV_SYNC_COMPONENT to max(modified_timestamp) of the processed set (held rows re-enter next run via
       the stage-1 component-hold pushback).
  Body = a 1-row run summary. gold/silver referenced as LITERAL relations (hook-written, not dbt outputs).

  PoC: component coding is a print/mattress concern -> near-empty for CTV, so this is largely a smoke test.
#}

-- depends_on: {{ ref('comp_sync_revxlate') }}

{%- set cand = 'iceberg.bronze.comp_sync_revxlate' -%}

{%- set merge_sql %}
merge into iceberg.gold.component_coding target
using (select * from {{ cand }} where holding_flag = false) source
on target.creative_id = source.creative_id and target.component_coding_id = source.component_coding_id
when matched then update set
  attribute_response = source.attribute_response,
  attribute_response_vx2 = source.attribute_response_vx2,
  is_logically_deleted = source.is_logically_deleted,
  modified_timestamp = source.modified_timestamp,
  creative_path = source.creative_path,
  status = source.status,
  modified_by = source.modified_by,
  order_number = source.order_number
when not matched then insert (
  component_coding_id, creative_id, legacy_creative_id, component_template_id, component_template_name,
  sequence, share, attribute_response, attribute_response_vx2, is_logically_deleted, created_timestamp,
  modified_timestamp, creative_path, page_no, height, width, area, x_offset, y_offset, status, modified_by, order_number)
values (
  source.component_coding_id, source.creative_id, source.legacy_creative_id, source.component_template_id, source.component_template_name,
  source.sequence, source.share, source.attribute_response, source.attribute_response_vx2, source.is_logically_deleted, source.created_timestamp,
  source.modified_timestamp, source.creative_path, source.page_no, source.height, source.width, source.area, source.x_offset, source.y_offset, source.status, source.modified_by, source.order_number)
{%- endset %}

{%- set hold_sql %}
merge into iceberg.silver.component_coding_translation_hold t
using (select distinct component_coding_id, holding_flag from {{ cand }}) s
on t.component_coding_id = s.component_coding_id
when matched and s.holding_flag = false then delete
when not matched and s.holding_flag = true then insert (component_coding_id) values (s.component_coding_id)
{%- endset %}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'SYNC_CREATIVES_TO_ICEBERG'],
    views_enabled=false,
    on_table_exists='drop',
    post_hook=[
      merge_sql,
      hold_sql,
      "{{ watermark_ts_finish_from_relation('CTV_SYNC_COMPONENT', 'iceberg.bronze.comp_sync_revxlate', 'modified_timestamp') }}"
    ]
) }}

select
    count(*)                                        as candidates,
    count_if(not holding_flag)                      as merged_to_gold,
    count_if(holding_flag)                          as held
from {{ ref('comp_sync_revxlate') }}
