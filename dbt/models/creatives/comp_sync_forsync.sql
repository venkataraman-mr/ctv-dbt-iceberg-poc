{#
  Piece 4 -- TASK 4, STAGE 1 of 4: run the component get_changes proc against the clones.
  Mirrors PsqlComponentSync.read_changes_from_psql:
    pre-hook 1-2: (re)build the component-hold clone tempwork.component_hold_creative_tmp_ctv_poc from the
                  Iceberg hold silver.component_coding_translation_hold (cross-catalog CTAS; empty on first run)
                  so the proc re-includes components parked last run;
    pre-hook 3:   CALL the cloned component proc (run-time watermark via p4_component_proc_call) ->
                  builds tempwork.component_coding_forsync_tmp_ctv_poc.
  Body: 1-row summary of the forsync. Transforms + writes are stages 2-4 (comp_sync_*), which depend on this.

  Prereqs: ddl/postgres/piece4_sync_procs_ctv_poc.sql (component proc clone); CTV_SYNC_COMPONENT seeded (ddl/08).
  NOTE: component coding is a print/mattress concern -- expect the CTV forsync to be near-empty.
#}

{#- Databricks DAG: last-seen → (component ∥ product-resync). Component's entry runs after last-seen. -#}
-- depends_on: {{ ref('crtv_lastseen_update') }}

{%- set fs = source('tempwork', 'component_coding_forsync_tmp_ctv_poc') -%}
{%- set comp_hold = 'postgres.tempwork.component_hold_creative_tmp_ctv_poc' -%}
{%- set hold_src  = 'iceberg.silver.component_coding_translation_hold' -%}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'SYNC_CREATIVES_TO_ICEBERG'],
    views_enabled=false,
    on_table_exists='drop',
    pre_hook=[
      'drop table if exists ' ~ comp_hold,
      'create table ' ~ comp_hold ~ ' as select component_coding_id from ' ~ hold_src,
      "{{ p4_component_proc_call('CTV_SYNC_COMPONENT') }}"
    ]
) }}

select
    count(*)                                                     as forsync_rows,
    count(distinct creative_id)                                  as distinct_creatives,
    count(distinct component_coding_id)                          as distinct_components,
    cast(max(modified_timestamp) as timestamp(6) with time zone) as max_modified_ts
from {{ fs }}
