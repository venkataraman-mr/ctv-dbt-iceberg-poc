{#
  Piece 4 — TASK 1, STAGE 1: run the creative get_changes proc against the clones.
  Mirrors PsqlCrtvSync steps 2.1–2.3:
    pre-hook 1-2: (re)build the advert-hold clone tempwork.creatives_advert_hold_tmp_ctv_poc from the
                  Iceberg reverse-translation hold silver.creative_mapping_translation_hold (cross-catalog
                  CTAS; empty on first run) — so the proc re-includes creatives parked last run;
    pre-hook 3:   CALL the cloned proc (run-time watermark via p4_creative_proc_call — pre_hook is captured
                  at PARSE so the watermark must be read at run) → builds tempwork.creative_forsync_tmp_ctv_poc.
                  Archive parked: a far-future ca_flag makes the proc's archive branches return 0 rows.
  Body: a 1-row summary of the forsync (row count + max updated_timestamp) to confirm the proc ran. The
  transforms + gold MERGE are stages 2-3 (crtv_sync_creative*), which depend on this model so the proc runs first.

  Prereqs (once): ddl/postgres/piece4_sync_procs_ctv_poc.sql (proc clones); CTV_SYNC_CREATIVE seeded (ddl/08).
  VALIDATE ON VM: the advert-hold CTAS + the cross-catalog CALL succeed and forsync populates.
#}

{#- Databricks DAG: (dedup ∥ first-seen) → creative. The creative chain's entry model runs after BOTH the dedup
    task (exit = crtv_sync_dedupe_map_delete) and the first-seen task. -#}
-- depends_on: {{ ref('crtv_sync_dedupe_map_delete') }}
-- depends_on: {{ ref('crtv_sync_first_seen') }}

{%- set fs = source('tempwork', 'creative_forsync_tmp_ctv_poc') -%}
{%- set advert_hold = 'postgres.tempwork.creatives_advert_hold_tmp_ctv_poc' -%}
{%- set hold_src = 'iceberg.silver.creative_mapping_translation_hold' -%}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'SYNC_CREATIVES_TO_ICEBERG'],
    views_enabled=false,
    pre_hook=[
      'drop table if exists ' ~ advert_hold,
      'create table ' ~ advert_hold ~ ' as select creative_id from ' ~ hold_src,
      "{{ p4_creative_proc_call('CTV_SYNC_CREATIVE') }}"
    ]
) }}

select
    count(*)                                            as forsync_rows,
    count(distinct creative_id)                         as distinct_creatives,
    cast(max(updated_timestamp) as timestamp(6) with time zone) as max_updated_ts
from {{ fs }}
