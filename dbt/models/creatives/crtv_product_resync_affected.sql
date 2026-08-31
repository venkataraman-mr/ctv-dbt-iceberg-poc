{#
  Piece 4 -- TASK 8, STAGE 1 of 4: change detection -> the small set of creatives touched by a productmap remap.
  Split out (with _prim / _sec / crtv_product_resync) because doing the whole thing in one model blew Trino's
  150-stage limit (CTEs referenced many times get their subtree re-planned each time) and its per-node memory
  (hashing the huge productmap). Here productmap is only ever a STREAMED filter scan (pm_cdf), never hashed.

  Watermark CTV_PRODUCT_RESYNC (change_dt). MUST be initialized to max(change_dt), not 1900 -- see ddl/nessie/08.
  Affected = gold.creative (created >= floor, not in translation hold) whose primary_product_id is in the
  changed set, OR whose secondary product array intersects the changed set (set_id = 0). Keeps prior vx1/vx2
  values + an is_secondary_touched flag for the final affected filter downstream.
#}

{#- Databricks DAG: last-seen → (component ∥ product-resync). Product-resync's entry runs after last-seen. -#}
-- depends_on: {{ ref('crtv_lastseen_update') }}

{%- set wm = 'CTV_PRODUCT_RESYNC' -%}
{%- set begin = watermark_ts_begin(wm) -%}
{%- set start = (begin.start_ts | string)[:19] if begin.start_ts is not none else '1900-01-01 00:00:00' -%}
{%- set launch_floor  = var('p4_resync_taxonomy_launch', '1900-01-01') -%}
{%- set created_floor = var('p4_resync_created_floor',  '2024-11-22') -%}
{%- set pm   = source('productcentral', 'productmap') -%}
{%- set hold = 'iceberg.silver.creative_mapping_translation_hold' -%}

{{ config(materialized='table', schema='bronze', tags=['creatives', 'SYNC_CREATIVES_TO_ICEBERG'], views_enabled=false, on_table_exists='drop') }}

with pm_cdf as (
    select distinct primary_product_id, set_id
    from {{ pm }}
    where change_dt > cast(timestamp '{{ launch_floor }} 00:00:00' as timestamp(6) with time zone)
      and change_dt >= cast(timestamp '{{ start }}' as timestamp(6) with time zone)
),

crtv as (
    select
        creative_id, legacy_creative_id, creative_url_hash, primary_product_id, mr_company_id,
        vx1_product_id, vx2_product_id, secondary_products, vx1_secondary_products, vx2_secondary_products,
        transform(
            cast(json_parse(coalesce(secondary_products, '[]')) as array(json)),
            x -> cast(json_extract_scalar(x, '$.product_id') as bigint)
        ) as sec_pid_array
    from iceberg.gold.creative
    where created_timestamp >= cast(timestamp '{{ created_floor }} 00:00:00' as timestamp(6) with time zone)
      and creative_id not in (select creative_id from {{ hold }})
),

secondary_touched as (
    select distinct c.creative_id
    from crtv c
    cross join unnest(c.sec_pid_array) as u(pid)
    join pm_cdf pmc on pmc.primary_product_id = u.pid and pmc.set_id = 0
)

select
    c.creative_id,
    c.creative_url_hash,
    c.legacy_creative_id,
    c.primary_product_id,
    c.mr_company_id,
    c.secondary_products,
    c.vx1_product_id            as old_vx1_product_id,
    c.vx2_product_id            as old_vx2_product_id,
    c.vx1_secondary_products    as old_vx1_secondary_products,
    c.vx2_secondary_products    as old_vx2_secondary_products,
    (c.creative_id in (select creative_id from secondary_touched)) as is_secondary_touched
from crtv c
where c.primary_product_id in (select primary_product_id from pm_cdf)
   or c.creative_id in (select creative_id from secondary_touched)
