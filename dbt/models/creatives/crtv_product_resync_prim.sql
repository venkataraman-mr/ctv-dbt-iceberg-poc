{#
  Piece 4 -- TASK 8, STAGE 2 of 4: primary product re-translation for the affected creatives.
  Reads the small _affected set, computes the primary md5 (task-1 stage-2b logic:
  upper(to_hex(md5(utf8( primary || '1'|'2' || '|'-joined SORTED secondary vx0 ids )))) ), and looks up the NEW
  vx1/vx2 target. productmap is STREAMED once and semi-joined down to just the needed mapping-hashes (pm_small),
  so the huge table is never hashed whole. Output: one row per creative with the re-translated primary vx1/vx2.
#}

{#- depends on _affected (ref below), which anchors the whole resync chain after last-seen. -#}

{%- set pm  = source('productcentral', 'productmap') -%}
{%- set aff = ref('crtv_product_resync_affected') -%}

{{ config(materialized='table', schema='bronze', tags=['creatives', 'p4_sync_creative_to_iceberg'], views_enabled=false, on_table_exists='drop') }}

with base as (
    select creative_id, primary_product_id, secondary_products from {{ aff }}
),
sec as (
    select b.creative_id, cast(json_extract_scalar(u.elem, '$.product_id') as bigint) as product_id
    from base b
    cross join unnest(cast(json_parse(b.secondary_products) as array(json))) as u(elem)
    where b.secondary_products is not null
),
sec_ids as (
    select creative_id,
        array_join(transform(array_sort(array_agg(product_id)), x -> cast(x as varchar)), '|') as sec_ids_joined
    from sec group by creative_id
),
prim as (
    select b.creative_id,
        upper(to_hex(md5(to_utf8(concat(cast(b.primary_product_id as varchar), '1', coalesce(si.sec_ids_joined, '')))))) as md5_vx1,
        upper(to_hex(md5(to_utf8(concat(cast(b.primary_product_id as varchar), '2', coalesce(si.sec_ids_joined, '')))))) as md5_vx2
    from base b
    left join sec_ids si on si.creative_id = b.creative_id
),
needed as (
    select md5_vx1 as h from prim
    union
    select md5_vx2 from prim
),
pm_small as (
    select mapping_hash, system_id, target_product_id
    from {{ pm }}
    where mapping_hash in (select h from needed)
)
select
    p.creative_id,
    pm1.target_product_id as new_vx1_product_id,
    pm2.target_product_id as new_vx2_product_id
from prim p
left join pm_small pm1 on pm1.mapping_hash = p.md5_vx1 and pm1.system_id = 1
left join pm_small pm2 on pm2.mapping_hash = p.md5_vx2 and pm2.system_id = 2
