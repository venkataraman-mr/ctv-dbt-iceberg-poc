{#
  Piece 4 -- TASK 8, STAGE 3 of 4: secondary product re-translation for the affected creatives.
  Explodes each affected creative's secondary_products, computes the per-secondary md5 (task-1 stage-2b:
  upper(to_hex(md5(utf8( product_id || '1'|'2' || '' )))) ), looks up the NEW vx1/vx2 target via the STREAMED
  pm_small semi-join, and rebuilds the vx1/vx2 secondary arrays -- nulling the whole array if ANY secondary
  didn't translate (the size-compact mismatch rule). Output: one row per creative with the new secondary arrays.
#}

{#- depends on _affected (ref below), which anchors the whole resync chain after last-seen. -#}

{%- set pm  = source('productcentral', 'productmap') -%}
{%- set aff = ref('crtv_product_resync_affected') -%}

{{ config(materialized='table', schema='bronze', tags=['creatives', 'p4_sync_creative_to_iceberg'], views_enabled=false, on_table_exists='drop') }}

with base as (
    select creative_id, secondary_products
    from {{ aff }}
    where secondary_products is not null
),
sec as (
    select creative_id, product_id, sort_order, type, subtype, is_dominant,
        upper(to_hex(md5(to_utf8(concat(cast(product_id as varchar), '1', ''))))) as md5_vx1,
        upper(to_hex(md5(to_utf8(concat(cast(product_id as varchar), '2', ''))))) as md5_vx2
    from (
        select b.creative_id,
            cast(json_extract_scalar(u.elem, '$.product_id') as bigint)  as product_id,
            cast(json_extract_scalar(u.elem, '$.sort_order') as integer) as sort_order,
            json_extract_scalar(u.elem, '$.type')                        as type,
            json_extract_scalar(u.elem, '$.subtype')                     as subtype,
            cast(json_extract_scalar(u.elem, '$.is_dominant') as boolean) as is_dominant
        from base b
        cross join unnest(cast(json_parse(b.secondary_products) as array(json))) as u(elem)
    )
),
needed as (
    select md5_vx1 as h from sec
    union
    select md5_vx2 from sec
),
pm_small as (
    select mapping_hash, system_id, target_product_id
    from {{ pm }}
    where mapping_hash in (select h from needed)
),
sec_x as (
    select s.creative_id, s.sort_order, s.type, s.subtype, s.is_dominant,
        pm1.target_product_id as vx1_product_id,
        pm2.target_product_id as vx2_product_id
    from sec s
    left join pm_small pm1 on pm1.mapping_hash = s.md5_vx1 and pm1.system_id = 1
    left join pm_small pm2 on pm2.mapping_hash = s.md5_vx2 and pm2.system_id = 2
)
select
    creative_id,
    case when count(vx1_product_id) != count(*) then null
         else json_format(cast(array_agg(cast(row(sort_order, type, subtype, vx1_product_id, is_dominant)
                as row(sort_order integer, type varchar, subtype varchar, product_id bigint, is_dominant boolean))) as json)) end
         as new_vx1_secondary_products,
    case when count(vx2_product_id) != count(*) then null
         else json_format(cast(array_agg(cast(row(sort_order, type, subtype, vx2_product_id, is_dominant)
                as row(sort_order integer, type varchar, subtype varchar, product_id bigint, is_dominant boolean))) as json)) end
         as new_vx2_secondary_products
from sec_x
group by creative_id
