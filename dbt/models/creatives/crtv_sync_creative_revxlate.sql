{#
  Piece 4 — TASK 1, STAGE 2b: reverse translation + competitor vx2 + holding_flag.
  Port of PsqlCrtvSync.rev_translate_creatives + get_reverse_translation + attr_competitor_translation.

  vx0 -> vx1/vx2 product translation via productcentral.productmap (md5-hash lookup):
    * PRIMARY hash  = upper(md5( primary_product_id || system_id || '|'-joined SORTED secondary vx0 ids ))
    * SECONDARY hash= upper(md5( sec_product_id || system_id || '' )) per secondary product
    Spark md5(str) == Trino to_hex(md5(to_utf8(str))) (both = hex of the MD5 of the UTF-8 bytes; to_hex is
    upper, matching Spark's Upper()). ****FIDELITY RISK — validate the hashes/targets match prod on a sample.****
  vx1/vx2 secondary arrays are rebuilt; if ANY secondary didn't translate (target null) the whole vxN
  secondary array is nulled (Databricks size-compact mismatch rule). Competitor: vx0 -> vx2_code via
  vx0_vx2_advertiser_mapping. holding_flag = advert whose product didn't reverse-translate (parked, not synced).

  Carries base.* (stage 2a) + vx1/vx2 + attribution_competitor_vx2 + holding_flag for the stage-3 gold MERGE.
#}

{%- set productmap = source('productcentral', 'productmap') -%}
{%- set adv_map    = source('productcentral', 'vx0_vx2_advertiser_mapping') -%}
{%- set advert = var('p4_advert_classification', "'Advert'") -%}

{{ config(materialized='table', schema='bronze', tags=['creatives','p4_sync_creative_to_iceberg'], views_enabled=false) }}

with base as (
    select * from {{ ref('crtv_sync_creative_raw') }}
),

-- explode secondary products (one row per secondary product) with the vx0 fields
sec as (
    select
        b.creative_id, b.creative_url_hash,
        cast(json_extract_scalar(u.elem, '$.product_id') as bigint)  as product_id,
        cast(json_extract_scalar(u.elem, '$.sort_order') as integer) as sort_order,
        json_extract_scalar(u.elem, '$.type')                        as type,
        json_extract_scalar(u.elem, '$.subtype')                     as subtype,
        cast(json_extract_scalar(u.elem, '$.is_dominant') as boolean) as is_dominant
    from base b
    cross join unnest(cast(json_parse(b.secondary_products) as array(json))) as u(elem)
    where b.secondary_products is not null
),

-- '|'-joined SORTED secondary vx0 product ids per creative (for the primary hash)
sec_ids as (
    select creative_id, creative_url_hash,
        array_join(transform(array_sort(array_agg(product_id)), x -> cast(x as varchar)), '|') as sec_ids_joined
    from sec group by creative_id, creative_url_hash
),

-- PRIMARY vx1/vx2 hashes + productmap lookup
prim as (
    select b.creative_id, b.creative_url_hash, b.primary_product_id,
        upper(to_hex(md5(to_utf8(concat(cast(b.primary_product_id as varchar), '1', coalesce(si.sec_ids_joined, '')))))) as md5_vx1,
        upper(to_hex(md5(to_utf8(concat(cast(b.primary_product_id as varchar), '2', coalesce(si.sec_ids_joined, '')))))) as md5_vx2
    from base b
    left join sec_ids si on si.creative_id = b.creative_id and si.creative_url_hash = b.creative_url_hash
),
prim_x as (
    select p.creative_id, p.creative_url_hash,
        pm1.target_product_id as vx1_product_id,
        pm2.target_product_id as vx2_product_id
    from prim p
    left join {{ productmap }} pm1 on pm1.mapping_hash = p.md5_vx1 and pm1.system_id = 1
    left join {{ productmap }} pm2 on pm2.mapping_hash = p.md5_vx2 and pm2.system_id = 2
),

-- SECONDARY vx1/vx2 targets per secondary product
sec_x as (
    select s.creative_id, s.creative_url_hash, s.sort_order, s.type, s.subtype, s.is_dominant,
        pm1.target_product_id as vx1_product_id,
        pm2.target_product_id as vx2_product_id
    from sec s
    left join {{ productmap }} pm1
        on pm1.mapping_hash = upper(to_hex(md5(to_utf8(concat(cast(s.product_id as varchar), '1', ''))))) and pm1.system_id = 1
    left join {{ productmap }} pm2
        on pm2.mapping_hash = upper(to_hex(md5(to_utf8(concat(cast(s.product_id as varchar), '2', ''))))) and pm2.system_id = 2
),
-- rebuild vx1/vx2 secondary arrays; null the whole array if any secondary didn't translate
sec_agg as (
    select creative_id, creative_url_hash,
        case when count(vx1_product_id) != count(*) then null
             else json_format(cast(array_agg(cast(row(sort_order, type, subtype, vx1_product_id, is_dominant)
                    as row(sort_order integer, type varchar, subtype varchar, product_id bigint, is_dominant boolean))) as json)) end
             as vx1_secondary_products,
        case when count(vx2_product_id) != count(*) then null
             else json_format(cast(array_agg(cast(row(sort_order, type, subtype, vx2_product_id, is_dominant)
                    as row(sort_order integer, type varchar, subtype varchar, product_id bigint, is_dominant boolean))) as json)) end
             as vx2_secondary_products
    from sec_x group by creative_id, creative_url_hash
),

-- competitor vx0 -> vx2_code
comp as (
    select b.creative_id, b.creative_url_hash,
        cast(json_extract_scalar(u.elem, '$.id') as bigint) as vx0_competitor_id
    from base b
    cross join unnest(cast(json_parse(b.attribution_competitor) as array(json))) as u(elem)
    where b.attribution_competitor is not null
),
comp_agg as (
    select c.creative_id, c.creative_url_hash,
        json_format(cast(array_agg(cast(row(m.vx2_code) as row(code varchar))) filter (where m.vx2_code is not null) as json)) as attribution_competitor_vx2
    from comp c
    left join {{ adv_map }} m on m.vx0_company_id = c.vx0_competitor_id
    group by c.creative_id, c.creative_url_hash
)

select
    b.*,
    px.vx1_product_id,
    px.vx2_product_id,
    sa.vx1_secondary_products,
    sa.vx2_secondary_products,
    ca.attribution_competitor_vx2,
    case when b.classification_type = {{ advert }}
          and ( px.vx1_product_id is null
                or ( px.vx1_product_id is not null
                     and sa.vx1_secondary_products is null
                     and coalesce(json_array_length(json_parse(b.secondary_products)), 0) > 0 ) )
         then true else false end as holding_flag
from base b
left join prim_x   px on px.creative_id = b.creative_id and px.creative_url_hash = b.creative_url_hash
left join sec_agg  sa on sa.creative_id = b.creative_id and sa.creative_url_hash = b.creative_url_hash
left join comp_agg ca on ca.creative_id = b.creative_id and ca.creative_url_hash = b.creative_url_hash
