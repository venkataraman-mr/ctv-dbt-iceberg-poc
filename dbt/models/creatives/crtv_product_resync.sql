{#
  Piece 4 -- TASK 8: product-translation resync -> gold.creative (+ silver.creative_product_translation_resync_log).
  Port of common/notebook_files/product_translation_resync_process.py. When productcentral.productmap remaps a
  product, the creatives that referenced it must be re-translated (their vx1/vx2 recomputed).

  Watermark CTV_PRODUCT_RESYNC on productcentral.productmap.change_dt (the only non-*.updated_timestamp
  watermark). Change set = productmap rows with change_dt >= watermark (and > a taxonomy-launch floor).
  Affected creatives (gold.creative, created >= floor, NOT in silver.creative_mapping_translation_hold):
    * PRIMARY: primary_product_id is in the changed productmap set AND the new md5->productmap target differs
      from the stored vx1/vx2 (the diff guard); OR
    * SECONDARY: a secondary product_id is in the changed set (productmap.set_id = 0) -- no diff guard, mirrors prod.
  Those creatives are re-translated with the SAME md5 logic as task-1 stage 2b (crtv_sync_creative_revxlate):
  primary hash = upper(to_hex(md5(utf8( primary || '1'|'2' || '|'-joined SORTED secondary vx0 ids )))); per-secondary
  hash = ...|| '1'|'2' || ''. Rebuild the vx1/vx2 secondary arrays; null the whole array if any secondary fails.

  Post-hooks: (1) MERGE new vx1_product_id/vx2_product_id/vx1_secondary_products/vx2_secondary_products into
  gold.creative on creative_id; (2) INSERT the affected set's PRIOR values into the resync log
  (silver.creative_product_translation_resync_log); (3) advance CTV_PRODUCT_RESYNC to max(change_dt).

  PoC note: with the watermark at its 1900 base the first run sweeps the whole productmap, so the SECONDARY branch
  re-translates every creative-with-secondaries (idempotent -- same productmap -> same targets). Real diffs need
  productmap churn. gold/silver referenced as LITERAL relations. VERIFY on the VM that productcentral.productmap
  has change_dt + set_id + primary_product_id (they may not have come across the reference sync).
#}

-- depends_on: {{ ref('crtv_sync_creative') }}

{%- set wm = 'CTV_PRODUCT_RESYNC' -%}
{%- set begin = watermark_ts_begin(wm) -%}
{%- set start = (begin.start_ts | string)[:19] if begin.start_ts is not none else '1900-01-01 00:00:00' -%}
{%- set launch_floor  = var('p4_resync_taxonomy_launch', '1900-01-01') -%}   {#- Databricks taxonomyLaunchDate floor -#}
{%- set created_floor = var('p4_resync_created_floor',  '2024-11-22') -%}

{%- set pm       = source('productcentral', 'productmap') -%}
{%- set pm_lit   = 'iceberg.productcentral.productmap' -%}   {#- literal for the run-time watermark hook -#}
{%- set hold     = 'iceberg.silver.creative_mapping_translation_hold' -%}
{%- set self_rel = 'iceberg.bronze.' ~ this.identifier -%}

{%- set merge_sql %}
merge into iceberg.gold.creative target
using {{ self_rel }} source
on target.creative_id = source.creative_id
when matched then update set
  vx1_product_id = source.new_vx1_product_id,
  vx2_product_id = source.new_vx2_product_id,
  vx1_secondary_products = source.new_vx1_secondary_products,
  vx2_secondary_products = source.new_vx2_secondary_products
{%- endset %}

{%- set log_sql %}
insert into iceberg.silver.creative_product_translation_resync_log
  (creative_id, legacy_creative_id, primary_product_id, vx1_product_id, vx2_product_id, mr_company_id,
   secondary_products, vx1_secondary_products, vx2_secondary_products, mr_secondary_company_ids, created_timestamp)
select
  creative_id, legacy_creative_id, primary_product_id, old_vx1_product_id, old_vx2_product_id, mr_company_id,
  secondary_products, old_vx1_secondary_products, old_vx2_secondary_products,
  cast(null as varchar) as mr_secondary_company_ids,
  cast(current_timestamp as timestamp(6) with time zone) as created_timestamp
from {{ self_rel }}
{%- endset %}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'p4_sync'],
    views_enabled=false,
    on_table_exists='drop',
    post_hook=[
      merge_sql,
      log_sql,
      "{{ watermark_ts_advance_from_source('CTV_PRODUCT_RESYNC', 'iceberg.productcentral.productmap', 'change_dt') }}"
    ]
) }}

-- changed productmap rows since the watermark (and after the taxonomy-launch floor)
with pm_cdf as (
    select primary_product_id, set_id
    from {{ pm }}
    where change_dt > cast(timestamp '{{ launch_floor }} 00:00:00' as timestamp(6) with time zone)
      and change_dt >= cast(timestamp '{{ start }}' as timestamp(6) with time zone)
),

-- candidate creatives (not held), with vx0 secondary product_ids extracted
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

-- primary-remapped creatives whose NEW md5->productmap target differs from the stored vx1/vx2
primary_hash as (
    select c.*,
        upper(to_hex(md5(to_utf8(concat(cast(c.primary_product_id as varchar), '1',
            coalesce(array_join(transform(array_sort(c.sec_pid_array), x -> cast(x as varchar)), '|'), ''))))) ) as md5_vx1,
        upper(to_hex(md5(to_utf8(concat(cast(c.primary_product_id as varchar), '2',
            coalesce(array_join(transform(array_sort(c.sec_pid_array), x -> cast(x as varchar)), '|'), ''))))) ) as md5_vx2
    from crtv c
    where c.primary_product_id in (select primary_product_id from pm_cdf)
),
primary_diff as (
    select ph.creative_id
    from primary_hash ph
    left join {{ pm }} pm1 on ph.md5_vx1 = pm1.mapping_hash and pm1.system_id = 1
    left join {{ pm }} pm2 on ph.md5_vx2 = pm2.mapping_hash and pm2.system_id = 2
    where (pm1.system_id = 1 and coalesce(ph.vx1_product_id, 0) <> coalesce(pm1.target_product_id, 0))
       or (pm2.system_id = 2 and coalesce(ph.vx2_product_id, 0) <> coalesce(pm2.target_product_id, 0))
),

-- secondary-remapped creatives (no diff guard, mirrors prod), set_id = 0
secondary_affected as (
    select c.creative_id
    from crtv c
    where cardinality(array_intersect(
            c.sec_pid_array,
            (select array_agg(primary_product_id) from pm_cdf where set_id = 0)
          )) > 0
),

affected_ids as (
    select creative_id from primary_diff
    union
    select creative_id from secondary_affected
),

-- the affected creatives (prior values kept for the resync log)
base as (
    select c.creative_id, c.creative_url_hash, c.legacy_creative_id, c.primary_product_id, c.mr_company_id,
           c.secondary_products,
           c.vx1_product_id as old_vx1_product_id, c.vx2_product_id as old_vx2_product_id,
           c.vx1_secondary_products as old_vx1_secondary_products,
           c.vx2_secondary_products as old_vx2_secondary_products
    from crtv c
    where c.creative_id in (select creative_id from affected_ids)
),

-- ===== re-translation (same md5 logic as task-1 stage 2b) =====
sec as (
    select b.creative_id,
        cast(json_extract_scalar(u.elem, '$.product_id') as bigint)  as product_id,
        cast(json_extract_scalar(u.elem, '$.sort_order') as integer) as sort_order,
        json_extract_scalar(u.elem, '$.type')                        as type,
        json_extract_scalar(u.elem, '$.subtype')                     as subtype,
        cast(json_extract_scalar(u.elem, '$.is_dominant') as boolean) as is_dominant
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
    select b.creative_id, b.primary_product_id,
        upper(to_hex(md5(to_utf8(concat(cast(b.primary_product_id as varchar), '1', coalesce(si.sec_ids_joined, '')))))) as md5_vx1,
        upper(to_hex(md5(to_utf8(concat(cast(b.primary_product_id as varchar), '2', coalesce(si.sec_ids_joined, '')))))) as md5_vx2
    from base b
    left join sec_ids si on si.creative_id = b.creative_id
),
prim_x as (
    select p.creative_id, pm1.target_product_id as vx1_product_id, pm2.target_product_id as vx2_product_id
    from prim p
    left join {{ pm }} pm1 on pm1.mapping_hash = p.md5_vx1 and pm1.system_id = 1
    left join {{ pm }} pm2 on pm2.mapping_hash = p.md5_vx2 and pm2.system_id = 2
),
sec_x as (
    select s.creative_id, s.sort_order, s.type, s.subtype, s.is_dominant,
        pm1.target_product_id as vx1_product_id, pm2.target_product_id as vx2_product_id
    from sec s
    left join {{ pm }} pm1 on pm1.mapping_hash = upper(to_hex(md5(to_utf8(concat(cast(s.product_id as varchar), '1', ''))))) and pm1.system_id = 1
    left join {{ pm }} pm2 on pm2.mapping_hash = upper(to_hex(md5(to_utf8(concat(cast(s.product_id as varchar), '2', ''))))) and pm2.system_id = 2
),
sec_agg as (
    select creative_id,
        case when count(vx1_product_id) != count(*) then null
             else json_format(cast(array_agg(cast(row(sort_order, type, subtype, vx1_product_id, is_dominant)
                    as row(sort_order integer, type varchar, subtype varchar, product_id bigint, is_dominant boolean))) as json)) end
             as vx1_secondary_products,
        case when count(vx2_product_id) != count(*) then null
             else json_format(cast(array_agg(cast(row(sort_order, type, subtype, vx2_product_id, is_dominant)
                    as row(sort_order integer, type varchar, subtype varchar, product_id bigint, is_dominant boolean))) as json)) end
             as vx2_secondary_products
    from sec_x group by creative_id
)

select
    b.creative_id,
    b.creative_url_hash,
    b.legacy_creative_id,
    b.primary_product_id,
    b.mr_company_id,
    b.secondary_products,
    b.old_vx1_product_id,
    b.old_vx2_product_id,
    b.old_vx1_secondary_products,
    b.old_vx2_secondary_products,
    px.vx1_product_id            as new_vx1_product_id,
    px.vx2_product_id            as new_vx2_product_id,
    sa.vx1_secondary_products    as new_vx1_secondary_products,
    sa.vx2_secondary_products    as new_vx2_secondary_products
from base b
left join prim_x  px on px.creative_id = b.creative_id
left join sec_agg sa on sa.creative_id = b.creative_id
