{#
  Piece 4 -- TASK 8: product-translation resync -> gold.creative (+ silver.creative_product_translation_resync_log).
  Port of common/notebook_files/product_translation_resync_process.py. When productcentral.productmap remaps a
  product, the creatives that referenced it are re-translated (their vx1/vx2 recomputed).

  MEMORY DISCIPLINE (productmap is a very large table):
    * Watermark CTV_PRODUCT_RESYNC must be initialized to max(productmap.change_dt) at deployment, NOT 1900 --
      a fresh deploy has no resync backlog (task 1 already translated everything against the current productmap),
      so we only catch FUTURE churn. That keeps `pm_cdf` (changed rows) small. See the one-time init in ddl/08.
    * productmap is NEVER hash-built: change-detection JOINs the small pm_cdf (no array_agg of the full table),
      and the md5->target lookups go through `pm_small` -- productmap STREAMED once and semi-joined down to just
      the mapping-hashes we actually need. So the big table is only ever scanned/streamed, never fully hashed.

  Change set = productmap rows with change_dt >= watermark (and > a taxonomy-launch floor). Affected creatives
  (gold.creative, created >= floor, NOT in silver.creative_mapping_translation_hold):
    * PRIMARY: primary_product_id is in the changed set AND the re-translated vx1/vx2 differ from the stored ones;
    * SECONDARY: a secondary product_id is in the changed set (productmap.set_id = 0) -- no diff guard, mirrors prod.
  Re-translation uses the SAME md5 logic as task-1 stage 2b. Post-hooks: MERGE new vx1/vx2 (+ secondary arrays)
  into gold.creative; INSERT the affected set's PRIOR values into the resync log; advance CTV_PRODUCT_RESYNC.

  VERIFY on the VM that productcentral.productmap carries change_dt + set_id + primary_product_id + mapping_hash.
#}

-- depends_on: {{ ref('crtv_sync_creative') }}

{%- set wm = 'CTV_PRODUCT_RESYNC' -%}
{%- set begin = watermark_ts_begin(wm) -%}
{%- set start = (begin.start_ts | string)[:19] if begin.start_ts is not none else '1900-01-01 00:00:00' -%}
{%- set launch_floor  = var('p4_resync_taxonomy_launch', '1900-01-01') -%}
{%- set created_floor = var('p4_resync_created_floor',  '2024-11-22') -%}

{%- set pm       = source('productcentral', 'productmap') -%}
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

-- SMALL when the watermark is initialized to max(change_dt) (see header). Streamed + filtered, never hashed whole.
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

-- change detection via JOINs to the SMALL pm_cdf (no array_agg of the full productmap)
primary_touched as (
    select c.* from crtv c
    where c.primary_product_id in (select primary_product_id from pm_cdf)
),
secondary_touched as (
    select distinct c.creative_id
    from crtv c
    cross join unnest(c.sec_pid_array) as u(pid)
    join pm_cdf pmc on pmc.primary_product_id = u.pid and pmc.set_id = 0
),

-- everything touched by a changed product (primary or secondary); prior values kept for the resync log
base as (
    select c.creative_id, c.creative_url_hash, c.legacy_creative_id, c.primary_product_id, c.mr_company_id,
           c.secondary_products,
           c.vx1_product_id as old_vx1_product_id, c.vx2_product_id as old_vx2_product_id,
           c.vx1_secondary_products as old_vx1_secondary_products,
           c.vx2_secondary_products as old_vx2_secondary_products
    from crtv c
    where c.creative_id in (select creative_id from primary_touched)
       or c.creative_id in (select creative_id from secondary_touched)
),

-- ===== re-translation (same md5 logic as task-1 stage 2b), targets via the STREAMED pm_small =====
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
sec_hash as (
    select creative_id, sort_order, type, subtype, is_dominant,
        upper(to_hex(md5(to_utf8(concat(cast(product_id as varchar), '1', ''))))) as md5_vx1,
        upper(to_hex(md5(to_utf8(concat(cast(product_id as varchar), '2', ''))))) as md5_vx2
    from sec
),

-- exactly the mapping-hashes we need; productmap is semi-joined (streamed) down to just these rows
needed_hashes as (
    select md5_vx1 as h from prim
    union select md5_vx2 from prim
    union select md5_vx1 from sec_hash
    union select md5_vx2 from sec_hash
),
pm_small as (
    select mapping_hash, system_id, target_product_id
    from {{ pm }}
    where mapping_hash in (select h from needed_hashes)
),

prim_x as (
    select p.creative_id, pm1.target_product_id as vx1_product_id, pm2.target_product_id as vx2_product_id
    from prim p
    left join pm_small pm1 on pm1.mapping_hash = p.md5_vx1 and pm1.system_id = 1
    left join pm_small pm2 on pm2.mapping_hash = p.md5_vx2 and pm2.system_id = 2
),
sec_x as (
    select sh.creative_id, sh.sort_order, sh.type, sh.subtype, sh.is_dominant,
        pm1.target_product_id as vx1_product_id, pm2.target_product_id as vx2_product_id
    from sec_hash sh
    left join pm_small pm1 on pm1.mapping_hash = sh.md5_vx1 and pm1.system_id = 1
    left join pm_small pm2 on pm2.mapping_hash = sh.md5_vx2 and pm2.system_id = 2
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
),

translated as (
    select
        b.creative_id, b.creative_url_hash, b.legacy_creative_id, b.primary_product_id, b.mr_company_id,
        b.secondary_products,
        b.old_vx1_product_id, b.old_vx2_product_id, b.old_vx1_secondary_products, b.old_vx2_secondary_products,
        px.vx1_product_id         as new_vx1_product_id,
        px.vx2_product_id         as new_vx2_product_id,
        sa.vx1_secondary_products as new_vx1_secondary_products,
        sa.vx2_secondary_products as new_vx2_secondary_products
    from base b
    left join prim_x  px on px.creative_id = b.creative_id
    left join sec_agg sa on sa.creative_id = b.creative_id
)

-- keep only genuinely-affected: primary target changed, OR the creative was secondary-touched (prod rule)
select *
from translated t
where coalesce(t.new_vx1_product_id, 0) <> coalesce(t.old_vx1_product_id, 0)
   or coalesce(t.new_vx2_product_id, 0) <> coalesce(t.old_vx2_product_id, 0)
   or t.creative_id in (select creative_id from secondary_touched)
