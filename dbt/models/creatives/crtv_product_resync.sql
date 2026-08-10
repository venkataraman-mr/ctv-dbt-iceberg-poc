{#
  Piece 4 -- TASK 8, STAGE 4 of 4 (writer): join the affected set with its re-translated primary + secondary
  results, keep only genuinely-affected creatives, and write. Port of product_translation_resync_process.py's
  MERGE + resync-log + watermark advance. Split from the translation (memory + Trino 150-stage limits) --
  productmap is never touched here (only the three small bronze stage tables).

  Affected filter: primary target changed (new vx1/vx2 <> old), OR the creative was secondary-touched (prod rule).
  Post-hooks: (1) MERGE new vx1/vx2 + secondary arrays into gold.creative on creative_id; (2) INSERT the PRIOR
  values into silver.creative_product_translation_resync_log; (3) advance CTV_PRODUCT_RESYNC to max(change_dt).
  Body materializes the final candidate; the hooks read it (self) so MERGE/log/watermark all see one stable set.

  PoC: with the watermark at max(change_dt) this is a clean ~0-row no-op (no productmap churn). Real diffs need churn.
#}

{#- depends on _affected / _prim / _sec (refs below); _affected anchors the chain after last-seen. -#}

{%- set aff  = ref('crtv_product_resync_affected') -%}
{%- set prim = ref('crtv_product_resync_prim') -%}
{%- set sec  = ref('crtv_product_resync_sec') -%}
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
    tags=['creatives', 'p4_sync_creative_to_iceberg'],
    views_enabled=false,
    on_table_exists='drop',
    post_hook=[
      merge_sql,
      log_sql,
      "{{ watermark_ts_advance_from_source('CTV_PRODUCT_RESYNC', 'iceberg.productcentral.productmap', 'change_dt') }}"
    ]
) }}

with joined as (
    select
        a.creative_id, a.creative_url_hash, a.legacy_creative_id, a.primary_product_id, a.mr_company_id,
        a.secondary_products,
        a.old_vx1_product_id, a.old_vx2_product_id, a.old_vx1_secondary_products, a.old_vx2_secondary_products,
        a.is_secondary_touched,
        p.new_vx1_product_id, p.new_vx2_product_id,
        s.new_vx1_secondary_products, s.new_vx2_secondary_products
    from {{ aff }} a
    left join {{ prim }} p on p.creative_id = a.creative_id
    left join {{ sec }}  s on s.creative_id = a.creative_id
)
select
    creative_id, creative_url_hash, legacy_creative_id, primary_product_id, mr_company_id, secondary_products,
    old_vx1_product_id, old_vx2_product_id, old_vx1_secondary_products, old_vx2_secondary_products,
    new_vx1_product_id, new_vx2_product_id, new_vx1_secondary_products, new_vx2_secondary_products
from joined
where coalesce(new_vx1_product_id, 0) <> coalesce(old_vx1_product_id, 0)
   or coalesce(new_vx2_product_id, 0) <> coalesce(old_vx2_product_id, 0)
   or is_secondary_touched
