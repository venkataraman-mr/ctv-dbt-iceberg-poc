{#
  Piece 4 -- TASK 4, STAGE 2 of 4: explode the vx0 attribute_response and translate each element.
  Port of PsqlComponentSync.reverse_translation_comp_crtv (the complete/explode/rev_trans CTEs), split out so
  each Trino model stays under the stage limit and the HUGE productmap is only STREAMED (semi-join), never hashed.

  For each 'Coding Completed' component, unnest attribute_response (json array of {fkey, value, id, referenceType,
  unit}; kept in order via WITH ORDINALITY). Per element compute:
    * Product      -> md5(id||'2'||'') -> productmap(system_id=2).target_product_id (vx2_id) + d_product.prname
    * Manufacturer -> vx0_vx2_advertiser_mapping.vx2_code (vx2_manufacture) + d_advertiser.name (vx2_manufacture_name)
    * (any fkey)   -> vx0_vx2_component_mapping.vx2_component_value  (for Promotional Gift / Return Policy Duration)
  advert_product (per component) = first 'Mattress Product' value -- carried for the stage-3 mattress-seq lookup.

  FIDELITY RISK (validate on real component data -- near-empty for CTV): the lookup COLUMN NAMES (prseq/prname,
  code/name, vx2_code, vx0_company_id, vx0_component_value/vx2_component_value) are the prod names lower-cased for
  Trino; confirm they match the synced/Postgres copies. attribute_response arrives as Postgres jsonb -> Trino json.
#}

-- depends_on: {{ ref('comp_sync_forsync') }}

{%- set fs          = source('tempwork', 'component_coding_forsync_tmp_ctv_poc') -%}
{%- set productmap  = source('productcentral', 'productmap') -%}
{%- set d_product   = source('vx2_taxonomy', 'd_product') -%}
{%- set adv_map     = source('productcentral', 'vx0_vx2_advertiser_mapping') -%}
{%- set d_adv       = source('vx2_taxonomy', 'd_advertiser') -%}
{%- set comp_map    = source('km_preparation_db', 'vx0_vx2_component_mapping') -%}

{{ config(materialized='table', schema='bronze', tags=['creatives', 'p4_sync'], views_enabled=false, on_table_exists='drop') }}

with fs as (
    select component_coding_id, creative_id, status,
           cast(json_parse(json_format(attribute_response)) as array(json)) as ar
    from {{ fs }}
),

exploded as (
    select
        f.component_coding_id, f.creative_id, f.status, u.idx as seq,
        json_extract_scalar(u.elem, '$.fkey')          as fkey,
        json_extract_scalar(u.elem, '$.value')         as fkey_value,
        json_extract_scalar(u.elem, '$.id')            as id,
        json_extract_scalar(u.elem, '$.referenceType') as reference_type,
        json_extract_scalar(u.elem, '$.unit')          as unit
    from fs f
    cross join unnest(f.ar) with ordinality as u(elem, idx)
),

-- per component: first 'Mattress Product' value (carried onto every row for stage 3)
advert as (
    select component_coding_id, creative_id,
        min(case when fkey = 'Mattress Product' then fkey_value end) as advert_product
    from exploded
    group by component_coding_id, creative_id
),

coded as (
    select e.*,
        a.advert_product,
        -- translate only 'Coding Completed' components (matches the driver's complete_creative_cte filter)
        case when e.fkey = 'Product' and e.status = 'Coding Completed'
             then upper(to_hex(md5(to_utf8(concat(e.id, '2', ''))))) end as md5_hash_vx2,
        case when e.fkey = 'Manufacturer' and e.status = 'Coding Completed' then e.id end as manufacture_id
    from exploded e
    join advert a on a.component_coding_id = e.component_coding_id and a.creative_id = e.creative_id
),

-- productmap (system_id = 2) STREAMED, semi-joined to just the Product hashes we need
pm_small as (
    select mapping_hash, target_product_id
    from {{ productmap }}
    where system_id = 2 and mapping_hash in (select md5_hash_vx2 from coded where md5_hash_vx2 is not null)
),
-- d_product (potentially large) STREAMED, semi-joined to the resolved target ids
dprod_small as (
    select prseq, prname
    from {{ d_product }}
    where prseq in (select target_product_id from pm_small)
)

select
    c.component_coding_id, c.creative_id, c.status, c.seq,
    c.fkey, c.fkey_value, c.id, c.reference_type, c.unit, c.advert_product,
    pm.target_product_id       as vx2_id,
    dp.prname                  as prname,
    man.vx2_code               as vx2_manufacture,
    man_name.name              as vx2_manufacture_name,
    cm.vx2_component_value     as vx2_component_value
from coded c
left join pm_small   pm       on pm.mapping_hash = c.md5_hash_vx2
left join dprod_small dp      on dp.prseq = pm.target_product_id
left join {{ adv_map }} man    on man.vx0_company_id = try_cast(c.manufacture_id as bigint)
left join {{ d_adv }} man_name on man_name.code = man.vx2_code
left join {{ comp_map }} cm    on cm.vx0_component_value = c.fkey_value
