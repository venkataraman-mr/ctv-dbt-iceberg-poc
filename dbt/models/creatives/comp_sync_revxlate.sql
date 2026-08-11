{#
  Piece 4 -- TASK 4, STAGE 3 of 4: rebuild the vx2 attribute array + holding_flag, joined to the full forsync row.
  Port of reverse_translation_comp_crtv's vx2_response_cte + final select. Reads the per-element translations
  (comp_sync_explode), rebuilds per component:
    * attribute_response      = the vx0 array, keys renamed value->fkey_value (schema_process), for ALL rows;
    * attribute_response_vx2  = the rebuilt vx2 array (Product->prname/vx2_id, Manufacturer->name/vx2_code,
      Promotional Gift/Return Policy Duration->vx2_component_value, Mattress Product->id replaced by the
      component's mattress cmp_seq); NULL for non-'Coding Completed' components (driver left-join semantics);
    * holding_flag = 'Coding Completed' AND (#Product ids in vx0) != (#Product ids that translated to vx2).
  Each json element is built as cast(map(keys, values) as json) so it nests as an object (json_object/parse_json
  would produce json-typed values Trino can't array-agg cleanly). Order preserved via the stage-2 seq.

  FIDELITY RISK (near-empty for CTV; validate on real component data): the exact vx2 json key set, the
  cross-element mattress-seq rule (advert_product x manufacturer vx2_code), and lookup column names.
#}

-- depends_on: {{ ref('comp_sync_forsync') }}

{%- set explode  = ref('comp_sync_explode') -%}
{%- set fs       = source('tempwork', 'component_coding_forsync_tmp_ctv_poc') -%}
{%- set mattress = source('productcentral', 'vx0_vx2_mattress_product_mapping') -%}

{{ config(materialized='table', schema='bronze', tags=['creatives', 'SYNC_CREATIVES_TO_ICEBERG'], views_enabled=false, on_table_exists='drop') }}

-- per component: the manufacturer's vx2_code + advert product -> mattress cmp_seq
with comp_mfr as (
    select component_coding_id, creative_id,
        max(vx2_manufacture) as mfr_vx2_code,
        max(advert_product)  as advert_product
    from {{ explode }}
    group by component_coding_id, creative_id
),
mattress as (
    select cm.component_coding_id, cm.creative_id, tst.cmp_seq as mattress_seq
    from comp_mfr cm
    left join {{ mattress }} tst
      on tst.vx2_mattress_product = cm.advert_product
     and tst.vx2_advertiser_code = coalesce(cm.mfr_vx2_code, '')
),

per_elem as (
    select
        e.component_coding_id, e.creative_id, e.status, e.seq, e.fkey, e.id, e.vx2_id,
        -- vx0 element (original values; value -> fkey_value)
        cast(map(
            array['fkey','fkey_value','id','referenceType','unit'],
            array[e.fkey, e.fkey_value, e.id, e.reference_type, e.unit]
        ) as json) as vx0_elem,
        -- vx2 element (per-fkey reconstruction; Mattress Product id -> component mattress_seq)
        cast(map(
            array['fkey','fkey_value','id','referenceType','unit'],
            array[
                e.fkey,
                case when e.fkey = 'Product' then e.prname
                     when e.fkey = 'Manufacturer' then e.vx2_manufacture_name
                     when e.fkey in ('Promotional Gift','Return Policy Duration') then e.vx2_component_value
                     else e.fkey_value end,
                case when e.fkey = 'Product' then cast(e.vx2_id as varchar)
                     when e.fkey = 'Manufacturer' then e.vx2_manufacture
                     when e.fkey = 'Mattress Product' then cast(m.mattress_seq as varchar)
                     else e.id end,
                e.reference_type,
                e.unit
            ]
        ) as json) as vx2_elem
    from {{ explode }} e
    left join mattress m on m.component_coding_id = e.component_coding_id and m.creative_id = e.creative_id
),

rebuilt as (
    select
        component_coding_id, creative_id,
        json_format(cast(array_agg(vx0_elem order by seq) as json)) as attribute_response,
        case when max(status) = 'Coding Completed'
             then json_format(cast(array_agg(vx2_elem order by seq) as json)) end as attribute_response_vx2,
        count(case when fkey = 'Product' and id is not null then 1 end)     as n_prod_vx0,
        count(case when fkey = 'Product' and vx2_id is not null then 1 end) as n_prod_vx2
    from per_elem
    group by component_coding_id, creative_id
)

select
    cast(f.component_coding_id as integer)                       as component_coding_id,
    cast(f.creative_id as bigint)                               as creative_id,
    cast(f.legacy_creative_id as bigint)                        as legacy_creative_id,
    cast(f.component_template_id as smallint)                   as component_template_id,
    cast(f.component_template_name as varchar)                  as component_template_name,
    cast(f.sequence as smallint)                                as sequence,
    cast(f.share as smallint)                                   as share,
    r.attribute_response,
    r.attribute_response_vx2,
    cast(f.is_logically_deleted as boolean)                     as is_logically_deleted,
    cast(f.created_timestamp as timestamp(6) with time zone)    as created_timestamp,
    cast(f.modified_timestamp as timestamp(6) with time zone)   as modified_timestamp,
    cast(f.creative_path as varchar)                            as creative_path,
    cast(f.page_no as smallint)                                 as page_no,
    cast(f.height as real)                                      as height,
    cast(f.width as real)                                       as width,
    cast(f.area as real)                                        as area,
    cast(f.x_offset as real)                                    as x_offset,
    cast(f.y_offset as real)                                    as y_offset,
    cast(f.status as varchar)                                   as status,
    cast(f.modified_by as integer)                              as modified_by,
    cast(f.order_number as integer)                             as order_number,
    case when f.status = 'Coding Completed'
          and coalesce(r.n_prod_vx0, 0) <> coalesce(r.n_prod_vx2, 0)
         then true else false end                               as holding_flag
from {{ fs }} f
left join rebuilt r on r.component_coding_id = f.component_coding_id and r.creative_id = f.creative_id
