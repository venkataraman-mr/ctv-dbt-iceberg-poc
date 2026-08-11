{#
  Piece 5 -- HALF A, STAGE 2: persist the deployment chains, roles, and mediators from the new raw occurrences.
  Port of DigitalRawocctoGoldocc.persist_roles_mediator + persist_deployment_chain (STEP-2).

  daisy_chain (VARCHAR json) = array<struct<id, role, index, roleId, mediator>>. Per occurrence chain, sorted by
  `index`, we build:
    * daisy_chain_transformed_1 = ','-joined  id(roleId)
    * daisy_chain_transformed_2 = '|'-joined  id.roleId
    * purchase_method_daisy_chain_2_md5_hashcode = upper(to_hex(md5(utf8( purchase_method_id || col2 ))))
      (Spark md5 == Trino to_hex(md5(to_utf8(...))); same as the creative reverse-translation).
  Body = one row per distinct chain (the candidate for the digital_deployment_chain MERGE).

  Post-hooks (all WHEN NOT MATCHED INSERT, keyed on the natural id):
    1. gold.digital_deployment_chain      on the md5 hashcode. deployment_chain_id is an IDENTITY column in prod;
       Iceberg has none, so we derive a STABLE surrogate = from_big_endian_64(xxhash64(md5_hashcode)) (PoC choice
       -- deterministic + unique per chain; the gate only needs it to exist/join, downstream just stores it).
    2. gold.digital_deployment_chain_role     (roleId -> role_id, role -> role_name)   from the raw explode.
    3. gold.digital_deployment_chain_mediator (id -> mediator_id, mediator -> mediator_name) from the raw explode.
#}

-- depends_on: {{ ref('digital_occ_raw_cdf') }}

{%- set raw = 'iceberg.bronze.digital_occ_raw_cdf' -%}
{%- set self_rel = 'iceberg.bronze.' ~ this.identifier -%}

{%- set chain_merge %}
merge into iceberg.gold.digital_deployment_chain tgt
using (
    select *, from_big_endian_64(xxhash64(to_utf8(md5_hashcode))) as deployment_chain_id
    from {{ self_rel }}
) src
on tgt.purchase_method_daisy_chain_2_md5_hashcode = src.md5_hashcode
when not matched then insert (
    deployment_chain_id, daisy_chain, daisy_chain_transformed_1, daisy_chain_transformed_2,
    purchase_method_id, purchase_method_daisy_chain_2_md5_hashcode, created_timestamp)
values (
    src.deployment_chain_id, src.daisy_chain, src.daisy_chain_transformed_1, src.daisy_chain_transformed_2,
    cast(src.purchase_method_id as smallint), src.md5_hashcode, cast(current_timestamp as timestamp(6) with time zone))
{%- endset %}

{%- set role_merge %}
merge into iceberg.gold.digital_deployment_chain_role tgt
using (
    select distinct
        cast(json_extract_scalar(u.elem, '$.roleId') as bigint) as role_id,
        json_extract_scalar(u.elem, '$.role')                   as role_name
    from {{ raw }} r
    cross join unnest(cast(json_parse(r.daisy_chain) as array(json))) as u(elem)
    where r.daisy_chain is not null
) src
on tgt.role_id = src.role_id
when not matched and src.role_id is not null then insert (role_id, role_name, created_timestamp, updated_timestamp)
values (src.role_id, src.role_name, cast(current_timestamp as timestamp(6) with time zone), cast(current_timestamp as timestamp(6) with time zone))
{%- endset %}

{%- set mediator_merge %}
merge into iceberg.gold.digital_deployment_chain_mediator tgt
using (
    select distinct
        cast(json_extract_scalar(u.elem, '$.id') as bigint) as mediator_id,
        json_extract_scalar(u.elem, '$.mediator')           as mediator_name
    from {{ raw }} r
    cross join unnest(cast(json_parse(r.daisy_chain) as array(json))) as u(elem)
    where r.daisy_chain is not null
) src
on tgt.mediator_id = src.mediator_id
when not matched and src.mediator_id is not null then insert (mediator_id, mediator_name, created_timestamp, updated_timestamp)
values (src.mediator_id, src.mediator_name, cast(current_timestamp as timestamp(6) with time zone), cast(current_timestamp as timestamp(6) with time zone))
{%- endset %}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['occurrences', 'p5_digital_raw_to_gold_occ'],
    views_enabled=false,
    on_table_exists='drop',
    post_hook=[ chain_merge, role_merge, mediator_merge ]
) }}

with exploded as (
    select
        r.purchase_method_id,
        r.daisy_chain,
        cast(json_extract_scalar(u.elem, '$.index')  as bigint) as idx,
        cast(json_extract_scalar(u.elem, '$.id')     as bigint) as id,
        cast(json_extract_scalar(u.elem, '$.roleId') as bigint) as role_id
    from {{ ref('digital_occ_raw_cdf') }} r
    cross join unnest(cast(json_parse(r.daisy_chain) as array(json))) as u(elem)
    where r.daisy_chain is not null
),

per_chain as (
    select
        purchase_method_id,
        daisy_chain,
        array_join(transform(array_sort(array_agg(cast(row(idx, id, role_id) as row(idx bigint, id bigint, role_id bigint)))),
                             x -> concat(cast(x.id as varchar), '(', cast(x.role_id as varchar), ')')), ',') as daisy_chain_transformed_1,
        array_join(transform(array_sort(array_agg(cast(row(idx, id, role_id) as row(idx bigint, id bigint, role_id bigint)))),
                             x -> concat(cast(x.id as varchar), '.', cast(x.role_id as varchar))), '|') as daisy_chain_transformed_2
    from exploded
    group by purchase_method_id, daisy_chain
)

select
    purchase_method_id,
    daisy_chain,
    daisy_chain_transformed_1,
    daisy_chain_transformed_2,
    upper(to_hex(md5(to_utf8(concat(cast(purchase_method_id as varchar), daisy_chain_transformed_2))))) as md5_hashcode
from per_chain
where coalesce(daisy_chain_transformed_2, '') != ''
