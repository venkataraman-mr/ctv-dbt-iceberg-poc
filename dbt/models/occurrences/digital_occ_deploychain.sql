{#
  Piece 5 -- HALF A, STAGE 2: persist the deployment chains, roles, and mediators from the new raw occurrences.
  Port of DigitalRawocctoGoldocc.persist_roles_mediator + persist_deployment_chain (STEP-2).

  daisy_chain (VARCHAR json) = array<struct<id, role, index, roleId, mediator>>. Per DISTINCT chain, sorted by
  `index`, we build (transforming the array IN PLACE per row -- Databricks does the same; do NOT explode+array_agg
  across occurrences or the same chain's elements get duplicated once per occurrence -> "concatenated string too
  large"):
    * daisy_chain_transformed_1 = ','-joined  id(roleId)
    * daisy_chain_transformed_2 = '|'-joined  id.roleId
    * purchase_method_daisy_chain_2_md5_hashcode = upper(to_hex(md5(utf8( purchase_method_id || col2 )))).
  Body = one row per distinct (purchase_method_id, daisy_chain).

  Post-hooks (WHEN NOT MATCHED INSERT, on the natural id):
    1. gold.digital_deployment_chain      on the md5 hashcode. deployment_chain_id (IDENTITY in prod; Iceberg has
       none) = STABLE surrogate from_big_endian_64(xxhash64(md5_hashcode)).
    2. gold.digital_deployment_chain_role     (roleId->role_id, role->role_name)
    3. gold.digital_deployment_chain_mediator (id->mediator_id, mediator->mediator_name)
       -- both explode the DISTINCT chains (not every raw row) then SELECT DISTINCT.
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
    from (select distinct daisy_chain from {{ raw }} where daisy_chain is not null) dc
    cross join unnest(cast(json_parse(dc.daisy_chain) as array(json))) as u(elem)
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
    from (select distinct daisy_chain from {{ raw }} where daisy_chain is not null) dc
    cross join unnest(cast(json_parse(dc.daisy_chain) as array(json))) as u(elem)
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

with distinct_chains as (
    select distinct purchase_method_id, daisy_chain
    from {{ ref('digital_occ_raw_cdf') }}
    where daisy_chain is not null
),

-- transform each chain's array IN PLACE (sort by index; no explode, no array_agg across occurrences)
parsed as (
    select
        purchase_method_id,
        daisy_chain,
        array_sort(transform(
            cast(json_parse(daisy_chain) as array(json)),
            e -> cast(row(
                     cast(json_extract_scalar(e, '$.index')  as bigint),
                     cast(json_extract_scalar(e, '$.id')     as bigint),
                     cast(json_extract_scalar(e, '$.roleId') as bigint)
                 ) as row(idx bigint, id bigint, role_id bigint))
        )) as sorted_elems
    from distinct_chains
),

transformed as (
    select
        purchase_method_id,
        daisy_chain,
        array_join(transform(sorted_elems, x -> concat(cast(x.id as varchar), '(', cast(x.role_id as varchar), ')')), ',') as daisy_chain_transformed_1,
        array_join(transform(sorted_elems, x -> concat(cast(x.id as varchar), '.', cast(x.role_id as varchar))), '|') as daisy_chain_transformed_2
    from parsed
)

select
    purchase_method_id,
    daisy_chain,
    daisy_chain_transformed_1,
    daisy_chain_transformed_2,
    upper(to_hex(md5(to_utf8(concat(cast(purchase_method_id as varchar), daisy_chain_transformed_2))))) as md5_hashcode
from transformed
where coalesce(daisy_chain_transformed_2, '') != ''
