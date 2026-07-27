{#
  Port of the legacy Databricks view hive_metastore.km_preparation_gold_db.media_property_flatten_vx0_vw.
  Nessie can't hold Iceberg views, so this is an EPHEMERAL dbt model: it stores nothing and is
  inlined as a CTE into any downstream model that ref()s it — so it always reflects the latest
  reference data (no physical table, no refresh step). Because it's ephemeral it has no schema and
  can't be queried directly or by non-dbt tools; it exists only within the dbt DAG (as intended,
  it's needed only during transformations). Name kept verbatim (…_vw) for downstream compatibility.
#}
{{ config(
    materialized='ephemeral',
    tags=['reference']
) }}

select
    mpf.*,
    coalesce(cpar.company_id, csub.company_id, 0) as vx0_parent_company_id
from {{ source('km_preparation_gold_db', 'media_property_flatten') }} mpf
left join {{ source('productcentral', 'company') }} cpar
    on  mpf.parent_company_id = cpar.legacy_id
    and cpar.companylevel_id = 3
    and cpar.legacy_id != 9999
left join {{ source('productcentral', 'company') }} csub
    on  mpf.parent_company_id = csub.legacy_id
    and csub.companylevel_id = 2
    and csub.legacy_id != 9999
where mpf.media_property_status_id = 92
  and mpf.legacy_country_code = 'US'
