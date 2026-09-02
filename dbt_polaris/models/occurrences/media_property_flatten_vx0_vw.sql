{#
  Polaris clone of the legacy Databricks view hive_metastore.km_preparation_gold_db.media_property_flatten_vx0_vw.
  Kept as an EPHEMERAL dbt model (as in Nessie): it stores nothing and is inlined as a CTE into any
  downstream model that ref()s it — so it always reflects the latest reference data (no physical table,
  no refresh step). Polaris CAN hold Iceberg views, but we keep this ephemeral for parity with the
  Nessie build and because nothing needs to query it outside the dbt DAG. Name kept verbatim (…_vw)
  for downstream compatibility. Sources resolve via the consolidated models/sources.yml (catalog =
  polaris). No VARIANT columns. Lives in occurrences/ per the domain-folder rule (runbook §2).
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
