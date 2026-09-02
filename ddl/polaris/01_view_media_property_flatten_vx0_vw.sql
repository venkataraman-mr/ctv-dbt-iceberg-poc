-- Iceberg VIEW: polaris.km_preparation_gold_db.media_property_flatten_vx0_vw
-- Port of the legacy Databricks view hive_metastore.km_preparation_gold_db.media_property_flatten_vx0_vw.
--
-- On Nessie we had to fake this as an EPHEMERAL dbt model (Nessie's native connector can't hold
-- Iceberg views). Polaris DOES support Iceberg views (validated in the catalog feature tests), so we
-- create it as a real view here — restoring the original Databricks shape and letting non-dbt tools
-- query it directly.
--
-- Run AFTER the reference sync has loaded the base tables (km_preparation_gold_db.media_property_flatten
-- and productcentral.company):
--   docker exec -i trino trino -f /dev/stdin < ddl/polaris/01_view_media_property_flatten_vx0_vw.sql
--
-- Downstream dbt models read it via source('km_preparation_gold_db','media_property_flatten_vx0_vw').

CREATE OR REPLACE VIEW polaris.km_preparation_gold_db.media_property_flatten_vx0_vw AS
select
    mpf.*,
    coalesce(cpar.company_id, csub.company_id, 0) as vx0_parent_company_id
from polaris.km_preparation_gold_db.media_property_flatten mpf
left join polaris.productcentral.company cpar
    on  mpf.parent_company_id = cpar.legacy_id
    and cpar.companylevel_id = 3
    and cpar.legacy_id != 9999
left join polaris.productcentral.company csub
    on  mpf.parent_company_id = csub.legacy_id
    and csub.companylevel_id = 2
    and csub.legacy_id != 9999
where mpf.media_property_status_id = 92
  and mpf.legacy_country_code = 'US';
