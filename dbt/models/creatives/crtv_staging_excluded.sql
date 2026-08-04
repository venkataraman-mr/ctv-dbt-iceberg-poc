{#
  Job A exclude auto-chaff = exclude_auto_chaff_records -> tmp_digital_raw_occ_media_exclude_auto_chaff.
  FAITHFUL 1:1: tmp_digital_raw_occ_media (tdr) ANTI JOIN tmp_digital_occ_auto_chaff (ac) on
  creative_url_hash + provider_code. (No-op for CTV; ac is empty.) Spark->Trino: ANTI JOIN ->
  LEFT JOIN ... IS NULL. Output = the candidate columns, unchanged.

  Legacy comment: as auto-chaff is not applied on BISSocial, exclude_auto_chaff_df carries BISSocial
  as-is alongside non-auto-chaffed BIS per the anti join. The new/previous-creative split
  (get_new_creatives / get_previous_creratives) happens downstream in crtv_staging_final.
#}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'job_a'],
    views_enabled=false
) }}

select tdr.*
from {{ ref('crtv_staging_candidate') }} tdr
left join {{ ref('crtv_autochaff') }} ac
       on tdr.creative_url_hash = ac.creative_url_hash
      and tdr.provider_code     = ac.provider_code
where ac.creative_url_hash is null
