{#- THROWAWAY probe: can a SINGLE dbt incremental model write `variant` directly, now that sorted_by is
    gone? If yes, we can drop the staging+promote+run-operation split. Delete after the test. -#}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='bronze',
    properties={
      'format_version': '3',
      'partitioning': "ARRAY['capture_month']"
    },
    views_enabled=false
) }}
select
    provider_occurrence_id,
    capture_month,
    cast(json_parse(raw_json) as variant) as raw_json
from {{ ref('digital_raw_occurrence_stg') }}
