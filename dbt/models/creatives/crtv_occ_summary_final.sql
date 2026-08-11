{#
  Job B — occurrence summary push + park/release. FAITHFUL 1:1 transliteration of CrtvOccSummary
  STEP-3 (occurence_summary_for_psql_df -> CALL sp_dbx_digital_upsert_to_crtv_occ_summary), STEP-4
  (MERGE the buffer bronze.missing_digital_occurrence_for_summary), STEP-5 (advance the version watermark).

  BODY = the aggregate pushed to Postgres: from crtv_occ_summary_candidate WHERE creative_id and
  media_property_id are non-null, grouped to (creative_id, hash, country, market, media, property,
  capture_date) with min/max capture_timestamp + occ_cnt.

  POST-HOOKS (ordered as legacy):
    1 drop + 2 create the PG temp table (cross-catalog CTAS from this aggregate)
    3 pg_call the upsert proc  (accumulates occurrence_count + first_run/last_run + 7-day count)
    4 park/release MERGE into missing_digital_occurrence_for_summary FROM the candidate (TEMP_all):
        WHEN MATCHED AND creative_id resolved -> DELETE ; WHEN NOT MATCHED AND creative_id null -> INSERT
    5 watermark_version_finish (run-time string, execute=True)

  Iceberg MERGE with DELETE is used for the park/release (unlike Job A). If Nessie's row-level delete
  misbehaves, decompose into DELETE + INSERT. Scratch model (tagged CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY).
#}

{%- set wm_name = 'DIGITAL_RAW_OCC_SUMMARY_PSQL' -%}
{%- set cand = 'iceberg.bronze.crtv_occ_summary_candidate' -%}
{%- set missing_rel = 'iceberg.bronze.missing_digital_occurrence_for_summary' -%}
{%- set self_rel = this.database ~ '.bronze.' ~ this.identifier -%}
{%- set tmp_pg = 'tempwork.tmp_raw_occ_for_crtv_occ_summary_ctv_poc' -%}
{%- set tmp_pg_cat = 'postgres.' ~ tmp_pg -%}

{#- ---- post-hooks ------------------------------------------------------------------------------- -#}
{%- set h_pg_drop = 'drop table if exists ' ~ tmp_pg_cat -%}
{%- set h_pg_ctas -%}
create table {{ tmp_pg_cat }} as
select
    creative_id,
    creative_url_hash,
    country_iso_2_code,
    market_id,
    media_id,
    media_property_id,
    capture_date,
    cast(min_capture_timestamp as timestamp(6)) as min_capture_timestamp,
    cast(max_capture_timestamp as timestamp(6)) as max_capture_timestamp,
    occ_cnt
from {{ self_rel }}
{%- endset -%}
{%- set h_call = pg_call("call tempwork.sp_dbx_digital_upsert_to_crtv_occ_summary_ctv_poc('" ~ tmp_pg ~ "')") -%}
{%- set h_merge_missing -%}
merge into {{ missing_rel }} t
using {{ cand }} s
   on t.provider_occurrence_id = s.provider_occurrence_id
  and t.provider_code = s.provider_code
when matched and s.creative_id is not null then delete
when not matched and s.creative_id is null then insert
    (provider_occurrence_id, creative_url_hash, provider_code, country_iso_2_code,
     source_channel, provider_dma_city_name, publisher_id, capture_date, capture_timestamp)
    values (s.provider_occurrence_id, s.creative_url_hash, s.provider_code, s.country_iso_2_code,
            s.source_channel, s.provider_dma_city_name, s.publisher_id, s.capture_date, s.capture_timestamp)
{%- endset -%}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY'],
    views_enabled=false,
    post_hook=[h_pg_drop, h_pg_ctas, h_call, h_merge_missing,
               "{{ watermark_version_finish('" ~ wm_name ~ "') }}"]
) }}

{#- STEP-3 occurence_summary_for_psql_df -#}
select
    creative_id,
    creative_url_hash,
    country_iso_2_code,
    market_id,
    media_id,
    media_property_id,
    capture_date,
    min(capture_timestamp) as min_capture_timestamp,
    max(capture_timestamp) as max_capture_timestamp,
    count(*) as occ_cnt
from {{ ref('crtv_occ_summary_candidate') }}
where creative_id is not null
  and media_property_id is not null
group by
    creative_id,
    creative_url_hash,
    country_iso_2_code,
    market_id,
    media_id,
    media_property_id,
    capture_date
