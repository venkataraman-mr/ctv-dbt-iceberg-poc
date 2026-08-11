{#
  Piece 5 -- HALF A, STAGE 1: read new raw occurrences from the append-only bronze.digital_raw_occurrence.
  Port of DigitalRawocctoGoldocc.get_new_raw_occurrence_data_cdf (STEP-1).

  VERSION watermark DIGITAL_RAW_OCC_TO_GOLD_OCC (bronze is append-only -> Trino system.table_changes is valid;
  same engine adaptation as Piece 1 / Job A). watermark_version_begin pins the end snapshot (current_commit_version,
  InProgress); the Half-A writer (digital_occ_gold) promotes it with watermark_version_finish AFTER the gold write, so a
  failure mid-run doesn't advance the watermark.
    * first run (start_version none) -> full read `for version as of end_snap`;
    * no change (start==end)         -> empty;
    * incremental                    -> system.table_changes(start_snap, end_snap) where _change_type='insert'.
  Filter: country = 'US', not retransmit; dedup to the latest record per (country, provider_code,
  provider_occurrence_id) by capture_timestamp desc (Trino has no _commit_timestamp CDF column -> capture_timestamp
  is the faithful proxy for "keep the newest").

  Body = the STEP-1 column set (incl. daisy_chain, purchase_method_id, ad_insertion_point, raw_json) that the
  deployment-chain persist + the combine/classify stages consume.
#}

{%- set wm_name = 'DIGITAL_RAW_OCC_TO_GOLD_OCC' -%}
{%- set raw_rel = ref('digital_raw_occurrence') -%}
{%- set wm = watermark_version_begin(wm_name, raw_rel) -%}
{%- set country_code_US = "'US'" -%}

{{ config(materialized='table', schema='bronze', tags=['occurrences', 'p5_digital_raw_to_gold_occ'], views_enabled=false, on_table_exists='drop') }}

with cdf_source as (
{%- if wm.start_version is none %}
    select * from {{ raw_rel }}{% if wm.end_version is not none %} for version as of {{ wm.end_version }}{% endif %}
{%- elif wm.start_version == wm.end_version %}
    select * from {{ raw_rel }} where false
{%- else %}
    select *
    from table(system.table_changes(
        schema_name       => 'bronze',
        table_name        => 'digital_raw_occurrence',
        start_snapshot_id => {{ wm.start_version }},
        end_snapshot_id   => {{ wm.end_version }}))
    where _change_type = 'insert'
{%- endif %}
),

raw_changes as (
    select *,
        row_number() over (partition by country_iso_2_code, provider_code, provider_occurrence_id
                           order by capture_timestamp desc) as rnum
    from cdf_source raw_occ
    where country_iso_2_code = {{ country_code_US }}
      and coalesce(retransmit, false) = false
)

select
    country_iso_2_code,
    provider_code,
    source_channel,
    provider_occurrence_id,
    provider_creative_id,
    capture_date,
    capture_month,
    capture_timestamp,
    region_dma_id,
    region_dma_name,
    region_country_code,
    region_city_id,
    region_city_name,
    region_state_id,
    region_state_name,
    publisher_id,
    creative_url_hash,
    provider_campaign_id,
    provider_campaign_product_id,
    provider_campaign_advertiser_id,
    provider_campaign_name,
    provider_campaign_product_name,
    provider_campaign_advertiser_name,
    provider_campaign_landing_page,
    daisy_chain,
    purchase_method_id,
    ad_insertion_point,
    raw_json
from raw_changes
where rnum = 1
