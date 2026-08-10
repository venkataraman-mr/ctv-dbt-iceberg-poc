{#
  Piece 4 -- FIRST-SEEN OCCURRENCE-ID: set gold.creative.first_seen_occurrence_id (the internal BIGINT
  occurrence id) by matching the creative's first-seen provider occurrence back to the occurrence gold table.
  Port of common/notebook_files/update_crtv_first_seen_occurrence_id.py.

  NO WATERMARK: the real job self-limits on `first_seen_occurrence_id IS NULL` (once set, a creative is
  never reprocessed) plus an `updated_timestamp` date floor -- there is no watermark advance. Our seeded
  CTV_FIRST_SEEN_OCC_ID row is therefore UNUSED (kept for inventory symmetry).

  Match: gold.creative.creative_url_hash = occ.provider_original_creative_url_hash
     AND gold.creative.first_seen_provider_occurrence_id = occ.provider_occurrence_id
  -> set first_seen_occurrence_id = occ.occurrence_id.

  PIECE-5-GATED: reads gold.digital_gold_occurrence (Piece 5 output; tv_gold_occurrence is dropped for the
  CTV PoC -- digital only). Until Piece 5 populates it, this is a clean 0-update no-op. Validate after Piece 5.
  gold tables referenced as LITERAL relations (hook-written, not dbt outputs). Candidate deduped to one row
  per (creative_id, creative_url_hash) to avoid a Trino multi-match MERGE error if an occurrence pair repeats.
#}

{#- Databricks DAG: first-seen-info → occurrence-id. creative is transitive (fsinfo depends on creative). -#}
-- depends_on: {{ ref('crtv_fsinfo_update') }}

{%- set floor = var('p4_occid_updated_floor', '2025-10-13') -%}   {#- prod backfill cutoff; our data is newer so it passes -#}
{%- set self_rel = 'iceberg.bronze.' ~ this.identifier -%}

{%- set merge_sql %}
merge into iceberg.gold.creative tgt
using {{ self_rel }} src
on tgt.creative_url_hash = src.creative_url_hash and tgt.creative_id = src.creative_id
when matched then update set
  first_seen_occurrence_id = src.occurrence_id,
  updated_timestamp = cast(current_timestamp as timestamp(6) with time zone)
{%- endset %}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'p4_sync_creative_to_iceberg'],
    views_enabled=false,
    on_table_exists='drop',
    post_hook=[ merge_sql ]
) }}

with cte_crtv_data as (
    select creative_id, creative_url_hash, first_seen_provider_occurrence_id
    from iceberg.gold.creative
    where first_seen_occurrence_id is null
      and cast(updated_timestamp as date) >= date '{{ floor }}'
),

-- digital only (tv_gold_occurrence dropped for the CTV PoC); recent capture months only
cte_occ as (
    select occurrence_id, provider_original_creative_url_hash, provider_occurrence_id
    from iceberg.gold.digital_gold_occurrence
    where capture_month >= (year(current_date - interval '1' month) * 100
                            + month(current_date - interval '1' month))
),

matched as (
    select
        c.creative_id,
        c.creative_url_hash,
        go.occurrence_id,
        row_number() over (partition by c.creative_id, c.creative_url_hash
                           order by go.occurrence_id) as rn
    from cte_crtv_data c
    inner join cte_occ go
        on c.creative_url_hash = go.provider_original_creative_url_hash
       and c.first_seen_provider_occurrence_id = go.provider_occurrence_id
)

select creative_id, creative_url_hash, occurrence_id
from matched
where rn = 1
