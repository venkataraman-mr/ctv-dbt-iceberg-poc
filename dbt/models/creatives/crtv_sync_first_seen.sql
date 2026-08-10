{#
  Piece 4 — TASK 2: creative first-seen sync  (watermark CTV_SYNC_FIRST_SEEN).
  Port of SyncCreativeFirstSeenFromPsql: read changed rows from the SEEDED clone
  tempwork.creative_first_seen_ctv_poc and MERGE into iceberg.gold.creative_first_seen on
  creative_url_hash (UPDATE all non-key cols; INSERT new). The clone is already scoped to our CTV
  creatives + imported parents, so the prod provider_id filter is unnecessary.

  Timestamp watermark (MERGE-written target -> no version CDF): read updated_timestamp > (start - 1 min).
  UTC/no-miss discipline (docs/ctv_creative_sync_plan.md §2):
    * watermark stored UTC; the clone's updated_timestamp is tz-naive UTC (prod writes AT TIME ZONE 'UTC'),
      so the pushed-down predicate uses a NAIVE-UTC literal (no implicit tz shift);
    * 1-min inclusive-overlap lag so a boundary/skew row is never skipped;
    * advance the watermark to the max updated_timestamp actually read (not now());
    * idempotent MERGE on creative_url_hash -> over-read is safe.
  Types are cast to the gold schema (media_id is VARCHAR in gold; ids sized; naive Postgres timestamps
  -> timestamp(6) with time zone, session TZ = UTC).

  Prereqs (once, on the VM): seed CTV_SYNC_FIRST_SEEN (ddl/08) and
    ALTER TABLE iceberg.gold.creative_first_seen ADD COLUMN provider_campaign_landing_page VARCHAR;
#}

{%- set wm_name = 'CTV_SYNC_FIRST_SEEN' -%}
{%- set src = source('tempwork', 'creative_first_seen_ctv_poc') -%}
{%- set tgt = 'iceberg.gold.creative_first_seen' -%}
{%- set self_rel = this.database ~ '.bronze.' ~ this.identifier -%}

{#- Data-provider list (Databricks data_provider_list_to_sync_from_psql = '2,11,13,16,18'):
    BIS (2), TV (11), PlayOn (13), Social (16), BIS CTV (18). Override:
    dbt run ... --vars 'p4_first_seen_provider_ids: "2, 11, 13, 16, 18"'. -#}
{%- set provider_ids = var('p4_first_seen_provider_ids', '2, 11, 13, 16, 18') -%}

{%- set wm = watermark_ts_begin(wm_name) -%}
{%- set start_ts = wm.start_ts -%}
{%- set start_ts_naive = (start_ts | string)[:19] if start_ts is not none else '1900-01-01 00:00:00' -%}

{%- set cols = [
  'creative_id','creative_url_hash','provider_creative_id','country_iso_2_code','media_id','occurrence_id',
  'occurrence_timestamp','occurrence_timestamp_local','provider_id','media_property_id','media_property_name',
  'media_category_id','media_category_code','provider_creative_link_url','provider_publisher_id',
  'provider_publisher_domain','provider_campaign_id','provider_campaign_name','provider_advertiser_id',
  'provider_advertiser_name','provider_product_id','provider_product_name','due_timestamp','market_id',
  'market_name','daypart_id','daypart_name','affiliate_id','affiliate_name','created_timestamp',
  'updated_timestamp','edition_name','section_name','edition_id','section_id','provider_campaign_landing_page'
] -%}

{#- MERGE candidate -> gold on creative_url_hash. SET = all cols except the key + creative_id
    (mirrors the Databricks first-seen MERGE, which does not re-set creative_id/url_hash on match). -#}
{%- set merge_sql -%}
merge into {{ tgt }} m
using {{ self_rel }} s
  on m.creative_url_hash = s.creative_url_hash
when matched then update set
  {% for c in cols if c not in ['creative_id','creative_url_hash'] %}{{ c }} = s.{{ c }}{{ "," if not loop.last }}
  {% endfor %}
when not matched then insert ({{ cols | join(', ') }})
  values ({% for c in cols %}s.{{ c }}{{ ", " if not loop.last }}{% endfor %})
{%- endset -%}

{#- post-hooks run at RUN time in order: (1) MERGE the candidate batch into gold; (2) advance the
    watermark to the max updated_timestamp in the candidate. Both are registered at PARSE (config is
    parse-time). The watermark hook is a run-time TEMPLATE STRING (self_rel embedded as a literal) so
    the advance is computed at run — never conditionally appended at parse (that was the bug). -#}
{%- set wm_finish = "{{ watermark_ts_finish_from_relation('" ~ wm_name ~ "', '" ~ self_rel ~ "', 'updated_timestamp') }}" -%}

{{ config(
    materialized='table',
    schema='bronze',
    tags=['creatives', 'p4_sync_creative_to_iceberg'],
    views_enabled=false,
    post_hook=[merge_sql, wm_finish]
) }}

select
    cast(creative_id                 as bigint)                       as creative_id,
    cast(creative_url_hash           as bigint)                       as creative_url_hash,
    cast(provider_creative_id        as bigint)                       as provider_creative_id,
    cast(country_iso_2_code          as varchar)                      as country_iso_2_code,
    cast(media_id                    as varchar)                      as media_id,
    cast(occurrence_id               as varchar)                      as occurrence_id,
    cast(occurrence_timestamp        as timestamp(6) with time zone)  as occurrence_timestamp,
    cast(occurrence_timestamp_local  as timestamp(6) with time zone)  as occurrence_timestamp_local,
    cast(provider_id                 as smallint)                     as provider_id,
    cast(media_property_id           as integer)                      as media_property_id,
    cast(media_property_name         as varchar)                      as media_property_name,
    cast(media_category_id           as integer)                      as media_category_id,
    cast(media_category_code         as varchar)                      as media_category_code,
    cast(provider_creative_link_url  as varchar)                      as provider_creative_link_url,
    cast(provider_publisher_id       as bigint)                       as provider_publisher_id,
    cast(provider_publisher_domain   as varchar)                      as provider_publisher_domain,
    cast(provider_campaign_id        as bigint)                       as provider_campaign_id,
    cast(provider_campaign_name      as varchar)                      as provider_campaign_name,
    cast(provider_advertiser_id      as bigint)                       as provider_advertiser_id,
    cast(provider_advertiser_name    as varchar)                      as provider_advertiser_name,
    cast(provider_product_id         as bigint)                       as provider_product_id,
    cast(provider_product_name       as varchar)                      as provider_product_name,
    cast(due_timestamp               as timestamp(6) with time zone)  as due_timestamp,
    cast(market_id                   as smallint)                     as market_id,
    cast(market_name                 as varchar)                      as market_name,
    cast(daypart_id                  as smallint)                     as daypart_id,
    cast(daypart_name                as varchar)                      as daypart_name,
    cast(affiliate_id                as integer)                      as affiliate_id,
    cast(affiliate_name              as varchar)                      as affiliate_name,
    cast(created_timestamp           as timestamp(6) with time zone)  as created_timestamp,
    cast(updated_timestamp           as timestamp(6) with time zone)  as updated_timestamp,
    cast(edition_name                as varchar)                      as edition_name,
    cast(section_name                as varchar)                      as section_name,
    cast(edition_id                  as integer)                      as edition_id,
    cast(section_id                  as integer)                      as section_id,
    cast(provider_campaign_landing_page as varchar)                   as provider_campaign_landing_page
from {{ src }}
where updated_timestamp > timestamp '{{ start_ts_naive }}' - interval '1' minute
  and provider_id in ({{ provider_ids }})
