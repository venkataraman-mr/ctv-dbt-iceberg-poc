{#
  Promote the VARCHAR staging batch (bronze.digital_raw_occurrence_stg) into the pre-created
  v3+VARIANT target bronze.digital_raw_occurrence, casting the two JSON-text columns to `variant`,
  then advance the version watermark.

  WHY a run-operation (NOT a model post-hook): dbt batches a model's statements (CTAS + renames +
  post-hooks) into ONE transaction, and Trino's Iceberg connector rejects a `variant` write inside a
  transaction that also ran DDL ("Unsupported Hive type: variant"). Run as its own operation, the
  INSERT executes cleanly — exactly like a plain autocommit `trino --execute` (proven: 811,764 rows).
  Idempotent via NOT EXISTS on (provider_occurrence_id, capture_month).

  Invoke AFTER `dbt run --select digital_raw_occurrence_stg`:
      dbt run-operation promote_digital_raw_occurrence
#}
{% macro promote_digital_raw_occurrence() %}
  {% set wm_name = 'BIS_CTV_US_INGESTION_STG_TO_RAW_OCC' %}
  {% set insert_sql %}
insert into {{ target.database }}.bronze.digital_raw_occurrence (
    country_iso_2_code, provider_code, source_channel, provider_occurrence_id,
    provider_creative_id, provider_source_id, capture_date, capture_month, capture_timestamp,
    eventhub_enqueued_timestamp, created_timestamp, region_dma_id, region_dma_name,
    region_country_code, region_city_id, region_city_name, region_state_id, region_state_name,
    creative_type, creative_mime_type, publisher_id, publisher_domain, creative_width,
    creative_height, creative_duration, creative_url, creative_url_hash, retransmit,
    provider_campaign_id, provider_campaign_product_id, provider_campaign_advertiser_id,
    provider_campaign_name, provider_campaign_product_name, provider_campaign_advertiser_name,
    provider_campaign_description, provider_campaign_landing_page, occurrence_description,
    occurrence_link_url, daisy_chain, purchase_method_id, ad_insertion_point, raw_json
)
select
    country_iso_2_code, provider_code, source_channel, provider_occurrence_id,
    provider_creative_id, provider_source_id, capture_date, capture_month, capture_timestamp,
    eventhub_enqueued_timestamp, created_timestamp, region_dma_id, region_dma_name,
    region_country_code, region_city_id, region_city_name, region_state_id, region_state_name,
    creative_type, creative_mime_type, publisher_id, publisher_domain, creative_width,
    creative_height, creative_duration, creative_url, creative_url_hash, retransmit,
    provider_campaign_id, provider_campaign_product_id, provider_campaign_advertiser_id,
    provider_campaign_name, provider_campaign_product_name, provider_campaign_advertiser_name,
    provider_campaign_description, provider_campaign_landing_page, occurrence_description,
    occurrence_link_url,
    cast(json_parse(daisy_chain) as variant) as daisy_chain,   -- VARCHAR JSON text -> variant
    purchase_method_id, ad_insertion_point,
    cast(json_parse(raw_json) as variant)    as raw_json        -- VARCHAR JSON text -> variant
from {{ target.database }}.bronze.digital_raw_occurrence_stg s
where not exists (
    select 1
    from {{ target.database }}.bronze.digital_raw_occurrence t
    where t.provider_occurrence_id = s.provider_occurrence_id
      and t.capture_month = s.capture_month
)
  {% endset %}
  {% if execute %}
    {% do log("promote_digital_raw_occurrence: inserting new rows into bronze.digital_raw_occurrence ...", info=true) %}
    {% do run_query(insert_sql) %}
    {% do run_query(watermark_version_finish(wm_name)) %}
    {% do log("promote_digital_raw_occurrence: done; watermark " ~ wm_name ~ " advanced.", info=true) %}
  {% endif %}
{% endmacro %}
