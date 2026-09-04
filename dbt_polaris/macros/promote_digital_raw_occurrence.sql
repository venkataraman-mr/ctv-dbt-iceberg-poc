{#
  Direct INSERT ... SELECT that promotes the VARCHAR staging batch
  (bronze.digital_raw_occurrence_stg, produced by the dbt model) into the pre-created v3+VARIANT
  target bronze.digital_raw_occurrence, casting the two JSON-text columns to `variant`.

  WHY a direct insert (not a dbt materialization): dbt-trino's incremental strategy stages new rows in
  an intermediate relation (…__dbt_tmp) that is NOT v3, and a non-v3 Iceberg table/view cannot hold a
  `variant` column ("Unsupported Hive type: variant"). A direct INSERT into the already-v3 target does
  not create any intermediate, so the variant cast succeeds (proven by the catalog feature test). This
  runs as a post-hook on the staging model. Idempotent via NOT EXISTS on (provider_occurrence_id,
  capture_month) — the same key the Nessie model used.
#}
{% macro promote_digital_raw_occurrence() %}
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
from {{ this }} s   {# the staging model's own relation; this macro runs as its post-hook, so {{ this }} = bronze.digital_raw_occurrence_stg. Using ref() here would create a self-cycle. #}
where not exists (
    select 1
    from {{ target.database }}.bronze.digital_raw_occurrence t
    where t.provider_occurrence_id = s.provider_occurrence_id
      and t.capture_month = s.capture_month
)
{% endmacro %}
