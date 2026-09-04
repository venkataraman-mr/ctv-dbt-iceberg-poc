-- FALLBACK (autocommit) promote for Step 2, if `dbt run-operation promote_digital_raw_occurrence`
-- can't autocommit the variant write. Run directly against Trino (autocommit — proven to work):
--   docker exec -i trino trino -f /dev/stdin < scripts/catalog/polaris/promote_digital_raw_occurrence.sql
--
-- Promotes the VARCHAR staging batch (bronze.digital_raw_occurrence_stg) into the pre-created
-- v3+VARIANT bronze.digital_raw_occurrence, casting daisy_chain/raw_json to variant, then advances
-- the version watermark. Idempotent via NOT EXISTS. Same SQL the run-operation macro renders.

INSERT INTO polaris.bronze.digital_raw_occurrence (
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
SELECT
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
    CAST(json_parse(daisy_chain) AS variant) AS daisy_chain,
    purchase_method_id, ad_insertion_point,
    CAST(json_parse(raw_json) AS variant)    AS raw_json
FROM polaris.bronze.digital_raw_occurrence_stg s
WHERE NOT EXISTS (
    SELECT 1
    FROM polaris.bronze.digital_raw_occurrence t
    WHERE t.provider_occurrence_id = s.provider_occurrence_id
      AND t.capture_month = s.capture_month
);

-- advance the version watermark (mirrors watermark_version_finish)
UPDATE polaris.silver.watermark_control
   SET last_commit_version = current_commit_version,
       transaction_status  = 'SUCCEEDED',
       updated_timestamp   = CAST(current_timestamp AS timestamp(6) with time zone)
 WHERE watermark_name = 'BIS_CTV_US_INGESTION_STG_TO_RAW_OCC';
