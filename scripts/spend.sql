-- CREATE TABLE iceberg.tempwork.apply_spend_to_occurrence as
-- WITH joined AS (
-- SELECT
--     qc.provider_code,
--     qc.capture_month,
--     qc.advertiser_type,
--     qc.media_property_id,
--     qc.primary_product_id,
--     qc.advertiser_id,
--     qc.advertiser_name,
--     qc.industry_class_id,
--     qc.cpm_class_id,
--     qc.brand_id,
--     qc.cpm_type,
--     qc.device_name,
--     qc.provider_occurrence_ids,
--     cf.factored_spend as spend_final_calib,
--     cf.factored_imps as imps_final_calib
-- FROM iceberg.tempwork.unified_ott_ctv_spend_qc_report_w_occ_ids_nk qc
-- INNER JOIN iceberg.tempwork.ctv_ott_spend_data_from_ops cf
--         ON  qc.provider_code      = cf.provider_code
--         AND qc.capture_month      = cf.capture_month
--         AND qc.advertiser_type    = cf.advertiser_type
--         AND qc.media_property_id  = cf.media_property_id
--         AND qc.primary_product_id = cf.primary_product_id
--         AND qc.advertiser_id      = cf.advertiser_id
--         AND qc.industry_class_id  = cf.industry_class_id
--         AND qc.cpm_class_id       = cf.cpm_class_id
--         AND qc.brand_id           = cf.brand_id
--         AND qc.device_name        = cf.device_name
--         AND qc.cpm_type           = cf.cpm_type
-- WHERE qc.capture_month = 202607
-- ),
-- agg_across_devices AS (
-- SELECT
--     provider_code,
--     capture_month,
--     advertiser_type,
--     media_property_id,
--     primary_product_id,
--     advertiser_id,
--     industry_class_id,
--     cpm_class_id,
--     brand_id,
--     cpm_type,
--     ANY_VALUE(provider_occurrence_ids) AS provider_occurrence_ids,
--     SUM(spend_final_calib) AS total_spend_final_calib,
--     SUM(imps_final_calib)  AS total_imps_final_calib
-- FROM joined
-- GROUP BY
--     provider_code, capture_month, advertiser_type, media_property_id,
--     primary_product_id, advertiser_id, industry_class_id, cpm_class_id,
--     brand_id, cpm_type
--     ),
-- exploded AS (
--     SELECT
--         a.provider_code,
--         a.capture_month,
--         a.advertiser_type,
--         a.media_property_id,
--         a.primary_product_id,
--         a.advertiser_id,
--         a.industry_class_id,
--         a.cpm_class_id,
--         a.brand_id,
--         a.cpm_type,
--         u.provider_occurrence_id,
--         CARDINALITY(a.provider_occurrence_ids) AS occ_count,
--         a.total_spend_final_calib,
--         a.total_imps_final_calib,
--         ROUND(a.total_spend_final_calib / CARDINALITY(a.provider_occurrence_ids), 6) AS allocated_spend,
--         ROUND(a.total_imps_final_calib / CARDINALITY(a.provider_occurrence_ids), 6) AS allocated_impressions
--     FROM agg_across_devices a
--     CROSS JOIN UNNEST(a.provider_occurrence_ids) AS u(provider_occurrence_id)
-- )
-- SELECT * FROM exploded


-- SELECT * 
-- FROM iceberg.tempwork.apply_spend_to_occurrence
-- LIMIT 10 

-- SELECT provider_occurrence_id 
-- FROM iceberg.gold.digital_gold_occurrence
-- LIMIT 10 ;

-- DESCRIBE iceberg.gold.digital_gold_occurrence;

-- SELECT COUNT(*) 
-- FROM iceberg.tempwork.apply_spend_to_occurrence x 
-- INNER JOIN iceberg.gold.digital_gold_occurrence o 
-- ON x.provider_occurrence_id = o.provider_occurrence_id ;
------> 590782

-- SELECT capture_month, COUNT(*) 
-- FROM iceberg.gold.digital_gold_occurrence 
-- GROUP BY 1 ;

SELECT provider_occurrence_id, final_spend, final_impressions
FROM iceberg.gold.digital_gold_occurrence
WHERE  final_spend IS NOT NULL
LIMIT 10;

-- MERGE INTO iceberg.gold.digital_gold_occurrence AS m
-- USING (
--     SELECT *
--     FROM iceberg.tempwork.apply_spend_to_occurrence
--     WHERE provider_code = 'AVOD BISCTV'
-- ) AS s
-- ON (
--        m.provider_occurrence_id = s.provider_occurrence_id
--    AND m.capture_month          = s.capture_month
--    AND m.delete_flag            = FALSE
--    AND m.provider_code          = 'AVOD BISCTV'
-- )
-- WHEN MATCHED THEN
--     UPDATE SET
--         final_spend = s.allocated_spend,
--         final_impressions = s.allocated_impressions,
--         updated_timestamp = current_timestamp;