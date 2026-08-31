-- Piece 5 (gold occurrence flow) watermark seeds — idempotent (INSERT only the missing rows).
-- Two consumers of the occurrence pipeline, mirroring raw_occ_to_gold_occ.py:
--   * DIGITAL_RAW_OCC_TO_GOLD_OCC   — Half A. VERSION watermark on the APPEND-ONLY bronze.digital_raw_occurrence
--       (Trino system.table_changes works on append-only snapshots). last_commit_version NULL -> first run is a
--       full read (for version as of end_snap), like Piece 1 / Job A.
--   * DIGITAL_CRTV_CHANGES_TO_GOLD_OCC — Half B. Databricks uses a VERSION watermark on gold.creative, but
--       gold.creative is MERGE-written (delete files) so Trino version-CDF is invalid there -> we use a
--       TIMESTAMP watermark on gold.creative.updated_timestamp (same adaptation as first-seen-info).
--       end_timestamp seeded to 1900-01-01 UTC (first run sees all creative changes; nothing to release until
--       Half A has parked holds, so it is a no-op early on).

INSERT INTO iceberg.silver.watermark_control
    (watermark_name, start_timestamp, end_timestamp, last_commit_version, current_commit_version,
     transaction_status, created_timestamp, updated_timestamp)
SELECT v.watermark_name, v.start_timestamp, v.end_timestamp, v.last_commit_version, v.current_commit_version,
       'INIT' AS transaction_status,
       CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS created_timestamp,
       CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS updated_timestamp
FROM (
    VALUES
      -- Half A: version watermark (timestamps null, version null -> first run full read)
      ('DIGITAL_RAW_OCC_TO_GOLD_OCC',
         CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE),
         CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)),
      -- Half B: timestamp watermark on gold.creative.updated_timestamp (1900 base)
      ('DIGITAL_CRTV_CHANGES_TO_GOLD_OCC',
         CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), TIMESTAMP '1900-01-01 00:00:00 UTC',
         CAST(NULL AS BIGINT), CAST(NULL AS BIGINT))
) AS v(watermark_name, start_timestamp, end_timestamp, last_commit_version, current_commit_version)
WHERE v.watermark_name NOT IN (SELECT watermark_name FROM iceberg.silver.watermark_control);
