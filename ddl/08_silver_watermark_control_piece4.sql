-- =====================================================================================
-- Piece 4 (creative sync-back) — seed the TIMESTAMP watermarks in iceberg.silver.watermark_control.
-- Run once (idempotent: only inserts names that don't already exist). All are timestamp-based
-- (MERGE-written / Postgres sources, per docs/ctv_creative_sync_plan.md §2/§5). end_timestamp seeded
-- to 1900-01-01 UTC so the first run reads the full history; reads apply a 1-min UTC safety lag.
-- watermark_control is partitioned by watermark_name (ddl/03) -> each process's row is isolated.
-- =====================================================================================

INSERT INTO iceberg.silver.watermark_control
    (watermark_name, start_timestamp, end_timestamp, last_commit_version, current_commit_version,
     transaction_status, created_timestamp, updated_timestamp)
SELECT v.watermark_name, v.start_timestamp, v.end_timestamp, v.last_commit_version, v.current_commit_version,
       v.transaction_status, v.created_timestamp, v.updated_timestamp
FROM (
    VALUES
      -- task 1 creative sync (prod creatives.creative.updated_timestamp)
      ('CTV_SYNC_CREATIVE',          CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), TIMESTAMP '1900-01-01 00:00:00 UTC'),
      -- task 2 first-seen sync (prod creative_first_seen.updated_timestamp)
      ('CTV_SYNC_FIRST_SEEN',        CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), TIMESTAMP '1900-01-01 00:00:00 UTC'),
      -- task 3 dedup-map sync (upsert = dedupe_map.updated_timestamp; delete = creative.updated_timestamp)
      ('CTV_SYNC_DEDUP_UPSERT',      CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), TIMESTAMP '1900-01-01 00:00:00 UTC'),
      ('CTV_SYNC_DEDUP_DELETE',      CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), TIMESTAMP '1900-01-01 00:00:00 UTC'),
      -- task 5 first-seen-info update (Iceberg gold.creative_first_seen / gold.creative updated_timestamp)
      ('CTV_FSINFO_FROM_FIRSTSEEN',  CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), TIMESTAMP '1900-01-01 00:00:00 UTC'),
      ('CTV_FSINFO_FROM_CREATIVE',   CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), TIMESTAMP '1900-01-01 00:00:00 UTC'),
      -- first-seen occurrence-id fixup (Piece-5 gated; gold.creative.updated_timestamp)
      ('CTV_FIRST_SEEN_OCC_ID',      CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), TIMESTAMP '1900-01-01 00:00:00 UTC'),
      -- task 6 last-seen update (Piece-5 gated; gold.digital_gold_occurrence, column TBD w/ Piece 5)
      ('CTV_LAST_SEEN_DIGITAL',      CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), TIMESTAMP '1900-01-01 00:00:00 UTC'),
      -- task 4 component sync (prod component_coding.modified)
      ('CTV_SYNC_COMPONENT',         CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), TIMESTAMP '1900-01-01 00:00:00 UTC'),
      -- task 8 product-translation resync (reference productcentral.productmap.change_dt)
      ('CTV_PRODUCT_RESYNC',         CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), TIMESTAMP '1900-01-01 00:00:00 UTC')
) AS v(watermark_name, start_timestamp, end_timestamp)
CROSS JOIN (SELECT CAST(NULL AS BIGINT) AS last_commit_version,
                   CAST(NULL AS BIGINT) AS current_commit_version,
                   'INIT' AS transaction_status,
                   CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS created_timestamp,
                   CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS updated_timestamp) c
WHERE v.watermark_name NOT IN (SELECT watermark_name FROM iceberg.silver.watermark_control);
