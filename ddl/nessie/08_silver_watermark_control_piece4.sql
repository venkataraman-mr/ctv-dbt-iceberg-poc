-- =====================================================================================
-- Piece 4 (creative sync-back) — seed the TIMESTAMP watermarks in iceberg.silver.watermark_control.
-- Run once (idempotent: only inserts names that don't already exist). All are timestamp-based
-- (MERGE-written / Postgres sources, per docs/pipeline/ctv_creative_sync_plan.md §2/§5). end_timestamp seeded
-- to 1900-01-01 UTC so the first run reads the full history; reads apply a 1-min UTC safety lag.
-- watermark_control is partitioned by watermark_name (ddl/nessie/03) -> each process's row is isolated.
-- =====================================================================================

INSERT INTO iceberg.silver.watermark_control
    (watermark_name, start_timestamp, end_timestamp, last_commit_version, current_commit_version,
     transaction_status, created_timestamp, updated_timestamp)
SELECT v.watermark_name, v.start_timestamp, v.end_timestamp, c.last_commit_version, c.current_commit_version,
       c.transaction_status, c.created_timestamp, c.updated_timestamp
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

-- ONE-TIME INIT for the product-resync watermark (task 8): unlike the others it must NOT start at 1900.
-- A fresh deploy has no resync backlog -- task 1 already translated every creative against the CURRENT
-- productmap -- so resync only needs to catch FUTURE productmap churn. Starting at 1900 makes `change_dt >= wm`
-- select the ENTIRE (very large) productmap, which blows the Trino node memory limit. Initialize it to the
-- current max(change_dt) so the first run is a correct, cheap no-op:
--   UPDATE iceberg.silver.watermark_control
--   SET start_timestamp = end_timestamp,
--       end_timestamp   = cast((SELECT max(change_dt) FROM iceberg.productcentral.productmap) as timestamp(6) with time zone),
--       transaction_status = 'INIT', updated_timestamp = cast(current_timestamp as timestamp(6) with time zone)
--   WHERE watermark_name = 'CTV_PRODUCT_RESYNC';
