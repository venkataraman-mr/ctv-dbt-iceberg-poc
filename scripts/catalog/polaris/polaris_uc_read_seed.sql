-- ============================================================
-- Seed a stand-in "gold occurrence" table in POLARIS for the Databricks UC-cluster read test.
-- Run in DBeaver on Trino (catalog=polaris). Read-only, non-VARIANT columns — mirrors what the
-- first-seen/last-seen creative-sync job actually needs from gold.digital_gold_occurrence:
--   occurrence_id, the creative match key (creative_url_hash), and the timestamps.
-- v3 on purpose: also proves a Databricks UC (Dedicated) cluster can read a Polaris *v3* table
-- (we only select non-VARIANT columns, so no VARIANT decode is involved).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS polaris.ctv_catalog_poc;
DROP TABLE IF EXISTS polaris.ctv_catalog_poc.gold_occurrence_sample;

CREATE TABLE polaris.ctv_catalog_poc.gold_occurrence_sample (
    occurrence_id          bigint,
    creative_url_hash      bigint,          -- the cross-system creative match key
    provider_occurrence_id varchar,
    capture_month          varchar,
    capture_timestamp      timestamp(6) with time zone,   -- drives first-seen
    updated_timestamp      timestamp(6) with time zone,   -- drives last-seen
    delete_flag            boolean          -- gold is soft-delete only
) WITH (format_version = 3);

-- two creatives, a few occurrences each, spread across time so first/last-seen differ
INSERT INTO polaris.ctv_catalog_poc.gold_occurrence_sample VALUES
 (75000000001, 111111111111, 'occ-a1', '2026-07', TIMESTAMP '2026-07-01 09:00:00 UTC', TIMESTAMP '2026-07-01 09:00:00 UTC', false),
 (75000000002, 111111111111, 'occ-a2', '2026-07', TIMESTAMP '2026-07-03 12:30:00 UTC', TIMESTAMP '2026-07-05 08:15:00 UTC', false),
 (75000000003, 111111111111, 'occ-a3', '2026-08', TIMESTAMP '2026-08-10 22:05:00 UTC', TIMESTAMP '2026-08-10 22:05:00 UTC', false),
 (75000000004, 222222222222, 'occ-b1', '2026-08', TIMESTAMP '2026-08-02 01:10:00 UTC', TIMESTAMP '2026-08-02 01:10:00 UTC', false),
 (75000000005, 222222222222, 'occ-b2', '2026-08', TIMESTAMP '2026-08-09 18:45:00 UTC', TIMESTAMP '2026-08-12 06:20:00 UTC', false);

-- quick check (Trino side): per-creative first-seen / last-seen the job would compute
SELECT creative_url_hash,
       min(capture_timestamp) AS first_seen,
       max(updated_timestamp) AS last_seen,
       count(*)               AS occ_count
FROM polaris.ctv_catalog_poc.gold_occurrence_sample
WHERE delete_flag = false
GROUP BY creative_url_hash
ORDER BY creative_url_hash;

-- cleanup when the test is done:
-- DROP TABLE IF EXISTS polaris.ctv_catalog_poc.gold_occurrence_sample;
