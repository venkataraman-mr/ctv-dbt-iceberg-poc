-- ============================================================
-- Cross-engine interop check (Trino side) for the Lakekeeper catalog.
-- Pairs with databricks/lakekeeper_crosscloud_crud_test.py section 6.
-- Run in DBeaver on Trino (catalog=lakekeeper, schema=ctv_catalog_poc).
-- ============================================================

-- PART 1: Trino CREATES a v3+VARIANT table for Databricks to READ.
CREATE SCHEMA IF NOT EXISTS lakekeeper.ctv_catalog_poc;
DROP TABLE IF EXISTS lakekeeper.ctv_catalog_poc.xeng_lk_from_trino;
CREATE TABLE lakekeeper.ctv_catalog_poc.xeng_lk_from_trino (
    id bigint, source varchar, payload variant
) WITH (format_version = 3);
INSERT INTO lakekeeper.ctv_catalog_poc.xeng_lk_from_trino VALUES
 (1, 'trino', CAST(JSON '{"advertiser":"FromTrino","spend":77}' AS VARIANT));
SELECT id, source, CAST(payload['advertiser'] AS varchar) AS advertiser
FROM lakekeeper.ctv_catalog_poc.xeng_lk_from_trino ORDER BY id;

-- PART 2: Trino READS the v3+VARIANT table that DATABRICKS created (notebook cell 6).
-- Run this AFTER the notebook has created xeng_lk_from_dbx.
SELECT id, source, CAST(payload['advertiser'] AS varchar) AS advertiser
FROM lakekeeper.ctv_catalog_poc.xeng_lk_from_dbx ORDER BY id;   -- expect advertiser = 'FromDatabricks'

-- PART 3: cleanup (run from either engine)
DROP TABLE IF EXISTS lakekeeper.ctv_catalog_poc.xeng_lk_from_trino;
DROP TABLE IF EXISTS lakekeeper.ctv_catalog_poc.xeng_lk_from_dbx;
