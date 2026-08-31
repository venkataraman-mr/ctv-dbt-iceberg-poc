-- ============================================================
-- Catalog PoC — feature tests for POLARIS  (catalog=polaris, schema=ctv_catalog_poc)
-- Generated from scripts/catalog/catalog_feature_tests.sql (the <CATALOG>/<SCHEMA> template).
-- Run top to bottom in DBeaver (on the Trino connection); note pass/fail per block.
--
-- Covers the hard/soft requirements testable in Trino SQL: v3, VARIANT, DML, views.
-- RBAC + credential vending are validated separately (see docs/catalog/catalog_poc_runbook.md Part A §5-6).
-- ============================================================

-- 0. namespace + clean slate
CREATE SCHEMA IF NOT EXISTS polaris.ctv_catalog_poc;
DROP TABLE IF EXISTS polaris.ctv_catalog_poc.zz_feat_test;

-- 1. v3 CREATE with a VARIANT column  (R1 — the make-or-break)
CREATE TABLE polaris.ctv_catalog_poc.zz_feat_test (
    id      bigint,
    source  varchar,
    payload variant
) WITH (format_version = 3);

-- 2. confirm it is really v3 + the VARIANT column (not silently v2)
SHOW CREATE TABLE polaris.ctv_catalog_poc.zz_feat_test;   -- expect format_version = 3, payload variant
DESCRIBE polaris.ctv_catalog_poc.zz_feat_test;

-- 3. INSERT VARIANT payloads  (R1)
INSERT INTO polaris.ctv_catalog_poc.zz_feat_test VALUES
 (1, 'ctv',  CAST(JSON '{"advertiser":"ACME","spend":123.45,"tags":["ctv","q3"]}' AS VARIANT)),
 (2, 'ctv',  CAST(JSON '{"advertiser":"Globex","nested":{"k":"v"}}' AS VARIANT)),
 (3, 'avod', NULL);

-- 4. READ + extract VARIANT fields  (R1)
SELECT id, source,
       CAST(payload['advertiser'] AS varchar) AS advertiser,
       CAST(payload['spend']      AS double)  AS spend,
       CAST(payload             AS json)      AS payload_json
FROM polaris.ctv_catalog_poc.zz_feat_test ORDER BY id;

-- 5. Row-level DML on v3 (UPDATE / DELETE / MERGE)
UPDATE polaris.ctv_catalog_poc.zz_feat_test
   SET payload = CAST(JSON '{"advertiser":"ACME-2","spend":200}' AS VARIANT) WHERE id = 1;
DELETE FROM polaris.ctv_catalog_poc.zz_feat_test WHERE id = 2;
MERGE INTO polaris.ctv_catalog_poc.zz_feat_test t
USING (VALUES (4, 'avod', CAST(JSON '{"advertiser":"Initech"}' AS VARIANT))) AS s(id, source, payload)
   ON t.id = s.id
WHEN NOT MATCHED THEN INSERT (id, source, payload) VALUES (s.id, s.source, s.payload);
SELECT id, source, CAST(payload['advertiser'] AS varchar) AS advertiser
FROM polaris.ctv_catalog_poc.zz_feat_test ORDER BY id;   -- expect id 1 (ACME-2), 3 (null), 4 (Initech)

-- 6. VIEWS  (soft requirement)
CREATE OR REPLACE VIEW polaris.ctv_catalog_poc.zz_feat_view AS
SELECT id, CAST(payload['advertiser'] AS varchar) AS advertiser
FROM polaris.ctv_catalog_poc.zz_feat_test;
SELECT * FROM polaris.ctv_catalog_poc.zz_feat_view ORDER BY id;

-- 7. cleanup
DROP VIEW  IF EXISTS polaris.ctv_catalog_poc.zz_feat_view;
DROP TABLE IF EXISTS polaris.ctv_catalog_poc.zz_feat_test;

-- ============================================================
-- Interpretation:
--  1-4 pass  -> Polaris serves v3 + VARIANT over REST (the make-or-break R1; Nessie failed here).
--  5 passes  -> row-level DML on v3 works.
--  6 passes  -> Iceberg views work (soft req).
--  Trino connected with NO S3 keys (vended-credentials-enabled=true) -> credential vending works.
-- ============================================================
