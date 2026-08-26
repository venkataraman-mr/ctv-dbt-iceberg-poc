-- ============================================================
-- Catalog PoC — feature tests (run in DBeaver on Trino, once per catalog)
--
-- Replace <CATALOG> with the Trino catalog name for the catalog under test:
--   * polaris      (uses infra/trino/catalog/polaris.properties)
--   * lakekeeper   (uses infra/trino/catalog/lakekeeper.properties)
-- Replace <SCHEMA> with a test namespace (e.g. ctv_catalog_poc).
--
-- Covers the hard/soft requirements that are testable in Trino SQL: v3, VARIANT, DML, views.
-- RBAC and credential vending are validated separately (see docs/catalog_poc_runbook.md Part A §5-6).
-- Run top to bottom; note pass/fail for each numbered block in the results matrix.
-- ============================================================

-- 0. namespace + clean slate
CREATE SCHEMA IF NOT EXISTS <CATALOG>.<SCHEMA>;
DROP TABLE IF EXISTS <CATALOG>.<SCHEMA>.zz_feat_test;

-- 1. v3 CREATE with a VARIANT column  (R1)
CREATE TABLE <CATALOG>.<SCHEMA>.zz_feat_test (
    id      bigint,
    source  varchar,
    payload variant
) WITH (format_version = 3);

-- 2. confirm it is really v3 + the VARIANT column (not silently v2)
SHOW CREATE TABLE <CATALOG>.<SCHEMA>.zz_feat_test;   -- expect format_version = 3, payload variant
DESCRIBE <CATALOG>.<SCHEMA>.zz_feat_test;

-- 3. INSERT VARIANT payloads  (R1)
INSERT INTO <CATALOG>.<SCHEMA>.zz_feat_test VALUES
 (1, 'ctv',  CAST(JSON '{"advertiser":"ACME","spend":123.45,"tags":["ctv","q3"]}' AS VARIANT)),
 (2, 'ctv',  CAST(JSON '{"advertiser":"Globex","nested":{"k":"v"}}' AS VARIANT)),
 (3, 'avod', NULL);

-- 4. READ + extract VARIANT fields  (R1)
SELECT id, source,
       CAST(payload['advertiser'] AS varchar) AS advertiser,
       CAST(payload['spend']      AS double)  AS spend,
       CAST(payload             AS json)      AS payload_json
FROM <CATALOG>.<SCHEMA>.zz_feat_test ORDER BY id;

-- 5. Row-level DML on v3 (UPDATE / DELETE / MERGE)
UPDATE <CATALOG>.<SCHEMA>.zz_feat_test
   SET payload = CAST(JSON '{"advertiser":"ACME-2","spend":200}' AS VARIANT) WHERE id = 1;
DELETE FROM <CATALOG>.<SCHEMA>.zz_feat_test WHERE id = 2;
MERGE INTO <CATALOG>.<SCHEMA>.zz_feat_test t
USING (VALUES (4, 'avod', CAST(JSON '{"advertiser":"Initech"}' AS VARIANT))) AS s(id, source, payload)
   ON t.id = s.id
WHEN NOT MATCHED THEN INSERT (id, source, payload) VALUES (s.id, s.source, s.payload);
SELECT id, source, CAST(payload['advertiser'] AS varchar) AS advertiser
FROM <CATALOG>.<SCHEMA>.zz_feat_test ORDER BY id;   -- expect id 1 (ACME-2), 3 (null), 4 (Initech)

-- 6. VIEWS  (soft requirement)
CREATE OR REPLACE VIEW <CATALOG>.<SCHEMA>.zz_feat_view AS
SELECT id, CAST(payload['advertiser'] AS varchar) AS advertiser
FROM <CATALOG>.<SCHEMA>.zz_feat_test;
SELECT * FROM <CATALOG>.<SCHEMA>.zz_feat_view ORDER BY id;

-- 7. cleanup
DROP VIEW  IF EXISTS <CATALOG>.<SCHEMA>.zz_feat_view;
DROP TABLE IF EXISTS <CATALOG>.<SCHEMA>.zz_feat_test;

-- ============================================================
-- Interpretation:
--  1-4 pass  -> catalog serves v3 + VARIANT over REST (the make-or-break R1).
--  5 passes  -> row-level DML on v3 works.
--  6 passes  -> Iceberg views work (soft req).
--  Trino connected with NO S3 keys (vended-credentials-enabled=true) -> credential vending works.
--  RBAC: validate per the catalog's model (Polaris grants / Lakekeeper OpenFGA+OPA) -- see runbook.
-- ============================================================
