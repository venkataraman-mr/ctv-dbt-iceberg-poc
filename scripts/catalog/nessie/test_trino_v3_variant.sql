-- ============================================================
-- Trino v3 + VARIANT capability test  (run in DBeaver on the Trino connection)
--
-- This Trino build has a FIRST-CLASS `variant` column type (not the JSON->VARIANT mapping some docs show).
--   * write a VARIANT value:  CAST(JSON '{...}' AS VARIANT)
--   * read a field:           payload['key']  (returns VARIANT) then CAST(... AS <type>)
--   * show whole value:       CAST(payload AS json)
-- VARIANT requires format_version = 3.
--
-- SCOPE: WRITER-SIDE only. Passing here does NOT make v3+VARIANT cross-engine readable --
--   Nessie's REST catalog doesn't serve v3, and Databricks can't federate to Nessie.
--
-- Run 0 -> 8. Step 7 is EXPECTED TO FAIL. Step 8 cleans up.
-- ============================================================

-- 0. clean slate
DROP TABLE IF EXISTS iceberg.bronze.zz_v3_variant_test;

-- 1. CREATE a v3 table with a VARIANT column
CREATE TABLE iceberg.bronze.zz_v3_variant_test (
    id      bigint,
    source  varchar,
    payload variant
) WITH (format_version = 3);

-- 2a. confirm v3 + column type  (expect: format_version = 3, payload variant)
SHOW CREATE TABLE iceberg.bronze.zz_v3_variant_test;

-- 2b. column types
DESCRIBE iceberg.bronze.zz_v3_variant_test;

-- 3. INSERT VARIANT payloads via CAST(JSON ... AS VARIANT): nested object, array, and a NULL
INSERT INTO iceberg.bronze.zz_v3_variant_test VALUES
 (1, 'ctv',  CAST(JSON '{"advertiser":"ACME","spend":123.45,"tags":["ctv","q3"]}' AS VARIANT)),
 (2, 'ctv',  CAST(JSON '{"advertiser":"Globex","nested":{"k":"v"}}' AS VARIANT)),
 (3, 'avod', NULL);

-- 4. READ back + extract fields from the VARIANT (subscript [] then CAST)
--    NOTE: json_extract_scalar(payload, ...) does NOT work on a VARIANT column -- it wants json/varchar.
--    Use the subscript form below, or first cast the VARIANT to json:
--      json_extract_scalar(CAST(payload AS json), '$.advertiser')
SELECT id, source,
       CAST(payload['advertiser'] AS varchar) AS advertiser,
       CAST(payload['spend']      AS double)  AS spend,
       CAST(payload['nested']     AS json)    AS nested,
       CAST(payload              AS json)     AS payload_json
FROM iceberg.bronze.zz_v3_variant_test
ORDER BY id;

-- 5a. v3 row-level UPDATE (deletion vectors / merge-on-read)
UPDATE iceberg.bronze.zz_v3_variant_test
SET payload = CAST(JSON '{"advertiser":"ACME-2","spend":200}' AS VARIANT)
WHERE id = 1;

-- 5b. v3 row-level DELETE
DELETE FROM iceberg.bronze.zz_v3_variant_test WHERE id = 2;

-- 5c. read back after DML  (expect id=1 advertiser ACME-2, id=2 gone, id=3 null)
SELECT id, source, CAST(payload['advertiser'] AS varchar) AS advertiser
FROM iceberg.bronze.zz_v3_variant_test
ORDER BY id;

-- 6. history  (native Nessie shows a single snapshot by design -- expect one row)
SELECT made_current_at, snapshot_id
FROM iceberg.bronze."zz_v3_variant_test$history"
ORDER BY made_current_at;

-- 7. CROSS-CHECK -- create the same v3 table via the REST catalog. EXPECTED TO FAIL.
--    CONFIRMED (2026-08-24): fails as expected -> Nessie's REST catalog does NOT serve v3/VARIANT.
--    This is the cross-engine blocker: Trino can do v3+VARIANT via the NATIVE Nessie catalog, but not
--    over REST -- and REST is the only way other engines (incl. Databricks) would reach these tables.
CREATE TABLE iceberg_rest.bronze.zz_v3_variant_rest (
    id      bigint,
    payload variant
) WITH (format_version = 3);
-- if it unexpectedly succeeds, drop it:
-- DROP TABLE IF EXISTS iceberg_rest.bronze.zz_v3_variant_rest;

-- 8. cleanup
DROP TABLE IF EXISTS iceberg.bronze.zz_v3_variant_test;
