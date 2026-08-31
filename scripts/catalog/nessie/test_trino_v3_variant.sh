#!/usr/bin/env bash
# Trino v3 + VARIANT capability test (run on the VM).
#
# Tests Trino's OWN round-trip of an Iceberg v3 table with a VARIANT column, via the native
# `iceberg` (Nessie) catalog -- the same catalog that holds your v3 clone. In Trino you declare a
# VARIANT column as a JSON column with WITH (format_version = 3); Trino maps JSON <-> Iceberg VARIANT
# (experimental in Trino 483).
#
# SCOPE: this is a WRITER-SIDE test only. Even if every step passes, it does NOT make v3+VARIANT
# cross-engine readable -- Nessie's REST catalog doesn't serve v3, and Databricks can't federate to
# Nessie. Interpret results as "can Trino itself do v3+VARIANT", not "cross-cloud works".
#
# Usage:  bash scripts/catalog/nessie/test_trino_v3_variant.sh
# Each step prints a PASS/label and its output or error; failures don't stop the run (so you see the
# whole matrix). Cleanup drops the scratch table at the end.

set +e
TBL="iceberg.bronze.zz_v3_variant_test"
run() { echo; echo "==== $1 ===="; shift; docker exec -i trino trino --execute "$1" 2>&1; echo "(exit $?)"; }

echo "##### Trino version / iceberg connector #####"
docker exec -i trino trino --execute "SELECT version()" 2>&1

# 0) clean slate
run "0. drop if exists (clean slate)" "DROP TABLE IF EXISTS ${TBL}"

# 1) CREATE v3 table with a VARIANT (JSON) column
run "1. CREATE v3 table with VARIANT(JSON) column" \
"CREATE TABLE ${TBL} (id bigint, source varchar, payload json) WITH (format_version = 3)"

# 2) Confirm it is really v3 + the column type
run "2a. SHOW CREATE TABLE (expect format_version = 3, payload json)" \
"SHOW CREATE TABLE ${TBL}"
run "2b. column types" \
"DESCRIBE ${TBL}"

# 3) INSERT VARIANT/JSON payloads
run "3. INSERT JSON payloads (-> Iceberg VARIANT)" \
"INSERT INTO ${TBL} VALUES
 (1, 'ctv',  JSON '{\"advertiser\":\"ACME\",\"spend\":123.45,\"tags\":[\"ctv\",\"q3\"]}'),
 (2, 'ctv',  JSON '{\"advertiser\":\"Globex\",\"nested\":{\"k\":\"v\"}}'),
 (3, 'avod', NULL)"

# 4) READ back + extract fields from the VARIANT
run "4. SELECT + extract JSON/VARIANT fields" \
"SELECT id, source,
        json_extract_scalar(payload, '\$.advertiser') AS advertiser,
        json_extract_scalar(payload, '\$.spend')       AS spend,
        json_extract(payload, '\$.nested')             AS nested,
        payload
 FROM ${TBL} ORDER BY id"

# 5) v3 row-level DML (deletion vectors) -- UPDATE then DELETE
run "5a. UPDATE a VARIANT value (v3 merge-on-read)" \
"UPDATE ${TBL} SET payload = JSON '{\"advertiser\":\"ACME-2\",\"spend\":200}' WHERE id = 1"
run "5b. DELETE a row (v3 merge-on-read)" \
"DELETE FROM ${TBL} WHERE id = 2"
run "5c. read back after DML" \
"SELECT id, source, json_extract_scalar(payload,'\$.advertiser') AS advertiser FROM ${TBL} ORDER BY id"

# 6) snapshots / history (native Nessie shows a single snapshot by design)
run "6. history" "SELECT made_current_at, snapshot_id FROM \"${TBL}\$history\" ORDER BY made_current_at"

# 7) OPTIONAL cross-check: does the REST catalog reject v3? (expect failure -> confirms Nessie REST gap)
run "7. (cross-check) CREATE v3 via REST catalog -- EXPECTED TO FAIL" \
"CREATE TABLE iceberg_rest.bronze.zz_v3_variant_rest (id bigint, payload json) WITH (format_version = 3)"
docker exec -i trino trino --execute "DROP TABLE IF EXISTS iceberg_rest.bronze.zz_v3_variant_rest" >/dev/null 2>&1

# 8) cleanup
run "8. cleanup (drop scratch table)" "DROP TABLE IF EXISTS ${TBL}"

echo
echo "##### DONE #####"
echo "Interpretation:"
echo " - Steps 1-5 PASS  -> Trino can create/write/read/mutate v3+VARIANT on the NATIVE Nessie catalog."
echo " - Step 4 extracts fields -> VARIANT is queryable via Trino JSON functions."
echo " - Step 5 PASS -> v3 row-level DML (deletion vectors) works in Trino."
echo " - Step 7 FAILS -> confirms Nessie's REST catalog does not serve v3 (the cross-engine blocker)."
echo " - Reminder: writer-side only. Cross-cloud read still blocked by Nessie-REST(no v3) + Databricks(no Nessie federation)."
