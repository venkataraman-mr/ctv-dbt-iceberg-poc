#!/usr/bin/env bash
# Two-engine smoke test: prove Trino and PyIceberg share ONE Nessie catalog on S3.
set -euo pipefail
cd "$(dirname "$0")/.."          # run from repo root (where docker-compose.yml lives)

echo "== 1. services up =="
docker compose ps

echo "== waiting for Trino to accept queries (up to ~2.5 min) =="
for i in $(seq 1 30); do
  if docker exec trino trino --execute "SELECT 1" >/dev/null 2>&1; then echo "Trino ready"; break; fi
  echo "  ...waiting ($i/30)"; sleep 5
done

echo "== 2. Trino: create schema + table on S3, insert, read =="
docker exec -i trino trino <<'SQL'
CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
CREATE TABLE IF NOT EXISTS iceberg.bronze.smoke (id BIGINT, note VARCHAR);
INSERT INTO iceberg.bronze.smoke VALUES (1, 'written-by-trino');
SELECT * FROM iceberg.bronze.smoke;
SQL

echo "== 3. PyIceberg: append to the SAME table via Nessie =="
docker exec -i ingestion python - <<'PY'
import pyarrow as pa
from ingestion.common.catalog import get_catalog
t = get_catalog().load_table(("bronze", "smoke"))
t.append(pa.table({"id": [2], "note": ["written-by-pyiceberg"]}))
print("pyiceberg rows now:", t.scan().to_arrow().num_rows)
PY

echo "== 4. Trino reads PyIceberg's row (thesis proven) =="
docker exec -i trino trino --execute "SELECT * FROM iceberg.bronze.smoke ORDER BY id;"

echo "== 5. dbt connectivity =="
docker compose exec dbt dbt debug || true

echo "SMOKE TEST DONE — verify objects landed under s3://dataplatformpoc-venketa/warehouse/"
