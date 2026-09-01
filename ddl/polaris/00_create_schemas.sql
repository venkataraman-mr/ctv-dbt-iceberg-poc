-- Schemas (Iceberg namespaces) for the CTV pipeline on the POLARIS catalog. Idempotent.
-- Run once before creating tables:  docker exec -i trino trino -f /dev/stdin < ddl/polaris/00_create_schemas.sql
--
-- Mirrors ddl/nessie/00 but on catalog `polaris` (not `iceberg`), and creates ALL schemas the
-- pipeline + sources touch up front (the reference/spend/km_* schemas are otherwise auto-created by
-- the reference sync; listed here with IF NOT EXISTS so a fresh Polaris env is fully provisioned in
-- one place). Postgres schemas (creatives / vx2_taxonomy / tempwork) are NOT Iceberg namespaces —
-- they live in the Trino `postgres` catalog and are created by the Postgres DDL, not here.

-- pipeline write layers
CREATE SCHEMA IF NOT EXISTS polaris.bronze;
CREATE SCHEMA IF NOT EXISTS polaris.silver;
CREATE SCHEMA IF NOT EXISTS polaris.gold;

-- reference / lookup layers (synced from Azure Delta -> Iceberg; read-only sources)
CREATE SCHEMA IF NOT EXISTS polaris.reference;
CREATE SCHEMA IF NOT EXISTS polaris.km_preparation_db;
CREATE SCHEMA IF NOT EXISTS polaris.km_preparation_gold_db;
CREATE SCHEMA IF NOT EXISTS polaris.productcentral;
CREATE SCHEMA IF NOT EXISTS polaris.spend;
