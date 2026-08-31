-- Schemas (Iceberg namespaces) for the CTV pipeline. Idempotent.
-- Run once before creating tables. reference/ already exists (created by the Option C sync);
-- included here with IF NOT EXISTS so a fresh environment is fully provisioned in one place.
CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
CREATE SCHEMA IF NOT EXISTS iceberg.silver;
CREATE SCHEMA IF NOT EXISTS iceberg.gold;
CREATE SCHEMA IF NOT EXISTS iceberg.reference;
