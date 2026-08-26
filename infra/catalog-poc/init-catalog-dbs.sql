-- Runs once on first Postgres init (mounted into /docker-entrypoint-initdb.d/).
-- POSTGRES_DB already created the `polaris` database; add the Lakekeeper one here so a single
-- Postgres container serves both catalogs (isolated databases).
CREATE DATABASE lakekeeper;
