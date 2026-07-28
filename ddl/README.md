# DDL — persistent table structures (create before running the pipeline)

The new-stack pipeline only **appends/overwrites data**; table structure is created up front by
these DDL scripts. Jobwork/temporary tables are *not* here — they stay runtime-created. Run these
once per environment (idempotent: every statement is `IF NOT EXISTS`).

## Run order

| # | Script | Creates | Owner / writer |
|---|--------|---------|----------------|
| 00 | `00_schemas.sql` | schemas `bronze`, `silver`, `gold`, `reference` | — |
| 01 | `01_bronze_digtial_raw_occurrence_ctv_staging.sql` | `bronze.digtial_raw_occurrence_ctv_staging` (CTV staging landing, append-only) | Python landing (`ingestion/ctv_ingestion.py`) |
| 02 | `02_bronze_digital_raw_occurrence.sql` | `bronze.digital_raw_occurrence` (canonical raw, 42 cols, partitioned) | dbt model `digital_raw_occurrence` (Piece 1) |
| 03 | `03_silver_watermark_control.sql` | `silver.watermark_control` (version + timestamp watermarks, one shared table) | dbt watermark macros |
| 04 | `04_bronze_creative.sql` | `bronze.creative_unique_urls`, `creative_autochaff`, `missing_digital_occurrence_for_summary` | Piece 3 creative push |
| 05 | `05_silver_pieces_3_5.sql` | `silver.creative_dedupe_map`, `digital_staging_occurrence`, `component_coding_translation_hold`, `creative_mapping_translation_hold`, `creative_product_translation_resync_log`, `gold_creative_change_log` | Pieces 4–5 dbt models |
| 06 | `06_gold_creative.sql` | `gold.creative` (106 cols), `creative_first_seen`, `component_coding`, `digital_deployment_chain` (+`_role`, `_mediator`), `digital_spend_availability` | Piece 4 sync-back |
| 07 | `07_gold_occurrence.sql` | `gold.digital_gold_occurrence` (partitioned by `capture_month`) | Piece 5 occurrence gate |

Scripts 04–07 were generated from the Databricks `table_ddl` notebooks (`bronze.py`/`silver.py`/`gold.py`)
by the Spark→Trino mapping in each file's header (STRING→VARCHAR, TIMESTAMP→`TIMESTAMP(6) WITH TIME ZONE`,
VARIANT→VARCHAR, IDENTITY/DEFAULT dropped since `occurrence_id`/`creative_id` come from Postgres
sequences, CLUSTER BY→partitioning/sorted_by). They pre-create the structures the Piece 3–5 models write into.

The **14 reference tables** (`km_preparation_db.*`, `km_preparation_gold_db.*`, `productcentral.*`) are
provisioned by the Option C reference sync (`ingestion/reference_sync.py`) — see `docs/reference_tables.md`.
Three more reference dims used by Pieces 3–5 (`reference.creative_match_type`, `global_market`,
`provider_global_market_map`) are declared as dbt sources but **not yet synced** — their Delta source
path differs from the 14 (needs confirmation before adding to the sync). Postgres read tables
(`creatives.*`, Piece 4) are dbt sources via the Trino `postgres` catalog (`dbt/models/creatives/sources.yml`).

## How to run (on the VM)

Execute against Trino (the `trino` container). From the repo root:

```bash
for f in ddl/0[0-7]_*.sql; do
  echo "== $f =="; docker exec -i trino trino --catalog iceberg -f "/dev/stdin" < "$f"
done
```

Or one at a time, e.g.:

```bash
docker exec -i trino trino -f /dev/stdin < ddl/02_bronze_digital_raw_occurrence.sql
```

Verify:

```bash
docker exec -i trino trino --execute "SHOW TABLES FROM iceberg.bronze"
docker exec -i trino trino --execute "DESCRIBE iceberg.bronze.digital_raw_occurrence"
```

## Notes

- `digital_raw_occurrence` DDL matches the dbt model's `config(properties=...)` (partitioning by
  `capture_month`, sorted by `provider_occurrence_id`), so a `dbt run --full-refresh` recreates it
  identically. Normal `dbt run` appends into the pre-created table.
- Types match the model's `SELECT` casts and the landing Arrow schema exactly — keep them in sync if
  either side changes.
- All tables are Iceberg (`format = 'PARQUET'`) on the Nessie catalog + S3 warehouse.
