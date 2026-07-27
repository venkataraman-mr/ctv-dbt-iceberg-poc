# DDL — persistent table structures (create before running the pipeline)

The new-stack pipeline only **appends/overwrites data**; table structure is created up front by
these DDL scripts. Jobwork/temporary tables are *not* here — they stay runtime-created. Run these
once per environment (idempotent: every statement is `IF NOT EXISTS`).

## Run order

| # | Script | Creates | Owner / writer |
|---|--------|---------|----------------|
| 00 | `00_schemas.sql` | schemas `bronze`, `silver`, `gold`, `reference` | — |
| 01 | `01_bronze_digtial_raw_occurrence_ctv_staging.sql` | `bronze.digtial_raw_occurrence_ctv_staging` (CTV staging landing, append-only) | Python landing (`ingestion/ctv_ingestion.py`) |
| 02 | `02_bronze_digital_raw_occurrence.sql` | `bronze.digital_raw_occurrence` (canonical raw, 42 cols, partitioned) | dbt model `digital_raw_occurrence` |
| 03 | `03_silver_watermark_control.sql` | `silver.watermark_control` (version + timestamp watermarks, one shared table) | dbt watermark macros (from Piece 2 on) |

The **14 reference tables** (`reference.*`) are already provisioned by the Option C reference sync
(`ingestion/reference_sync.py`) — see `docs/reference_tables.md`. Tables for Pieces 2–5 will be
added here as each piece is designed.

## How to run (on the VM)

Execute against Trino (the `trino` container). From the repo root:

```bash
for f in ddl/00_schemas.sql ddl/01_*.sql ddl/02_*.sql ddl/03_*.sql; do
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
