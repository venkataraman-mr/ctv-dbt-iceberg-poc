# Piece 1 — CTV ingestion (landing + staging → raw)

This is the first pipeline piece: land raw CTV occurrence files into a bronze **staging** table,
then transform them into the canonical **`digital_raw_occurrence`** bronze table. It ports the
legacy Databricks flow (`BZ2FileToStagingCtvIngestion` → `StagingToRawOccurrenceBisCtvUS`) onto the
open-source stack, mirroring the legacy tables, column names, filters and idempotency exactly,
with only the deliberate changes noted below.

## Shape of the flow

A **Python landing step** (`ingestion/ctv_ingestion.py`) reads `.bz2` files from S3, decompresses
and parses them, and appends one row per JSON occurrence object into the append-only staging table
`iceberg.bronze.digtial_raw_occurrence_ctv_staging` (the legacy name is kept verbatim, including the
historical `digtial` misspelling). A **dbt-trino incremental model**
(`dbt/models/bronze/digital_raw_occurrence.sql`) then parses each staged JSON, dedups to the latest
row per occurrence id, keeps only the video / `video/mp4` / whitelisted-publisher rows, and appends
the canonical 42-column occurrence to `iceberg.bronze.digital_raw_occurrence`. Downstream pieces
`ref()` the raw table, not a source.

Both persistent tables are **pre-created by DDL** (`ddl/01_*.sql`, `ddl/02_*.sql`) before the
pipeline runs — the pipeline only appends. The landing step loads (never creates) the staging table
and fails fast with a pointer to the DDL if it is missing; the dbt model appends into the
pre-created raw table (its `config` partitioning/sort matches the DDL, so a `--full-refresh`
recreates it identically). See `ddl/README.md`.

## Source is S3 (not the Azure queue)

For the PoC the source of truth is S3, not the Azure Storage Queue the legacy job drains. Sample
`.bz2` files are dropped under `s3://dataplatformpoc-venketa/landing/ctv/ingestion/`; the landing
step moves each processed file to `s3://dataplatformpoc-venketa/landing/ctv/archive/<YYYY-MM-DD>/`.
These prefixes are configurable (`CTV_LANDING_INGESTION` / `CTV_LANDING_ARCHIVE` in `.env`, defaults
in `ingestion/config.py`) and are kept separate from the Iceberg `warehouse/` prefix so raw source
files never mingle with table data. Landing appends first, then archives, so a crash never loses a
source file — at worst a file is reprocessed and the staging → raw anti-join dedups it. Automating
the Azure-blob → S3 copy (so `ingestion/` fills when a new blob is dropped) is deferred future work.

The `.bz2` decompresses to a single JSON object, a JSON array, or JSONL; `extract_json_objects`
tries those in the same order as the legacy `AzureBZ2JsonProcessor`. Each object becomes a staging
row with `json_data` (the object's JSON text — VARIANT → string), `record_index`, `source_filename`,
`blob_name`, and `created_timestamp` (UTC, no zone).

## `creative_url_hash`: precomputed at landing, exact to Spark

`creative_url_hash` is the join key used across every downstream piece, so it must equal the legacy
Spark value exactly:

```
CAST(xxhash64(CONCAT(SPLIT_PART(creative.url,'.',1),'.',
                     SPLIT_PART(creative.url,'.',2),'.',
                     SPLIT_PART(creative.url,'.',-1))) AS BIGINT)
```

Spark's `xxhash64` is standard XXH64 over the UTF-8 bytes with **seed 42**; Trino's built-in
`xxhash64` uses **seed 0** (and returns varbinary), so it cannot reproduce the value in SQL. We
therefore **precompute the hash in the Python landing step** (`ingestion/common/spark_hash.py`,
using `xxhash.xxh64(..., seed=42)` reinterpreted as a signed int64) and carry it as a column on the
staging table; the dbt model passes it straight through. This adds one column to the legacy
staging schema — the single, deliberate deviation from "mirror legacy exactly", forced by the
exact-hash requirement.

Parity was verified against **real PySpark 3.5** for the exact expression above (including
`SPLIT_PART` edge cases — negative index, out-of-range → empty string): every test URL matched.

## The staging → raw model

The model reproduces `StagingToRawOccurrenceBisCtvUS` in Trino SQL:

- **Parse** each `json_data` with `json_parse` / `json_extract_scalar`, mapping to the same 42
  canonical columns and types as the legacy DDL (`provider_code = 'AVOD BISCTV'`, `capture_month =
  yyyymm`, `creative_duration = duration/1000`, `daisy_chain = unifiedChain` JSON, etc.).
- **Dedup** with `row_number() over (partition by occurrence.id order by <staging load ts> desc)`,
  keeping `r_num = 1` — latest-landed row per occurrence id wins (the staging `created_timestamp`
  stands in for the legacy CDF `_commit_timestamp`).
- **Filter** to `creative_type = 'video'` AND `creative_mime_type = 'video/mp4'` AND `publisher_id`
  in the 11-publisher whitelist — identical to legacy.
- **Idempotency** is the legacy `LEFT ANTI JOIN` on `(provider_occurrence_id, capture_month)`
  against the target, applied on incremental runs only.
- The Iceberg table is partitioned by `capture_month` and sorted by `provider_occurrence_id`
  (the legacy `CLUSTER BY`).

Two fidelity notes: `raw_json` stores the original occurrence JSON text (legacy stored a
schema-normalized re-serialization — the raw text is a closer record of the source); and the model
assumes the **Trino session time zone is UTC** so `captureDate` parses to UTC as it did in Spark.

## Known caveat — incremental scan bound

With no Trino change-data-feed, an incremental run rescans current staging (the anti-join keeps the
result correct, never duplicated). For the PoC's sample volumes this is fine. To bound it at scale,
add a `created_timestamp` watermark on staging (the `watermark_control` table + macros are already
in the repo) or truncate staging after a successful raw load — a deliberate next step, not wired yet.

## Run it (on the VM)

```bash
# 1. rebuild the ingestion image (adds xxhash) and bring up the stack
docker compose build ingestion && docker compose up -d

# 2. create the persistent table structures ONCE (schemas + staging + raw + watermark_control)
for f in ddl/00_schemas.sql ddl/01_*.sql ddl/02_*.sql ddl/03_*.sql; do
  echo "== $f =="; docker exec -i trino trino -f /dev/stdin < "$f"; done

# 3. copy sample .bz2 files to the ingestion prefix (from a machine with AWS creds)
aws s3 cp ./samples/ s3://dataplatformpoc-venketa/landing/ctv/ingestion/ --recursive --exclude "*" --include "*.bz2"

# 4. land: S3 .bz2 -> bronze staging (appends to the pre-created table), then archive
docker compose exec ingestion python -m ingestion.ctv_ingestion

# 5. staging -> raw (appends into the pre-created iceberg.bronze.digital_raw_occurrence)
docker compose exec dbt dbt run --select digital_raw_occurrence

# 6. verify
docker exec -i trino trino --execute "SELECT count(*) FROM iceberg.bronze.digtial_raw_occurrence_ctv_staging"
docker exec -i trino trino --execute "SELECT count(*), min(capture_month), max(capture_month) FROM iceberg.bronze.digital_raw_occurrence"
docker exec -i trino trino --execute "SELECT provider_occurrence_id, capture_month, creative_url_hash FROM iceberg.bronze.digital_raw_occurrence LIMIT 5"
```

Once a real sample `.bz2` is available, confirm the occurrence JSON matches the mapped paths (esp.
the `captureDate` string format the parser handles) before scaling up.
