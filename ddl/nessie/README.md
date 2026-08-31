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
| 06 | `06_gold_creative.sql` | `gold.creative` (107 cols), `creative_first_seen`, `component_coding`, `digital_deployment_chain` (+`_role`, `_mediator`), `digital_spend_availability` | Piece 4 sync-back |
| 07 | `07_gold_occurrence.sql` | `gold.digital_gold_occurrence` (partitioned by `capture_month`) | Piece 5 occurrence gate |
| 08 | `08_silver_watermark_control_piece4.sql` | 10 Piece-4 sync-back **timestamp** watermarks in `silver.watermark_control` (idempotent; 1900-01-01 base) | Piece 4 sync-back dbt models |
| 09 | `09_silver_watermark_control_piece5.sql` | 2 Piece-5 gold-occurrence watermarks — `DIGITAL_RAW_OCC_TO_GOLD_OCC` (**version**, Half A) + `DIGITAL_CRTV_CHANGES_TO_GOLD_OCC` (**timestamp**, Half B) | Piece 5 gold-occurrence dbt models |

Note (ddl/nessie/06): two columns were added to match prod (the sync writes them). `gold.creative_first_seen` gained
`provider_campaign_landing_page`; `gold.creative` gained `first_seen_provider_campaign_landing_page` (bringing it
to 107 cols). Already-created VM tables need the metadata-only retrofits:
`ALTER TABLE iceberg.gold.creative_first_seen ADD COLUMN provider_campaign_landing_page VARCHAR;` and
`ALTER TABLE iceberg.gold.creative ADD COLUMN first_seen_provider_campaign_landing_page VARCHAR;`

Scripts 04–07 were generated from the Databricks `table_ddl` notebooks (`bronze.py`/`silver.py`/`gold.py`)
by the Spark→Trino mapping in each file's header (STRING→VARCHAR, TIMESTAMP→`TIMESTAMP(6) WITH TIME ZONE`,
VARIANT→VARCHAR, IDENTITY/DEFAULT dropped since `occurrence_id`/`creative_id` come from Postgres
sequences, CLUSTER BY→partitioning/sorted_by). They pre-create the structures the Piece 3–5 models write into.

**Postgres side (Piece 3):** `ddl/postgres/nessie/piece3_tempwork_ctv_poc.sql` (run once on prod Postgres via a
SQL client — `psql` is not on the VM) creates the creative-push **clone** objects in the `tempwork`
schema with a `_ctv_poc` suffix — clone tables (`creative_staging_ctv_poc`, `creative_first_seen_ctv_poc`,
`creative_occurrence_summary_ctv_poc`), the three cloned stored procs (insert / first-seen update /
occ-summary upsert), the `creative_id_seq_ctv_poc` sequence, and the id-block table + reservation proc.
Real `creatives.*` are untouched. Requires membership in `tempwork_admin_role` (a DBA event trigger
reassigns new `tempwork` tables to it: `GRANT tempwork_admin_role TO <login>;`). See
`docs/pipeline/ctv_creative_push.md`. **Iceberg changes for Job B:** `bronze.missing_digital_occurrence_for_summary`
gains `capture_timestamp` (ddl/nessie/04); `silver.watermark_control` is **partitioned by `watermark_name`**
(ddl/nessie/03) so concurrent watermarked jobs don't collide (retrofit = recreate preserving rows — see the
ddl/nessie/03 comment). Watermark **seed rows** for Piece 3 Job A/B (`DIGITAL_RAW_OCC_TO_CRTV_STAGING`,
`DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE`, `DIGITAL_RAW_OCC_SUMMARY_PSQL` — all version-based) are in
`ddl/nessie/03` — run once before the first Job A/B run.

**Postgres side (Piece 4 seeding prerequisite):** `ddl/postgres/nessie/piece4_seed_tempwork_ctv_poc.sql`
(run once on prod Postgres via a SQL client) clones the rest of the creative table family the Piece 4
sync-back proc reads — `creative`, `creative_product`, `creative_celebrity`, `creative_competitor`,
`creative_dedupe_map`, `creative_classification_engine_holding`, `ml_results.creative_ai_classification_staging_vx0`,
`creatives.component_coding` (task-4 component sync), plus a `watermark_control` clone — and adds a two-mode seeding proc,
`tempwork.sp_seed_creative_clones_ctv_poc(p_mode)`, that copies production data into them keyed on
`creative_url_hash`. Run it adhoc: `CALL tempwork.sp_seed_creative_clones_ctv_poc('ALL');` (or `'NEW'` /
`'UPDATE'`). Mode 1 (new inserts) is watermarked off clone staging; Mode 2 (creative updates) off prod
`creative.updated_timestamp` and also catches parent-attribute changes; both marks live in the
`watermark_control_ctv_poc` clone. Anchor creatives take the reserved PoC id; missing dedup parents keep
their prod id (the 26 B reserved boundary separates the two). Write strategy: `creative`/`staging_vx0`/`holding`
upsert on `creative_id`; the multi-row dependents + external-parent `first_seen`/`occ_summary` are
delete-in-scope + insert. Every load is scope-gated to our CTV clones + their related parents (dedupe_map
joined to the run's `_seed_idmap`); descriptor fields come from clone staging/first_seen for our creatives,
prod for external parents. Real `creatives.*`/`ml_results.*` are read-only; `creative_archive` and the
`reference.*`/`config.*`/`productcentral.*` lookups are not cloned. **Mode 1 validated on the clones; Mode 2
pending the first daily-ingestion run.** See `docs/pipeline/ctv_creative_seed.md`.

**Postgres side (Piece 4 sync-back):** `ddl/postgres/nessie/piece4_sync_procs_ctv_poc.sql` (run once) clones the two
prod `get_changes` procs — `sp_dbx_creative_get_changes_for_databricks` and
`sp_dbx_component_get_changes_for_databricks` — retargeted to the `tempwork *_ctv_poc` clones (jobwork scratch
→ tempwork; reference/config/`template` + read-only `creative_archive` kept as prod reads). They build the
`*_forsync_tmp_ctv_poc` payloads the dbt sync models read. Archive is inert (the dbt caller passes a future
`ca_flag`). Scratch index names are `_ctv_poc`-suffixed. See `docs/pipeline/ctv_creative_sync_plan.md`.

**Postgres side (Piece 5 occurrence-id sequence):** `ddl/postgres/nessie/piece5_occ_id_seq_ctv_poc.sql` (run once)
creates `tempwork.occurrence_id_seq_ctv_poc` (**START 75,000,000,000** per Venkat — occurrence_id must be a
real sequence, not a hash), the id-block table `tempwork.occurrence_id_block_ctv_poc`, and the reservation proc
`tempwork.sp_reserve_occurrence_ids_ctv_poc(p_n)`. The `digital_occ_gold` writer counts the run's Not-Hold rows,
calls the proc through the `reserve_occurrence_ids(n)` dbt macro (Trino `system.execute` + read the block back via
`system.query`), and assigns `block_start + row_number()-1` — the same block-reservation pattern as
`creative_id_seq_ctv_poc`. Requires `tempwork_admin_role`. `deployment_chain_id` (lower stakes, gate only
joins/existence-checks it) stays a deterministic `from_big_endian_64(xxhash64(md5))` surrogate — no sequence.
See `docs/pipeline/ctv_occurrence_gold_plan.md`.

The **14 reference tables** (`km_preparation_db.*`, `km_preparation_gold_db.*`, `productcentral.*`) are
provisioned by the Option C reference sync (`ingestion/reference_sync.py`) — see `docs/pipeline/reference_tables.md`.
Three more reference dims used by Pieces 3–5 (`reference.creative_match_type`, `global_market`,
`provider_global_market_map`) are declared as dbt sources but **not yet synced** — their Delta source
path differs from the 14 (needs confirmation before adding to the sync). Postgres read tables
(`creatives.*`, Piece 4) are dbt sources via the Trino `postgres` catalog (`dbt/models/creatives/sources.yml`).

## How to run (on the VM)

Execute against Trino (the `trino` container). From the repo root:

```bash
for f in ddl/nessie/0[0-7]_*.sql; do
  echo "== $f =="; docker exec -i trino trino --catalog iceberg -f "/dev/stdin" < "$f"
done
```

Or one at a time, e.g.:

```bash
docker exec -i trino trino -f /dev/stdin < ddl/nessie/02_bronze_digital_raw_occurrence.sql
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
