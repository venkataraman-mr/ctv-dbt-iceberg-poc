# Runbook — CTV dbt+Iceberg PoC

> This is the **infra / stand-up + per-piece build** runbook (VM setup, foundation, each piece's setup &
> validation history). For the consolidated **daily end-to-end pipeline run** (reference sync → ingestion →
> Pieces 1–5, in order, with all run + verify commands), see **`docs/ctv_daily_runbook.md`**.

## 0. VM prerequisites & setup

Follow **`scripts/vm_setup.md`** — a stage-by-stage guide (get repo → disk check → install
Docker/Compose → fill `.env` → build → start → per-service checks → smoke test), with
**disk-space checks at each heavy stage**. Run it one stage at a time so any error is caught
where it happens.

Quick reference — the only required host install is **Docker + Compose v2** (everything else is
containerized: Nessie, Trino, dbt, PyIceberg, delta-rs, DuckDB, Java). Amazon Linux 2023:
```bash
sudo dnf install -y docker git          # git optional (repo push); make optional for the Makefile
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user        # log out/in so the docker group applies
# Docker Compose v2 plugin:
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```
Verify: `docker --version` · `docker compose version` · `docker run hello-world`.

- **Already present:** AWS CLI + `~/.aws` credentials (containers read `AWS_*` from `.env`).
- **Not needed on the host:** Python, dbt, Trino, Nessie, PyIceberg, delta-rs, DuckDB, Java.
- **Confirm (not installs):** outbound internet for image pulls (ghcr.io, Docker Hub); disk
  headroom (`df -h` — Trino + Python images are a few GB); and create the `NESSIE_DATA_DIR`
  folder on the EBS-backed disk (e.g. `mkdir -p ~/CTV_dbt_iceberg_poc/nessie-data`).

## 1. Stand up
```bash
# .env is gitignored — create it on this machine (copy your local .env), fill REPLACE_* values; set NESSIE_DATA_DIR
docker compose build
docker compose up -d
docker compose ps
```

## 2. Smoke test (validate the foundation)
```bash
./scripts/smoke_test.sh
```
Pass = Trino and PyIceberg both read/write `iceberg.bronze.smoke` via one Nessie catalog, and
the data/metadata files land under `s3://dataplatformpoc-venketa/warehouse/`.

## 3. Reference sync (Option C) — VALIDATED (all 14 tables)

Mirrors 14 `hive_metastore` Delta reference tables from Azure ADLS into Iceberg on S3, each under a
schema named after its **source database** (`hive_metastore.<db>.<table>` → `iceberg.<db>.<table>`;
schemas `km_preparation_db`, `km_preparation_gold_db`, `productcentral`). **The full source→target
table inventory (names, ADLS paths, reader, row counts) is in `docs/reference_tables.md`.** The load
is **streamed + atomic**: it clears and reloads each table via one transaction (delete-all + append
batches), so it is memory-bounded (safe for multi-million-row tables) and never drops the table — a
mid-run failure leaves the prior data intact.

Run / operate:
```bash
docker compose exec ingestion python -m ingestion.reference_sync                 # all 14 tables
docker compose exec ingestion python -m ingestion.reference_sync --table origin  # one table
REF_SYNC_BATCH_ROWS=200000 docker compose exec ingestion python -m ingestion.reference_sync  # tune batch (default 1,000,000)
# verify:
docker exec -i trino trino --execute "SELECT count(*) FROM iceberg.km_preparation_db.data_provider"
docker exec -i trino trino --execute "SELECT table_schema, table_name FROM iceberg.information_schema.tables WHERE table_schema IN ('km_preparation_db','km_preparation_gold_db','productcentral') ORDER BY 1,2"
# schedule daily 02:15 UTC (logs to /var/log/ref_sync.log):
crontab scripts/cron/reference_sync.cron   # confirm: crontab -l
```

Two-reader design (in `ingestion/reference_sync.py`):
- **delta-rs** (primary) — authenticates via rustls (no CA setup needed). INT96 (legacy Spark)
  timestamps are coerced to microseconds at read (`coerce_int96_timestamp_unit="us"`), avoiding a
  lossy ns→us cast that errors on sub-microsecond/sentinel values.
- **DuckDB** (fallback) — for tables with `deletionVectors` / `v2Checkpoint`, which delta-rs cannot
  read (a fundamental limit of the PyArrow-dataset path, not a version gap). DuckDB needs the system
  CA bundle (installed in `infra/Dockerfile.ingestion`) **and** `SET azure_transport_option_type='curl'`
  so its libcurl transport can verify TLS to blob storage (its default Azure-SDK transport ignores
  the CA env vars). Pinned `duckdb==1.5.5`.

Per-batch normalization: binary columns → base64 strings (readable, matches how Databricks shows
binary); timestamps → UTC **with time zone** (Iceberg `timestamptz`), matching Databricks `TIMESTAMP`
(tz-aware sources keep their instant; naive sources are treated as UTC). The Iceberg schema is derived
from the normalized Arrow schema per run; additive source changes auto-evolve, type/removed-column
changes fail loudly, and the run stops at the first failing table.

The engine lives in `ingestion/common/ref_sync_engine.py`; `reference_sync.py` (hive) and
`uc_reference_sync.py` (Unity Catalog) are thin configs over it — identical behaviour, differing only
in source storage account + base path + table map.

### 3b. Unity Catalog reference sync (uc_reference_sync.py)
Mirrors the UC reference tables (in a **different Azure blob** — `vxxdbwcommonpesteu2`, base path
hardcoded in the `.py` as `abfss://dbwcontainer@vxxdbwcommonpesteu2.dfs.core.windows.net/deltas/mrdpp`)
into `iceberg.<schema>.*` using the same engine. Only the account credentials go in `.env`:
```bash
# .env  (UC blob — second storage account; NAME must match the account in the .py base path)
UC_AZURE_STORAGE_ACCOUNT_NAME=vxxdbwcommonpesteu2
UC_AZURE_STORAGE_ACCOUNT_KEY=<REDACTED>
```
```bash
docker compose up -d --force-recreate ingestion        # pick up the UC_* env
docker compose exec ingestion python -m ingestion.uc_reference_sync                     # all UC tables
docker compose exec ingestion python -m ingestion.uc_reference_sync --table creative_match_type
```
In-scope tables (creative dedupe + market mapping): `creative_match_type`, `global_market`,
`provider_global_market_map`. Add the CTV-out-of-scope UC reference tables to its `TABLE_MAP` (each
also gets a DDL structure + a dbt source) once identified.

## 4. Validate-at-stand-up items (configs here are starting points)
- **Nessie S3 / warehouse property names** for the pinned `NESSIE_VERSION` (Iceberg REST catalog).
  RESOLVED for 0.104.1: Nessie's catalog S3 uses STATIC auth via a secret URN
  (`nessie.catalog.service.s3.default-options.access-key` -> `nessie.catalog.secrets.access-key.{name,secret}`),
  and PyIceberg must pass the warehouse NAME (`warehouse`), not the s3:// URI. Also, Nessie vends
  `py-io-impl=FsspecFileIO` per table; we override it to PyArrow client-side in
  `ingestion/common/catalog.py::force_pyarrow_io` (avoids the s3fs dependency stack).
- **`table_changes` on delete-file snapshots** — confirm it errors (locks the Half B timestamp-watermark decision).
- **Trino Azure filesystem** props (`fs.native-azure.enabled`) if any table's data stays on ADLS.
- **Healthchecks / image versions** — pin and adjust.
- **VARIANT -> string** parsing for CTV query patterns — RESOLVED (staging `json_data` is VARCHAR;
  the staging->raw model parses it with `json_parse` / `json_extract_scalar`, and `daisy_chain` /
  `raw_json` are stored as VARCHAR via `json_format`).
- **Nessie has no view support** — the Nessie catalog implements neither view nor materialized-view
  management (`createView is not supported for Iceberg Nessie catalogs`). Consequences, both handled:
  dbt runs with `views_enabled: false` (its incremental temp relations become tables, not views), and
  a legacy Databricks view is ported as an **ephemeral** dbt model (`media_property_flatten_vx0_vw`).
  Real views would require a REST-type catalog against Nessie, or a separate view-capable catalog.

## 4b. CTV ingestion (Piece 1) — VALIDATED (2026-07-27)
End-to-end on the VM: S3 `.bz2`/plain-JSON → bronze staging (PyIceberg landing) → dbt-trino
staging->raw incremental (`bronze.digital_raw_occurrence`). Incremental reads via Trino
`system.table_changes` driven by the version watermark. `creative_url_hash` = exact Spark
`xxhash64(seed 42)` precomputed at landing. Persistent tables pre-created by DDL (`ddl/`). Full
detail, run steps, and design notes: **`docs/ctv_ingestion.md`**; table structures: **`ddl/README.md`**.

## 4c. Pieces 3–5 tables provisioned (2026-07-28)
All 20 persistent Iceberg tables pre-created by DDL (`ddl/00`–`07`): bronze staging/raw + creative (5),
silver watermark + Piece 4/5 (7), gold creative/occurrence/deployment (8). Scripts `04`–`07` were
generated from the Databricks `table_ddl` notebooks (Spark→Trino types; IDENTITY/DEFAULT dropped —
`occurrence_id`/`creative_id` come from Postgres sequences; CLUSTER BY → partition(`capture_month`) +
`sorted_by`). dbt sources wired for the UC `reference`/`spend` dims and Postgres reads (`creatives.*`,
`vx2_taxonomy.*` — provisional). Coverage audited vs the deep-dive; archiving excluded (N/A for CTV).
Run/verify: see **`ddl/README.md`**.

## 4d. Piece 3 — creative push, Job A — VALIDATED (2026-08-04)
New CTV creatives push end-to-end from `bronze.digital_raw_occurrence` into Postgres, **Trino/dbt-native
(no Python)**. dbt DAG (all bronze tables): `crtv_staging_candidate → crtv_autochaff →
crtv_autochaff_records → crtv_staging_excluded → crtv_staging_final`; the push runs in the final model's
ordered post-hooks (maintain `creative_unique_urls`/`creative_autochaff` → cross-catalog CTAS a Postgres
temp table → `CALL` the cloned insert proc via `postgres.system.execute` → advance the **version**
watermark last). `creative_id` = a Postgres sequence reserved as a `[start,end]` block (replaces the
Universal Creative API). All Postgres objects are `tempwork.*_ctv_poc` **clones** — real `creatives.*`
untouched. Full detail + build learnings: **`docs/ctv_creative_push.md`**.

Setup (once): run the Postgres clone bootstrap, then seed the Job A watermark row:
```bash
# Postgres (psql): ddl/postgres/piece3_tempwork_ctv_poc.sql  (idempotent; clones + procs + sequence + block table)
# Trino: seed DIGITAL_RAW_OCC_TO_CRTV_STAGING (ddl/03 — version watermark, last_commit_version NULL)
docker exec -i trino trino --execute "INSERT INTO iceberg.silver.watermark_control (watermark_name, start_timestamp, end_timestamp, last_commit_version, current_commit_version, transaction_status, created_timestamp, updated_timestamp) VALUES ('DIGITAL_RAW_OCC_TO_CRTV_STAGING', NULL, NULL, NULL, NULL, 'SUCCEEDED', current_timestamp, current_timestamp)"
```
Run / verify:
```bash
docker compose run --rm dbt dbt run --select +crtv_staging_final          # by model
docker compose run --rm dbt dbt run --select tag:RAW_OCCS_TO_CREATIVE_STAGING          # by job tag (same models, whole job)
docker exec -i trino trino --execute "SELECT count(*) FROM postgres.tempwork.creative_staging_ctv_poc"
docker exec -i trino trino --execute "SELECT count(*) FILTER (WHERE is_staged) FROM iceberg.bronze.creative_unique_urls"
docker exec -i trino trino --execute "SELECT last_commit_version, transaction_status FROM iceberg.silver.watermark_control WHERE watermark_name='DIGITAL_RAW_OCC_TO_CRTV_STAGING'"
```
First validated run: 26,592 creatives staged, `creative_first_seen` seeded 1:1, `creative_autochaff` 0,
watermark `SUCCEEDED`. (Reset commands for a clean re-test are in `docs/ctv_creative_push.md`.)

## 4e. Piece 3 — Job B (first-seen update + occurrence summary) — VALIDATED (2026-08-05)
Two independent **version-watermarked** sub-pipelines, run together, Trino/dbt-native. First-seen update
(`crtv_firstseen`, `DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE`) → `CALL` the cloned update proc (earliest
occurrence). Occurrence summary (`crtv_occ_summary_candidate` → `crtv_occ_summary_final`,
`DIGITAL_RAW_OCC_SUMMARY_PSQL`) → CDF unioned with the parked buffer `missing_digital_occurrence_for_summary`,
aggregate → `CALL` the upsert proc, then a park/release `MERGE` back into the buffer. Full detail:
**`docs/ctv_creative_push.md`**.

Setup (once):
```bash
# Postgres (PG client — psql is NOT on the VM): re-run ddl/postgres/piece3_tempwork_ctv_poc.sql
#   (adds creative_occurrence_summary_ctv_poc + its upsert proc). Requires membership in tempwork_admin_role:
#   GRANT tempwork_admin_role TO <login>;   (a DBA event trigger reassigns new tempwork tables to that role)
# Trino: add the buffer column, seed the two Job B version watermarks, and partition watermark_control:
docker exec -i trino trino --execute "ALTER TABLE iceberg.bronze.missing_digital_occurrence_for_summary ADD COLUMN capture_timestamp TIMESTAMP(6) WITH TIME ZONE"
docker exec -i trino trino --execute "INSERT INTO iceberg.silver.watermark_control (watermark_name,start_timestamp,end_timestamp,last_commit_version,current_commit_version,transaction_status,created_timestamp,updated_timestamp) VALUES ('DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE',NULL,NULL,NULL,NULL,'SUCCEEDED',current_timestamp,current_timestamp)"
docker exec -i trino trino --execute "INSERT INTO iceberg.silver.watermark_control (watermark_name,start_timestamp,end_timestamp,last_commit_version,current_commit_version,transaction_status,created_timestamp,updated_timestamp) VALUES ('DIGITAL_RAW_OCC_SUMMARY_PSQL',NULL,NULL,NULL,NULL,'SUCCEEDED',current_timestamp,current_timestamp)"
# partition watermark_control by watermark_name (recreate preserving rows — see ddl/03 comment):
docker exec -i trino trino <<'SQL'
CREATE TABLE iceberg.silver.watermark_control_bak AS SELECT * FROM iceberg.silver.watermark_control;
DROP TABLE iceberg.silver.watermark_control;
CREATE TABLE iceberg.silver.watermark_control (watermark_name VARCHAR, start_timestamp TIMESTAMP(6) WITH TIME ZONE, end_timestamp TIMESTAMP(6) WITH TIME ZONE, last_commit_version BIGINT, current_commit_version BIGINT, transaction_status VARCHAR, created_timestamp TIMESTAMP(6) WITH TIME ZONE, updated_timestamp TIMESTAMP(6) WITH TIME ZONE) WITH (format='PARQUET', partitioning=ARRAY['watermark_name'], max_commit_retry=20);
INSERT INTO iceberg.silver.watermark_control SELECT * FROM iceberg.silver.watermark_control_bak;
DROP TABLE iceberg.silver.watermark_control_bak;
SQL
```
Run / verify:
```bash
docker compose run --rm dbt dbt run --select crtv_firstseen crtv_occ_summary_candidate crtv_occ_summary_final          # by model
docker compose run --rm dbt dbt run --select tag:CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY          # by job tag (same models, whole job)
docker exec -i trino trino --execute "SELECT count(*) FROM postgres.tempwork.creative_occurrence_summary_ctv_poc"
docker exec -i trino trino --execute "SELECT count(*) FROM iceberg.bronze.missing_digital_occurrence_for_summary"   # parked-unresolved
```
**Concurrency:** `watermark_control` is written by every watermarked flow. Unpartitioned it's a single
data file, so concurrent writers (Job B's two sub-pipelines; Job A vs Job B on schedule) hit
`ICEBERG_COMMIT_ERROR "Found conflicting files"` — a serializable conflict Trino does **not** retry
(`max_commit_retry` doesn't help). Partitioning it by `watermark_name` (above) isolates each process's
row to its own file, so runs are concurrency-safe at `threads=4`.

## 4f. Piece 4 — seed production creative data into the clones — VALIDATED for new-data (2026-08-08)
Prerequisite for the Piece 4 sync-back: the unchanged proc `creatives.sp_dbx_creative_get_changes_for_databricks`
will run against `tempwork.*_ctv_poc` clones, so production creative data must be copied into them first. A
**pure Postgres seeding proc** does this — no Trino/dbt (both prod `creatives.*`/`ml_results.*` and `tempwork`
are in the same Postgres). Anchor = creatives in `creative_staging_ctv_poc` (keyed on `creative_url_hash`);
their one-hop dedup parents are pulled in too. Our creatives take the reserved PoC id; external parents keep
their prod id (26 B reserved boundary). Full design + decisions: **`docs/ctv_creative_seed.md`**.

Setup (once, on prod Postgres via a SQL client — `psql` is NOT on the VM; requires `tempwork_admin_role`):
```sql
-- creates the 8 new clones (creative, creative_product/celebrity/competitor, creative_dedupe_map,
-- creative_classification_engine_holding, creative_ai_classification_staging_vx0, component_coding),
-- the watermark_control clone (2 seed marks), and the seeding procs. Idempotent.
\i ddl/postgres/piece4_seed_tempwork_ctv_poc.sql
```
Run (adhoc / manual — later scheduled once daily ingestion starts):
```sql
CALL tempwork.sp_seed_creative_clones_ctv_poc('ALL');      -- Mode 1 (new inserts) then Mode 2 (creative updates); default
-- or a single phase:
CALL tempwork.sp_seed_creative_clones_ctv_poc('NEW');      -- new inserts only (watermark: clone staging.updated_timestamp)
CALL tempwork.sp_seed_creative_clones_ctv_poc('UPDATE');   -- creative updates only (watermark: prod creative.updated_timestamp)

-- inspect the two watermarks (high-water in table_tx_end):
SELECT watermark_name, table_tx_start, table_tx_end, tx_status, tx_message, tx_datetime
FROM tempwork.watermark_control_ctv_poc;
```
**Status:** Mode 1 (new data) VALIDATED on the clone tables. Mode 2 (creative-level updates) to be exercised
once daily ingestion is running. **Write strategy:** `creative`/`staging_vx0`/`holding` upsert
(`ON CONFLICT (creative_id)`); the multi-row tables (`product`/`celebrity`/`competitor`/`dedupe_map`) and the
external-parent `first_seen`/`occ_summary` rows are delete-in-scope + insert. **Scope gate:** every load is
restricted to our CTV clones + their related parents (dedupe_map joined back to the run's `_seed_idmap`), so a
non-CTV child of a CTV parent is never loaded. Descriptor fields (provider/type/media, etc.) are sourced from
the clone staging/first_seen for our creatives and from prod for external parents.

## 4g. Piece 4 — creative sync-back (Trino/dbt-native) — COMPLETE (all 8 built; 1/2/3/5 VALIDATED 2026-08-10)
Ports the Databricks `SYNC_CREATIVES_TO_DATABRICKS` job (8 tasks) reading the seeded `tempwork.*_ctv_poc`
clones → Iceberg `gold.*`/`silver.*`. Every Databricks Delta `table_changes` read becomes a **timestamp
(column) watermark** scan — Trino `table_changes` is append-only, and these gold tables are MERGE-written.
UTC, **no lag** (`> start`); idempotent MERGEs. Full plan + per-task map + all learnings + commands:
**`docs/ctv_creative_sync_plan.md` §8–12**.

Setup (once):
```bash
# Postgres (SQL client; needs tempwork_admin_role): clone the two get_changes procs (retargeted to tempwork)
\i ddl/postgres/piece4_sync_procs_ctv_poc.sql
# Trino: seed the 10 Piece-4 timestamp watermarks + add the two gold columns the sync writes
docker exec -i trino trino --catalog iceberg -f /dev/stdin < ddl/08_silver_watermark_control_piece4.sql
docker exec -i trino trino --execute "ALTER TABLE iceberg.gold.creative_first_seen ADD COLUMN provider_campaign_landing_page VARCHAR"
docker exec -i trino trino --execute "ALTER TABLE iceberg.gold.creative ADD COLUMN first_seen_provider_campaign_landing_page VARCHAR"
# Product-resync watermark must NOT start at 1900 (else full-productmap sweep -> OOM); init to max(change_dt):
docker exec -i trino trino --execute "UPDATE iceberg.silver.watermark_control SET start_timestamp = end_timestamp, end_timestamp = cast((SELECT max(change_dt) FROM iceberg.productcentral.productmap) as timestamp(6) with time zone), transaction_status='INIT', updated_timestamp=cast(current_timestamp as timestamp(6) with time zone) WHERE watermark_name='CTV_PRODUCT_RESYNC'"
```
Run the FULL job (all 8 tasks in the reconciled Databricks DAG order; roots/leaves parallelize with ≥2 threads):
```bash
docker compose run --rm dbt dbt ls  --select tag:SYNC_CREATIVES_TO_ICEBERG --output name   # 18 models
docker compose run --rm dbt dbt run --select tag:SYNC_CREATIVES_TO_ICEBERG                  # end-to-end
```
Or run a SINGLE task (chained tasks need the whole chain — scratch is dropped each run; single-model tasks read
already-built gold tables so the final model alone suffices):
```bash
# creative (1):  dbt run --select crtv_sync_creative_forsync crtv_sync_creative_raw crtv_sync_creative_revxlate crtv_sync_creative
# first-seen(2): dbt run --select crtv_sync_first_seen        # dedup(3): crtv_sync_dedupe_map crtv_sync_dedupe_map_delete
# fs-info (5):   dbt run --select crtv_fsinfo_update          # occ-id: crtv_occid_update   # last-seen(6): crtv_lastseen_update
# component (4): dbt run --select comp_sync_forsync comp_sync_explode comp_sync_revxlate comp_sync
# resync (8):    dbt run --select crtv_product_resync_affected crtv_product_resync_prim crtv_product_resync_sec crtv_product_resync
# dev inspection of a chained task's intermediates: add --vars 'keep_SYNC_CREATIVES_TO_ICEBERG_tables: true'
```
**Model pattern (all sync tasks):** a candidate table (`schema='bronze'`, tag `SYNC_CREATIVES_TO_ICEBERG`, casts
to the gold schema) → post-hooks MERGE candidate → gold + advance the watermark (`watermark_ts_finish_from_relation`
or `watermark_ts_advance_from_source`, run-time template-string hooks) → `on-run-end` drops the scratch (re-enabled;
opt out with the keep-var). The watermark READ is either `watermark_ts_begin` (first-seen/dedup/fs-info/last-seen,
which filter in the model body) or the stage-1 proc-call pre-hook (creative/component, server-side in the proc);
occurrence-id has NO watermark (null-check + floor). **In every post-hook, reference relations as LITERAL strings** —
`ref()`/`source()`/`this` re-render at run to the profile default schema (`silver`) and break (the Piece-3 trap).
Heavy tasks (component, product-resync) are **split into staged models** and `productmap`/`d_product` are **streamed
via semi-join** (never hashed) to fit Trino's memory + 150-stage limits. `QUALIFY` is unsupported → `row_number()`
subquery. **Status:** all 8 tasks built; **1/2/3/5 VALIDATED**; occurrence-id (**13,333 resolved**) + task 6 (last-seen,
**21,750 candidates, watermark advanced**) **VALIDATED 2026-08-11** once Piece 5 populated `gold.digital_gold_occurrence`;
task 4 (component) near-empty smoke-test; task 8 (product-resync) no-op at `max(change_dt)`. DAG reconciled to the
Databricks job; tag renamed; scratch cleanup re-enabled; first-seen 1-min lag removed. Archive parked (future `ca_flag`).

## 4h. Piece 5 — gold occurrence flow (Trino/dbt-native) — COMPLETE & VALIDATED (2026-08-11)
Ports the Databricks `DigitalRawocctoGoldocc` job → **6 staged models** (tag `DIGITAL_RAW_OCC_TO_GOLD_OCC`), two halves,
two watermarks. This is the **last piece** — the CTV occurrence flow now runs end-to-end on Trino/dbt/Iceberg. Full plan
+ all learnings + commands: **`docs/ctv_occurrence_gold_plan.md`**.

Setup (once):
```bash
# Postgres (SQL client; needs tempwork_admin_role): the 75-billion occurrence_id sequence + block table + reserve proc
\i ddl/postgres/piece5_occ_id_seq_ctv_poc.sql
# Trino: seed the 2 Piece-5 watermarks (DIGITAL_RAW_OCC_TO_GOLD_OCC version + DIGITAL_CRTV_CHANGES_TO_GOLD_OCC timestamp)
docker exec -i trino trino --catalog iceberg -f /dev/stdin < ddl/09_silver_watermark_control_piece5.sql
```
Run the FULL job (Half A then Half B, in DAG order; leaves parallelize with ≥2 threads):
```bash
docker compose run --rm dbt dbt ls  --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC --output name    # 6 models
docker compose run --rm dbt dbt run --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC                   # end-to-end
```
Or run a SINGLE stage (Half A is chained — scratch is dropped each run, so run the whole chain; Half B reads built gold):
```bash
# Half A: dbt run --select digital_occ_raw_cdf digital_occ_deploychain digital_occ_combined digital_occ_classified digital_occ_gold
# Half B: dbt run --select digital_occ_crtv_changes
# dev inspection of Half A intermediates: add --vars 'keep_DIGITAL_RAW_OCC_TO_GOLD_OCC_tables: true'
```
**Half A** (version watermark on append-only `bronze.digital_raw_occurrence`): `digital_occ_raw_cdf` (version-CDF read,
US/insert/non-retransmit, dedup) → `digital_occ_deploychain` (distinct daisy chains → **in-place** array transform →
`gold.digital_deployment_chain{,_role,_mediator}` MERGEs; `deployment_chain_id = from_big_endian_64(xxhash64(md5))`) →
`digital_occ_combined` (union new raw + the `silver.digital_staging_occurrence` **hold buffer** + media/market/source_channel
enrichment, reusing the validated Piece-3 CTEs) → `digital_occ_classified` (**the gate** vs `gold.creative`: computes
`occurrence_hold_flag`, `delete_flag`, `is_house_ad`, `creative_id`, `deployment_chain_id`) → `digital_occ_gold` (writer:
prelim spend + reserve **`occurrence_id`** from the 75 B Postgres sequence → 38-col MERGE into `gold.digital_gold_occurrence`;
park Hold / release Not-Hold in the staging buffer; **version-watermark finish**). **Half B** `digital_occ_crtv_changes`
(timestamp watermark on `gold.creative.updated_timestamp`): re-parent + delete_flag MERGEs on existing gold rows;
`update_house_ad_flag` **parked** (needs `gold.digital_spend_availability`). First run: **811,764 raw → 746,245 gold
occurrences** (occurrence_id [75,000,000,000 … 75,000,746,244]) + **65,519 held**; 2 deployment chains match prod; Half B
33,338 changed / 0 updates on a fresh gold. Same Trino traps as Piece 4 apply (unqualified MERGE SET, literal-string hooks,
no `QUALIFY`, `on_table_exists='drop'`, stream huge tables). Scratch cleanup on-run-end (opt out with the keep-var).

## 5. Prod Postgres — reachability RESOLVED, Trino catalog WIRED (2026-07-26)
Cross-cloud reachability was BLOCKED; DevOps opened the path (verified: `bash scripts/pg_connectivity_test.sh`
→ DNS ok, TCP OPEN, psql auth OK as `databricks_admin_user` @ `vxcentral`, PostgreSQL 16.4). The Trino
`postgresql` catalog is now wired at `infra/trino/catalog/postgres.properties`, reading creds from env
(`${ENV:PG_*}`) so no secrets sit in a committed file — the `trino` service in `docker-compose.yml`
passes `PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD` from `.env`. Adding/changing a catalog needs a Trino
recreate: `docker compose up -d` (or `--force-recreate trino`), then verify:
```bash
docker exec -i trino trino --execute "SHOW SCHEMAS FROM postgres"
docker exec -i trino trino --execute "SHOW TABLES FROM postgres.creatives"   # adjust schema
```
Writes to Postgres go via psycopg2 cloned stored procs (Piece 3), not Trino. `databricks_admin_user`
is a broad admin login — scope it down before anything beyond the PoC. If Trino errors on TLS
(e.g. "no encryption"/SSL), append `?sslmode=require` to the `connection-url`.

Connection hygiene (2026-07-28): `postgres.properties` sets `?ApplicationName=trino-ctv-poc` (so
Trino's Postgres connections are attributable in `pg_stat_activity` despite the shared login) plus
`metadata.cache-ttl=30m` to bound catalog re-queries. This came out of a DBA report of connection
churn on `databricks_admin_user`, traced to a **Unity Catalog foreign-catalog federation** polling
`pg_stat_user_tables` — not our stack. Durable fix (later): a dedicated, connection-limited Postgres
role per consumer (Trino + the UC federation each their own).

## 5b. Later
- **Airflow** — cron first; Airflow later.

## 6. Deferred library upgrades (future action item — not done, stack is validated as-is)
Investigated and parked to avoid destabilizing a working pipeline; revisit deliberately.
- **deltalake 0.25.5 → 1.6.2 (major).** 1.6.2's `QueryBuilder.execute(sql)` returns a streaming
  `RecordBatchReader` and applies **deletion vectors natively** (DataFusion, rustls) — so it could
  replace BOTH the delta-rs `to_pyarrow_dataset` path AND the DuckDB fallback with one reader,
  letting us delete DuckDB + the ca-certificates layer + the SSL/Azure-transport config. Cost: it
  drops pyarrow as a core dep (uses `arro3-core`), changes INT96/arro3↔pyarrow behavior, and needs a
  rework of `reference_sync.py` + full re-validation of all 14 tables. High value (simplification),
  non-trivial migration.
- **duckdb** — on 1.5.5 (done). One live deprecation: `fetch_record_batch()` → `to_arrow_reader()`.
- **pyarrow 20 → 25** — the `==20` pin's stated reason is wrong (pyiceberg 0.11.1 imports fine on 25);
  can relax, re-run the smoke test to confirm `force_pyarrow_io` still holds.
- **dbt-trino 1.10.1 → 1.10.2** (patch) — safe.
- **pyiceberg** — already latest (0.11.1). **Nessie 0.104.1 / Trino 476 images** — kept pinned;
  bumping re-opens the S3-auth/FileIO config validation.
- **DuckDB extension longevity** — `INSTALL delta/azure` downloads version-matched extensions at
  runtime; the repo eventually drops very old DuckDB versions, so keep a reasonably current pin.

## Credentials
- AWS: default provider chain reads `AWS_*` from `.env` (currently the `mukesh-s3-only-temp`
  IAM user keys). Swap to the instance-profile role when provisioned, then drop the keys.
- `.env` is **gitignored** — it is NOT committed (GitHub push protection blocks committed keys).
  Copy it to each machine manually (`scp`, or paste). If you want one versioned copy across
  machines, encrypt it (git-crypt / SOPS) or use a secrets manager — don't commit plaintext keys.
