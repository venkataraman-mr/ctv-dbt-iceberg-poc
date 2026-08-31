# CTV Occurrence Flow — dbt + Iceberg PoC

Open-source lakehouse PoC migrating the **CTV occurrence flow** off Azure Databricks to
**dbt + Apache Iceberg** on AWS (Trino + Nessie + S3). Runs as Docker Compose on a single EC2 VM.
Architecture source of truth: Google Drive → `CTV_occurrence_flow_architecture_MASTER`.

## Layout
```
docker-compose.yml     stack: nessie · trino · dbt · ingestion
infra/                 Dockerfiles + Trino/Nessie config
ddl/                   Trino + Postgres DDL, split by catalog: nessie/ (current) + polaris/ (PoC, empty) + postgres/{nessie,polaris}/ + README
dbt/                   dbt project — targets the NESSIE (`iceberg`) catalog (bronze/reference sources, staging->raw + ref models, watermark macros)
ingestion/             Python: reference sync (Option C) + CTV landing (PyIceberg) — writes to the NESSIE catalog
scripts/               catalog/ (nessie|polaris|lakekeeper catalog-PoC tooling) + ops (smoke_test.sh, pg_connectivity_test.sh, cron/, upload_ctv_sample.ps1, vm_setup.md)
docs/                  catalog/ · crosscloud/ · pipeline/ (per-piece) · runbooks/{nessie,polaris} · ctv_dbt_iceberg_poc.md (SSOT checkpoint) + architecture.md
```

> **Catalog binding — important.** The `dbt/` project (every `dbt run` / `dbt build`) **and** the `ingestion/`
> code (reference sync — hive + Unity Catalog — plus CTV landing) run against the **Nessie** (`iceberg`) catalog.
> The parallel **Polaris** PoC pipeline will be **separate cloned folders** — `dbt_polaris/` and
> `ingestion_polaris/` — targeting the `polaris` catalog. So any reference to `dbt/` or `ingestion/` = the
> **Nessie** pipeline, not Polaris.

## Prerequisites
- **Docker + Docker Compose v2** on the VM — the only required host install (everything else is
  containerized). Install steps: see `docs/runbooks/nessie/runbook.md` §0.
- AWS credentials for the S3 bucket — create `.env` (it's **gitignored**; not committed) and fill
  the `REPLACE_*` values. Currently the `mukesh-s3-only-temp` IAM user's keys; swap to the
  instance-profile role when provisioned. Copy `.env` to each machine manually (see runbook).
- (reference sync) Azure storage **account key**.
- (creative flow) prod Postgres reachability — RESOLVED (DevOps opened the cross-cloud path 2026-07-26);
  Trino `postgres` catalog wired, and Pieces 3–5 ran against it.

## Quick start
On the VM, follow the stage-by-stage guide **`scripts/vm_setup.md`** (disk check → install Docker
→ fill `.env` → build → start → smoke test) — run it one stage at a time so errors surface where
they happen. Once it's up, the manual bring-up / re-run is:
```bash
docker compose build
docker compose up -d
docker compose ps
bash scripts/smoke_test.sh    # two-engine proof: Trino write + PyIceberg write on one Nessie catalog
```
Full detail + prerequisites: `docs/runbooks/nessie/runbook.md` and `scripts/vm_setup.md`.

## Dev workflow (local → git → VM)
Edit locally, review, push; the VM pulls and runs. One source of truth for edits = the local copy.
1. **Edit** in the local copy (`C:\work\CTV_dbt_iceberg_poc`) in VS Code.
2. **Review** the diff, then **commit & push** to the git remote.
3. **On the VM** (Remote-SSH terminal or SSH): `git pull`, then `docker compose …` / `dbt …` to run.

Use a second VS Code window connected via **Remote-SSH** to the VM for *running and monitoring*
(docker/dbt/logs) — not for editing the same files, to avoid divergence between the two copies.

## Status
**Foundation + Reference sync (hive + UC) + Postgres + CTV ingestion (Piece 1) + Piece 3 (Job A creative push + Job B first-seen/occurrence-summary) VALIDATED & concurrency-safe. Piece 4 clone-seeding prerequisite VALIDATED (Mode 1; Mode 2 pending daily ingestion). Piece 4 sync-back COMPLETE (built): all 8 tasks ported + running end-to-end in DAG order (`dbt run --select tag:SYNC_CREATIVES_TO_ICEBERG`); tasks 1/2/3/5 VALIDATED, component near-empty smoke-test, product-resync no-op at max(change_dt). Piece 5 gold occurrence flow COMPLETE & VALIDATED (`dbt run --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC`): 811,764 raw → 746,245 gold occurrences + 65,519 held; it also lit up the Piece-4 occurrence-id + last-seen tasks. The whole PoC (Pieces 1–5) is complete — all five jobs run tag-based (BIS_CTV_BZ2FILE_TO_RAW_OCC · RAW_OCCS_TO_CREATIVE_STAGING · CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY · SYNC_CREATIVES_TO_ICEBERG · DIGITAL_RAW_OCC_TO_GOLD_OCC).**

*Foundation (2026-07-22):* the full stack (nessie · trino · dbt · ingestion) builds and runs on the
EC2 VM, and the two-engine smoke test passes — Trino and PyIceberg both read/write one Iceberg table
through a single Nessie catalog on S3, and dbt connects to Trino. Config nailed down at stand-up (in
`docs/runbooks/nessie/runbook.md` §4): Nessie RocksDB runs as root; its catalog S3 uses STATIC auth via a secret URN;
PyIceberg passes the warehouse *name*; and Nessie vends `py-io-impl=FsspecFileIO`, overridden to
PyArrow client-side (`force_pyarrow_io`).

*Reference sync — Option C (2026-07-25; extended 2026-07-28):* reference/lookup dims mirror to
`iceberg.<db>.<table>` (target schema = source db) via one shared streaming engine
(`ingestion/common/ref_sync_engine.py`) — atomic clean reload (memory-bounded; delta-rs primary,
DuckDB fallback for deletion-vector / v2Checkpoint tables; binary→base64 + UTC-with-time-zone
normalization). Two thin configs: **hive** (`reference_sync.py`, 14 tables from the stdlg2 common
Delta path) and **Unity Catalog** (`uc_reference_sync.py`, 6 tables — `reference.*` + `spend.*` — from
a second Azure blob). **20 reference tables loaded.** `vx2_taxonomy` dims are MANAGED (no reachable
path) → read from Postgres. Daily cron ready. Details in `docs/runbooks/nessie/runbook.md` §3/§3b + `docs/pipeline/reference_tables.md`.

*Prod Postgres + Trino catalog (2026-07-26):* the cross-cloud path (previously blocked) is open and
authenticated (verified via `scripts/pg_connectivity_test.sh` — psql OK as `databricks_admin_user` @
`vxcentral`, PostgreSQL 16.4). The Trino `postgres` catalog is wired
(`infra/trino/catalog/postgres.properties`, creds via `${ENV:PG_*}` — no secrets committed), so the
creative flow (Pieces 3–4) is unblocked. Connections are labeled `application_name=trino-ctv-poc`
with metadata caching to bound load (after a DBA flagged connection churn on the shared login —
traced to a Unity Catalog foreign-catalog federation, not our stack). Details in `docs/runbooks/nessie/runbook.md` §5.

*CTV ingestion — Piece 1 (2026-07-27):* the CTV occurrence flow lands and transforms end-to-end on
the VM. A Python landing step streams `.bz2`/plain-JSON files from `s3://…/landing/ctv/ingestion/`
into the bronze staging table (auto-detecting bzip2, memory-bounded batches, archiving processed
files), and a dbt-trino incremental model transforms staging → `bronze.digital_raw_occurrence`
(42-column canonical schema, dedup + video/`video/mp4`/publisher filter + anti-join). Reads are
watermark-driven via Trino `system.table_changes` (the Delta `table_changes` analog), so runs are
incremental, not full scans. `creative_url_hash` is the exact Spark `xxhash64(seed 42)`, precomputed
at landing (verified vs real PySpark). All timestamps are `timestamp(6) with time zone` (UTC), and
persistent tables are pre-created by DDL (`ddl/nessie/`). Details in `docs/pipeline/ctv_ingestion.md`.

*Views:* the **native** Nessie connector (`iceberg.catalog.type=nessie`) doesn't support Iceberg views,
so dbt runs with `views_enabled: false` (incremental temp relations become tables) and a legacy Databricks
view is ported as an **ephemeral** dbt model (`media_property_flatten_vx0_vw`). Nessie's **Iceberg REST
catalog** *does* support views — validated 2026-08-17 on Trino 483 via a scratch `iceberg_rest` catalog
(`CREATE VIEW` succeeds against the same tables). The pipeline stays on the native catalog by choice; the
REST path is the productionization route if real views are needed.

*Pieces 3–5 tables provisioned (2026-07-28):* all 20 persistent Iceberg tables are pre-created by DDL
(`ddl/nessie/00`–`07`) — bronze staging/raw/creative, silver watermark + Piece 4/5, gold creative/occurrence/
deployment — generated from the Databricks `table_ddl` notebooks (Spark→Trino types, IDENTITY/DEFAULT
dropped, CLUSTER BY → partition/sort). dbt sources are wired for the UC reference dims, `spend`, and
Postgres reads (`creatives.*`, `vx2_taxonomy.*` — provisional). Coverage was audited against the
deep-dive; archiving is deliberately excluded (N/A for CTV). See `ddl/nessie/README.md`.

*Piece 3 — creative push, Job A (2026-08-04):* new CTV creatives push end-to-end from
`bronze.digital_raw_occurrence` into Postgres, **Trino/dbt-native (no Python)**. Five dbt models
(`crtv_staging_candidate → crtv_autochaff → crtv_autochaff_records → crtv_staging_excluded →
crtv_staging_final`) are a 1:1 transliteration of the Databricks `DigitalRawocctoCrtvStaging` SQL
(auto-chaff path ported in full even though it's a no-op for CTV). The final model's ordered post-hooks
maintain `bronze.creative_unique_urls`/`creative_autochaff`, write a Postgres temp table via
cross-catalog CTAS, `CALL` the cloned insert proc (`postgres.system.execute`), and advance the version
watermark last. `creative_id` comes from a Postgres sequence reserved as a `[start,end]` block (replaces
the Universal Creative API). All Postgres objects are `tempwork.*_ctv_poc` **clones** (real `creatives.*`
untouched). Validated on the VM: **26,592** creatives staged. Details + build learnings in
`docs/pipeline/ctv_creative_push.md`; clone objects in `ddl/postgres/nessie/piece3_tempwork_ctv_poc.sql`.

*Piece 3 — Job B (2026-08-05):* two independent **version-watermarked** sub-pipelines, run together and
concurrency-safe. First-seen update (`crtv_firstseen`) `CALL`s the cloned update proc to pull each
`creative_first_seen` row back to its earliest occurrence; occurrence summary
(`crtv_occ_summary_candidate` → `_final`) reads the CDF unioned with the parked buffer
`missing_digital_occurrence_for_summary`, `CALL`s the upsert proc (occurrence counts + first/last run +
7-day), and runs the park/release `MERGE` back into the buffer. Concurrent watermark writes are made safe
by **partitioning `silver.watermark_control` by `watermark_name`** (a single-file control table otherwise
collides under Iceberg optimistic concurrency, which Trino won't retry). Details + all learnings in
`docs/pipeline/ctv_creative_push.md`.

*Piece 4 — seeding prerequisite (2026-08-05):* before the sync-back can run against clones, production
creative data must be copied into them. `ddl/postgres/nessie/piece4_seed_tempwork_ctv_poc.sql` clones the rest of
the creative table family the sync-back proc reads (`creative`, `creative_product`/`celebrity`/`competitor`,
`creative_dedupe_map`, `creative_classification_engine_holding`, `creative_ai_classification_staging_vx0`,
`component_coding` (task-4 component sync), plus a `watermark_control` clone) and adds a **two-mode Postgres seeding proc**
(`tempwork.sp_seed_creative_clones_ctv_poc`, run `CALL … ('ALL')`): Mode 1 seeds newly-staged creatives
(watermarked off clone staging), Mode 2 refreshes creatives changed in prod (watermarked off
`creative.updated_timestamp`, also catching parent-attribute changes), both pulling in one-hop dedup parents.
Anchors take the reserved PoC id; imported parents keep their prod id (26 B boundary guard).
`creative`/`staging_vx0`/`holding` upsert on `creative_id`; multi-row dependents + external-parent
`first_seen`/`occ_summary` are delete-in-scope + insert. Every load is scope-gated (dedupe_map joined to the
run's `_seed_idmap`) to our CTV clones + related parents; descriptor fields come from clone staging/first_seen
for our creatives, prod for external parents. Real `creatives.*`/`ml_results.*` stay read-only;
`creative_archive` and the `reference.*`/`config.*`/`productcentral.*` lookups are not cloned. **Mode 1 (new
data) validated on the clones; Mode 2 pending the first daily-ingestion run.** Details in `docs/pipeline/ctv_creative_seed.md`.

*Piece 4 — sync-back, COMPLETE (built) (2026-08-10):* the Databricks `SYNC_CREATIVES_TO_DATABRICKS`
job (8 tasks: dedup ∥ first-seen → creative → first-seen-info → occurrence-id → last-seen → component ∥
product-resync) is fully ported to Trino/dbt-native, reading the seeded `tempwork.*_ctv_poc` clones → Iceberg
`gold.*`/`silver.*`, and the **whole job runs end-to-end in DAG order** with one command:
`dbt run --select tag:SYNC_CREATIVES_TO_ICEBERG`. The two Postgres `get_changes` procs are cloned+retargeted;
every Databricks Delta `table_changes` read becomes a **column (timestamp) watermark** scan (Trino
`table_changes` is append-only), UTC, **no lag** (`> start`; dedup + first-seen both). **Tasks 1 (creative),
2 (first-seen), 3 (dedup), 5 (first-seen-info) VALIDATED.** Occurrence-id + task 6 (last-seen) were built
against `gold.digital_gold_occurrence` and **VALIDATED (2026-08-11)** once Piece 5 populated it — occurrence-id
resolved 13,333 `first_seen_occurrence_id`s, last-seen refreshed 21,750 creatives; task 4 (component,
near-empty for CTV) and task 8 (product-resync, no-op at `max(change_dt)`) are built and smoke-tested. Heavy tasks are **split into staged models** and the huge `productmap` is **streamed via
semi-join** (never hashed) to fit Trino's memory + 150-stage limits. The dbt DAG is **reconciled to the
Databricks job**; scratch is cleaned up on-run-end. Full plan + all learnings + single-task/full-job commands:
`docs/pipeline/ctv_creative_sync_plan.md` §8–12.

*Piece 5 — gold occurrence flow, COMPLETE (built) & VALIDATED (2026-08-11):* the Databricks
`DigitalRawocctoGoldocc` job ported to **6 staged Trino/dbt models** (tag `DIGITAL_RAW_OCC_TO_GOLD_OCC`),
two halves + two watermarks. **Half A** (version watermark on append-only `bronze.digital_raw_occurrence`):
`digital_occ_raw_cdf` → `digital_occ_deploychain` (daisy chains → `gold.digital_deployment_chain{,_role,_mediator}`)
→ `digital_occ_combined` (union new raw + the `silver.digital_staging_occurrence` hold buffer + media/market
enrichment, reusing the validated Piece-3 CTEs) → `digital_occ_classified` (the gate against `gold.creative`) →
`digital_occ_gold` (writer: prelim spend + **`occurrence_id` from a 75-billion Postgres sequence** → MERGE
`gold.digital_gold_occurrence`; park/release the buffer; version-watermark finish). **Half B**
`digital_occ_crtv_changes` (timestamp watermark on `gold.creative.updated_timestamp`): re-parent + delete_flag
updates to existing gold occurrences. First run: **811,764 raw → 746,245 gold occurrences** (`occurrence_id`
[75,000,000,000 … 75,000,746,244]) + 65,519 held; deployment chains match prod. Runs as
`dbt run --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC`. This also **lit up the Piece-4 occurrence-id (13,333
resolved) + last-seen tasks**. Full plan + learnings: `docs/pipeline/ctv_occurrence_gold_plan.md`.

**The whole PoC (Pieces 1–5) is now complete** — CTV occurrence flow running end-to-end on Trino/dbt/Iceberg,
no Databricks/Spark in the pipeline. Some library
upgrades are parked as a future action item (`docs/runbooks/nessie/runbook.md` §6).

**Resuming in a new window?** Start with `docs/runbooks/nessie/runbook.md`, `docs/pipeline/ctv_ingestion.md`, and `scripts/vm_setup.md`.
