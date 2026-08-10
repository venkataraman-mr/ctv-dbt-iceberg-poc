# CTV Occurrence Flow — dbt + Iceberg PoC

Open-source lakehouse PoC migrating the **CTV occurrence flow** off Azure Databricks to
**dbt + Apache Iceberg** on AWS (Trino + Nessie + S3). Runs as Docker Compose on a single EC2 VM.
Architecture source of truth: Google Drive → `CTV_occurrence_flow_architecture_MASTER`.

## Layout
```
docker-compose.yml     stack: nessie · trino · dbt · ingestion
infra/                 Dockerfiles + Trino/Nessie config
ddl/                   Trino DDL for persistent tables (create-before-run) + README
dbt/                   dbt project (bronze/reference sources, staging->raw + ref models, watermark macros)
ingestion/             Python: reference sync (Option C) + CTV landing (PyIceberg)
scripts/               vm_setup.md (staged setup) + smoke_test.sh + cron/ + upload_ctv_sample.ps1
docs/                  runbook + reference/ingestion docs + architecture pointer
```

## Prerequisites
- **Docker + Docker Compose v2** on the VM — the only required host install (everything else is
  containerized). Install steps: see `docs/runbook.md` §0.
- AWS credentials for the S3 bucket — create `.env` (it's **gitignored**; not committed) and fill
  the `REPLACE_*` values. Currently the `mukesh-s3-only-temp` IAM user's keys; swap to the
  instance-profile role when provisioned. Copy `.env` to each machine manually (see runbook).
- (reference sync) Azure storage **account key**.
- (creative flow, later) prod Postgres reachability — currently BLOCKED (DevOps).

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
Full detail + prerequisites: `docs/runbook.md` and `scripts/vm_setup.md`.

## Dev workflow (local → git → VM)
Edit locally, review, push; the VM pulls and runs. One source of truth for edits = the local copy.
1. **Edit** in the local copy (`C:\work\CTV_dbt_iceberg_poc`) in VS Code.
2. **Review** the diff, then **commit & push** to the git remote.
3. **On the VM** (Remote-SSH terminal or SSH): `git pull`, then `docker compose …` / `dbt …` to run.

Use a second VS Code window connected via **Remote-SSH** to the VM for *running and monitoring*
(docker/dbt/logs) — not for editing the same files, to avoid divergence between the two copies.

## Status
**Foundation + Reference sync (hive + UC) + Postgres + CTV ingestion (Piece 1) + Piece 3 (Job A creative push + Job B first-seen/occurrence-summary) VALIDATED & concurrency-safe. Piece 4 clone-seeding prerequisite VALIDATED (Mode 1; Mode 2 pending daily ingestion). Piece 4 sync-back IN PROGRESS: 8-task job planned, proc clones + watermark seeds built, tasks 1 (creative), 2 (first-seen), 3 (dedup) VALIDATED end-to-end on the VM (task 1 incl. watermark read→advance loop + idempotent re-run); tasks 5/4/8 + Piece-5-gated pair remaining. Piece 5 remaining.**

*Foundation (2026-07-22):* the full stack (nessie · trino · dbt · ingestion) builds and runs on the
EC2 VM, and the two-engine smoke test passes — Trino and PyIceberg both read/write one Iceberg table
through a single Nessie catalog on S3, and dbt connects to Trino. Config nailed down at stand-up (in
`docs/runbook.md` §4): Nessie RocksDB runs as root; its catalog S3 uses STATIC auth via a secret URN;
PyIceberg passes the warehouse *name*; and Nessie vends `py-io-impl=FsspecFileIO`, overridden to
PyArrow client-side (`force_pyarrow_io`).

*Reference sync — Option C (2026-07-25; extended 2026-07-28):* reference/lookup dims mirror to
`iceberg.<db>.<table>` (target schema = source db) via one shared streaming engine
(`ingestion/common/ref_sync_engine.py`) — atomic clean reload (memory-bounded; delta-rs primary,
DuckDB fallback for deletion-vector / v2Checkpoint tables; binary→base64 + UTC-with-time-zone
normalization). Two thin configs: **hive** (`reference_sync.py`, 14 tables from the stdlg2 common
Delta path) and **Unity Catalog** (`uc_reference_sync.py`, 6 tables — `reference.*` + `spend.*` — from
a second Azure blob). **20 reference tables loaded.** `vx2_taxonomy` dims are MANAGED (no reachable
path) → read from Postgres. Daily cron ready. Details in `docs/runbook.md` §3/§3b + `docs/reference_tables.md`.

*Prod Postgres + Trino catalog (2026-07-26):* the cross-cloud path (previously blocked) is open and
authenticated (verified via `scripts/pg_connectivity_test.sh` — psql OK as `databricks_admin_user` @
`vxcentral`, PostgreSQL 16.4). The Trino `postgres` catalog is wired
(`infra/trino/catalog/postgres.properties`, creds via `${ENV:PG_*}` — no secrets committed), so the
creative flow (Pieces 3–4) is unblocked. Connections are labeled `application_name=trino-ctv-poc`
with metadata caching to bound load (after a DBA flagged connection churn on the shared login —
traced to a Unity Catalog foreign-catalog federation, not our stack). Details in `docs/runbook.md` §5.

*CTV ingestion — Piece 1 (2026-07-27):* the CTV occurrence flow lands and transforms end-to-end on
the VM. A Python landing step streams `.bz2`/plain-JSON files from `s3://…/landing/ctv/ingestion/`
into the bronze staging table (auto-detecting bzip2, memory-bounded batches, archiving processed
files), and a dbt-trino incremental model transforms staging → `bronze.digital_raw_occurrence`
(42-column canonical schema, dedup + video/`video/mp4`/publisher filter + anti-join). Reads are
watermark-driven via Trino `system.table_changes` (the Delta `table_changes` analog), so runs are
incremental, not full scans. `creative_url_hash` is the exact Spark `xxhash64(seed 42)`, precomputed
at landing (verified vs real PySpark). All timestamps are `timestamp(6) with time zone` (UTC), and
persistent tables are pre-created by DDL (`ddl/`). Details in `docs/ctv_ingestion.md`.

*Views:* the Nessie catalog does not support Iceberg views or materialized views, so dbt runs with
`views_enabled: false` (incremental temp relations become tables), and a legacy Databricks view is
ported as an **ephemeral** dbt model (`media_property_flatten_vx0_vw`).

*Pieces 3–5 tables provisioned (2026-07-28):* all 20 persistent Iceberg tables are pre-created by DDL
(`ddl/00`–`07`) — bronze staging/raw/creative, silver watermark + Piece 4/5, gold creative/occurrence/
deployment — generated from the Databricks `table_ddl` notebooks (Spark→Trino types, IDENTITY/DEFAULT
dropped, CLUSTER BY → partition/sort). dbt sources are wired for the UC reference dims, `spend`, and
Postgres reads (`creatives.*`, `vx2_taxonomy.*` — provisional). Coverage was audited against the
deep-dive; archiving is deliberately excluded (N/A for CTV). See `ddl/README.md`.

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
`docs/ctv_creative_push.md`; clone objects in `ddl/postgres/piece3_tempwork_ctv_poc.sql`.

*Piece 3 — Job B (2026-08-05):* two independent **version-watermarked** sub-pipelines, run together and
concurrency-safe. First-seen update (`crtv_firstseen`) `CALL`s the cloned update proc to pull each
`creative_first_seen` row back to its earliest occurrence; occurrence summary
(`crtv_occ_summary_candidate` → `_final`) reads the CDF unioned with the parked buffer
`missing_digital_occurrence_for_summary`, `CALL`s the upsert proc (occurrence counts + first/last run +
7-day), and runs the park/release `MERGE` back into the buffer. Concurrent watermark writes are made safe
by **partitioning `silver.watermark_control` by `watermark_name`** (a single-file control table otherwise
collides under Iceberg optimistic concurrency, which Trino won't retry). Details + all learnings in
`docs/ctv_creative_push.md`.

*Piece 4 — seeding prerequisite (2026-08-05):* before the sync-back can run against clones, production
creative data must be copied into them. `ddl/postgres/piece4_seed_tempwork_ctv_poc.sql` clones the rest of
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
data) validated on the clones; Mode 2 pending the first daily-ingestion run.** Details in `docs/ctv_creative_seed.md`.

*Piece 4 — sync-back, in progress (2026-08-10):* porting the Databricks `SYNC_CREATIVES_TO_DATABRICKS`
job (8 tasks: dedup ∥ first-seen → creative → first-seen-info → occurrence-id → last-seen → component ∥
product-resync) to Trino/dbt-native, reading the seeded `tempwork.*_ctv_poc` clones → Iceberg `gold.*`/`silver.*`.
The two Postgres `get_changes` procs are cloned+retargeted (`ddl/postgres/piece4_sync_procs_ctv_poc.sql`);
10 timestamp watermarks seeded (`ddl/08`); every Databricks Delta `table_changes` read becomes a **column
(timestamp) watermark** scan (Trino `table_changes` is append-only) with UTC discipline + a 1-min no-miss lag.
**Tasks 2 (first-seen), 3 (dedup), and 1 (creative) are VALIDATED end-to-end on the VM.** Task 1 (the heavy
one) is staged — `crtv_sync_creative_forsync` (proc CALL) → `_raw` (schema collapse) → `_revxlate` (vx0→vx1/vx2
reverse translation) → `crtv_sync_creative` (107-col gold MERGE + change log + both hold loops + watermark);
its watermark read is the stage-1 proc call and the advance is a stage-3 non-held-max hook, confirmed with a
full-history reprocess and an idempotent incremental re-run (69 adverts held, expected). Archive is parked; the
two Piece-5-dependent tasks (last-seen, occurrence-id) are built later. Full plan + task-1 learnings (literal
hook relations, Trino MERGE bare LHS, json_object, on_table_exists): `docs/ctv_creative_sync_plan.md` §9–10.

Next up: finish the Piece 4 sync tasks (5 → 4/8), then Piece 5. Some library upgrades are parked as a
future action item (`docs/runbook.md` §6).

**Resuming in a new window?** Start with `docs/runbook.md`, `docs/ctv_ingestion.md`, and `scripts/vm_setup.md`.
