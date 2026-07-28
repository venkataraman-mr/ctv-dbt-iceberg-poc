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
**Foundation + Reference sync + Postgres connectivity + CTV ingestion (Piece 1) VALIDATED.**

*Foundation (2026-07-22):* the full stack (nessie · trino · dbt · ingestion) builds and runs on the
EC2 VM, and the two-engine smoke test passes — Trino and PyIceberg both read/write one Iceberg table
through a single Nessie catalog on S3, and dbt connects to Trino. Config nailed down at stand-up (in
`docs/runbook.md` §4): Nessie RocksDB runs as root; its catalog S3 uses STATIC auth via a secret URN;
PyIceberg passes the warehouse *name*; and Nessie vends `py-io-impl=FsspecFileIO`, overridden to
PyArrow client-side (`force_pyarrow_io`).

*Reference sync — Option C (2026-07-25):* all **14** hive_metastore Delta reference tables mirror to
`iceberg.<db>.<table>` — the target schema is the source database name (`km_preparation_db`,
`km_preparation_gold_db`, `productcentral`) — via a streamed, atomic clean reload (memory-bounded,
never drops the table). delta-rs is the primary reader; DuckDB is the fallback for deletion-vector /
v2Checkpoint tables (with the libcurl Azure transport for TLS). Binary→base64 and UTC-with-time-zone
timestamp normalization keep the output clean. Daily cron is ready. Details in `docs/runbook.md` §3.

*Prod Postgres + Trino catalog (2026-07-26):* the cross-cloud path (previously blocked) is open and
authenticated (verified via `scripts/pg_connectivity_test.sh` — psql OK as `databricks_admin_user` @
`vxcentral`, PostgreSQL 16.4). The Trino `postgres` catalog is wired
(`infra/trino/catalog/postgres.properties`, creds via `${ENV:PG_*}` — no secrets committed), so the
creative flow (Pieces 3–4) is unblocked. Details in `docs/runbook.md` §5.

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

Next up: Pieces 2–5 (silver/gold + creative flow) in order. Some library upgrades are parked as a
future action item (`docs/runbook.md` §6).

**Resuming in a new window?** Start with `docs/runbook.md`, `docs/ctv_ingestion.md`, and `scripts/vm_setup.md`.
