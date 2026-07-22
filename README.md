# CTV Occurrence Flow — dbt + Iceberg PoC

Open-source lakehouse PoC migrating the **CTV occurrence flow** off Azure Databricks to
**dbt + Apache Iceberg** on AWS (Trino + Nessie + S3). Runs as Docker Compose on a single EC2 VM.
Architecture source of truth: Google Drive → `CTV_occurrence_flow_architecture_MASTER_v2`.

## Layout
```
docker-compose.yml     stack: nessie · trino · dbt · ingestion
infra/                 Dockerfiles + Trino/Nessie config
dbt/                   dbt project (bronze sources, silver/gold models, watermark macros)
ingestion/             Python: reference sync (Option C) + CTV ingestion (PyIceberg)
scripts/               vm_setup.md (staged setup) + smoke_test.sh + cron/
docs/                  runbook + architecture pointer
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
Foundation + reference-sync + CTV-ingestion **skeletons**. Configs (Nessie S3/warehouse, image
versions, healthchecks) are starting points to validate at stand-up — see `docs/runbook.md`.
Creative push/sync-back is gated on prod Postgres access.
