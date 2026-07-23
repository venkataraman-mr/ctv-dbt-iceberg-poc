# CTV Occurrence Flow — dbt + Iceberg PoC — CHECKPOINT / HANDOFF

**Date:** 2026-07-22 · **Milestone:** Foundation validated — two-engine smoke test passing on the VM.

This is the resume document. A new chat/Cowork window should read this first (then `docs/runbook.md`
and `scripts/vm_setup.md`) to pick up exactly where we left off. It records what is standing, the
config that had to be nailed down (and how), the current state of each component, what is blocked,
and the next steps.

---

## 1. What this project is

Prototype migration of the real **CTV occurrence flow** off **Azure Databricks** (PySpark, Unity
Catalog, Delta, medallion) onto an **open-source lakehouse**: **dbt + Apache Iceberg**, query/compute
via **Trino**, catalog via **Nessie**, storage on **S3**, all as **Docker Compose on a single AWS
EC2 VM**. The two business drivers are **cost savings** and **open-source adoption** (avoid
proprietary lock-in). The medallion architecture (bronze → silver → gold) is **retained** and maps to
dbt as sources (bronze) → incremental models (silver) → table-materialized marts (gold).

Architecture source of truth: Google Drive → `CTV_occurrence_flow_architecture_MASTER_v2`
(folder id `1UK1t-JYEPEZu41ZcVZD7gK9szvzZ2kye`).

## 2. Target stack (as running)

| Component | Role | Notes |
|---|---|---|
| **Nessie 0.104.1** | Iceberg catalog (git-like) | RocksDB version store; two entry points — native `/api/v2` (Trino) and Iceberg REST `/iceberg/` (PyIceberg). Auth disabled. |
| **Trino 476** | SQL engine / dbt compute | Native Nessie catalog (`iceberg.catalog.type=nessie`); accesses S3 with its own AWS creds. |
| **dbt (dbt-trino 1.10.1)** | Transformations | `profiles.yml` → Trino; `dbt debug` passes. |
| **ingestion (PyIceberg 0.11.1)** | File ingestion + reference sync | No Spark. Talks to Nessie's Iceberg REST endpoint. |
| **S3** | Storage | bucket `dataplatformpoc-venketa`, warehouse `s3://dataplatformpoc-venketa/warehouse`, region `us-east-2`. |

**Compute path (EMR vs Kubernetes) is still undecided** — this single-VM Docker Compose is the PoC
harness, not the production compute decision.

## 3. Environment specifics

- **VM:** Amazon Linux 2023 EC2 (`ec2-user@ip-10-226-53-68`, elastic IP used for scp `3.145.213.86`).
  Repo cloned at `~/CTV_dbt_iceberg_poc`. Small 8 GB EBS root (~1.8 GB free with the stack up).
- **Git remote:** `https://github.com/venkataraman-mr/ctv-dbt-iceberg-poc.git` (branch `main`).
- **Local dev copy:** `C:\work\CTV_dbt_iceberg_poc` (this folder). Dev workflow = edit locally → commit/push → `git pull` on VM.
- **Nessie endpoints:** native `http://nessie:19120/api/v2`; Iceberg REST `http://nessie:19120/iceberg/`.
- **Secrets:** `.env` holds real AWS + Azure keys. It is **gitignored** (GitHub push protection blocks
  committed keys) and copied to the VM via `scp`. **Never print the secret values.** AWS creds flow
  via the default provider chain (`AWS_*` env); swap to an instance-profile role when provisioned.

## 4. Config gotchas resolved this session (the valuable memory)

The stack came up only after resolving a chain of version/config issues — all now fixed in the repo:

1. **Nessie RocksDB permission crash** (`Permission denied: /nessie/data/LOG`). The non-root Nessie
   container couldn't write to the root-owned bind-mount. Fix: `user: root` on the nessie service in
   `docker-compose.yml`.
2. **Nessie catalog S3 auth** — `Missing access key and secret for STATIC authentication mode`.
   Nessie's Iceberg REST catalog accesses S3 itself and defaults to STATIC auth; it does **not** read
   the `AWS_*` env vars. Fix (official pattern): a Nessie secret URN —
   `nessie.catalog.service.s3.default-options.access-key = urn:nessie-secret:quarkus:nessie.catalog.secrets.access-key`
   with `nessie.catalog.secrets.access-key.name/.secret` sourced from `${AWS_ACCESS_KEY_ID}` /
   `${AWS_SECRET_ACCESS_KEY}` at compose-parse time.
3. **PyIceberg warehouse identifier** — PyIceberg was passing the full `s3://…/warehouse` URI as the
   warehouse, but Nessie's Iceberg REST expects the warehouse **name** (`warehouse`, per
   `nessie.catalog.default-warehouse`). Fix: `config.NESSIE_WAREHOUSE = "warehouse"` in
   `ingestion/config.py`, used by `get_catalog()`.
4. **Nessie vends `py-io-impl=FsspecFileIO`** (the real puzzle). PyIceberg prefers PyArrow for `s3://`,
   but Nessie returns `py-io-impl=pyiceberg.io.fsspec.FsspecFileIO` in each table's config, and
   PyIceberg merges server config **after** client props — so Nessie's choice won and forced fsspec,
   which needs `s3fs`. `s3fs` drags in `aiobotocore` + an exact `fsspec` pin and wrecks dependency
   resolution. Fix: **override client-side** — `ingestion/common/catalog.py::force_pyarrow_io()`
   rebuilds the table's FileIO as PyArrow (installed, no extra deps). Applied in `load_table()` and in
   `reference_sync.py`. Also pinned `pyarrow==20.0.0` and dropped the unused `boto3` / the `s3fs`
   extra from `ingestion/requirements.txt`.
5. **Build/tooling fixes:** installed `docker buildx` (compose build needs ≥ 0.17); bumped
   `deltalake` 0.25.0 → 0.25.5; `.gitattributes` (`* text=auto eol=lf`) + a CRLF strip on `.env` to
   stop Windows line endings breaking bash on the Linux VM; `.env` reverted from committed → gitignored
   after GitHub push protection blocked the keys.

See `docs/runbook.md` §4 for the condensed "validated" list.

## 5. Current component state

- `docker compose ps` → **nessie, trino, dbt, ingestion all Up** (trino healthy).
- **Smoke test `bash scripts/smoke_test.sh` PASSES:** Trino creates schema/table + writes to S3 via
  Nessie; PyIceberg (`table io: PyArrowFileIO`) appends to the **same** table; Trino reads back the
  `written-by-pyiceberg` row; `dbt debug` → "All checks passed!". Data/metadata land under
  `s3://dataplatformpoc-venketa/warehouse/`.
- The `iceberg.bronze.smoke` table accumulates rows across runs — drop it to reset:
  `docker exec -i trino trino --execute "DROP TABLE iceberg.bronze.smoke"`.

## 6. Repo layout (key files)

```
docker-compose.yml                     4 services; nessie env holds the S3 secret-URN config
infra/Dockerfile.ingestion|dbt         images (ingestion COPYs requirements; code is bind-mounted)
infra/trino/catalog/iceberg.properties Trino native Nessie catalog + native S3
ingestion/config.py                    env-driven config (region, bucket, warehouse NAME, Nessie URIs)
ingestion/common/catalog.py            get_catalog(), force_pyarrow_io(), load_table()  ← the io fix
ingestion/reference_sync.py            Option C: Azure Delta → Iceberg on S3 (TABLE_MAP EMPTY)
ingestion/requirements.txt             pyiceberg[pyarrow]==0.11.1, pyarrow==20.0.0, deltalake, duckdb
dbt/profiles.yml, dbt/macros/watermark.sql   Trino profile + version-based watermark macros
scripts/vm_setup.md                    staged manual setup (git→docker→build→up→verify→smoke)
scripts/smoke_test.sh                  two-engine proof
docs/runbook.md                        stand-up, smoke, reference sync, validated-config list, blocked items
docs/CHECKPOINT_2026-07-22.md          THIS FILE
.env                                   REAL secrets — gitignored, scp'd to VM (never print)
```

## 7. Blocked / deferred

- **Prod Postgres** (creative push/sync-back) — VM → `azeus2-postgres-mrdpp-p-01.vivvix.net:5432`
  reachability BLOCKED (DevOps). When open: add `infra/trino/catalog/postgres.properties` (template in
  runbook §5) and restart Trino.
- **EBS disk** — 8 GB root is tight; grow when convenient (DevOps).
- **Streaming ingestion** — out of scope; file-based is the priority.

## 8. Next steps (in order)

1. **Reference sync (Option C):** fill `TABLE_MAP` in `ingestion/reference_sync.py` with the reference
   tables' `abfss://` paths, run one table end-to-end
   (`docker compose exec ingestion python -m ingestion.reference_sync --table <name>`), verify the
   Iceberg table appears in the `reference` schema and is readable from Trino.
2. **First real silver/gold models:** port a slice of the CTV occurrence flow into dbt using the
   watermark macros (Half A / Half B), materializing silver as incremental and gold as tables.
3. **Postgres creative flow:** once reachability opens, wire the Trino Postgres catalog and the
   clone-table push / sync-back.

## 9. How to resume in a new window

1. Open the folder `C:\work\CTV_dbt_iceberg_poc` in Cowork (it's the git working copy = the memory).
2. Read this file, then `docs/runbook.md` and `scripts/vm_setup.md`.
3. On the VM to bring the stack up / re-verify:
   ```bash
   cd ~/CTV_dbt_iceberg_poc && git pull
   docker compose up -d && docker compose ps
   bash scripts/smoke_test.sh
   ```
4. Dev loop: edit locally → commit & push → `git pull` on the VM → run. Code under `ingestion/` and
   `dbt/` is bind-mounted (a `git pull` is enough; no rebuild). Only `ingestion/requirements.txt`
   changes need `docker compose build ingestion`.
5. Keep the Databricks→dbt+Iceberg project instructions in mind: explain new tools by mapping to
   Databricks/UC/PySpark; retain medallion; prefer open-source and call out cost/lock-in tradeoffs;
   ground work in the real pipeline; search for current-state facts (versions/adapters move fast).
