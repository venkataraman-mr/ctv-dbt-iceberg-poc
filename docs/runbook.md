# Runbook — CTV dbt+Iceberg PoC

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
