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

Mirrors 14 `hive_metastore` Delta reference tables from Azure ADLS into Iceberg on S3 under the
`reference` schema. **The full source→target table inventory (names, ADLS paths, reader, row counts)
is in `docs/reference_tables.md`.** The load is **streamed + atomic**: it clears and reloads each table via one
transaction (delete-all + append batches), so it is memory-bounded (safe for multi-million-row
tables) and never drops the table — a mid-run failure leaves the prior data intact.

Run / operate:
```bash
docker compose exec ingestion python -m ingestion.reference_sync                 # all 14 tables
docker compose exec ingestion python -m ingestion.reference_sync --table origin  # one table
REF_SYNC_BATCH_ROWS=200000 docker compose exec ingestion python -m ingestion.reference_sync  # tune batch (default 1,000,000)
# verify:
docker exec -i trino trino --execute "SELECT count(*) FROM iceberg.reference.data_provider"
docker exec -i trino trino --execute "SELECT table_name FROM iceberg.information_schema.tables WHERE table_schema='reference' ORDER BY 1"
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
binary); tz-aware timestamps → UTC wall-clock **without** zone (Iceberg `timestamp`), so values read
as UTC in every engine/client (matching the source Delta `…Z`). The Iceberg schema is derived from
the normalized Arrow schema per run; additive source changes auto-evolve, type/removed-column
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
- **VARIANT -> string** parsing for CTV query patterns.

## 5. Blocked / later
- **Prod Postgres** (creative push/sync-back) — reachability was BLOCKED (DevOps). When it opens,
  first verify with `bash scripts/pg_connectivity_test.sh` (DNS → TCP host+container → optional
  auth). Then add a Trino Postgres catalog: create `infra/trino/catalog/postgres.properties` with the
  template below, then restart Trino. (Kept out of `catalog/` until now so Trino doesn't try
  to load an unreachable catalog at startup.)
  ```
  connector.name=postgresql
  connection-url=jdbc:postgresql://azeus2-postgres-mrdpp-p-01.vivvix.net:5432/<db>
  connection-user=<user>
  connection-password=<password>
  allow-drop-table=true
  ```
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
