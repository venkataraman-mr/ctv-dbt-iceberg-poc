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

## 3. Reference sync (Option C)
- Fill `TABLE_MAP` in `ingestion/reference_sync.py` with the reference tables' abfss paths.
- Run once: `docker compose exec ingestion python -m ingestion.reference_sync`
- Schedule: `scripts/cron/reference_sync.cron`.

## 4. Validate-at-stand-up items (configs here are starting points)
- **Nessie S3 / warehouse property names** for the pinned `NESSIE_VERSION` (Iceberg REST catalog).
- **`table_changes` on delete-file snapshots** — confirm it errors (locks the Half B timestamp-watermark decision).
- **Trino Azure filesystem** props (`fs.native-azure.enabled`) if any table's data stays on ADLS.
- **Healthchecks / image versions** — pin and adjust.
- **VARIANT -> string** parsing for CTV query patterns.

## 5. Blocked / later
- **Prod Postgres** (creative push/sync-back) — reachability BLOCKED (DevOps). When it opens,
  add a Trino Postgres catalog: create `infra/trino/catalog/postgres.properties` with the
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

## Credentials
- AWS: default provider chain reads `AWS_*` from `.env` (currently the `mukesh-s3-only-temp`
  IAM user keys). Swap to the instance-profile role when provisioned, then drop the keys.
- `.env` is **gitignored** — it is NOT committed (GitHub push protection blocks committed keys).
  Copy it to each machine manually (`scp`, or paste). If you want one versioned copy across
  machines, encrypt it (git-crypt / SOPS) or use a secrets manager — don't commit plaintext keys.
