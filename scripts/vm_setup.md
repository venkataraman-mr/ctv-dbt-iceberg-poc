# VM Setup — manual, stage by stage

Run each stage **one at a time** and check the "✓ expect" note before moving on, so any error
is caught at its stage. Amazon Linux 2023. Run from the repo root unless noted.

> **Disk space matters** — Docker images for this stack total several GB. Check space at every
> heavy stage (marked 💾). Keep **≥ ~10 GB free** on `/`. If low, clean up (Stage 9) or grow the
> EBS volume before continuing.

---

## Stage 0 — Install git, then get the repo on the VM
git is not preinstalled on Amazon Linux 2023, so install it first:
```bash
sudo dnf install -y git
git --version
git clone https://github.com/venkataraman-mr/ctv-dbt-iceberg-poc.git ~/CTV_dbt_iceberg_poc
cd ~/CTV_dbt_iceberg_poc
ls -a && git status
```
✓ expect: a git version prints, then the project files (`docker-compose.yml`, `infra/`, `dbt/`,
`ingestion/`, …). Note: **`.env` is NOT in git** (it holds secrets) — you create it in Stage 4.

## Stage 1 — 💾 Baseline disk check
```bash
df -h /
```
✓ expect: **Avail ≥ ~10 GB**. If less, grow the EBS volume (or clean up) before installing Docker —
image pulls will fail with "no space left on device" otherwise.

## Stage 2 — Install Docker
```bash
sudo dnf install -y docker      # git already installed in Stage 0
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
docker --version
```
✓ expect: a Docker version prints. Then **log out and back in** (or `newgrp docker`) so the group
applies, and confirm you can talk to Docker **without sudo**:
```bash
docker info >/dev/null && echo "docker OK (no sudo)"
```

## Stage 3 — Install Docker Compose v2
```bash
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
docker compose version
```
✓ expect: a Compose v2 version prints.

## Stage 4 — Create `.env` + the Nessie data dir
`.env` is **not** in the repo (it holds secrets — GitHub blocks committed keys), so put it on the
VM manually: copy your local `.env` across (`scp`, or paste into `nano .env`). Then verify + make
the data dir:
```bash
test -f .env && echo ".env present" || echo "!! create .env first (copy from your local copy)"
# confirm the required secrets are filled (only PG_* placeholders may remain for now):
grep -E 'REPLACE_(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AZURE_STORAGE_ACCOUNT_KEY)' .env \
  && echo "!! fill these in .env first" || echo "AWS/Azure secrets set"
# create the Nessie RocksDB dir (on the EBS-backed disk):
mkdir -p "$(grep -E '^NESSIE_DATA_DIR=' .env | cut -d= -f2-)"
```
✓ expect: ".env present", "AWS/Azure secrets set", and the dir created.

## Stage 5 — 💾 Pre-build disk check
```bash
df -h /
docker system df        # shows how much Docker is already using
```
✓ expect: still **≥ ~10 GB free**. `docker system df` should be near-empty on a fresh VM.

## Stage 6 — Build the images (heaviest step)
```bash
docker compose build
```
✓ expect: dbt + ingestion images build with no errors. If a pip pin fails, note the package and
we bump it. **Then re-check space:**
```bash
df -h /
docker images
```
💾 ✓ expect: still comfortable free space; `nessie`, `trino`, `dbt`, `ingestion` images listed.

## Stage 7 — Start the stack
```bash
docker compose up -d
docker compose ps
```
✓ expect: all four services **Up** (not `Restarting`). If one restarts, read its logs before
continuing, e.g.:
```bash
docker compose logs nessie | tail -50
docker compose logs trino  | tail -50
```

## Stage 8 — Verify each service individually (catch errors per component)
```bash
# Nessie alive:
curl -fs http://localhost:19120/api/v2/config | head -c 300; echo
# Trino accepting queries:
docker exec trino trino --execute "SELECT 1"
# ingestion package imports (PyIceberg catalog loader):
docker exec ingestion python -c "import ingestion.common.catalog; print('ingestion import OK')"
# dbt can reach Trino:
docker compose exec dbt dbt debug
```
✓ expect: Nessie returns JSON; Trino returns `1`; "ingestion import OK"; dbt "All checks passed!".

## Stage 9 — Smoke test (run pieces individually)
```bash
# a) Trino writes to S3 via Nessie
docker exec -i trino trino <<'SQL'
CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
CREATE TABLE IF NOT EXISTS iceberg.bronze.smoke (id BIGINT, note VARCHAR);
INSERT INTO iceberg.bronze.smoke VALUES (1, 'written-by-trino');
SELECT * FROM iceberg.bronze.smoke;
SQL

# b) PyIceberg appends to the SAME table via Nessie  (the key cross-engine check)
docker exec -i ingestion python - <<'PY'
import pyarrow as pa
from ingestion.common.catalog import get_catalog
t = get_catalog().load_table(("bronze", "smoke"))
t.append(pa.table({"id": [2], "note": ["written-by-pyiceberg"]}))
print("pyiceberg rows now:", t.scan().to_arrow().num_rows)
PY

# c) Trino reads PyIceberg's row back
docker exec trino trino --execute "SELECT * FROM iceberg.bronze.smoke ORDER BY id;"

# d) confirm files landed on S3
aws s3 ls s3://dataplatformpoc-venketa/warehouse/ --recursive | head
```
✓ expect: two rows (Trino + PyIceberg) visible from Trino, and objects under `warehouse/` on S3.
(If (b) fails on the catalog, that's the Nessie `nessie.catalog.*` config to validate — send the error.)
The equivalent all-in-one is `bash scripts/smoke_test.sh`.

## Stage 10 — 💾 Ongoing space checks & cleanup
Check periodically (rebuilds and Iceberg-metadata churn add up):
```bash
df -h /
docker system df
```
If space gets tight, reclaim unused Docker data (careful — removes stopped/unused items):
```bash
docker image prune -f          # dangling images only (safe)
docker system prune -f         # + stopped containers, unused networks
# docker system prune -af      # AGGRESSIVE: also removes unused images (re-pull/rebuild needed)
```
Nessie state (`nessie-data/`) and any local logs also grow — keep an eye on them.
