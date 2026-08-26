# Catalog PoC runbook — Polaris & Lakekeeper on the VM

Stand up Apache Polaris and Lakekeeper **alongside the existing stack** (one Trino, many catalogs) and test the
requirements. Decision framework + landscape: [`iceberg_catalog_evaluation.md`](iceberg_catalog_evaluation.md).

## Design (settled)
- **One Trino, many catalogs** — add `polaris` (then `lakekeeper`) next to the existing `iceberg` / `iceberg_rest`
  / `unity_catalog` / `postgres`.
- **Shared S3 bucket, per-catalog prefix:** `s3://dataplatformpoc-venketa/polaris/`, `.../lakekeeper/`
  (Nessie keeps `warehouse/`).
- **Metadata store:** one **Postgres 16** container, a database per catalog (`polaris`, `lakekeeper`), data on a
  named volume (EBS-backed) — the durability analog of Nessie's RocksDB-on-EBS. → RDS in production.
- **S3 credentials:** Trino uses its **own AWS keys** first (`fs.native-s3`, same as Nessie); credential
  **vending** is a pass-2 test.
- Catalog services (`catalog_postgres`, `polaris`, later `lakekeeper`) live in the **main `docker-compose.yml`**
  and start with the stack — they're candidates to **replace Nessie**, so they're treated as first-class, not a
  throwaway overlay. (No `depends_on` from trino → them: Iceberg catalogs load lazily, so a candidate-catalog
  issue never blocks the pipeline.)

## Files
| File | Purpose |
| :-- | :-- |
| `docker-compose.yml` (main) | `catalog_postgres`, `polaris` (+ `polaris_bootstrap`), `lakekeeper` (+ `lakekeeper_migrate`) services |
| `infra/catalog-poc/init-catalog-dbs.sql` | Creates the `lakekeeper` DB (Postgres init) |
| `scripts/polaris_bootstrap.sh` | Creates the Polaris catalog + principal + RBAC grants |
| `scripts/polaris.properties.staged` | Trino → Polaris catalog config (creds via `${ENV:POLARIS_OAUTH2_CREDENTIAL}`) |
| `scripts/lakekeeper_bootstrap.sh` | Bootstraps Lakekeeper + creates the `ctv_lakekeeper` S3 warehouse |
| `scripts/lakekeeper.properties.staged` | Trino → Lakekeeper catalog config |
| `scripts/catalog_feature_tests.sql` | v3+VARIANT + DML + views tests (`<CATALOG>`/`<SCHEMA>` template) |
| `scripts/catalog_feature_tests_polaris.sql` / `_lakekeeper.sql` | Pre-filled per-catalog test scripts (DBeaver) |

---

## Order of operations (Polaris) — and the two meanings of "bootstrap"

Trino is the **last** step and is just a config file — it is *not* "bootstrapped." The sequence:

1. **`docker compose up -d`** → `catalog_postgres` comes up (creates the empty `polaris` + `lakekeeper` DBs) and
   becomes healthy.
2. **Polaris schema bootstrap** *(bootstrap sense #1 — now automated)* — the one-shot `polaris_bootstrap` service
   (`apache/polaris-admin-tool`) runs `bootstrap -r POLARIS -c POLARIS,root,<secret>`, which creates Polaris'
   **metastore schema (`polaris_schema.*`) + the root principal** inside the `polaris` DB, then exits. **Polaris
   1.7 does NOT self-create this on startup** — `POLARIS_BOOTSTRAP_CREDENTIALS` on the server only makes the root
   creds *available*; without the admin-tool run the first API call fails with
   `relation "polaris_schema.entities" does not exist`. The `polaris` server then starts (it `depends_on` the
   bootstrap completing). Idempotent in 1.7, so it's safe on every `up`. This is about **Polaris**, not Trino.
3. **Catalog bootstrap** *(bootstrap sense #2 — `scripts/polaris_bootstrap.sh`)* — with the root creds, create a
   **catalog `ctv_poc`** (→ `s3://…/polaris`), a **principal `trino_poc`**, and **roles/grants**. Output: the
   `trino_poc` **client_id:client_secret**. Still entirely inside Polaris.
4. **Wire Trino (last)** — put those creds in `polaris.properties`, copy it into `infra/trino/catalog/`, restart
   Trino. `SHOW CATALOGS` now includes `polaris` — just like `iceberg.properties` points Trino at Nessie. Trino
   isn't "bootstrapped"; it just gets a catalog config pointing at the already-set-up Polaris.

Dependency chain: `polaris` needs `catalog_postgres`; the bootstrap script needs `polaris` healthy; the Trino
wiring needs the principal creds from step 3. The detailed commands for each are in Part A below.

---

## Part A — Apache Polaris (first)

1. **Deploy** — the catalog services are in the main compose, so they come up with the stack. Order is enforced
   by `depends_on`: `catalog_postgres` (healthy) → `polaris_bootstrap` (runs once, exits) → `polaris`:
   ```bash
   docker compose up -d                 # catalog_postgres -> polaris_bootstrap -> polaris, all with the stack
   docker logs polaris_bootstrap        # expect "Realm 'POLARIS' successfully bootstrapped."
   docker logs polaris --tail 50        # expect "started ... Listening on http://0.0.0.0:8181"
   # verify the realm is live (should return an access_token, not a 500):
   curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
     -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
     -d scope=PRINCIPAL_ROLE:ALL ; echo
   ```
   Validated on **Polaris 1.7.0** (server + admin-tool via `latest`, 2026-08-26). If you ever bootstrap manually
   (e.g. against a different realm), the equivalent one-off is:
   ```bash
   NET=$(docker inspect polaris -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
   docker run --rm --network "$NET" \
     -e polaris.persistence.type=relational-jdbc -e quarkus.datasource.db-kind=postgresql \
     -e quarkus.datasource.jdbc.url=jdbc:postgresql://catalog_postgres:5432/polaris \
     -e quarkus.datasource.username=catalog -e quarkus.datasource.password=catalogpoc \
     apache/polaris-admin-tool:1.7.0 bootstrap -r POLARIS -c POLARIS,root,s3cr3t
   ```
2. **Bootstrap** the catalog + a Trino principal + grants:
   ```bash
   bash scripts/polaris_bootstrap.sh        # creates catalog `ctv_poc` -> s3://.../polaris, principal `trino_poc`
   ```
   Capture the `trino_poc` **client_id:client_secret** from step 3 of the script output.
3. **Wire Trino:** put those creds into `scripts/polaris.properties.staged`
   (`iceberg.rest-catalog.oauth2.credential=<id>:<secret>`), then activate it:
   ```bash
   cp scripts/polaris.properties.staged infra/trino/catalog/polaris.properties
   docker compose restart trino
   docker exec -i trino trino --execute "SHOW CATALOGS"     # expect `polaris` in the list
   ```
4. **Feature tests** (DBeaver on Trino): run `scripts/catalog_feature_tests.sql` with `<CATALOG>` = `polaris`,
   `<SCHEMA>` = `ctv_catalog_poc`. Record pass/fail per block in the matrix below.
5. **RBAC test:** create a 2nd principal with a **read-only** catalog role (see the note at the end of
   `polaris_bootstrap.sh`); connect Trino as it; verify SELECT works but INSERT is denied.
6. **Credential vending (pass 2):** flip `polaris.properties` to the vended-creds block (commented in the file),
   restart Trino, re-run the tests with no S3 keys.

## Part B — Lakekeeper

Same "one Trino, many catalogs" model. Lakekeeper reuses `catalog_postgres` (the `lakekeeper` DB) and the shared
bucket (`.../lakekeeper/` prefix). It listens on 8181 internally like Polaris, so it's published on **host 8282**
(`8282:8181`) — Polaris keeps host 8181. Endpoints: `/catalog` (Iceberg REST), `/management`, `/ui`.

Lifecycle mirrors Polaris but with Lakekeeper's own tooling: **migrate (schema) → serve → bootstrap (admin +
project) → create warehouse (S3 storage) → wire Trino → test**. This PoC runs Lakekeeper **unsecured** (no
OpenID); clients send a throwaway bearer token. Storage uses an **access-key credential with STS off** (no IAM
role) — Lakekeeper then does S3 **remote signing** (the Polaris skip-subscoping analog).

1. **Deploy** — services are in the main compose; order is enforced `catalog_postgres` (healthy) →
   `lakekeeper_migrate` (runs once, exits) → `lakekeeper`:
   ```bash
   docker compose up -d
   docker logs lakekeeper_migrate      # expect a successful migration, then exit 0
   docker logs lakekeeper --tail 40    # expect it serving on :8181 (published to host 8282)
   curl -s http://localhost:8282/health ; echo    # or the server-info endpoint
   ```
   ⚠️ Image tag + `LAKEKEEPER__*` env are **version-specific** — if it doesn't start, reconcile against
   docs.lakekeeper.io and paste `docker logs lakekeeper`.
2. **Bootstrap + warehouse:**
   ```bash
   bash scripts/lakekeeper_bootstrap.sh    # bootstraps the server + creates warehouse `ctv_lakekeeper`
   ```
3. **Wire Trino:**
   ```bash
   cp scripts/lakekeeper.properties.staged infra/trino/catalog/lakekeeper.properties
   docker compose up -d trino
   docker exec -i trino trino --execute "SHOW CATALOGS"     # expect `lakekeeper`
   ```
4. **Feature tests:** run `scripts/catalog_feature_tests_lakekeeper.sql` (or the `<CATALOG>` template with
   `lakekeeper` / `ctv_catalog_poc`) top-to-bottom in DBeaver. Record pass/fail per block in the matrix.
5. **RBAC + cross-cloud** as for Polaris (Lakekeeper uses OpenFGA/Cedar authz — soft req; the Databricks
   cross-cloud test is on hold pending the 8181/8282 firewall opening).

## Part C — Cross-engine (Spark + Databricks)
- **Spark** (EMR/K8s or local Spark 4.x + iceberg 1.11): `spark.sql.catalog.X.type=rest` at the catalog uri +
  warehouse → create/read v3+VARIANT.
- **Databricks** (non-UC DBR 18, manual attach): same REST config → read a v3+VARIANT table. **The decisive
  Databricks-read test against a real v3 catalog** (never valid against Nessie).

---

## Results matrix
Legend: ✅ pass · ⏳ pending · 🚧 blocked · ➖ not applicable. Tested on Polaris 1.7.0, Lakekeeper 0.13.3, Trino 483 (2026-08-26).

| Feature (→ req) | Polaris | Lakekeeper |
| :-- | :-- | :-- |
| v3 CREATE (R1) | ✅ | ✅ |
| VARIANT create/insert/read (R1) | ✅ | ✅ |
| Row-level DML (UPDATE/DELETE/MERGE) | ✅ | ✅ |
| Views (soft) | ✅ | ✅ |
| DROP table/view | ✅ (needs DROP_WITH_PURGE_ENABLED) | ✅ (purge via LK background task) |
| Trino R/W (R2) | ✅ | ✅ |
| Spark R/W (R2) | ⏳ | ⏳ |
| Databricks CRUD incl v3+VARIANT (R2) | 🚧 firewall 8181 | 🚧 firewall 8282 |
| RBAC (soft) | ⏳ (Polaris grants) | ⏳ (OpenFGA/Cedar) |
| Storage auth used | own keys (skip-subscoping; no IAM role) | own keys (Trino 483 doesn't consume LK remote signing) |
| Credential vending (pass 2) | ⏳ needs roleArn | ➖ needs STS role (remote signing not consumed by Trino 483) |
| Deploy on VM | ✅ running | ✅ running |

**Read so far:** both OSS catalogs meet the v3+VARIANT + external-Trino-R/W hard requirements that Nessie failed.
Neither did credential vending in this PoC (no IAM role) — both fell back to Trino's own S3 keys. The decisive
Databricks-CRUD test (incl. v3+VARIANT) is pending the 8181/8282 firewall opening.

**Decision** (per `iceberg_catalog_evaluation.md` §7d): a catalog passing v3 CREATE + VARIANT + Trino/Spark R/W
(+ Databricks read or fallback) → adopt it. Neither → escalate for the paid AWS S3 Tables fallback.

## Notes
- `catalog_postgres` + `polaris` are in the main `docker-compose.yml` and start with `docker compose up`. No
  `depends_on` from trino → polaris (lazy catalog load), so a candidate-catalog issue never blocks the pipeline.
- Start with own AWS keys; test vending second — fastest path to the make-or-break v3+VARIANT result.
- `catalog_postgres` data persists in the `catalog_pg_data` named volume; `docker compose down -v` wipes it.
- The Trino **catalog** (`polaris.properties`) is wired only *after* bootstrap (staged → live), even though the
  Polaris **container** starts with the stack — so an un-bootstrapped Polaris never errors Trino.
