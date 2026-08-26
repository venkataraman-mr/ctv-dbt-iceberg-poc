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
| `docker-compose.yml` (main) | `catalog_postgres` + `polaris` services (start with the stack) |
| `infra/catalog-poc/init-catalog-dbs.sql` | Creates the `lakekeeper` DB (Postgres init) |
| `scripts/polaris_bootstrap.sh` | Creates the Polaris catalog + principal + RBAC grants |
| `scripts/polaris.properties.staged` | Trino → Polaris catalog config (fill creds, then copy live) |
| `scripts/lakekeeper.properties.staged` | Trino → Lakekeeper (for Part B) |
| `scripts/catalog_feature_tests.sql` | v3+VARIANT + DML + views tests (run per catalog) |

---

## Order of operations (Polaris) — and the two meanings of "bootstrap"

Trino is the **last** step and is just a config file — it is *not* "bootstrapped." The sequence:

1. **`docker compose up -d`** → `catalog_postgres` creates the empty `polaris` + `lakekeeper` DBs; `polaris`
   starts and connects to the `polaris` DB.
2. **Polaris self-initializes** *(bootstrap sense #1)* — on first start Polaris creates its **own metastore
   schema + a root principal** inside the `polaris` DB (from `POLARIS_BOOTSTRAP_CREDENTIALS`, or a one-time
   `polaris-admin bootstrap` run — version-dependent; the logs tell us which). This is about **Polaris**, not Trino.
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

1. **Deploy** — the catalog services are in the main compose, so they come up with the stack:
   ```bash
   docker compose up -d                 # brings the whole stack incl. catalog_postgres + polaris
   docker logs polaris --tail 50
   ```
   ⚠️ The `polaris` service env is **version-specific** — if it doesn't start, reconcile the image tag + env
   against the official getting-started (polaris.apache.org/guides/trino). Paste the log and I'll adjust.
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

## Part B — Lakekeeper (after Polaris passes/decides)
To be wired next: add a `lakekeeper` service to the main `docker-compose.yml` (uses the `lakekeeper` DB +
`.../lakekeeper/` S3 prefix), bootstrap a warehouse (UI/endpoint), then `cp scripts/lakekeeper.properties.staged
infra/trino/catalog/lakekeeper.properties` and restart Trino. Same feature tests with `<CATALOG>` = `lakekeeper`.

## Part C — Cross-engine (Spark + Databricks)
- **Spark** (EMR/K8s or local Spark 4.x + iceberg 1.11): `spark.sql.catalog.X.type=rest` at the catalog uri +
  warehouse → create/read v3+VARIANT.
- **Databricks** (non-UC DBR 18, manual attach): same REST config → read a v3+VARIANT table. **The decisive
  Databricks-read test against a real v3 catalog** (never valid against Nessie).

---

## Results matrix (fill in)
| Feature (→ req) | Polaris | Lakekeeper |
| :-- | :-- | :-- |
| v3 CREATE (R1) | | |
| VARIANT create/insert/read (R1) | | |
| Row-level DML | | |
| Views (soft) | | |
| Trino R/W (R2) | | |
| Spark R/W (R2) | | |
| Databricks read (R2) | | |
| RBAC (soft) | | |
| Credential vending (pass 2) | | |
| Deploy on VM/K8s + HA | | |

**Decision** (per `iceberg_catalog_evaluation.md` §7d): a catalog passing v3 CREATE + VARIANT + Trino/Spark R/W
(+ Databricks read or fallback) → adopt it. Neither → escalate for the paid AWS S3 Tables fallback.

## Notes
- `catalog_postgres` + `polaris` are in the main `docker-compose.yml` and start with `docker compose up`. No
  `depends_on` from trino → polaris (lazy catalog load), so a candidate-catalog issue never blocks the pipeline.
- Start with own AWS keys; test vending second — fastest path to the make-or-break v3+VARIANT result.
- `catalog_postgres` data persists in the `catalog_pg_data` named volume; `docker compose down -v` wipes it.
- The Trino **catalog** (`polaris.properties`) is wired only *after* bootstrap (staged → live), even though the
  Polaris **container** starts with the stack — so an un-bootstrapped Polaris never errors Trino.
