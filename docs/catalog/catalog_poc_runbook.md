# Catalog PoC runbook — Iceberg catalogs on AWS (Nessie, Polaris, Lakekeeper)

Stand up and test open-source Iceberg REST catalogs on the single AWS VM, **alongside the existing stack** (one
Trino, many catalogs). The incumbent **Nessie** is the baseline that failed the v3+VARIANT hard requirement;
**Apache Polaris** and **Lakekeeper** are the candidates that replace it. This runbook covers deploy → bootstrap →
wire Trino → feature tests → cross-cloud (Databricks) for both candidates, plus the config fixes we hit along the
way (see **Gotchas & fixes** at the end). Decision framework + full catalog landscape:
[`iceberg_catalog_evaluation.md`](iceberg_catalog_evaluation.md). Databricks cross-cloud details:
[`../../databricks/README_catalog_crosscloud.md`](../../databricks/README_catalog_crosscloud.md).

**Status (2026-08-26):** both Polaris (1.7.0) and Lakekeeper (0.13.3) **pass every hard requirement** — v3+VARIANT,
external Trino R/W, and Databricks cross-cloud CRUD incl. v3+VARIANT. Nessie is ruled out. See the results matrix.

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
| `scripts/catalog/polaris/init-catalog-dbs.sql` | Creates the `lakekeeper` DB (Postgres init) |
| `scripts/catalog/polaris/polaris_bootstrap.sh` | Creates the Polaris catalog + principal + RBAC grants |
| `scripts/catalog/polaris/polaris.properties.staged` | Trino → Polaris catalog config (creds via `${ENV:POLARIS_OAUTH2_CREDENTIAL}`) |
| `scripts/catalog/lakekeeper/lakekeeper_bootstrap.sh` | Bootstraps Lakekeeper + creates the `ctv_lakekeeper` S3 warehouse |
| `scripts/catalog/lakekeeper/lakekeeper.properties.staged` | Trino → Lakekeeper catalog config |
| `scripts/catalog/catalog_feature_tests.sql` | v3+VARIANT + DML + views tests (`<CATALOG>`/`<SCHEMA>` template) |
| `scripts/catalog/polaris/catalog_feature_tests_polaris.sql`, `scripts/catalog/lakekeeper/catalog_feature_tests_lakekeeper.sql` | Pre-filled per-catalog test scripts (DBeaver) |
| `scripts/catalog/polaris/polaris_crossengine_verify.sql`, `scripts/catalog/lakekeeper/lakekeeper_crossengine_verify.sql` | Trino-side cross-engine round-trip with Databricks |
| `scripts/catalog/nessie/test_trino_v3_variant.sql` / `.sh` | Trino v3+VARIANT capability test on the native Nessie (`iceberg`) catalog |
| `../../databricks/README_catalog_crosscloud.md` | Databricks cross-cloud runbook (Nessie/Polaris/Lakekeeper) + notebooks |

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
3. **Catalog bootstrap** *(bootstrap sense #2 — `scripts/catalog/polaris/polaris_bootstrap.sh`)* — with the root creds, create a
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
   bash scripts/catalog/polaris/polaris_bootstrap.sh        # creates catalog `ctv_poc` -> s3://.../polaris, principal `trino_poc`
   ```
   Capture the `trino_poc` **client_id:client_secret** from step 3 of the script output.
3. **Wire Trino:** put those creds into `scripts/catalog/polaris/polaris.properties.staged`
   (`iceberg.rest-catalog.oauth2.credential=<id>:<secret>`), then activate it:
   ```bash
   cp scripts/catalog/polaris/polaris.properties.staged infra/trino/catalog/polaris.properties
   docker compose up -d trino          # `up -d` (not `restart`) so the new POLARIS_OAUTH2_CREDENTIAL env loads
   docker exec -i trino trino --execute "SHOW CATALOGS"     # expect `polaris` in the list
   ```
4. **Feature tests** (DBeaver on Trino): run `scripts/catalog/catalog_feature_tests.sql` with `<CATALOG>` = `polaris`,
   `<SCHEMA>` = `ctv_catalog_poc`. Record pass/fail per block in the matrix below.
5. **RBAC test:** create a 2nd principal with a **read-only** catalog role (see the note at the end of
   `polaris_bootstrap.sh`); connect Trino as it; verify SELECT works but INSERT is denied.
6. **Credential vending (pass 2):** flip `polaris.properties` to the vended-creds block (commented in the file),
   restart Trino, re-run the tests with no S3 keys.

**Polaris storage config (already set in `docker-compose.yml`, learned the hard way):**
- `polaris.features."SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION" = true` — without an IAM role, Polaris otherwise
  tries `sts:AssumeRole` to vend scoped creds on table create and fails with **"Failed to create transaction"**.
  This flag makes it skip STS and use its own ambient AWS creds; Trino/Spark then use their own keys.
- `polaris.features."DROP_WITH_PURGE_ENABLED" = true` — Trino's `DROP` sends `purgeRequested=true`; Polaris blocks
  purge by default (**403 "Unable to purge entity"**). Enabling it gives clean teardown. (Production may leave
  this OFF so a DROP only removes the catalog pointer and S3 data survives.)
- Trino's OAuth2 credential is read from `${ENV:POLARIS_OAUTH2_CREDENTIAL}` (in `.env`, gitignored) so no secret
  lands in a committed file. Recreate Trino with `docker compose up -d trino` (not `restart`) when adding that env.

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
2. **Bootstrap + warehouse:** the script needs AWS creds in the shell, so source `.env` first. Re-running is
   safe — the server-bootstrap step returns a harmless `CatalogAlreadyBootstrapped` (400) and the script
   continues to (re)create the warehouse.
   ```bash
   set -a; source .env; set +a
   bash scripts/catalog/lakekeeper/lakekeeper_bootstrap.sh    # bootstraps the server + creates warehouse `ctv_lakekeeper`
   ```
   ⚠️ The script uses `LK_`-prefixed variables (`LK_WAREHOUSE`, `LK_BUCKET`, `LK_PREFIX`) precisely because a
   plain `WAREHOUSE` collides with Nessie's `WAREHOUSE` in `.env` — with the collision the warehouse gets named
   `s3://…/warehouse` instead of `ctv_lakekeeper`.
3. **Wire Trino:**
   ```bash
   cp scripts/catalog/lakekeeper/lakekeeper.properties.staged infra/trino/catalog/lakekeeper.properties
   docker compose up -d trino
   docker exec -i trino trino --execute "SHOW CATALOGS"     # expect `lakekeeper`
   ```
   Trino uses its **own S3 keys** here: Lakekeeper's warehouse (access-key + STS off) does S3 **remote signing**,
   which **Trino 483 does not consume** (it fails with `accessKey is null` if asked to). So `lakekeeper.properties`
   sets `fs.native-s3` + `s3.region` and does *not* enable vended credentials.
4. **Feature tests:** run `scripts/catalog/lakekeeper/catalog_feature_tests_lakekeeper.sql` (or the `<CATALOG>` template with
   `lakekeeper` / `ctv_catalog_poc`) top-to-bottom in DBeaver. Record pass/fail per block in the matrix.
5. **RBAC + cross-cloud:** Lakekeeper authz is OpenFGA/Cedar (soft req). Databricks cross-cloud CRUD incl.
   v3+VARIANT **passed** — see Part C and `../../databricks/README_catalog_crosscloud.md`. Note the **BASE_URI**
   requirement below.

**Lakekeeper BASE_URI (the cross-cloud gotcha):** Lakekeeper advertises `LAKEKEEPER__BASE_URI` in `GET /config`,
and a *fresh* external client (Databricks) **follows it** — so for the Databricks test `BASE_URI` must be the VM
**public** URL (`http://<VM>:8282`), set via `LAKEKEEPER_BASE_URI` in `.env`. Trino does **not** follow it (it
keeps its configured internal `LAKEKEEPER_URI=http://lakekeeper:8181/catalog`), so **BASE_URI public + Trino URI
internal** lets both engines work at once. Do NOT point Trino's URI at the public IP — the VM can't reach its own
public 8282 (SG allows only Databricks' source) and Trino hangs.

## Part C — Cross-cloud / cross-engine (Databricks + Spark)

**Databricks (done — PASS for both catalogs).** Non-UC DBR 18 LTS cluster + manual Spark Iceberg REST attach
(UC can't federate a generic REST catalog). Full CRUD incl. **v3+VARIANT** works against both Polaris and
Lakekeeper, plus cross-engine round-trip (a table one engine writes, the other reads). Full steps, cluster Spark
config, and per-catalog auth are in [`../../databricks/README_catalog_crosscloud.md`](../../databricks/README_catalog_crosscloud.md).
Key points learned:
- **Cluster Spark config, not the notebook** — `spark.sql.extensions` is a *static* config (`spark.conf.set`
  fails with `CANNOT_MODIFY_STATIC_CONFIG`); register catalogs in the cluster Spark config and restart.
- **`client.region` is required** — Iceberg's AWS client factory reads `client.region`, not `s3.region`; without
  it the executor S3 write fails "Unable to load region from any of the providers".
- **Own S3 keys** (no vending), same as Trino. Auth: Polaris = OAuth2 `trino_poc` creds; Lakekeeper = static
  bearer `dummy` (unsecured) + BASE_URI = public.
- Trino-side cross-engine verification: `scripts/catalog/polaris/polaris_crossengine_verify.sql` /
  `scripts/catalog/lakekeeper/lakekeeper_crossengine_verify.sql`.

**Standalone Spark (EMR/K8s) — not yet run.** Same REST config (`spark.sql.catalog.X.type=rest` at the catalog
uri + warehouse) would create/read v3+VARIANT. Databricks already exercises the Spark/Iceberg path, so this is a
low-priority confirmation for the target compute (EMR vs K8s still undecided).

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
| Databricks CRUD incl v3+VARIANT (R2) | ✅ (DBR 18, non-UC, own keys) | ✅ (DBR 18, non-UC, own keys) |
| Cross-engine (Trino <-> Databricks) | ✅ | ✅ |
| RBAC (soft) | ⏳ (Polaris grants) | ⏳ (OpenFGA/Cedar) |
| Storage auth used | own keys (skip-subscoping; no IAM role) | own keys (Trino 483 doesn't consume LK remote signing) |
| Credential vending (pass 2) | ⏳ needs roleArn | ➖ needs STS role (remote signing not consumed by Trino 483) |
| Deploy on VM | ✅ running | ✅ running |

**Result:** both OSS catalogs pass **every hard requirement** — v3 CREATE, VARIANT create/insert/read, row-level
DML, views, DROP, external-Trino-R/W, **and Databricks cross-cloud CRUD incl. v3+VARIANT** (DBR 18 LTS, non-UC
cluster, own S3 keys), plus cross-engine round-trip (a table one engine writes, the other reads). This is exactly
the set Nessie failed. Neither did credential vending in this PoC (no IAM role) — both used Trino/Spark own keys.

Behavioural differences to weigh for the decision (none block a hard req):
- **Drop cleanup:** Polaris needs `DROP_WITH_PURGE_ENABLED`; Lakekeeper purges via a background task.
- **S3 auth:** Polaris skip-subscoping; Lakekeeper's native remote signing isn't consumed by Trino 483 (own-keys).
- **Cross-cloud networking:** Polaris advertises relative paths, so internal Trino and external Databricks each
  keep their own URL with zero extra config. Lakekeeper advertises a single `BASE_URI` in `/config`, which the
  *fresh* Databricks client follows — so `BASE_URI` must be the public URL. Trino does NOT follow it (keeps its
  configured internal URI), so both still work simultaneously; but it's an extra config subtlety Polaris avoids.
- **Table paths:** Databricks writes clean locations; Trino appends a UUID suffix (`iceberg.unique-table-location`,
  default on) — cosmetic, per-connector, documented in the catalog `.properties`.

**Decision** (per `iceberg_catalog_evaluation.md` §7d): a catalog passing v3 CREATE + VARIANT + Trino/Spark R/W
(+ Databricks read or fallback) → adopt it. Neither → escalate for the paid AWS S3 Tables fallback.

## Gotchas & fixes (chronological — what we actually hit)
| Symptom | Cause | Fix |
| :-- | :-- | :-- |
| Polaris token: `relation "polaris_schema.entities" does not exist` | Server doesn't self-create schema | one-shot `polaris_bootstrap` (admin-tool `migrate`/`bootstrap`), idempotent |
| Polaris `CREATE TABLE` → "Failed to create transaction" (`sts:AssumeRole`) | No IAM role to vend creds | `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION=true` + own keys |
| Polaris `DROP` → 403 "Unable to purge entity" | Purge disabled by default | `DROP_WITH_PURGE_ENABLED=true` |
| Lakekeeper warehouse named `s3://…/warehouse` | `.env` `WAREHOUSE` collided with script var | `LK_`-prefixed script vars |
| Lakekeeper `CREATE TABLE` → `accessKey is null` | Trino 483 doesn't consume LK remote signing | own keys (`fs.native-s3`), drop vended-credentials |
| Databricks `spark.conf.set` → `CANNOT_MODIFY_STATIC_CONFIG` | `spark.sql.extensions` is static | put catalog config in the cluster Spark config, restart |
| Databricks INSERT → "Unable to load region from any of the providers" | Region not read from `s3.region` | add `spark.sql.catalog.X.client.region` |
| Databricks `UnknownHostException: lakekeeper` | LK advertises internal BASE_URI in `/config` | set `LAKEKEEPER_BASE_URI` to the public URL; keep Trino URI internal |
| New tables from Trino get a UUID path suffix | `iceberg.unique-table-location=true` (default) | cosmetic; documented (commented) in catalog `.properties` |
| Firewall: Databricks → 8181/8282 timeouts | SG only had Nessie's 19120 | network team added 8181 (Polaris) + 8282 (Lakekeeper) |

## Notes
- `catalog_postgres` + `polaris` are in the main `docker-compose.yml` and start with `docker compose up`. No
  `depends_on` from trino → polaris (lazy catalog load), so a candidate-catalog issue never blocks the pipeline.
- Start with own AWS keys; test vending second — fastest path to the make-or-break v3+VARIANT result.
- `catalog_postgres` data persists in the `catalog_pg_data` named volume; `docker compose down -v` wipes it.
- The Trino **catalog** (`polaris.properties`) is wired only *after* bootstrap (staged → live), even though the
  Polaris **container** starts with the stack — so an un-bootstrapped Polaris never errors Trino.
