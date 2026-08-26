# Databricks ↔ AWS Iceberg catalogs — cross-cloud testing

One place for testing **Azure Databricks** reading/writing the **AWS** new-stack Iceberg tables across all three
catalogs we evaluated: **Nessie**, **Apache Polaris**, and **Lakekeeper**. Each has its own section below; the
shared cluster/network setup is common to all three.

**Mechanism (all three):** a **non-UC Databricks cluster** with a **manual Spark Iceberg REST catalog attach**.
Unity Catalog cannot federate to a generic Iceberg REST catalog (only Glue / HMS / Snowflake Horizon), and
Databricks' native Iceberg v3/VARIANT support is documented only for *UC-managed* tables — so reading/writing an
**external** catalog goes through the non-UC + manual-attach path.

**Bottom line (see the results table at the end):**

| Catalog | Cross-cloud result |
| :-- | :-- |
| **Nessie** | Read + v2 write work; **no v3+VARIANT** (Nessie REST doesn't serve v3); only latest snapshot visible (no version/CDC reads). Ruled out. |
| **Polaris** | **Full CRUD incl. v3+VARIANT — PASS.** |
| **Lakekeeper** | **Full CRUD incl. v3+VARIANT — PASS** (one extra networking step for its advertised base URL). |

---

## Common setup (all catalogs)

### Cluster

- **Databricks Runtime:** for v3+VARIANT use **DBR 18 LTS or newer** (newest bundled Iceberg; v3/VARIANT is GA
  there). DBR ships its own ("bundled") Iceberg that shadows Maven libs — older runtimes report
  `IcebergBuild.version() = unspecified` and reject VARIANT. For the Nessie *read-only* test, DBR 16.4 LTS with
  matched Iceberg libs is also fine (see the Nessie section).
- **Access mode: non-UC** (Assigned / "No isolation shared" — *not* a Unity Catalog access mode). UC-mode
  clusters block the custom `spark.sql.catalog.*` configs and won't attach a foreign catalog.
- **Libraries:** on DBR 18 try first with **no** extra libraries (bundled Iceberg). If diagnostics show a
  stale/`unspecified` Iceberg or VARIANT fails, attach matched Maven coords and restart:
  `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0` + `org.apache.iceberg:iceberg-aws-bundle:1.11.0`
  (the Scala/Spark suffix must match the runtime; `3.5_2.12:1.9.1` for a Spark-3.5 runtime).

### Catalog config goes in the CLUSTER Spark config, not the notebook

`spark.sql.extensions` is a **static** Spark config; setting it at runtime fails with
`CANNOT_MODIFY_STATIC_CONFIG`. Register the catalogs in **Cluster → Edit → Advanced options → Spark → Spark
config** and **restart the cluster**. One block can hold all three (fill in the VM public host, creds, AWS keys):

```
spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# --- Polaris (OAuth2 client-credentials) ---
spark.sql.catalog.polaris org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.polaris.type rest
spark.sql.catalog.polaris.uri http://<AWS_VM_HOST>:8181/api/catalog
spark.sql.catalog.polaris.warehouse ctv_poc
spark.sql.catalog.polaris.credential <trino_poc_clientId>:<trino_poc_clientSecret>
spark.sql.catalog.polaris.scope PRINCIPAL_ROLE:ALL
spark.sql.catalog.polaris.io-impl org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.polaris.client.region us-east-2
spark.sql.catalog.polaris.s3.region us-east-2
spark.sql.catalog.polaris.s3.access-key-id <AWS_ACCESS_KEY_ID>
spark.sql.catalog.polaris.s3.secret-access-key <AWS_SECRET_ACCESS_KEY>

# --- Lakekeeper (static bearer token; unsecured PoC) ---
spark.sql.catalog.lakekeeper org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakekeeper.type rest
spark.sql.catalog.lakekeeper.uri http://<AWS_VM_HOST>:8282/catalog
spark.sql.catalog.lakekeeper.warehouse ctv_lakekeeper
spark.sql.catalog.lakekeeper.token dummy
spark.sql.catalog.lakekeeper.io-impl org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lakekeeper.client.region us-east-2
spark.sql.catalog.lakekeeper.s3.region us-east-2
spark.sql.catalog.lakekeeper.s3.access-key-id <AWS_ACCESS_KEY_ID>
spark.sql.catalog.lakekeeper.s3.secret-access-key <AWS_SECRET_ACCESS_KEY>
```

- **`client.region` is required** — Iceberg's AWS client factory reads the region from `client.region`, not
  `s3.region`; without it the executor's S3 write fails with "Unable to load region from any of the providers".
- **Own S3 keys** — none of the PoC catalogs vend credentials (no IAM role), so Spark reaches S3 with its own
  keys via `S3FileIO`. Plaintext-secret cluster config is only acceptable because this cluster has no secret-scope
  access; production should use a secret scope or an assume-role/instance profile.

### Network / security groups

Catalogs run plaintext HTTP on the VM (no TLS), so use `http://…`. The EC2 **security group** must allow inbound
from the Databricks egress/NAT IPs (never `0.0.0.0/0` — these endpoints have weak/no auth):

| Catalog | Port | REST path |
| :-- | :-- | :-- |
| Nessie | 19120 | `/iceberg/main/` (branch in path) |
| Polaris | 8181 | `/api/catalog` |
| Lakekeeper | 8282 | `/catalog` |

A `Connection timed out` from Databricks = the security group / network path, not the notebook.

### Files

| File | Catalog | Purpose |
| :-- | :-- | :-- |
| `connectivity_test_crosscloud.py` | Nessie | Read-only connectivity (attach + 3 read checks) |
| `write_test_crosscloud.py` | Nessie | v2 write test |
| `cdc_read_test_crosscloud.py` | Nessie | Snapshot/CDC read attempt |
| `v3_variant_write_test_crosscloud.py` | Nessie | v3+VARIANT write attempt (fails — see findings) |
| `polaris_crosscloud_crud_test.py` | Polaris | Full CRUD incl. v3+VARIANT |
| `lakekeeper_crosscloud_crud_test.py` | Lakekeeper | Full CRUD incl. v3+VARIANT |
| `../scripts/polaris_crossengine_verify.sql` | Polaris | Trino-side cross-engine round-trip |
| `../scripts/lakekeeper_crossengine_verify.sql` | Lakekeeper | Trino-side cross-engine round-trip |

---

## 1. Nessie

**Scope:** read-only connectivity first, then v2 write / snapshot / v3+VARIANT attempts — this is where the v3
gap that motivated the whole catalog PoC was proven.

**Auth:** none — Nessie runs `security=NONE` on the VM. No OAuth, no token. Plaintext `http://<host>:19120/…`.

**Cluster:** non-UC. For the read-only test, **DBR 16.4 LTS** + matched Iceberg libs is the stable choice:
`org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1` + `iceberg-aws-bundle:1.9.1` (Spark 3.5 / Scala 2.12).
The v3+VARIANT attempt was run on DBR 18 with `4.0_2.13:1.11.0` libs.

**Connect (mirrors `infra/trino/catalog/iceberg_rest.properties`):** `uri = http://<host>:19120/iceberg/main/`
(branch `main` in the path), `warehouse = warehouse` (the Nessie warehouse **name**, not the S3 path),
`aws_region`, plus AWS key/secret (the connectivity notebook takes these as widgets, typed at runtime).

**Run order:** import `connectivity_test_crosscloud.py`, set widgets, run top to bottom. Pass = namespaces list
(endpoint reachable) + a count/sample rows (S3 read + creds work); `occ_rows = 0` still passes (empty table).

**Findings (why Nessie was ruled out):**

- **Read + v2 write work** from Databricks (non-UC + manual attach).
- **No v3 + VARIANT.** Nessie's REST catalog doesn't serve Iceberg v3, so a VARIANT table can't be created/read
  over REST; the v3+VARIANT write attempt fails (`Unknown Iceberg primitive type 'variant'` / metadata read
  errors, compounded by DBR's bundled Iceberg). This is the hard-requirement failure that triggered the catalog
  evaluation.
- **Only the latest snapshot is visible** — Nessie exposes a single snapshot by design, so version-based
  (time-travel) reads and snapshot CDC aren't available; history lives in Nessie's git-style commit log instead.

---

## 2. Apache Polaris — **PASS (full CRUD incl. v3+VARIANT)**

**Auth:** OAuth2 client-credentials using the `trino_poc` principal's `clientId:clientSecret` (from
`scripts/polaris_bootstrap.sh`), scope `PRINCIPAL_ROLE:ALL`.

**Endpoint:** `http://<VM>:8181/api/catalog`, warehouse `ctv_poc`.

**Storage:** own S3 keys — Polaris runs with `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION=true` (no IAM role), so it
does **not** vend scoped creds; Spark uses its own keys via `S3FileIO` and does not request access delegation.
Production would instead give the catalog a `roleArn` and turn on real credential vending.

**Run order:**

1. `scripts/polaris_crossengine_verify.sql` **part 1** (DBeaver/Trino) — creates `xeng_from_trino` for the
   notebook to read.
2. Import `polaris_crosscloud_crud_test.py`, fill cell 0 (`AWS_VM_HOST`, the `trino_poc` creds, AWS keys), run
   top to bottom. (Cell 2 only *verifies* the catalog — registration is in the cluster Spark config above.)
3. `scripts/polaris_crossengine_verify.sql` **part 2** — Trino reads `xeng_from_dbx` created by the notebook.
4. `scripts/polaris_crossengine_verify.sql` **part 3** — cleanup.

**Reading the result:**

| Notebook block | Proves |
| :-- | :-- |
| 1 (curl) | port 8181 reachable |
| 3 SHOW NAMESPACES | OAuth2 auth + REST connectivity |
| 4 v2 CRUD | cross-cloud CRUD works (networking/auth/S3) |
| **5 v3+VARIANT CRUD** | **the hard requirement — Databricks CRUD on v3+VARIANT** |
| 6 | same tables read/written by both Trino and Databricks |

**Result:** all blocks pass — Polaris clears the Databricks-side hard requirement Nessie couldn't. No BASE_URI
quirk: Polaris advertises relative paths, so internal Trino and external Databricks each keep their own connect
URL with zero extra config.

---

## 3. Lakekeeper — **PASS (full CRUD incl. v3+VARIANT)**

**Auth:** unsecured PoC (no OpenID) — send a static bearer token (`dummy`); any value is accepted. (If OpenID is
later enabled, switch to an OAuth2 credential + token endpoint like Polaris.)

**Endpoint:** `http://<VM>:8282/catalog`, warehouse `ctv_lakekeeper`. (Host 8282 → container 8181; Polaris owns
host 8181.)

**Storage:** own S3 keys. Lakekeeper's warehouse uses an access-key credential with STS off, so it does S3
**remote signing** — which Trino 483 doesn't consume, and the notebook mirrors that by using own keys.

**One extra networking step — the base URL:** Lakekeeper advertises its `LAKEKEEPER__BASE_URI` in `GET /config`,
and a *fresh* external client (Databricks) **follows it**. So `BASE_URI` must be the **public** URL
(`http://<VM>:8282`) for Databricks to work — set `LAKEKEEPER_BASE_URI` in the VM `.env`. Trino does **not**
follow it (it keeps its own internal `lakekeeper:8181` URI), so both engines work at once: **BASE_URI public +
Trino URI internal**. Do *not* point Trino's URI at the public IP — the VM can't reach its own public 8282 (the
SG allows only Databricks' source), so Trino would hang. Polaris avoids all of this by using relative paths.

**Run order:** same shape as Polaris —

1. `scripts/lakekeeper_crossengine_verify.sql` **part 1** (Trino) — creates `xeng_lk_from_trino`.
2. Import `lakekeeper_crosscloud_crud_test.py`, fill cell 0 (`AWS_VM_HOST`, AWS keys — token is `dummy`), run
   top to bottom.
3. `scripts/lakekeeper_crossengine_verify.sql` **part 2/3** — Trino reads `xeng_lk_from_dbx`, then cleanup.

**Result:** all blocks pass — v3+VARIANT CRUD from Databricks and cross-engine round-trip both work once
`BASE_URI` is the public URL.

---

## Results summary

| Check | Nessie | Polaris | Lakekeeper |
| :-- | :-- | :-- | :-- |
| Connectivity + auth | ✅ (none) | ✅ (OAuth2) | ✅ (token) |
| Read | ✅ | ✅ | ✅ |
| v2 write / CRUD | ✅ | ✅ | ✅ |
| **v3 + VARIANT CRUD** | ❌ (no v3 over REST) | ✅ | ✅ |
| Version/CDC reads | ❌ (single snapshot) | ✅ (Iceberg snapshots) | ✅ (Iceberg snapshots) |
| Cross-engine (Trino↔Databricks) | read-only | ✅ | ✅ |
| Extra config quirk | — | none | BASE_URI must be public |

Cluster (Polaris/Lakekeeper): DBR 18 LTS, non-UC, own S3 keys, `client.region` set, catalog config in the cluster
Spark config. Full decision context: `../docs/iceberg_catalog_evaluation.md`; stand-up steps:
`../docs/catalog_poc_runbook.md`.
