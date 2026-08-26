# Cross-cloud CRUD test: Databricks → Apache Polaris (Iceberg REST on AWS)

Goal: prove an **external Databricks** engine can do full **CRUD**, including the **v3 + VARIANT** hard
requirement, against Polaris-managed Iceberg tables in AWS S3 — the same tables Trino reads/writes.

This is the reverse of Databricks' documented Iceberg v3 support, which only covers **Unity Catalog-managed**
tables. UC cannot federate to a generic REST catalog like Polaris (only Glue / HMS / Snowflake Horizon), so we
use the **non-UC cluster + manual Spark Iceberg REST attach** path — the same mechanism the Nessie connectivity
test used, but with OAuth2 and our own S3 keys. Whether DBR's Iceberg client decodes VARIANT from an *external*
catalog is exactly what this test answers; it is bleeding-edge, so treat the result as the finding.

## Prerequisites

1. **Firewall / security group** — the network team opened Databricks → VM on Nessie's port **19120**. Polaris
   listens on **8181**; that port needs the **same rule added**, or the notebook's connection just times out.
   Notebook cell 1 is a `curl` precheck that fails fast if 8181 isn't reachable.
2. **Polaris is up and bootstrapped on the VM** (`docker compose up -d`; realm bootstrapped; `ctv_poc` catalog +
   `trino_poc` principal created — see `docs/catalog_poc_runbook.md`).
3. A **cluster** configured as below.

## Cluster setup

* **Databricks Runtime: 18 LTS or newer.** DBR ships its own ("bundled") Iceberg that shadows Maven libs; older
  runtimes report `IcebergBuild.version() = unspecified` and reject VARIANT. 18 LTS has the newest Iceberg and is
  where v3/VARIANT went GA — best chance of decoding VARIANT from Polaris.
* **Access mode: non-UC** (Assigned / "No isolation shared" — *not* a Unity Catalog access mode). UC-mode
  clusters block the custom `spark.sql.catalog.*` configs this test needs.
* **Libraries (fallback):** try first with **no** extra libraries (use DBR's bundled Iceberg). If cell 3 shows a
  stale/`unspecified` Iceberg or VARIANT fails, attach these Maven coordinates and re-run:
  * `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0` (match the Scala/Spark of your DBR)
  * `org.apache.iceberg:iceberg-aws-bundle:1.11.0`
* **Catalog config timing:** the notebook sets `spark.sql.catalog.*` at runtime via `spark.conf.set`. If Spark
  rejects a runtime set, move these into **Cluster → Advanced → Spark config** and restart:

  ```
  spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  spark.sql.catalog.polaris org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.polaris.type rest
  spark.sql.catalog.polaris.uri http://<AWS_VM_HOST>:8181/api/catalog
  spark.sql.catalog.polaris.warehouse ctv_poc
  spark.sql.catalog.polaris.credential <clientId>:<clientSecret>
  spark.sql.catalog.polaris.scope PRINCIPAL_ROLE:ALL
  spark.sql.catalog.polaris.io-impl org.apache.iceberg.aws.s3.S3FileIO
  spark.sql.catalog.polaris.s3.region us-east-2
  spark.sql.catalog.polaris.s3.access-key-id <AWS_ACCESS_KEY_ID>
  spark.sql.catalog.polaris.s3.secret-access-key <AWS_SECRET_ACCESS_KEY>
  ```

## Why own S3 keys (not vending)

The PoC runs Polaris with `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION=true` (no IAM role to assume), so Polaris does
**not** vend scoped S3 credentials. Spark therefore reaches S3 with its **own** keys via `S3FileIO`
(`s3.access-key-id` / `s3.secret-access-key`), and the notebook does **not** request access delegation. In
production the opposite applies: give the catalog a `roleArn` and turn on real credential vending, and Spark
needs no keys.

## Run order

1. `scripts/polaris_crossengine_verify.sql` **part 1** in DBeaver (Trino) — creates `xeng_from_trino` for the
   notebook to read.
2. Import `polaris_crosscloud_crud_test.py` as a Databricks notebook, fill in cell 0 (`AWS_VM_HOST`,
   `POLARIS_CRED`, AWS keys), and run top to bottom.
3. `scripts/polaris_crossengine_verify.sql` **part 2** (Trino) — reads `xeng_from_dbx` created by the notebook.
4. `scripts/polaris_crossengine_verify.sql` **part 3** — cleanup.

## Reading the result

| Notebook block | Proves |
| :-- | :-- |
| 1 curl 200 | port 8181 reachable (firewall OK) |
| 3 SHOW NAMESPACES | OAuth2 auth + REST connectivity |
| 4 v2 CRUD PASS | cross-cloud CRUD works (networking/auth/S3 all good) |
| **5 v3+VARIANT CRUD PASS** | **the hard requirement — Databricks CRUD on v3+VARIANT via Polaris** |
| 6A / 6B | same tables read/written by both Trino and Databricks |

If everything passes through block 5, Polaris clears the Databricks-side hard requirement that Nessie could not.
If block 5 fails on VARIANT while block 4 passes, the gap is DBR's Iceberg **client** VARIANT support (runtime /
library), not Polaris or S3 — capture the exact error and the cell-3 Iceberg version for the results matrix.
