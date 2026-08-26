# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-cloud CRUD test: Databricks -> Apache Polaris (Iceberg REST) on AWS
# MAGIC
# MAGIC Tests whether an **external Databricks** engine can do full **CRUD** against Polaris-managed Iceberg
# MAGIC tables in AWS S3 — including the **v3 + VARIANT** hard requirement.
# MAGIC
# MAGIC **Mechanism:** non-UC cluster + manual Spark Iceberg REST catalog attach (UC canNOT federate to a generic
# MAGIC REST catalog like Polaris — only Glue/HMS/Snowflake Horizon). So this is the manual-attach path, same as
# MAGIC the Nessie connectivity test, but with **OAuth2** and **own S3 keys** (Polaris runs with credential
# MAGIC sub-scoping OFF for the PoC, so it does not vend S3 creds).
# MAGIC
# MAGIC **Cluster requirements** (see README_catalog_crosscloud.md):
# MAGIC  * **DBR 18 LTS or newer** (newest bundled Iceberg — best chance at VARIANT decode).
# MAGIC  * **Non-UC access mode** (so custom `spark.sql.catalog.*` configs are allowed).
# MAGIC  * Firewall/SG must allow this cluster -> `AWS_VM_HOST:8181` (Nessie's 19120 rule does NOT cover 8181).
# MAGIC
# MAGIC Run top to bottom. Each CRUD block prints a clear PASS/observation. The v3+VARIANT block is the
# MAGIC make-or-break; if DBR's Iceberg client can't decode VARIANT from an external catalog, it fails there.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Connection parameters
# MAGIC Fill these in. For a real deployment use Databricks **secret scopes** instead of inline literals —
# MAGIC this notebook inlines them only because this PoC cluster has no secret-scope access.

# COMMAND ----------

# ---- EDIT THESE ----
AWS_VM_HOST   = "REPLACE_WITH_VM_PUBLIC_HOST_OR_IP"        # same host used for Nessie, but port 8181
POLARIS_URI   = f"http://{AWS_VM_HOST}:8181/api/catalog"   # Polaris Iceberg REST endpoint
WAREHOUSE     = "ctv_poc"                                  # the Polaris catalog created at bootstrap
POLARIS_CRED  = "REPLACE_trino_poc_clientId:clientSecret"  # from scripts/polaris_bootstrap.sh step 3 (do NOT commit)
OAUTH_SCOPE   = "PRINCIPAL_ROLE:ALL"

# S3 access: Polaris does NOT vend creds in this PoC, so Spark reads/writes S3 with its OWN keys.
AWS_ACCESS_KEY = "REPLACE_WITH_AWS_ACCESS_KEY_ID"
AWS_SECRET_KEY = "REPLACE_WITH_AWS_SECRET_ACCESS_KEY"
AWS_REGION     = "us-east-2"

CATALOG = "polaris"                     # the Spark catalog name we register below
SCHEMA  = "ctv_catalog_poc"             # namespace to test in
# --------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Network precheck (does 8181 even reach the VM?)
# MAGIC If this hangs/times out, it is a firewall/security-group gap on port 8181 — NOT a Spark problem.
# MAGIC Ask the network team to extend the Databricks->VM rule from 19120 to 8181.

# COMMAND ----------

# MAGIC %sh
# MAGIC # expects an OAuth token JSON (200), not a timeout. Uses the trino_poc creds.
# MAGIC set -a
# MAGIC HOST="REPLACE_WITH_VM_PUBLIC_HOST_OR_IP"
# MAGIC CID="REPLACE_trino_poc_clientId"; CSEC="REPLACE_trino_poc_clientSecret"
# MAGIC curl -s --max-time 15 -X POST "http://${HOST}:8181/api/catalog/v1/oauth/tokens" \
# MAGIC   -d grant_type=client_credentials -d "client_id=${CID}" -d "client_secret=${CSEC}" \
# MAGIC   -d scope=PRINCIPAL_ROLE:ALL | head -c 300 ; echo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Catalog registration — done in CLUSTER Spark config, NOT here
# MAGIC `spark.sql.extensions` is a **static** Spark config and cannot be set at runtime on Databricks
# MAGIC (`CANNOT_MODIFY_STATIC_CONFIG`). So the `polaris` catalog is registered in
# MAGIC **Cluster → Edit → Advanced options → Spark → Spark config** (see README_catalog_crosscloud.md), then the
# MAGIC cluster is restarted. This cell only verifies the catalog is visible.

# COMMAND ----------

# Confirms the cluster Spark config was applied. If this prints NOT SET, add the config block from the README to
# the cluster Spark config and restart the cluster.
print("polaris catalog class:", spark.conf.get(f"spark.sql.catalog.{CATALOG}", "NOT SET — configure the cluster and restart"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Diagnostics — which Iceberg is actually loaded?
# MAGIC If this shows `unspecified` / a very old version, DBR's bundled Iceberg is shadowing the OSS lib and the
# MAGIC VARIANT block below will likely fail — attach the OSS `iceberg-spark-runtime` + `iceberg-aws-bundle`
# MAGIC Maven libs (README) and/or move to DBR 18 LTS.

# COMMAND ----------

try:
    ver = spark._jvm.org.apache.iceberg.IcebergBuild.version()
    full = spark._jvm.org.apache.iceberg.IcebergBuild.fullVersion()
    print("Iceberg version:", ver, "| full:", full)
except Exception as e:
    print("Could not read IcebergBuild version:", e)

# list namespaces = the real connectivity + auth check
spark.sql(f"SHOW NAMESPACES IN {CATALOG}").show(truncate=False)
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Baseline CRUD on a v2 table (no VARIANT)
# MAGIC Isolates networking/OAuth/S3 from the VARIANT question. If this passes, cross-cloud CRUD works;
# MAGIC anything that fails only in block 5 is specifically a v3/VARIANT client-support gap.

# COMMAND ----------

t2 = f"{CATALOG}.{SCHEMA}.dbx_crud_v2"
spark.sql(f"DROP TABLE IF EXISTS {t2}")

# CREATE
spark.sql(f"""
CREATE TABLE {t2} (id BIGINT, source STRING, spend DOUBLE)
USING iceberg TBLPROPERTIES ('format-version'='2')
""")
# INSERT
spark.sql(f"INSERT INTO {t2} VALUES (1,'ctv',123.45),(2,'avod',67.8),(3,'ctv',10.0)")
print("after INSERT:"); spark.sql(f"SELECT * FROM {t2} ORDER BY id").show()
# UPDATE
spark.sql(f"UPDATE {t2} SET spend = 999.0 WHERE id = 1")
# DELETE
spark.sql(f"DELETE FROM {t2} WHERE id = 2")
print("after UPDATE+DELETE:"); spark.sql(f"SELECT * FROM {t2} ORDER BY id").show()
# MERGE
spark.sql(f"""
MERGE INTO {t2} t USING (SELECT 4 AS id, 'avod' AS source, 5.0 AS spend) s
ON t.id = s.id WHEN NOT MATCHED THEN INSERT *
""")
print("after MERGE:"); spark.sql(f"SELECT * FROM {t2} ORDER BY id").show()
print("v2 CRUD: PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. THE HARD REQUIREMENT — CRUD on a v3 table with a VARIANT column
# MAGIC Create/insert/read/update/delete a v3+VARIANT table entirely from Databricks against Polaris.
# MAGIC This is the make-or-break for the migration.

# COMMAND ----------

t3 = f"{CATALOG}.{SCHEMA}.dbx_crud_v3"
spark.sql(f"DROP TABLE IF EXISTS {t3}")

# CREATE v3 + VARIANT (Databricks syntax: USING iceberg + VARIANT column => v3)
spark.sql(f"""
CREATE TABLE {t3} (id BIGINT, source STRING, payload VARIANT)
USING iceberg TBLPROPERTIES ('format-version'='3')
""")
spark.sql(f"SHOW TBLPROPERTIES {t3}").show(truncate=False)   # expect format-version = 3

# COMMAND ----------

# INSERT VARIANT payloads (parse_json produces a VARIANT in Spark/DBR)
spark.sql(f"""
INSERT INTO {t3} VALUES
 (1,'ctv',  parse_json('{{"advertiser":"ACME","spend":123.45,"tags":["ctv","q3"]}}')),
 (2,'ctv',  parse_json('{{"advertiser":"Globex","nested":{{"k":"v"}}}}')),
 (3,'avod', NULL)
""")

# READ + extract VARIANT fields
spark.sql(f"""
SELECT id, source,
       variant_get(payload, '$.advertiser', 'string') AS advertiser,
       variant_get(payload, '$.spend',     'double') AS spend,
       to_json(payload) AS payload_json
FROM {t3} ORDER BY id
""").show(truncate=False)

# COMMAND ----------

# UPDATE / DELETE / MERGE on v3+VARIANT
spark.sql(f"UPDATE {t3} SET payload = parse_json('{{\"advertiser\":\"ACME-2\",\"spend\":200}}') WHERE id = 1")
spark.sql(f"DELETE FROM {t3} WHERE id = 2")
spark.sql(f"""
MERGE INTO {t3} t
USING (SELECT 4 AS id, 'avod' AS source, parse_json('{{"advertiser":"Initech"}}') AS payload) s
ON t.id = s.id WHEN NOT MATCHED THEN INSERT *
""")
print("after v3 UPDATE/DELETE/MERGE:")
spark.sql(f"""
SELECT id, source, variant_get(payload, '$.advertiser', 'string') AS advertiser
FROM {t3} ORDER BY id
""").show(truncate=False)   # expect id 1 (ACME-2), 3 (null), 4 (Initech)
print("v3 + VARIANT CRUD from Databricks: PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cross-engine interop (the real 'external read/write' proof)
# MAGIC  * **A:** read a table that **Trino** created (run scripts/polaris_crossengine_verify.sql part 1 first).
# MAGIC  * **B:** the `dbx_crud_v3` table above is now readable **by Trino** (part 2 of that SQL file).
# MAGIC This shows the same Polaris tables are read/written by both engines — Trino AND Databricks.

# COMMAND ----------

# A: read the v3+VARIANT table that TRINO created (run scripts/polaris_crossengine_verify.sql part 1 first)
try:
    spark.sql(f"""
    SELECT id, source, variant_get(payload, '$.advertiser', 'string') AS advertiser
    FROM {CATALOG}.{SCHEMA}.xeng_from_trino ORDER BY id
    """).show(truncate=False)
    print("read Trino-created v3+VARIANT table from Databricks: PASS")
except Exception as e:
    print("xeng_from_trino not found yet (run the Trino seed) or read failed:", e)

# COMMAND ----------

# B: create a v3+VARIANT table FROM Databricks that Trino will read back
#    (leave it in place; part 2 of the Trino SQL selects from it, then you can drop from either engine)
xeng = f"{CATALOG}.{SCHEMA}.xeng_from_dbx"
spark.sql(f"DROP TABLE IF EXISTS {xeng}")
spark.sql(f"CREATE TABLE {xeng} (id BIGINT, source STRING, payload VARIANT) USING iceberg TBLPROPERTIES ('format-version'='3')")
spark.sql(f"INSERT INTO {xeng} VALUES (1,'dbx',parse_json('{{\"advertiser\":\"FromDatabricks\",\"spend\":42}}'))")
print("created", xeng, "- now run part 2 of scripts/polaris_crossengine_verify.sql in Trino to read it back")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cleanup

# COMMAND ----------

for tbl in [t2, t3]:
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
print("dropped test tables")
