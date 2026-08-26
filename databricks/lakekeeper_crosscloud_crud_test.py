# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-cloud CRUD test: Databricks -> Lakekeeper (Iceberg REST) on AWS
# MAGIC
# MAGIC Sibling of `polaris_crosscloud_crud_test.py`. Same goal — full **CRUD incl. v3 + VARIANT** from an
# MAGIC external Databricks engine — but against **Lakekeeper** instead of Polaris. Differences:
# MAGIC  * URI is `http://<VM>:8282/catalog` (host 8282 -> container 8181).
# MAGIC  * Auth is a **static bearer token** (unsecured Lakekeeper accepts any) — NOT OAuth2 client-credentials.
# MAGIC  * Warehouse is `ctv_lakekeeper`.
# MAGIC  * S3 access uses **own keys** (Trino 483 didn't consume Lakekeeper remote signing; we mirror that here).
# MAGIC    If Databricks' Iceberg client DOES support Lakekeeper remote signing, the own-keys path still works.
# MAGIC
# MAGIC Cluster: **DBR 18 LTS+**, **non-UC access mode** (see README_catalog_crosscloud.md — same requirements).

# COMMAND ----------

# ---- EDIT THESE ----
AWS_VM_HOST    = "REPLACE_WITH_VM_PUBLIC_HOST_OR_IP"
LK_URI         = f"http://{AWS_VM_HOST}:8282/catalog"
WAREHOUSE      = "ctv_lakekeeper"
LK_TOKEN       = "dummy"                                  # unsecured Lakekeeper: any bearer accepted
AWS_ACCESS_KEY = "REPLACE_WITH_AWS_ACCESS_KEY_ID"
AWS_SECRET_KEY = "REPLACE_WITH_AWS_SECRET_ACCESS_KEY"
AWS_REGION     = "us-east-2"

CATALOG = "lakekeeper"
SCHEMA  = "ctv_catalog_poc"
# --------------------

# COMMAND ----------

# MAGIC %md ## 1. Network precheck (port 8282)

# COMMAND ----------

# MAGIC %sh
# MAGIC HOST="REPLACE_WITH_VM_PUBLIC_HOST_OR_IP"
# MAGIC curl -s --max-time 15 http://${HOST}:8282/health ; echo

# COMMAND ----------

# MAGIC %md ## 2. Catalog registration — done in CLUSTER Spark config, NOT here
# MAGIC `spark.sql.extensions` is a **static** Spark config and cannot be set at runtime on Databricks
# MAGIC (`CANNOT_MODIFY_STATIC_CONFIG`). Register the `lakekeeper` catalog in the **cluster Spark config** (see
# MAGIC README_catalog_crosscloud.md — the combined block covers both catalogs), then restart the cluster.

# COMMAND ----------

print("lakekeeper catalog class:", spark.conf.get(f"spark.sql.catalog.{CATALOG}", "NOT SET — configure the cluster and restart"))

# COMMAND ----------

# MAGIC %md ## 3. Diagnostics + connectivity

# COMMAND ----------

try:
    print("Iceberg version:", spark._jvm.org.apache.iceberg.IcebergBuild.version(),
          "| full:", spark._jvm.org.apache.iceberg.IcebergBuild.fullVersion())
except Exception as e:
    print("Could not read IcebergBuild version:", e)

spark.sql(f"SHOW NAMESPACES IN {CATALOG}").show(truncate=False)
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md ## 4. Baseline CRUD on a v2 table (no VARIANT)

# COMMAND ----------

t2 = f"{CATALOG}.{SCHEMA}.dbx_lk_v2"
spark.sql(f"DROP TABLE IF EXISTS {t2}")
spark.sql(f"CREATE TABLE {t2} (id BIGINT, source STRING, spend DOUBLE) USING iceberg TBLPROPERTIES ('format-version'='2')")
spark.sql(f"INSERT INTO {t2} VALUES (1,'ctv',123.45),(2,'avod',67.8),(3,'ctv',10.0)")
spark.sql(f"UPDATE {t2} SET spend = 999.0 WHERE id = 1")
spark.sql(f"DELETE FROM {t2} WHERE id = 2")
spark.sql(f"MERGE INTO {t2} t USING (SELECT 4 AS id,'avod' AS source,5.0 AS spend) s ON t.id=s.id WHEN NOT MATCHED THEN INSERT *")
spark.sql(f"SELECT * FROM {t2} ORDER BY id").show()
print("v2 CRUD: PASS")

# COMMAND ----------

# MAGIC %md ## 5. THE HARD REQUIREMENT — CRUD on v3 + VARIANT

# COMMAND ----------

t3 = f"{CATALOG}.{SCHEMA}.dbx_lk_v3"
spark.sql(f"DROP TABLE IF EXISTS {t3}")
spark.sql(f"CREATE TABLE {t3} (id BIGINT, source STRING, payload VARIANT) USING iceberg TBLPROPERTIES ('format-version'='3')")
spark.sql(f"SHOW TBLPROPERTIES {t3}").show(truncate=False)

# COMMAND ----------

spark.sql(f"""
INSERT INTO {t3} VALUES
 (1,'ctv',  parse_json('{{"advertiser":"ACME","spend":123.45,"tags":["ctv","q3"]}}')),
 (2,'ctv',  parse_json('{{"advertiser":"Globex","nested":{{"k":"v"}}}}')),
 (3,'avod', NULL)
""")
spark.sql(f"""
SELECT id, source,
       variant_get(payload,'$.advertiser','string') AS advertiser,
       variant_get(payload,'$.spend','double')      AS spend,
       to_json(payload) AS payload_json
FROM {t3} ORDER BY id
""").show(truncate=False)

# COMMAND ----------

spark.sql(f"UPDATE {t3} SET payload = parse_json('{{\"advertiser\":\"ACME-2\",\"spend\":200}}') WHERE id = 1")
spark.sql(f"DELETE FROM {t3} WHERE id = 2")
spark.sql(f"""
MERGE INTO {t3} t
USING (SELECT 4 AS id,'avod' AS source, parse_json('{{"advertiser":"Initech"}}') AS payload) s
ON t.id = s.id WHEN NOT MATCHED THEN INSERT *
""")
spark.sql(f"SELECT id, source, variant_get(payload,'$.advertiser','string') AS advertiser FROM {t3} ORDER BY id").show(truncate=False)
print("v3 + VARIANT CRUD from Databricks via Lakekeeper: PASS")

# COMMAND ----------

# MAGIC %md ## 6. Cross-engine: create from Databricks, read back in Trino
# MAGIC After this, run in Trino/DBeaver:
# MAGIC `SELECT id, CAST(payload['advertiser'] AS varchar) FROM lakekeeper.ctv_catalog_poc.xeng_lk_from_dbx;`

# COMMAND ----------

xeng = f"{CATALOG}.{SCHEMA}.xeng_lk_from_dbx"
spark.sql(f"DROP TABLE IF EXISTS {xeng}")
spark.sql(f"CREATE TABLE {xeng} (id BIGINT, source STRING, payload VARIANT) USING iceberg TBLPROPERTIES ('format-version'='3')")
spark.sql(f"INSERT INTO {xeng} VALUES (1,'dbx',parse_json('{{\"advertiser\":\"FromDatabricks\"}}'))")
print("created", xeng, "- read it back from Trino to confirm cross-engine interop")

# COMMAND ----------

# MAGIC %md ## 7. Cleanup

# COMMAND ----------

for tbl in [t2, t3]:
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
print("dropped test tables (left xeng_lk_from_dbx for the Trino cross-engine read)")
