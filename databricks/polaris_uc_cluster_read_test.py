# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-cloud read on a UC cluster: Databricks (UC) → Polaris occurrence table (AWS)
# MAGIC
# MAGIC **Question this answers:** the first-seen / last-seen creative-sync job runs on a **Unity Catalog** cluster
# MAGIC (it reads/writes the Azure UC tables), but it must also read the **AWS gold occurrence** Iceberg table in
# MAGIC **Polaris**. Our earlier cross-cloud test used a *non-UC* cluster. Can a UC cluster do both in one session?
# MAGIC
# MAGIC **Answer we're validating:** yes — on a **Dedicated (single-user) access-mode** UC cluster. UC has two modes:
# MAGIC  * **Standard** (old "Shared") — locked down: blocks `spark.jars`, `spark.jars.packages`, custom classpath,
# MAGIC    so you can't attach the OSS Iceberg libs / a custom catalog. *Not* usable for the manual Polaris attach.
# MAGIC  * **Dedicated** (old "Single-user / Assigned") — **UC-enabled but permissive**: allows custom
# MAGIC    `spark.sql.catalog.*`, compute-scoped libraries, and non-80/443 ports. This is the one to use.
# MAGIC
# MAGIC So this notebook proves a **Dedicated UC cluster** reads a **UC table** AND the **Polaris** occurrence table
# MAGIC in the same session — the whole first/last-seen read path, no non-UC cluster needed.
# MAGIC
# MAGIC **We only read non-VARIANT columns** (ids + timestamps), so there's no VARIANT decode / bundled-Iceberg issue.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster setup (Dedicated UC cluster)
# MAGIC 1. **Access mode: Dedicated** (single-user / assigned) — **not** Standard/Shared. UC stays enabled.
# MAGIC 2. **DBR 18 LTS** (recent Iceberg). 3. **Libraries** (compute-scoped, allowed on Dedicated):
# MAGIC    `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0` + `org.apache.iceberg:iceberg-aws-bundle:1.11.0`.
# MAGIC 4. **Cluster Spark config** — register the Polaris catalog (same as the non-UC test; `spark.sql.extensions`
# MAGIC    is static so it must live here, not in the notebook):
# MAGIC ```
# MAGIC spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
# MAGIC spark.sql.catalog.polaris org.apache.iceberg.spark.SparkCatalog
# MAGIC spark.sql.catalog.polaris.type rest
# MAGIC spark.sql.catalog.polaris.uri http://<AWS_VM_HOST>:8181/api/catalog
# MAGIC spark.sql.catalog.polaris.warehouse ctv_poc
# MAGIC spark.sql.catalog.polaris.credential <trino_poc_clientId>:<trino_poc_clientSecret>
# MAGIC spark.sql.catalog.polaris.scope PRINCIPAL_ROLE:ALL
# MAGIC spark.sql.catalog.polaris.io-impl org.apache.iceberg.aws.s3.S3FileIO
# MAGIC spark.sql.catalog.polaris.client.region us-east-2
# MAGIC spark.sql.catalog.polaris.s3.region us-east-2
# MAGIC spark.sql.catalog.polaris.s3.access-key-id <AWS_ACCESS_KEY_ID>
# MAGIC spark.sql.catalog.polaris.s3.secret-access-key <AWS_SECRET_ACCESS_KEY>
# MAGIC ```
# MAGIC 5. Firewall: this cluster's egress must reach `AWS_VM_HOST:8181` (the rule we already opened).
# MAGIC
# MAGIC **Prereq:** run `scripts/polaris_uc_read_seed.sql` on Trino first to create
# MAGIC `polaris.ctv_catalog_poc.gold_occurrence_sample`.

# COMMAND ----------

# ---- EDIT THESE ----
AWS_VM_HOST = "REPLACE_WITH_VM_PUBLIC_HOST_OR_IP"        # for the network precheck only
OCC_TABLE   = "polaris.ctv_catalog_poc.gold_occurrence_sample"   # the AWS Polaris occurrence stand-in
UC_TABLE    = "REPLACE_WITH_A_UC_TABLE_YOU_CAN_READ"     # e.g. main.default.some_small_table — proves UC access
# Optional coexistence join: a UC creative table + its key column (leave as None to skip)
UC_CREATIVE_TABLE = None                                 # e.g. "main.ctv.creative_unique_urls"
UC_CREATIVE_KEY   = "creative_url_hash"
# --------------------

# COMMAND ----------

# MAGIC %md ## 1. Network precheck (port 8181)

# COMMAND ----------

# MAGIC %sh
# MAGIC HOST="REPLACE_WITH_VM_PUBLIC_HOST_OR_IP"
# MAGIC curl -s --max-time 15 -o /dev/null -w "polaris 8181 -> HTTP %{http_code}\n" http://${HOST}:8181/api/catalog/v1/config || echo "unreachable (firewall?)"

# COMMAND ----------

# MAGIC %md ## 2. Prove Unity Catalog still works on this cluster

# COMMAND ----------

print("current catalog:", spark.sql("SELECT current_catalog()").first()[0])
print("catalogs visible (UC + polaris):")
spark.sql("SHOW CATALOGS").show(truncate=False)
print(f"read a UC table ({UC_TABLE}):")
spark.sql(f"SELECT count(*) AS uc_row_count FROM {UC_TABLE}").show()

# COMMAND ----------

# MAGIC %md ## 3. Prove the Polaris (AWS) occurrence table reads in the SAME session

# COMMAND ----------

try:
    print("Iceberg version:", spark._jvm.org.apache.iceberg.IcebergBuild.version())
except Exception as e:
    print("IcebergBuild version unavailable:", e)

spark.sql("SHOW NAMESPACES IN polaris").show(truncate=False)
print(f"raw read of {OCC_TABLE} (non-VARIANT columns only):")
spark.sql(f"""
SELECT occurrence_id, creative_url_hash, capture_timestamp, updated_timestamp, delete_flag
FROM {OCC_TABLE}
ORDER BY occurrence_id
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md ## 4. The actual first-seen / last-seen aggregation (what the job computes)

# COMMAND ----------

firstlast = spark.sql(f"""
SELECT creative_url_hash,
       MIN(capture_timestamp) AS first_seen,
       MAX(updated_timestamp) AS last_seen,
       COUNT(*)               AS occ_count
FROM {OCC_TABLE}
WHERE delete_flag = false
GROUP BY creative_url_hash
""")
firstlast.createOrReplaceTempView("occ_first_last")
firstlast.orderBy("creative_url_hash").show(truncate=False)
print("Polaris occurrence aggregation computed on a UC cluster: PASS")

# COMMAND ----------

# MAGIC %md ## 5. Coexistence: join the AWS Polaris aggregate with an Azure UC table (optional)
# MAGIC The real proof — a single query touching **both** the Polaris catalog and a UC-governed table.

# COMMAND ----------

if UC_CREATIVE_TABLE:
    spark.sql(f"""
    SELECT f.creative_url_hash, f.first_seen, f.last_seen, f.occ_count, c.{UC_CREATIVE_KEY} AS matched_uc_key
    FROM occ_first_last f
    LEFT JOIN {UC_CREATIVE_TABLE} c
      ON f.creative_url_hash = c.{UC_CREATIVE_KEY}
    ORDER BY f.creative_url_hash
    """).show(truncate=False)
    print("Cross-cloud join (Polaris occurrence x UC creative) on a UC cluster: PASS")
else:
    print("Set UC_CREATIVE_TABLE to run the cross-catalog join. Blocks 2+3+4 already prove the read path.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plan B (UC-compatible): read Polaris via **PyIceberg**, not a Spark catalog
# MAGIC Unity Catalog governs the Spark catalog namespace, so a custom `spark.sql.catalog` foreign REST catalog may
# MAGIC not resolve on a UC cluster **regardless of access mode** (this is a UC constraint, not a Standard-vs-Dedicated
# MAGIC one). PyIceberg sidesteps it entirely: it talks to Polaris REST **as a library**, returns Arrow/pandas, and we
# MAGIC hand that to Spark for the `MERGE`. Works on Dedicated (and even Standard, via compute-scoped Python libs).

# COMMAND ----------

# MAGIC %pip install "pyiceberg[pyarrow]" boto3
# MAGIC # dbutils.library.restartPython()   # run if the import below fails right after %pip

# COMMAND ----------

from pyiceberg.catalog.rest import RestCatalog

cat = RestCatalog("polaris", **{
    "uri": f"http://{AWS_VM_HOST}:8181/api/catalog",
    "warehouse": "ctv_poc",
    "credential": "REPLACE_trino_poc_clientId:clientSecret",   # do NOT commit
    "scope": "PRINCIPAL_ROLE:ALL",
    # own S3 keys (Polaris doesn't vend in the PoC)
    "s3.access-key-id": "REPLACE_AWS_ACCESS_KEY_ID",
    "s3.secret-access-key": "REPLACE_AWS_SECRET_ACCESS_KEY",
    "s3.region": "us-east-2",
})
print("namespaces:", cat.list_namespaces())

tbl = cat.load_table(("ctv_catalog_poc", "gold_occurrence_sample"))
pdf = tbl.scan(
    selected_fields=("occurrence_id", "creative_url_hash", "capture_timestamp", "updated_timestamp", "delete_flag")
).to_pandas()
print("rows read via PyIceberg:", len(pdf))

sdf = spark.createDataFrame(pdf)
sdf.createOrReplaceTempView("occ_pyiceberg")
spark.sql("""
SELECT creative_url_hash, MIN(capture_timestamp) AS first_seen, MAX(updated_timestamp) AS last_seen, COUNT(*) AS occ_count
FROM occ_pyiceberg WHERE delete_flag = false GROUP BY creative_url_hash
""").show(truncate=False)
print("Polaris read via PyIceberg on a UC cluster: PASS — MERGE occ_pyiceberg into the UC creative table next")
# NOTE: if load_table() errors on v3 metadata, PyIceberg's v3 read support is the gap — tell me and we fall back to
# the two-task pattern (a non-UC task writes a projected UC table; this UC job MERGEs from it).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretation
# MAGIC - **Blocks 2–5 (Spark catalog attach):** if block 3 reads Polaris, great — but UC clusters commonly reject a
# MAGIC   custom `spark.sql.catalog` foreign catalog. That's the reported error to capture.
# MAGIC - **Plan B (PyIceberg):** the reliable UC-cluster path — reads Polaris as a library (no Spark catalog), then
# MAGIC   Spark `MERGE`s into the UC creative table on the same cluster. This is the likely production shape.
# MAGIC - **Fallback if PyIceberg can't read v3:** two-task workflow — a non-UC task reads Polaris and writes a
# MAGIC   projected UC table (ids + timestamps, no VARIANT); this UC job just `MERGE`s from that UC table.
