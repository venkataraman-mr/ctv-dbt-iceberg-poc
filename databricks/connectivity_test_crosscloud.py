# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-cloud CONNECTIVITY TEST  (read-only, no writes)
# MAGIC
# MAGIC Goal: prove that **Azure Databricks** can reach and read the **AWS** new-stack
# MAGIC `gold.digital_gold_occurrence` Iceberg table through the **Nessie Iceberg REST Catalog**.
# MAGIC
# MAGIC This notebook does **nothing but read** — no MERGE, no watermark, no writes to any table.
# MAGIC Run the cells top to bottom. If the last cell prints rows, the cross-cloud read path works and the
# MAGIC full sync (`creative_sync_crosscloud.py`) is just SQL on top of this.
# MAGIC
# MAGIC **Before running** (one-time, see `README.md`): non-UC cluster + Iceberg Maven libs whose version
# MAGIC MATCHES the cluster's Spark (Spark 3.5 -> `iceberg-spark-runtime-3.5_2.12:1.9.1` + `iceberg-aws-bundle:1.9.1`);
# MAGIC network path from Databricks to the Nessie endpoint + S3.
# MAGIC
# MAGIC **AWS creds:** for this test we pass them as widgets below (typed at runtime, not stored in code). Use a
# MAGIC temporary / least-privilege key (S3 read on the warehouse prefix only) and rotate it after. In production,
# MAGIC use a Databricks secret scope or an assume-role instead of inline keys.

# COMMAND ----------
# MAGIC %md ## Step 1 — Parameters

# COMMAND ----------
dbutils.widgets.text("nessie_uri",   "http://<NESSIE_HOST>:19120/iceberg/main/", "Nessie IRC uri (http:// — plaintext; branch 'main' in path)")
dbutils.widgets.text("warehouse",    "warehouse",                                  "Nessie warehouse NAME (NOT the s3 path)")
dbutils.widgets.text("aws_region",   "us-east-2",                                   "AWS region")
dbutils.widgets.text("aws_access_key_id",     "",                                   "AWS access key id (test only)")
dbutils.widgets.text("aws_secret_access_key", "",                                   "AWS secret access key (test only)")

NESSIE_URI = dbutils.widgets.get("nessie_uri")
WAREHOUSE  = dbutils.widgets.get("warehouse")     # Nessie warehouse NAME (e.g. 'warehouse'); Nessie resolves the s3 location
AWS_REGION = dbutils.widgets.get("aws_region")
AWS_KEY    = dbutils.widgets.get("aws_access_key_id")
AWS_SECRET = dbutils.widgets.get("aws_secret_access_key")
OCC        = "aws_occ.gold.digital_gold_occurrence"

# COMMAND ----------
# MAGIC %md ## Step 2 — Attach the AWS Nessie Iceberg REST catalog (read-only)
# MAGIC Branch `main` is in the URI path. `S3FileIO` reads the data files; AWS creds come from the secret scope.

# COMMAND ----------
spark.conf.set("spark.sql.catalog.aws_occ", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.aws_occ.type", "rest")
spark.conf.set("spark.sql.catalog.aws_occ.uri", NESSIE_URI)
spark.conf.set("spark.sql.catalog.aws_occ.warehouse", WAREHOUSE)   # Nessie warehouse NAME, not the s3 path
spark.conf.set("spark.sql.catalog.aws_occ.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.aws_occ.s3.region", AWS_REGION)
spark.conf.set("spark.sql.catalog.aws_occ.client.region", AWS_REGION)
spark.conf.set("spark.sql.catalog.aws_occ.s3.access-key-id",     AWS_KEY)     # test only — from widget
spark.conf.set("spark.sql.catalog.aws_occ.s3.secret-access-key", AWS_SECRET)  # test only — from widget
# If Nessie requires a bearer token, also set the rest auth props here (see README / design doc).
print("catalog aws_occ configured ->", NESSIE_URI)

# COMMAND ----------
# MAGIC %md ## Step 3 — Catalog reachable?  (metadata only — proves the Nessie REST endpoint)

# COMMAND ----------
display(spark.sql("SHOW NAMESPACES IN aws_occ"))

# COMMAND ----------
display(spark.sql("SHOW TABLES IN aws_occ.gold"))

# COMMAND ----------
# MAGIC %md ## Step 4 — Can we read the data files?  (proves S3 access + creds)
# MAGIC A `count` + min/max forces Spark to open the Iceberg metadata **and** the S3 data files.

# COMMAND ----------
display(spark.sql(f"""
    SELECT count(*)                AS occ_rows,
           min(capture_date)       AS min_capture_date,
           max(capture_date)       AS max_capture_date,
           max(updated_timestamp)  AS max_updated_ts
    FROM {OCC}
"""))

# COMMAND ----------
# MAGIC %md ## Step 5 — Sample rows  (final proof the read returns real data)

# COMMAND ----------
display(spark.sql(f"""
    SELECT occurrence_id, creative_id, capture_timestamp, updated_timestamp, delete_flag
    FROM {OCC}
    ORDER BY updated_timestamp DESC
    LIMIT 10
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ### How to read the results
# MAGIC - **Step 3 works, Step 4/5 fail** → the Nessie REST endpoint is reachable but S3 read failed → check
# MAGIC   the AWS creds in the secret scope, the `iceberg-aws-bundle` library, and S3 network/egress.
# MAGIC - **Step 3 fails** → Databricks can't reach the Nessie endpoint → check the URI, port, and the
# MAGIC   Databricks→Nessie network path (and Nessie auth if enabled).
# MAGIC - **All steps return** → cross-cloud read confirmed. Proceed to `creative_sync_crosscloud.py`.
# MAGIC - `occ_rows = 0` is still a PASS for connectivity (the table just has no data yet).
