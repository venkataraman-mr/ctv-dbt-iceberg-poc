# Databricks notebook source
# MAGIC %md
# MAGIC # WRITE TEST (scratch) — Databricks → AWS Nessie / Iceberg
# MAGIC
# MAGIC Proves Spark can `INSERT` / `UPDATE` / `DELETE` / `MERGE` into an AWS Nessie-cataloged Iceberg table.
# MAGIC Uses a **throwaway** table (`aws_occ.gold.zz_dbx_write_test`) and **drops it at the end** — it never
# MAGIC touches pipeline tables.
# MAGIC
# MAGIC **Capability test only — this is OFF the productionization design.** The plan makes the AWS stack the
# MAGIC writer-of-record and Databricks a reader; don't point this at pipeline tables.
# MAGIC
# MAGIC **Prerequisite:** the S3 key must have **write** perms (`s3:PutObject`, `DeleteObject`, multipart) on the
# MAGIC warehouse bucket — the read-only key from the connectivity test is not enough.

# COMMAND ----------
dbutils.widgets.text("nessie_uri",   "http://18.222.25.33:19120/iceberg/main/", "Nessie IRC uri")
dbutils.widgets.text("warehouse",    "warehouse",                                "Nessie warehouse NAME")
dbutils.widgets.text("aws_region",   "us-east-2",                                "AWS region")
dbutils.widgets.text("aws_access_key_id",     "", "AWS access key id (WRITE-capable, test only)")
dbutils.widgets.text("aws_secret_access_key", "", "AWS secret access key (WRITE-capable, test only)")

spark.conf.set("spark.sql.catalog.aws_occ", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.aws_occ.type", "rest")
spark.conf.set("spark.sql.catalog.aws_occ.uri", dbutils.widgets.get("nessie_uri"))
spark.conf.set("spark.sql.catalog.aws_occ.warehouse", dbutils.widgets.get("warehouse"))
spark.conf.set("spark.sql.catalog.aws_occ.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.aws_occ.s3.region", dbutils.widgets.get("aws_region"))
spark.conf.set("spark.sql.catalog.aws_occ.client.region", dbutils.widgets.get("aws_region"))
spark.conf.set("spark.sql.catalog.aws_occ.s3.access-key-id",     dbutils.widgets.get("aws_access_key_id"))
spark.conf.set("spark.sql.catalog.aws_occ.s3.secret-access-key", dbutils.widgets.get("aws_secret_access_key"))

SCRATCH = "aws_occ.gold.zz_dbx_write_test"

# COMMAND ----------
# MAGIC %md ## 1. Clean slate + CREATE + INSERT

# COMMAND ----------
spark.sql(f"DROP TABLE IF EXISTS {SCRATCH}")
spark.sql(f"CREATE TABLE {SCRATCH} (id BIGINT, note STRING) USING iceberg")
spark.sql(f"INSERT INTO {SCRATCH} VALUES (1,'a'), (2,'b'), (3,'c')")
display(spark.sql(f"SELECT * FROM {SCRATCH} ORDER BY id"))   # expect 3 rows

# COMMAND ----------
# MAGIC %md ## 2. UPDATE + DELETE  (row-level, merge-on-read)

# COMMAND ----------
spark.sql(f"UPDATE {SCRATCH} SET note = 'b-updated' WHERE id = 2")
spark.sql(f"DELETE FROM {SCRATCH} WHERE id = 3")
display(spark.sql(f"SELECT * FROM {SCRATCH} ORDER BY id"))   # expect id=1 'a', id=2 'b-updated'

# COMMAND ----------
# MAGIC %md ## 3. MERGE (upsert)

# COMMAND ----------
spark.sql("CREATE OR REPLACE TEMP VIEW _src AS SELECT * FROM VALUES (2,'b-merged'), (4,'d-new') AS t(id, note)")
spark.sql(f"""
MERGE INTO {SCRATCH} t USING _src s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET note = s.note
WHEN NOT MATCHED THEN INSERT (id, note) VALUES (s.id, s.note)
""")
display(spark.sql(f"SELECT * FROM {SCRATCH} ORDER BY id"))   # expect id=1 'a', id=2 'b-merged', id=4 'd-new'

# COMMAND ----------
# MAGIC %md ## 4. Commit history (proves the writes committed to Nessie)

# COMMAND ----------
display(spark.sql(f"SELECT made_current_at, snapshot_id, is_current_ancestor FROM {SCRATCH}.history ORDER BY made_current_at"))

# COMMAND ----------
# MAGIC %md ## 5. Cleanup — drop the scratch table

# COMMAND ----------
spark.sql(f"DROP TABLE IF EXISTS {SCRATCH}")
print("scratch table dropped")

# COMMAND ----------
# MAGIC %md
# MAGIC ### How to read the results
# MAGIC - All four (INSERT/UPDATE/DELETE/MERGE) succeed → Databricks Spark has **full DML** on AWS Nessie Iceberg
# MAGIC   (unlike Trino → UC-managed, which was append-only).
# MAGIC - A write fails with **403 / AccessDenied** → the S3 key lacks write permission.
# MAGIC - CREATE/commit fails (not a 403) → the Nessie write/commit path (auth or catalog) — check the message.
# MAGIC - Reminder: this is a capability check. Keep the AWS stack as writer-of-record in production.
