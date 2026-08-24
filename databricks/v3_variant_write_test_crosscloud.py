# Databricks notebook source
# MAGIC %md
# MAGIC # v3 + VARIANT write test (scratch) — Spark → AWS Nessie / Iceberg
# MAGIC
# MAGIC Tests whether a **Spark-written** Iceberg **v3** table with a **VARIANT** column round-trips on the AWS
# MAGIC Nessie catalog — i.e., whether writing v3 from a v3-mature engine (Spark 4.0) avoids the metadata-parse
# MAGIC failure we hit on the **Trino-written** v3+VARIANT table.
# MAGIC
# MAGIC Uses a **throwaway** table (`aws_occ.bronze.zz_dbx_v3_variant_test`) and drops it at the end.
# MAGIC
# MAGIC **Requires:** DBR 17.x/18 (Spark 4.x, native VARIANT) + `iceberg-spark-runtime-4.0_2.13:1.11.0`
# MAGIC + `iceberg-aws-bundle:1.11.0`, and an S3 **write-capable** key.

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

T = "aws_occ.bronze.zz_dbx_v3_variant_test"

# COMMAND ----------
# MAGIC %md ## 1. Create the v3 table with a VARIANT column
# MAGIC A VARIANT column requires format-version 3; we also set it explicitly.

# COMMAND ----------
spark.sql(f"DROP TABLE IF EXISTS {T}")
spark.sql(f"""
CREATE TABLE {T} (
    id       BIGINT,
    source   STRING,
    payload  VARIANT
) USING iceberg
TBLPROPERTIES ('format-version' = '3')
""")
print("created", T)

# COMMAND ----------
# MAGIC %md ## 2. Confirm it's v3 (and inspect properties)

# COMMAND ----------
display(spark.sql(f"SHOW TBLPROPERTIES {T}"))   # look for format-version = 3

# COMMAND ----------
# MAGIC %md ## 3. Insert rows with VARIANT payloads (parse_json → VARIANT)

# COMMAND ----------
spark.sql(f"""
INSERT INTO {T} SELECT * FROM VALUES
  (1, 'ctv',    parse_json('{{"advertiser":"ACME","spend":123.45,"tags":["ctv","q3"]}}')),
  (2, 'ctv',    parse_json('{{"advertiser":"Globex","spend":98.10,"nested":{{"k":"v"}}}}')),
  (3, 'avod',   parse_json('null'))
AS t(id, source, payload)
""")
print("inserted 3 rows")

# COMMAND ----------
# MAGIC %md ## 4. Read it back — raw VARIANT, JSON form, and typed field extraction

# COMMAND ----------
display(spark.sql(f"""
SELECT
    id,
    source,
    to_json(payload)                              AS payload_json,
    variant_get(payload, '$.advertiser', 'string') AS advertiser,
    variant_get(payload, '$.spend',      'double') AS spend
FROM {T}
ORDER BY id
"""))

# COMMAND ----------
# MAGIC %md ## 5. Cleanup

# COMMAND ----------
spark.sql(f"DROP TABLE IF EXISTS {T}")
print("dropped", T)

# COMMAND ----------
# MAGIC %md
# MAGIC ### How to read the results
# MAGIC - **Steps 1–4 succeed** → a **Spark-written** v3+VARIANT table round-trips on the AWS Nessie catalog →
# MAGIC   confirms the earlier failure was **Trino's experimental v3 write**, not v3/VARIANT itself or the reader.
# MAGIC - **Create/insert fails** → capture the error; likely the DBR/Iceberg version or S3 write perms.
# MAGIC
# MAGIC ### Optional cross-engine follow-up (run on the VM / Trino, not here)
# MAGIC Point Trino at this Spark-written table (before the DROP) and `SELECT` it — that tells you whether
# MAGIC **Trino can *read*** a spec-compliant Spark-written v3+VARIANT table, even though Trino can't *write* one
# MAGIC cleanly. (Keep the table if you want to try this: comment out step 5 first.)
