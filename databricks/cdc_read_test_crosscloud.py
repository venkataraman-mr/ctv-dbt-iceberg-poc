# Databricks notebook source
# MAGIC %md
# MAGIC # CDC READ TEST (read-only) — Databricks reading incremental changes from the AWS occurrence table
# MAGIC
# MAGIC Read-only. Tries the two ways to read "what changed" from `aws_occ.gold.digital_gold_occurrence`:
# MAGIC 1. **Timestamp-watermark filtered scan** — robust, snapshot-type-agnostic (the design's approach).
# MAGIC 2. **Iceberg incremental / changelog** — Spark's native CDC (`start/end-snapshot-id`, `create_changelog_view`).
# MAGIC
# MAGIC Nothing is written. A read-only S3 key is fine.
# MAGIC
# MAGIC **IMPORTANT — Nessie exposes only ONE Iceberg snapshot per table** (by design, to keep git-like
# MAGIC branch/tag isolation; history lives in Nessie's commit log, not the Iceberg snapshot log). So the
# MAGIC snapshot-range mechanisms below (2a / 2b) have no range to diff and will error / return nothing over a
# MAGIC Nessie catalog. **Step 1 (timestamp watermark) is the CDC-read mechanism here.** For true commit-level
# MAGIC CDC you'd diff two Nessie refs/commits (a Nessie feature), not the Iceberg snapshot APIs.

# COMMAND ----------
dbutils.widgets.text("nessie_uri",   "http://18.222.25.33:19120/iceberg/main/", "Nessie IRC uri")
dbutils.widgets.text("warehouse",    "warehouse",                                "Nessie warehouse NAME")
dbutils.widgets.text("aws_region",   "us-east-2",                                "AWS region")
dbutils.widgets.text("aws_access_key_id",     "", "AWS access key id (read, test only)")
dbutils.widgets.text("aws_secret_access_key", "", "AWS secret access key (read, test only)")
dbutils.widgets.text("watermark", "2026-08-01 00:00:00", "Last-processed watermark (updated_timestamp >)")

spark.conf.set("spark.sql.catalog.aws_occ", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.aws_occ.type", "rest")
spark.conf.set("spark.sql.catalog.aws_occ.uri", dbutils.widgets.get("nessie_uri"))
spark.conf.set("spark.sql.catalog.aws_occ.warehouse", dbutils.widgets.get("warehouse"))
spark.conf.set("spark.sql.catalog.aws_occ.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.aws_occ.s3.region", dbutils.widgets.get("aws_region"))
spark.conf.set("spark.sql.catalog.aws_occ.client.region", dbutils.widgets.get("aws_region"))
spark.conf.set("spark.sql.catalog.aws_occ.s3.access-key-id",     dbutils.widgets.get("aws_access_key_id"))
spark.conf.set("spark.sql.catalog.aws_occ.s3.secret-access-key", dbutils.widgets.get("aws_secret_access_key"))

OCC = "aws_occ.gold.digital_gold_occurrence"
WM  = dbutils.widgets.get("watermark")

# COMMAND ----------
# MAGIC %md ## 0. Snapshots — pick start/end ids for the Iceberg CDC tests below

# COMMAND ----------
display(spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {OCC}.snapshots ORDER BY committed_at"))

# COMMAND ----------
# MAGIC %md ## 1. Timestamp-watermark filtered scan  (robust — the design's CDC read)
# MAGIC Immune to merge-on-read / delete files / deletion vectors. Catches inserts, updates AND soft-deletes
# MAGIC (delete_flag flips and updated_timestamp bumps).

# COMMAND ----------
display(spark.sql(f"""
    SELECT count(*) AS changed_rows, min(updated_timestamp) AS min_upd, max(updated_timestamp) AS max_upd
    FROM {OCC} WHERE updated_timestamp > timestamp '{WM}'
"""))
display(spark.sql(f"""
    SELECT occurrence_id, creative_id, capture_timestamp, updated_timestamp, delete_flag
    FROM {OCC} WHERE updated_timestamp > timestamp '{WM}'
    ORDER BY updated_timestamp DESC LIMIT 20
"""))

# COMMAND ----------
# MAGIC %md ## 2a. Iceberg incremental APPEND read (between two snapshots)
# MAGIC Append-only: returns rows added between the snapshots. **Errors** if the range includes an
# MAGIC overwrite/delete snapshot — that error is itself informative (same limit as Trino's table_changes).
# MAGIC Set START_SNAP / END_SNAP from the snapshots table above.

# COMMAND ----------
START_SNAP = "<oldest_snapshot_id>"   # <-- fill from the snapshots table
END_SNAP   = "<newest_snapshot_id>"   # <-- fill from the snapshots table
try:
    df = (spark.read.format("iceberg")
          .option("start-snapshot-id", START_SNAP)
          .option("end-snapshot-id", END_SNAP)
          .load(OCC))
    print("incremental append rows:", df.count())
    display(df.select("occurrence_id", "creative_id", "updated_timestamp").limit(20))
except Exception as e:
    print("incremental append read failed (expected if the range has overwrite/delete snapshots):")
    print(type(e).__name__, str(e)[:400])

# COMMAND ----------
# MAGIC %md ## 2b. Iceberg CHANGELOG view (row-level insert/delete/update pre & post images)
# MAGIC Spark's native CDC procedure. Databricks/Spark handles v2 delete-file snapshots better than Trino did,
# MAGIC so this may return true row-level changes where Trino's `table_changes` refused. If it errors on the
# MAGIC table's features, that's the engine limit → fall back to the timestamp-watermark scan (step 1).

# COMMAND ----------
try:
    spark.sql(f"""
    CALL aws_occ.system.create_changelog_view(
      table => 'gold.digital_gold_occurrence',
      options => map('start-snapshot-id', '{START_SNAP}', 'end-snapshot-id', '{END_SNAP}'),
      changelog_view => 'occ_changelog'
    )""")
    display(spark.sql("SELECT _change_type, count(*) AS n FROM occ_changelog GROUP BY _change_type ORDER BY _change_type"))
    display(spark.sql("SELECT occurrence_id, creative_id, updated_timestamp, _change_type, _change_ordinal FROM occ_changelog ORDER BY _change_ordinal LIMIT 30"))
except Exception as e:
    print("changelog view failed on this table's features:")
    print(type(e).__name__, str(e)[:500])

# COMMAND ----------
# MAGIC %md
# MAGIC ### How to read the results
# MAGIC - **Step 1 always works** — that's the mechanism the last-seen design uses.
# MAGIC - **Step 2a** returns rows only if the snapshot range is append-only; an error there just means the range
# MAGIC   included updates/deletes.
# MAGIC - **Step 2b** returning `_change_type` rows (insert/delete/update_preimage/update_postimage) means
# MAGIC   Spark can do **true row-level CDC** on these AWS tables — better than Trino managed. If it errors,
# MAGIC   use step 1.
