"""Central config for the ingestion package — all values from environment (see .env)."""
import os

AWS_REGION   = os.environ.get("AWS_REGION", "us-east-2")
S3_BUCKET    = os.environ.get("S3_BUCKET", "dataplatformpoc-venketa")
WAREHOUSE    = os.environ.get("WAREHOUSE", f"s3://{S3_BUCKET}/warehouse")

# Nessie's Iceberg REST endpoint (PyIceberg talks to this; Trino uses the native /api/v2).
NESSIE_ICEBERG_URI = os.environ.get("NESSIE_ICEBERG_URI", "http://nessie:19120/iceberg/")
# The warehouse NAME registered in Nessie (nessie.catalog.default-warehouse), not the s3:// URI.
NESSIE_WAREHOUSE   = os.environ.get("NESSIE_WAREHOUSE", "warehouse")

# Azure ADLS (hive_metastore reference sync). Account key works for delta-rs and the DuckDB fallback.
AZURE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_KEY     = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

# Unity Catalog reference sync (uc_reference_sync.py) — a SECOND Azure storage account (the UC blob).
# Same engine as the hive sync; only the account key/name differ (the base path is hardcoded in
# uc_reference_sync.py, like reference_sync.py). UC_AZURE_STORAGE_ACCOUNT_NAME must be the account in
# that base path (vxxdbwcommonpesteu2).
UC_AZURE_ACCOUNT = os.environ.get("UC_AZURE_STORAGE_ACCOUNT_NAME")
UC_AZURE_KEY     = os.environ.get("UC_AZURE_STORAGE_ACCOUNT_KEY")

# CTV landing (Piece 1). Source of truth is now S3: sample .bz2 files are dropped under
# LANDING_INGESTION; the landing step decompresses/parses them into bronze staging and moves the
# processed file under LANDING_ARCHIVE/<YYYY-MM-DD>/. (Later: an Azure->S3 copier will feed
# LANDING_INGESTION automatically when a new blob is dropped.) Kept separate from the Iceberg
# warehouse prefix so raw source files never mingle with table data/metadata.
LANDING_INGESTION = os.environ.get("CTV_LANDING_INGESTION", f"{S3_BUCKET}/landing/ctv/ingestion")
LANDING_ARCHIVE   = os.environ.get("CTV_LANDING_ARCHIVE",   f"{S3_BUCKET}/landing/ctv/archive")
