"""Central config for the ingestion_polaris package — all values from environment (see .env).

Polaris clone of ingestion/config.py. Two things differ from Nessie: (1) the catalog binding —
this talks to the **Polaris** Iceberg REST endpoint (OAuth2 client-credentials); and (2) the CTV
landing prefix — `landing_polaris/` instead of `landing/`, so the two pipelines' archive-on-read
never collide. Everything else (AWS/Azure/UC storage) is identical to the Nessie ingestion.
"""
import os

AWS_REGION   = os.environ.get("AWS_REGION", "us-east-2")
S3_BUCKET    = os.environ.get("S3_BUCKET", "dataplatformpoc-venketa")
WAREHOUSE    = os.environ.get("WAREHOUSE", f"s3://{S3_BUCKET}/warehouse")

# --- Polaris Iceberg REST catalog (PyIceberg talks to this; Trino uses the same /api/catalog) ---
# The REST base URL. PyIceberg derives the OAuth token endpoint from this (…/v1/oauth/tokens).
POLARIS_ICEBERG_URI = os.environ.get("POLARIS_ICEBERG_URI", "http://polaris:8181/api/catalog")
# The Polaris CATALOG (warehouse) name — the internal catalog we created (not the s3:// URI).
POLARIS_WAREHOUSE   = os.environ.get("POLARIS_WAREHOUSE", "ctv_poc")
# OAuth2 client-credentials for the trino_poc principal: "<clientId>:<clientSecret>" (from .env,
# same secret the `polaris` Trino catalog uses). Kept out of the repo — .env only.
POLARIS_OAUTH2_CREDENTIAL = os.environ.get("POLARIS_OAUTH2_CREDENTIAL")
# Principal-role scope requested at token time.
POLARIS_SCOPE       = os.environ.get("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL")

# Azure ADLS (hive_metastore reference sync). Account key works for delta-rs and the DuckDB fallback.
AZURE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_KEY     = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

# Unity Catalog reference sync (uc_reference_sync.py) — a SECOND Azure storage account (the UC blob).
# Same engine as the hive sync; only the account key/name differ (the base path is hardcoded in
# uc_reference_sync.py, like reference_sync.py). UC_AZURE_STORAGE_ACCOUNT_NAME must be the account in
# that base path (vxxdbwcommonpesteu2).
UC_AZURE_ACCOUNT = os.environ.get("UC_AZURE_STORAGE_ACCOUNT_NAME")
UC_AZURE_KEY     = os.environ.get("UC_AZURE_STORAGE_ACCOUNT_KEY")

# CTV landing (Piece 1). Source of truth is S3: sample .bz2 files are dropped under LANDING_INGESTION;
# the landing step decompresses/parses them into bronze staging and moves the processed file under
# LANDING_ARCHIVE/<YYYY-MM-DD>/. Kept separate from the Iceberg warehouse prefix so raw source files
# never mingle with table data/metadata.
#
# *** SEPARATE prefix from Nessie (landing_polaris/, NOT landing/). ***  The landing step ARCHIVES
# (moves) each processed file, so if Polaris and Nessie shared one prefix they'd steal each other's
# files. The Polaris pipeline reads/archives entirely under landing_polaris/ so the two runs never
# collide. Upload the day's .bz2 to landing_polaris/ctv/ingestion (see the runbook / upload script
# -Prefix). This is the deliberate one difference from the Nessie ingestion config.
LANDING_INGESTION = os.environ.get("CTV_LANDING_INGESTION", f"{S3_BUCKET}/landing_polaris/ctv/ingestion")
LANDING_ARCHIVE   = os.environ.get("CTV_LANDING_ARCHIVE",   f"{S3_BUCKET}/landing_polaris/ctv/archive")
