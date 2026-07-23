"""Load the shared Nessie Iceberg catalog for PyIceberg.

Uses the default PyArrow FileIO for S3: it reads AWS credentials from the default provider
chain (AWS_* env) plus s3.region. PyIceberg here only writes/reads S3 — Azure reads are done
by delta-rs / DuckDB in the reference sync, so no s3fs/adlfs is needed.
"""
from pyiceberg.catalog import load_catalog
from ingestion import config


def get_catalog():
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": config.NESSIE_ICEBERG_URI,
            # Nessie's Iceberg REST expects the warehouse NAME registered on the server
            # (nessie.catalog.default-warehouse: warehouse), NOT the s3:// location. Passing
            # the URI makes Nessie treat it as an ad-hoc warehouse that skips the configured
            # S3 auth-mode -> "Missing access key and secret for STATIC authentication mode".
            "warehouse": config.NESSIE_WAREHOUSE,
            "s3.region": config.AWS_REGION,
        },
    )
