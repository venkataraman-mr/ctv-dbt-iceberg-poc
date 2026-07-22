"""Load the shared Nessie Iceberg catalog for PyIceberg.

AWS creds come from the default provider chain (AWS_* env). ADLS creds (account key) are
provided so PyIceberg can also read Azure-resident data files where needed.
"""
from pyiceberg.catalog import load_catalog
from ingestion import config


def get_catalog():
    props = {
        "type": "rest",
        "uri": config.NESSIE_ICEBERG_URI,
        "warehouse": config.WAREHOUSE,
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.region": config.AWS_REGION,
    }
    if config.AZURE_ACCOUNT and config.AZURE_KEY:
        props["adls.account-name"] = config.AZURE_ACCOUNT
        props["adls.account-key"] = config.AZURE_KEY
    return load_catalog("nessie", **props)
