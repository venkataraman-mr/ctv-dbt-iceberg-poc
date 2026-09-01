"""Load the shared POLARIS Iceberg catalog for PyIceberg, and load tables with a PyArrow FileIO.

Polaris clone of ingestion/common/catalog.py. Differences from Nessie:
  - REST auth is OAuth2 client-credentials (Polaris), not anonymous. We pass `credential`
    ("<clientId>:<clientSecret>") + `scope`; PyIceberg fetches a bearer token from
    `<uri>/v1/oauth/tokens` (Polaris' token endpoint) and uses it for every catalog call.
  - `warehouse` is the Polaris CATALOG name (ctv_poc), not a Nessie warehouse name.

Same FileIO wrapper as Nessie: the REST server may vend `pyiceberg.io.fsspec.FsspecFileIO` in each
table's config (which drags s3fs). PyArrow reads S3 with no extra deps and picks up AWS creds from
the default provider chain (AWS_* env) + s3.region, so `load_table()` forces PyArrow FileIO.

Polaris runs with SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION=true (no IAM role -> no vended S3 creds),
so PyIceberg reaches S3 with its OWN keys (AWS_* env), same as the Databricks cross-cloud test.
"""
from pyiceberg.catalog import load_catalog
from pyiceberg.io import load_file_io
from ingestion_polaris import config

_ARROW_IO = "pyiceberg.io.pyarrow.PyArrowFileIO"


def get_catalog():
    props = {
        "type": "rest",
        "uri": config.POLARIS_ICEBERG_URI,
        "warehouse": config.POLARIS_WAREHOUSE,
        "scope": config.POLARIS_SCOPE,
        # We set this too, but the server's per-table config may override it (see load_table).
        "py-io-impl": _ARROW_IO,
        "s3.region": config.AWS_REGION,
    }
    # OAuth2 client-credentials (clientId:clientSecret). Absent only in a misconfigured env.
    if config.POLARIS_OAUTH2_CREDENTIAL:
        props["credential"] = config.POLARIS_OAUTH2_CREDENTIAL
    return load_catalog("polaris", **props)


def force_pyarrow_io(tbl):
    """Rebuild a table's FileIO as PyArrow, overriding any vended FsspecFileIO. Apply this to any
    table returned by the catalog (load_table / create_table_*) before reading/writing."""
    props = dict(tbl.io.properties)
    props["py-io-impl"] = _ARROW_IO
    tbl.io = load_file_io(props, tbl.metadata_location)
    return tbl


def load_table(identifier):
    """Load a table via the shared Polaris catalog, with the FileIO forced to PyArrow."""
    return force_pyarrow_io(get_catalog().load_table(identifier))
