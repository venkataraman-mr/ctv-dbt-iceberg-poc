"""Load the shared Nessie Iceberg catalog for PyIceberg, and load tables with a PyArrow FileIO.

Why the wrapper: Nessie's Iceberg REST server vends `py-io-impl=pyiceberg.io.fsspec.FsspecFileIO`
in each table's config. PyIceberg merges that server config AFTER our catalog properties, so
Nessie's choice wins and the table ends up with FsspecFileIO — which imports s3fs. We don't ship
s3fs (it drags aiobotocore + an exact fsspec pin and wrecks dependency resolution). PyArrow works
for S3 with no extra deps, so `load_table()` rebuilds the table's FileIO forcing PyArrow. PyArrow
reads AWS creds from the default provider chain (AWS_* env) + s3.region.
"""
from pyiceberg.catalog import load_catalog
from pyiceberg.io import load_file_io
from ingestion import config

_ARROW_IO = "pyiceberg.io.pyarrow.PyArrowFileIO"


def get_catalog():
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": config.NESSIE_ICEBERG_URI,
            # We set this too, but Nessie's per-table config overrides it (see load_table).
            "py-io-impl": _ARROW_IO,
            # Nessie's Iceberg REST expects the warehouse NAME (nessie.catalog.default-warehouse),
            # NOT the s3:// location.
            "warehouse": config.NESSIE_WAREHOUSE,
            "s3.region": config.AWS_REGION,
        },
    )


def force_pyarrow_io(tbl):
    """Rebuild a table's FileIO as PyArrow, overriding Nessie's vended FsspecFileIO. Apply this
    to any table returned by the catalog (load_table / create_table_*) before reading/writing."""
    props = dict(tbl.io.properties)
    props["py-io-impl"] = _ARROW_IO
    tbl.io = load_file_io(props, tbl.metadata_location)
    return tbl


def load_table(identifier):
    """Load a table via the shared catalog, with the FileIO forced to PyArrow."""
    return force_pyarrow_io(get_catalog().load_table(identifier))
