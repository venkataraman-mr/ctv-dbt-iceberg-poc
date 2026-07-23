"""Reference-data sync (Option C): Azure ADLS Delta -> Iceberg on S3 (Nessie).

Reads each reference Delta table with delta-rs (DuckDB fallback for deletion-vector /
v2-checkpoint tables), converts to Arrow, and OVERWRITES a native Iceberg table in the
`reference` schema. Full replace per run (slow-changing dimensions). Schedule via cron.

Auth: Azure storage ACCOUNT KEY (works for both delta-rs and the DuckDB fallback).

Usage:
  python -m ingestion.reference_sync                 # all tables in TABLE_MAP
  python -m ingestion.reference_sync --table data_provider
"""
import argparse
import sys
import io

import duckdb
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import DeltaProtocolError
from pyiceberg.io.pyarrow import _ConvertToIcebergWithoutIDs, visit_pyarrow
from pyiceberg.schema import assign_fresh_schema_ids

from ingestion import config
from ingestion.common.catalog import get_catalog, force_pyarrow_io

# One entry per reference table. delta_path = the table's own abfss:// directory.
TABLE_MAP = [
    # hive_metastore.km_preparation_db.data_provider (container 'databricks', ADLS Gen2 dfs endpoint)
    {"delta_path": "abfss://databricks@stdlg2commondbrickspeu2.dfs.core.windows.net/delta/km_preparation_db/data_provider",
     "target_schema": "reference", "target_table": "data_provider"},
    # TODO: add the remaining reference tables once this one validates end-to-end.
]

_STORAGE = {"account_name": config.AZURE_ACCOUNT, "account_key": config.AZURE_KEY}


def _read_via_duckdb(delta_path: str) -> pa.Table:
    """Fallback for Delta tables using reader features delta-rs can't read (v2Checkpoint,
    deletionVectors). DuckDB's delta extension has more current protocol support."""
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; INSTALL azure; LOAD azure;")
    con.execute(
        "CREATE SECRET (TYPE azure, CONNECTION_STRING ?)",
        [f"DefaultEndpointsProtocol=https;AccountName={config.AZURE_ACCOUNT};"
         f"AccountKey={config.AZURE_KEY};EndpointSuffix=core.windows.net"],
    )
    return con.sql(f"SELECT * FROM delta_scan('{delta_path}')").arrow()


def read_arrow(delta_path: str) -> pa.Table:
    try:
        return DeltaTable(delta_path, storage_options=_STORAGE).to_pyarrow_table()
    except (DeltaProtocolError, Exception):
        return _read_via_duckdb(delta_path)


def sync_one(catalog, entry: dict) -> int:
    schema_name, table_name = entry["target_schema"], entry["target_table"]
    arrow = read_arrow(entry["delta_path"])
    catalog.create_namespace_if_not_exists(schema_name)
    ice_schema = assign_fresh_schema_ids(visit_pyarrow(arrow.schema, _ConvertToIcebergWithoutIDs()))
    tbl = force_pyarrow_io(catalog.create_table_if_not_exists(f"{schema_name}.{table_name}", schema=ice_schema))
    if tbl.schema().as_arrow() != arrow.schema:
        with tbl.update_schema() as u:
            u.union_by_name(arrow.schema)
        tbl = force_pyarrow_io(catalog.load_table(f"{schema_name}.{table_name}"))
    tbl.overwrite(arrow)
    print(f"  {schema_name}.{table_name}: replaced with {arrow.num_rows:,} rows")
    return arrow.num_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", help="sync only this target_table")
    args = ap.parse_args()
    entries = [e for e in TABLE_MAP if not args.table or e["target_table"] == args.table]
    if not entries:
        print("No matching TABLE_MAP entries — add reference tables first.")
        sys.exit(1)
    catalog = get_catalog()
    for e in entries:
        sync_one(catalog, e)
    print("Done.")


if __name__ == "__main__":
    main()
