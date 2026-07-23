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
    """Fallback for Delta tables delta-rs can't read — v2Checkpoint / deletionVectors, OR that
    trip delta-rs's ns->us timestamp cast (DuckDB reads TIMESTAMP at us precision, sidestepping
    the overflow and yielding Iceberg-friendly timestamp[us]). NOTE: DuckDB's CREATE SECRET does
    NOT accept bound params (?), so the connection string is inlined; an Azure account key is
    base64 (no single quotes) so there's nothing to escape."""
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; INSTALL azure; LOAD azure;")
    conn_str = (f"DefaultEndpointsProtocol=https;AccountName={config.AZURE_ACCOUNT};"
                f"AccountKey={config.AZURE_KEY};EndpointSuffix=core.windows.net")
    con.execute(f"CREATE OR REPLACE SECRET az_ref (TYPE azure, CONNECTION_STRING '{conn_str}')")
    return con.sql(f"SELECT * FROM delta_scan('{delta_path}')").arrow()


def _remap_ts(schema: pa.Schema, unit: str) -> pa.Schema:
    """Copy a schema, changing every timestamp field to the given unit (keeps tz)."""
    return pa.schema([f.with_type(pa.timestamp(unit, tz=f.type.tz))
                      if pa.types.is_timestamp(f.type) else f for f in schema])


def _delta_to_arrow(delta_path: str) -> pa.Table:
    """Read a Delta table via delta-rs. Some Spark/INT96 tables store timestamps at nanosecond
    precision with sub-microsecond (or sentinel) values, so delta-rs's *safe* ns->us cast raises
    ArrowInvalid ('would lose data'). Recovery: re-read at the physical ns precision, then
    truncate to us (Iceberg precision) with an unsafe cast."""
    dt = DeltaTable(delta_path, storage_options=_STORAGE)
    try:
        return dt.to_pyarrow_table()
    except pa.lib.ArrowInvalid:
        dataset = dt.to_pyarrow_dataset()
        raw = dataset.replace_schema(_remap_ts(dataset.schema, "ns")).to_table()  # physical ns
        return raw.cast(_remap_ts(raw.schema, "us"), safe=False)                  # truncate -> us


def read_arrow(delta_path: str) -> pa.Table:
    try:
        return _delta_to_arrow(delta_path)
    except Exception:
        # Last resort for tables delta-rs genuinely can't read (deletionVectors / v2Checkpoint).
        # Needs the DuckDB azure extension + system CA certs.
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
