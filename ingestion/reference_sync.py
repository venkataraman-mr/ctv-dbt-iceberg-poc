"""Reference-data sync (Option C): Azure ADLS Delta -> Iceberg on S3 (Nessie).

Streams each reference Delta table in record batches and reloads the target Iceberg table via a
single atomic transaction (delete-all + append-batches). This is:
  - MEMORY-BOUNDED — peak RAM is ~one batch, so 10GB+ tables load on a modest VM (no whole-table
    materialization).
  - CLEAN LOAD, NO DROP — the table (and its schema/history/field-IDs) is kept; every run replaces
    all rows. The delete + appends commit as ONE snapshot, so a mid-run failure leaves the previous
    data intact (never half-loaded, never empty).

Reader: delta-rs (rustls auth; INT96 timestamps coerced to microseconds). DuckDB is a per-table
fallback for tables delta-rs can't read (deletionVectors / v2Checkpoint); it needs the system CA
bundle (installed in the ingestion image).

Normalization applied to every batch (_normalize_table):
  - binary columns  -> base64 strings (Trino renders raw binary as '\\u0000...'; base64 is readable
    and matches how Databricks displays binary).
  - tz-aware timestamps -> UTC wall-clock WITHOUT zone (Iceberg 'timestamp'), so values render as
    UTC in every engine/client, matching the source Delta's '...Z'.

Policy: schema drift AUTO-EVOLVES additively (new source columns are added); anything else (type
change, removed column) fails on append. The daily run STOPS at the first table that fails.

Usage:
  python -m ingestion.reference_sync                    # all tables (daily cron)
  python -m ingestion.reference_sync --table origin     # one table
  REF_SYNC_BATCH_ROWS=500000 python -m ingestion.reference_sync   # tune batch size
"""
import argparse
import base64
import os
import sys

import duckdb
import pyarrow as pa
import pyarrow.dataset as pads
from deltalake import DeltaTable
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import _ConvertToIcebergWithoutIDs, visit_pyarrow
from pyiceberg.schema import assign_fresh_schema_ids

from ingestion import config
from ingestion.common.catalog import get_catalog, force_pyarrow_io

# Rows per streamed batch. Lower it if a very wide table still pressures memory.
BATCH_ROWS = int(os.environ.get("REF_SYNC_BATCH_ROWS", "200000"))

_BASE = "abfss://databricks@stdlg2commondbrickspeu2.dfs.core.windows.net/delta"


def _t(db: str, table: str) -> dict:
    """One reference-table entry: source Delta path -> Iceberg reference.<table>."""
    return {"delta_path": f"{_BASE}/{db}/{table}", "target_schema": "reference", "target_table": table}


# All hive_metastore reference tables to mirror into Iceberg (from the reference-table inventory).
TABLE_MAP = [
    _t("km_preparation_db",      "adscore_provided_adservers"),
    _t("km_preparation_db",      "data_provider"),
    _t("km_preparation_db",      "media_property_data_provider_map"),
    _t("km_preparation_db",      "origin"),
    _t("km_preparation_db",      "source_channel"),
    _t("km_preparation_db",      "standard_ad_size"),
    _t("km_preparation_db",      "standard_mime_type"),
    _t("km_preparation_db",      "vx0_vx2_component_mapping"),
    _t("km_preparation_gold_db", "media_property_flatten"),
    _t("productcentral",         "company"),
    _t("productcentral",         "product_flatten"),
    _t("productcentral",         "productmap"),
    _t("productcentral",         "vx0_vx2_advertiser_mapping"),
    _t("productcentral",         "vx0_vx2_mattress_product_mapping"),
]

_STORAGE = {"account_name": config.AZURE_ACCOUNT, "account_key": config.AZURE_KEY}


# --------------------------------------------------------------- normalize ----
def _norm_field(f: pa.Field) -> pa.Field:
    t = f.type
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return f.with_type(pa.string())
    if pa.types.is_timestamp(t) and t.tz is not None:
        return f.with_type(pa.timestamp(t.unit))
    return f


def _normalized_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema([_norm_field(f) for f in schema])


def _normalize_table(tbl: pa.Table) -> pa.Table:
    cols = []
    for i, f in enumerate(tbl.schema):
        t = f.type
        if pa.types.is_binary(t) or pa.types.is_large_binary(t):
            cols.append(pa.array([None if v is None else base64.b64encode(v).decode("ascii")
                                  for v in tbl.column(i).to_pylist()], type=pa.string()))
        elif pa.types.is_timestamp(t) and t.tz is not None:
            cols.append(tbl.column(i).cast(pa.int64()).cast(pa.timestamp(t.unit)))
        else:
            cols.append(tbl.column(i))
    return pa.Table.from_arrays(cols, schema=_normalized_schema(tbl.schema))


# --------------------------------------------------- streaming readers --------
def _duckdb_reader(delta_path: str):
    """Streaming RecordBatchReader over a Delta table via DuckDB (fallback for deletionVectors /
    v2Checkpoint, which delta-rs can't read). CREATE SECRET can't take a bound param, so the
    connection string is inlined (an Azure account key has no quotes)."""
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; INSTALL azure; LOAD azure;")
    # DuckDB's DEFAULT Azure transport is the Azure C++ SDK, which sets its own CA path and ignores
    # SSL_CERT_FILE/CURL_CA_BUNDLE — hence the 'SSL CA cert' failure to blob storage. Switch to
    # DuckDB's own libcurl transport, which honors ca_cert_file / the system CA bundle.
    for stmt in ("SET azure_transport_option_type='curl'",
                 "SET ca_cert_file='/etc/ssl/certs/ca-certificates.crt'"):
        try:
            con.execute(stmt)
        except Exception as e:
            print(f"  (duckdb setting skipped: {stmt} -> {type(e).__name__}: {e})")
    conn_str = (f"DefaultEndpointsProtocol=https;AccountName={config.AZURE_ACCOUNT};"
                f"AccountKey={config.AZURE_KEY};EndpointSuffix=core.windows.net")
    con.execute(f"CREATE OR REPLACE SECRET az_ref (TYPE azure, CONNECTION_STRING '{conn_str}')")
    return con.execute(f"SELECT * FROM delta_scan('{delta_path}')").fetch_record_batch(BATCH_ROWS)


def _reader(delta_path: str, source: str):
    """Return (raw_schema, iterable[RecordBatch]) for the chosen reader, streaming."""
    if source == "delta-rs":
        # Coerce INT96 (legacy Spark) timestamps straight to us so there's no lossy ns->us cast.
        dataset = DeltaTable(delta_path, storage_options=_STORAGE).to_pyarrow_dataset(
            parquet_read_options=pads.ParquetReadOptions(coerce_int96_timestamp_unit="us"))
        return dataset.schema, dataset.scanner(batch_size=BATCH_ROWS).to_batches()
    rbr = _duckdb_reader(delta_path)
    return rbr.schema, rbr


# ------------------------------------------------------------- load one --------
def _load(catalog, entry: dict, source: str) -> int:
    schema_name, table_name = entry["target_schema"], entry["target_table"]
    full = f"{schema_name}.{table_name}"
    raw_schema, batches = _reader(entry["delta_path"], source)
    norm_schema = _normalized_schema(raw_schema)

    catalog.create_namespace_if_not_exists(schema_name)
    ice_schema = assign_fresh_schema_ids(visit_pyarrow(norm_schema, _ConvertToIcebergWithoutIDs()))
    tbl = force_pyarrow_io(catalog.create_table_if_not_exists(full, schema=ice_schema))

    # Auto-evolve: add any NEW source columns to the existing table (additive only). Type changes /
    # removed columns are NOT reconciled here and will fail on append below (fail loudly, by design).
    have = {f.name for f in tbl.schema().as_arrow()}
    new = [f.name for f in norm_schema if f.name not in have]
    if new:
        print(f"  {full}: schema drift — adding columns {new}")
        with tbl.update_schema() as u:
            u.union_by_name(norm_schema)
        tbl = force_pyarrow_io(catalog.load_table(full))

    # Clean reload, streamed and atomic: clear all rows, then append batch-by-batch; the whole thing
    # commits as one snapshot, so the table keeps its prior data until this fully succeeds.
    rows = 0
    with tbl.transaction() as txn:
        txn.delete(delete_filter=AlwaysTrue())
        for rb in batches:
            batch_tbl = _normalize_table(pa.Table.from_batches([rb]))
            txn.append(batch_tbl)
            rows += batch_tbl.num_rows
    print(f"  {full}: reloaded {rows:,} rows (via {source})")
    return rows


def sync_one(catalog, entry: dict) -> int:
    """Load one table; try delta-rs, fall back to DuckDB. Both attempts are transactional, so a
    failed delta-rs attempt commits nothing and DuckDB retries from a clean slate."""
    try:
        return _load(catalog, entry, "delta-rs")
    except Exception as e:
        print(f"  {entry['target_table']}: delta-rs failed ({type(e).__name__}: {e}); retrying via DuckDB…")
        return _load(catalog, entry, "duckdb")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", help="sync only this target_table (default: all)")
    args = ap.parse_args()
    entries = [e for e in TABLE_MAP if not args.table or e["target_table"] == args.table]
    if not entries:
        print(f"No TABLE_MAP entry named {args.table!r}.")
        sys.exit(1)

    catalog = get_catalog()
    print(f"Reference sync — {len(entries)} table(s), batch={BATCH_ROWS:,} rows")
    done = 0
    for e in entries:
        # STOP at the first failure: no try/except here, so an unrecoverable table aborts the run
        # (and the already-loaded tables keep their fresh data — each commit is independent).
        sync_one(catalog, e)
        done += 1
    print(f"Done — {done}/{len(entries)} table(s) reloaded.")


if __name__ == "__main__":
    main()
