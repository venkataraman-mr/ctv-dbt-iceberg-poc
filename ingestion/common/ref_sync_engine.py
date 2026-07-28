"""Shared Delta -> Iceberg reference-sync engine.

Both reference syncs use this identical engine — they differ ONLY in the source Azure storage
account, the base ADLS path, and the table map:
  - ingestion/reference_sync.py     -> hive_metastore reference tables (stdlg2 common /delta path)
  - ingestion/uc_reference_sync.py  -> Unity Catalog reference tables (a different Azure blob)

Behaviour (unchanged from the original hive sync):
  - MEMORY-BOUNDED streaming: reads each Delta table in record batches; peak RAM ~= one batch.
  - CLEAN LOAD, NO DROP: clears + reloads each target Iceberg table in ONE atomic transaction
    (delete-all + append-batches), so a mid-run failure leaves the prior data intact.
  - Reader: delta-rs (rustls; INT96 -> us) primary; DuckDB per-table fallback for
    deletionVectors / v2Checkpoint (needs the system CA bundle + azure_transport_option_type='curl').
  - Normalization per batch: binary -> base64 string; timestamps -> UTC WITH time zone
    (Iceberg timestamptz), matching Databricks TIMESTAMP.
  - Target schema mirrors the source database name (hive/uc <db>.<table> -> iceberg.<db>.<table>).
  - Schema drift auto-evolves additively; the run STOPS at the first table that fails.

An `entry` is {"delta_path", "target_schema", "target_table"}. `storage` is
{"account_name", "account_key"} for the source Azure account (delta-rs + DuckDB both use it).
"""
import base64
import os

import duckdb
import pyarrow as pa
import pyarrow.dataset as pads
from deltalake import DeltaTable
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import _ConvertToIcebergWithoutIDs, visit_pyarrow
from pyiceberg.schema import assign_fresh_schema_ids

from ingestion.common.catalog import force_pyarrow_io

# Rows per streamed batch (shared by both syncs). Tune via REF_SYNC_BATCH_ROWS.
BATCH_ROWS = int(os.environ.get("REF_SYNC_BATCH_ROWS", "1000000"))


# --------------------------------------------------------------- normalize ----
def _norm_field(f: pa.Field) -> pa.Field:
    t = f.type
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return f.with_type(pa.string())
    if pa.types.is_timestamp(t):
        return f.with_type(pa.timestamp(t.unit, tz="UTC"))
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
        elif pa.types.is_timestamp(t):
            cols.append(tbl.column(i).cast(pa.timestamp(t.unit, tz="UTC")))
        else:
            cols.append(tbl.column(i))
    return pa.Table.from_arrays(cols, schema=_normalized_schema(tbl.schema))


# --------------------------------------------------- streaming readers --------
def _duckdb_reader(delta_path: str, storage: dict):
    """Streaming RecordBatchReader over a Delta table via DuckDB (fallback for deletionVectors /
    v2Checkpoint). CREATE SECRET can't take a bound param, so the connection string is inlined."""
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; INSTALL azure; LOAD azure;")
    # DuckDB's default Azure transport ignores SSL_CERT_FILE/CURL_CA_BUNDLE; use its libcurl transport.
    for stmt in ("SET azure_transport_option_type='curl'",
                 "SET ca_cert_file='/etc/ssl/certs/ca-certificates.crt'"):
        try:
            con.execute(stmt)
        except Exception as e:
            print(f"  (duckdb setting skipped: {stmt} -> {type(e).__name__}: {e})")
    conn_str = (f"DefaultEndpointsProtocol=https;AccountName={storage['account_name']};"
                f"AccountKey={storage['account_key']};EndpointSuffix=core.windows.net")
    con.execute(f"CREATE OR REPLACE SECRET az_ref (TYPE azure, CONNECTION_STRING '{conn_str}')")
    return con.execute(f"SELECT * FROM delta_scan('{delta_path}')").fetch_record_batch(BATCH_ROWS)


def _reader(delta_path: str, source: str, storage: dict):
    """Return (raw_schema, iterable[RecordBatch]) for the chosen reader, streaming."""
    if source == "delta-rs":
        dataset = DeltaTable(delta_path, storage_options=storage).to_pyarrow_dataset(
            parquet_read_options=pads.ParquetReadOptions(coerce_int96_timestamp_unit="us"))
        return dataset.schema, dataset.scanner(batch_size=BATCH_ROWS).to_batches()
    rbr = _duckdb_reader(delta_path, storage)
    return rbr.schema, rbr


# ------------------------------------------------------------- load one --------
def _load(catalog, entry: dict, source: str, storage: dict) -> int:
    schema_name, table_name = entry["target_schema"], entry["target_table"]
    full = f"{schema_name}.{table_name}"
    raw_schema, batches = _reader(entry["delta_path"], source, storage)
    norm_schema = _normalized_schema(raw_schema)

    catalog.create_namespace_if_not_exists(schema_name)
    ice_schema = assign_fresh_schema_ids(visit_pyarrow(norm_schema, _ConvertToIcebergWithoutIDs()))
    tbl = force_pyarrow_io(catalog.create_table_if_not_exists(full, schema=ice_schema))

    # Auto-evolve: add any NEW source columns (additive only). Type/removed changes fail on append.
    have = {f.name for f in tbl.schema().as_arrow()}
    new = [f.name for f in norm_schema if f.name not in have]
    if new:
        print(f"  {full}: schema drift — adding columns {new}")
        with tbl.update_schema() as u:
            u.union_by_name(norm_schema)
        tbl = force_pyarrow_io(catalog.load_table(full))

    rows = 0
    with tbl.transaction() as txn:
        txn.delete(delete_filter=AlwaysTrue())
        for rb in batches:
            batch_tbl = _normalize_table(pa.Table.from_batches([rb]))
            txn.append(batch_tbl)
            rows += batch_tbl.num_rows
    print(f"  {full}: reloaded {rows:,} rows (via {source})")
    return rows


def sync_one(catalog, entry: dict, storage: dict) -> int:
    """Load one table; try delta-rs, fall back to DuckDB. Both attempts are transactional."""
    try:
        return _load(catalog, entry, "delta-rs", storage)
    except Exception as e:
        print(f"  {entry['target_table']}: delta-rs failed ({type(e).__name__}: {e}); retrying via DuckDB…")
        return _load(catalog, entry, "duckdb", storage)


def run(catalog, entries: list, storage: dict):
    """Sync a list of entries with the given source storage account. STOPS at the first failure."""
    print(f"Reference sync — {len(entries)} table(s), batch={BATCH_ROWS:,} rows")
    done = 0
    for e in entries:
        sync_one(catalog, e, storage)   # no try/except: an unrecoverable table aborts the run
        done += 1
    print(f"Done — {done}/{len(entries)} table(s) reloaded.")
