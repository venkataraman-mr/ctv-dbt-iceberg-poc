"""CTV ingestion — Piece 1 landing step: S3 file -> bronze staging Iceberg (no Spark).

Mirrors the legacy Databricks landing (`BZ2FileToStagingCtvIngestion` +
`AzureBZ2JsonProcessor`), with deliberate changes for the PoC:

  1. Source is **S3**, not the Azure Storage Queue. Files are dropped under
     `s3://<bucket>/landing/ctv/ingestion/`; each processed file is moved to
     `.../landing/ctv/archive/<YYYY-MM-DD>/`. (Later an Azure->S3 copier feeds `ingestion/`.)
  2. `creative_url_hash` is **precomputed here** (exact Spark xxhash64, seed 42 — see
     `common/spark_hash.py`) and carried as a staging column, because Trino's built-in xxhash64
     (seed 0) can't reproduce Spark's value in the SQL staging->raw model.
  3. The input is read as **newline-delimited JSON** and streamed in bounded batches, so large
     files don't blow memory. bzip2 is auto-detected (magic `BZh`) and decompressed on the fly;
     otherwise the bytes are read as-is (some feed files carry a `.bz2` name but are plain JSON).

Everything else mirrors legacy: one staging row per JSON object, `json_data` = the object's JSON
text (VARIANT -> string), plus `record_index` (global within the file), `source_filename`,
`blob_name`, `created_timestamp`. The dedup / video-mp4 / publisher-whitelist / anti-join logic
stays in the dbt-trino staging->raw model. Bronze staging is APPEND-ONLY.

The staging table is **pre-created by DDL** (`ddl/polaris/01_bronze_digtial_raw_occurrence_ctv_staging.sql`)
— this step only appends; it does not create the table. Run the DDL once before first ingest.
"""
import bz2
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List

import pyarrow as pa
import pyarrow.fs as pafs
from pyiceberg.exceptions import NoSuchTableError

from ingestion_polaris import config
from ingestion_polaris.common.catalog import load_table
from ingestion_polaris.common.spark_hash import creative_url_hash

STAGING_IDENTIFIER = "bronze.digtial_raw_occurrence_ctv_staging"  # legacy misspelling, mirrored

# Rows appended per Iceberg append (memory bound). Each CTV object's json_data is a few KB, so the
# default keeps a batch to a few hundred MB. Tune via env if needed.
BATCH_ROWS = int(os.environ.get("CTV_LANDING_BATCH_ROWS", "50000"))
_READ_CHUNK = 8 << 20  # 8 MiB reads from S3

# Arrow schema appended to staging; must match ddl/polaris/01 (json_data VARIANT->string, record_index,
# source_filename, blob_name, created_timestamp tz-aware UTC = Iceberg timestamptz, creative_url_hash).
_ARROW_SCHEMA = pa.schema([
    ("json_data", pa.string()),
    ("record_index", pa.int32()),
    ("source_filename", pa.string()),
    ("blob_name", pa.string()),
    ("created_timestamp", pa.timestamp("us", tz="UTC")),  # tz-aware UTC (Iceberg timestamptz)
    ("creative_url_hash", pa.int64()),
])


# ---- streaming decode: S3 object -> decompressed byte chunks -> JSON objects ----------------

def _byte_chunks(fs, path: str) -> Generator[bytes, None, None]:
    """Yield decompressed byte chunks from an S3 object. Auto-detects bzip2 by magic (`BZh`);
    otherwise passes the bytes through unchanged. Handles multi-stream bzip2."""
    with fs.open_input_stream(path) as f:
        first = f.read(_READ_CHUNK)
        if first[:3] == b"BZh":
            dec = bz2.BZ2Decompressor()
            pending = first
            while True:
                if pending:
                    out = dec.decompress(pending)
                    if out:
                        yield out
                    while dec.eof and dec.unused_data:          # next concatenated stream
                        leftover = dec.unused_data
                        dec = bz2.BZ2Decompressor()
                        out = dec.decompress(leftover)
                        if out:
                            yield out
                pending = f.read(_READ_CHUNK)
                if not pending:
                    break
        else:
            if first:
                yield first
            while True:
                b = f.read(_READ_CHUNK)
                if not b:
                    break
                yield b


def _loads(raw: bytes) -> Generator[Dict[Any, Any], None, None]:
    """Parse one JSON line. Yields each element if it's an array, else the object. Skips (warns on)
    a malformed line rather than failing the whole file."""
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  WARN skipping malformed JSON line: {e}")
        return
    if isinstance(obj, list):
        yield from obj
    else:
        yield obj


def iter_json_objects(fs, path: str) -> Generator[Dict[Any, Any], None, None]:
    """Stream JSON objects from a (possibly bzip2) newline-delimited JSON file on S3. Splits on
    newlines across chunk boundaries so memory stays bounded (also handles a single-line
    array/object as a fallback)."""
    buf = b""
    for chunk in _byte_chunks(fs, path):
        buf += chunk
        parts = buf.split(b"\n")
        buf = parts.pop()                 # keep last (possibly partial) line for the next chunk
        for line in parts:
            line = line.strip()
            if line:
                yield from _loads(line)
    tail = buf.strip()
    if tail:
        yield from _loads(tail)


# ---- record shaping -------------------------------------------------------------------------

def _creative_url(obj: Dict[Any, Any]):
    """Nested, None-safe pull of occurrence.creative.url."""
    occ = obj.get("occurrence") if isinstance(obj, dict) else None
    creative = occ.get("creative") if isinstance(occ, dict) else None
    return creative.get("url") if isinstance(creative, dict) else None


def records_to_arrow(objs: List[Dict[Any, Any]], source_filename: str, blob_name: str,
                     created_ts: datetime, start_index: int) -> pa.Table:
    json_data, record_index, src, blob, ts, url_hash = [], [], [], [], [], []
    for i, obj in enumerate(objs):
        json_data.append(json.dumps(obj, ensure_ascii=False))
        record_index.append(start_index + i)
        src.append(source_filename)
        blob.append(blob_name)
        ts.append(created_ts)
        url_hash.append(creative_url_hash(_creative_url(obj)))
    return pa.table([json_data, record_index, src, blob, ts, url_hash], schema=_ARROW_SCHEMA)


# ---- S3 helpers -----------------------------------------------------------------------------

def _s3():
    return pafs.S3FileSystem(region=config.AWS_REGION)


def _list_bz2(fs, prefix: str) -> List[str]:
    infos = fs.get_file_info(pafs.FileSelector(prefix, recursive=False, allow_not_found=True))
    return sorted(i.path for i in infos if i.type == pafs.FileType.File and i.path.endswith(".bz2"))


def _archive(fs, src_path: str, archive_prefix: str, day: str) -> str:
    basename = src_path.rsplit("/", 1)[-1]
    dst = f"{archive_prefix}/{day}/{basename}"
    fs.copy_file(src_path, dst)
    fs.delete_file(src_path)
    return dst


def _load_staging_table():
    """Load the pre-created staging table (PyArrow FileIO). Fail with a clear pointer to the DDL
    if it hasn't been created yet — this step appends only, it never creates the table."""
    try:
        return load_table(STAGING_IDENTIFIER)
    except NoSuchTableError:
        raise SystemExit(
            f"Table iceberg.{STAGING_IDENTIFIER} does not exist. Create it first:\n"
            f"  trino -f ddl/polaris/01_bronze_digtial_raw_occurrence_ctv_staging.sql\n"
            f"(see docs/runbooks/polaris/polaris_pipeline_runbook.md for run order)."
        )


def _append_file(fs, table, path: str, day: str) -> int:
    """Stream one file into staging in bounded batches, then archive it. Returns rows written."""
    blob_name = path.split("/", 1)[-1]                              # key without bucket
    source_filename = blob_name[:-4] if blob_name.endswith(".bz2") else blob_name
    created_ts = datetime.now(timezone.utc)                         # tz-aware UTC (timestamptz)
    print(f"Processing {path} ...")

    total, batch = 0, []
    for obj in iter_json_objects(fs, path):
        batch.append(obj)
        if len(batch) >= BATCH_ROWS:
            table.append(records_to_arrow(batch, source_filename, blob_name, created_ts, total))
            total += len(batch)
            print(f"  appended {total} rows so far ...")
            batch = []
    if batch:
        table.append(records_to_arrow(batch, source_filename, blob_name, created_ts, total))
        total += len(batch)

    if total:
        print(f"  appended {total} rows to {STAGING_IDENTIFIER}")
    else:
        print("  no JSON records found")
    dst = _archive(fs, path, config.LANDING_ARCHIVE, day)
    print(f"  archived -> s3://{dst}")
    return total


def run() -> int:
    """Process every .bz2/plain-JSON file in the ingestion prefix: parse -> append -> archive.
    Per-file: append first, then archive, so a crash never loses the source file (at worst a file
    is reprocessed and the dbt anti-join dedups)."""
    fs = _s3()
    files = _list_bz2(fs, config.LANDING_INGESTION)
    if not files:
        print(f"No .bz2 files in s3://{config.LANDING_INGESTION} — nothing to do.")
        return 0

    table = _load_staging_table()
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total = sum(_append_file(fs, table, path, day) for path in files)
    print(f"Done. {total} staging rows from {len(files)} file(s).")
    return total


if __name__ == "__main__":
    sys.exit(0 if run() >= 0 else 1)
