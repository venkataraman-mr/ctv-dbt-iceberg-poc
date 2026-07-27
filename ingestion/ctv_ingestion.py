"""CTV ingestion — Piece 1 landing step: S3 .bz2 -> bronze staging Iceberg (no Spark).

Mirrors the legacy Databricks landing (`BZ2FileToStagingCtvIngestion` +
`AzureBZ2JsonProcessor`), with two deliberate changes for the PoC:

  1. Source is **S3**, not the Azure Storage Queue. Sample .bz2 files are dropped under
     `s3://<bucket>/landing/ctv/ingestion/`; each processed file is moved to
     `.../landing/ctv/archive/<YYYY-MM-DD>/`. (Later an Azure->S3 copier feeds `ingestion/`.)
  2. `creative_url_hash` is **precomputed here** (exact Spark xxhash64, seed 42 — see
     `common/spark_hash.py`) and carried as a staging column, because Trino's built-in xxhash64
     (seed 0) can't reproduce Spark's value in the SQL staging->raw model.

Everything else mirrors legacy: one staging row per JSON object, `json_data` = the object's JSON
text (VARIANT -> string), plus `record_index`, `source_filename`, `blob_name`, `created_timestamp`.
The dedup / video-mp4 / publisher-whitelist / anti-join logic stays in the dbt-trino staging->raw
model (`dbt/models/bronze/digital_raw_occurrence.sql`). Bronze staging is APPEND-ONLY.

The staging table is **pre-created by DDL** (`ddl/01_bronze_digtial_raw_occurrence_ctv_staging.sql`)
— this step only appends; it does not create the table. Run the DDL once before first ingest.
"""
import bz2
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List

import pyarrow as pa
import pyarrow.fs as pafs
from pyiceberg.exceptions import NoSuchTableError

from ingestion import config
from ingestion.common.catalog import load_table
from ingestion.common.spark_hash import creative_url_hash

STAGING_IDENTIFIER = "bronze.digtial_raw_occurrence_ctv_staging"  # legacy misspelling, mirrored

# The staging table is pre-created by DDL (ddl/01_...). The Arrow schema below is what we append;
# it must match that DDL (json_data VARIANT->string, record_index, source_filename, blob_name,
# created_timestamp WITHOUT zone = UTC wall-clock, creative_url_hash = precomputed exact hash).
_ARROW_SCHEMA = pa.schema([
    ("json_data", pa.string()),
    ("record_index", pa.int32()),
    ("source_filename", pa.string()),
    ("blob_name", pa.string()),
    ("created_timestamp", pa.timestamp("us")),  # tz-naive, UTC wall-clock
    ("creative_url_hash", pa.int64()),
])


# ---- JSON extraction (mirrors AzureBZ2JsonProcessor.extract_json_objects) -------------------

def extract_json_objects(content: str) -> Generator[Dict[Any, Any], None, None]:
    """Yield JSON objects from decompressed content. Handles a single object, a JSON array, or
    JSONL (one object per line) — same order of attempts as the legacy processor."""
    content = content.strip()
    try:
        obj = json.loads(content)
        if isinstance(obj, list):
            yield from obj
        else:
            yield obj
        return
    except json.JSONDecodeError:
        pass
    for line_num, line in enumerate(content.split("\n"), 1):
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError as e:
            print(f"  WARN invalid JSON on line {line_num}: {e}")


def _creative_url(obj: Dict[Any, Any]):
    """Nested, None-safe pull of occurrence.creative.url."""
    occ = obj.get("occurrence") if isinstance(obj, dict) else None
    creative = occ.get("creative") if isinstance(occ, dict) else None
    return creative.get("url") if isinstance(creative, dict) else None


def records_to_arrow(objs: List[Dict[Any, Any]], source_filename: str, blob_name: str,
                     created_ts: datetime) -> pa.Table:
    json_data, record_index, src, blob, ts, url_hash = [], [], [], [], [], []
    for idx, obj in enumerate(objs):
        json_data.append(json.dumps(obj, ensure_ascii=False))
        record_index.append(idx)
        src.append(source_filename)
        blob.append(blob_name)
        ts.append(created_ts)
        url_hash.append(creative_url_hash(_creative_url(obj)))
    return pa.table(
        [json_data, record_index, src, blob, ts, url_hash],
        schema=_ARROW_SCHEMA,
    )


# ---- S3 landing -----------------------------------------------------------------------------

def _s3():
    return pafs.S3FileSystem(region=config.AWS_REGION)


def _list_bz2(fs, prefix: str) -> List[str]:
    infos = fs.get_file_info(pafs.FileSelector(prefix, recursive=False, allow_not_found=True))
    return sorted(i.path for i in infos if i.type == pafs.FileType.File and i.path.endswith(".bz2"))


def _read_decompressed(fs, path: str) -> str:
    with fs.open_input_stream(path) as f:
        return bz2.decompress(f.read()).decode("utf-8")


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
            f"  trino -f ddl/01_bronze_digtial_raw_occurrence_ctv_staging.sql\n"
            f"(see ddl/README.md for the full run order)."
        )


def run() -> int:
    """Process every .bz2 in the ingestion prefix: parse -> append to staging -> archive.
    Returns the number of staging rows written. Per-file: append first, then archive, so a crash
    never loses the source file (at worst a file is reprocessed and the dbt anti-join dedups)."""
    fs = _s3()
    files = _list_bz2(fs, config.LANDING_INGESTION)
    if not files:
        print(f"No .bz2 files in s3://{config.LANDING_INGESTION} — nothing to do.")
        return 0

    table = _load_staging_table()
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total = 0
    for path in files:
        blob_name = path.split("/", 1)[-1]              # key without bucket
        source_filename = blob_name[:-4] if blob_name.endswith(".bz2") else blob_name
        created_ts = datetime.now(timezone.utc).replace(tzinfo=None)  # tz-naive UTC
        print(f"Processing {path} ...")
        objs = list(extract_json_objects(_read_decompressed(fs, path)))
        if objs:
            table.append(records_to_arrow(objs, source_filename, blob_name, created_ts))
            total += len(objs)
            print(f"  appended {len(objs)} rows to {STAGING_IDENTIFIER}")
        else:
            print("  no JSON records found")
        dst = _archive(fs, path, config.LANDING_ARCHIVE, day)
        print(f"  archived -> s3://{dst}")
    print(f"Done. {total} staging rows from {len(files)} file(s).")
    return total


if __name__ == "__main__":
    sys.exit(0 if run() >= 0 else 1)
