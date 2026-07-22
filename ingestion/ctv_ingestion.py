"""CTV ingestion (Piece 1-2): land raw files -> bronze Iceberg via PyIceberg (no Spark).

Skeleton. The landing (Storage Queue drain + .bz2 decompress + JSON parse) is a Python
pre-step; the staging->raw dedup/anti-join is a dbt incremental model. Bronze is APPEND-ONLY
(PyIceberg append -> no delete files -> table_changes stays usable downstream).

TODO (pending finalized CTV logic):
  - queue drain + .bz2 decompress + JSON parse -> canonical ~40-col schema
  - occurrence_id from the prod Postgres sequence (block-reserved); creative_url_hash computed
  - map VARIANT -> string(json)
"""
import time
from datetime import datetime, timezone

import pyarrow as pa
from ingestion.common.catalog import get_catalog

BRONZE_STAGING = ("bronze", "ctv_raw_occurrence_staging")


def add_operational_timestamps(tbl: pa.Table) -> pa.Table:
    now = datetime.now(timezone.utc)
    n = tbl.num_rows
    return (tbl.append_column("created_timestamp", pa.array([now] * n, pa.timestamp("us", tz="UTC")))
               .append_column("updated_timestamp", pa.array([now] * n, pa.timestamp("us", tz="UTC"))))


def ingest(arrow_batch: pa.Table):
    """Append one parsed batch into the bronze staging Iceberg table (append-only)."""
    catalog = get_catalog()
    table = catalog.load_table(f"{BRONZE_STAGING[0]}.{BRONZE_STAGING[1]}")
    table.append(add_operational_timestamps(arrow_batch))


if __name__ == "__main__":
    print("CTV ingestion skeleton — wire the landing/parse step, then call ingest(batch).")
    _ = time.time()
