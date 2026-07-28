"""Reference-data sync (Option C) — hive_metastore tables: Azure ADLS Delta -> Iceberg on S3.

Thin config over the shared engine (ingestion/common/ref_sync_engine.py). The engine holds all the
logic (streaming + atomic reload, delta-rs / DuckDB readers, binary->base64 + timestamptz-UTC
normalization); this file only declares the source storage account, base path, and table map.
The Unity Catalog sibling (uc_reference_sync.py) is identical but points at a different account.

Each source table hive_metastore.<db>.<table> mirrors to iceberg.<db>.<table> (target schema =
source database name).

Usage:
  python -m ingestion.reference_sync                    # all tables (daily cron)
  python -m ingestion.reference_sync --table origin     # one table
  REF_SYNC_BATCH_ROWS=500000 python -m ingestion.reference_sync   # tune batch size
"""
import argparse
import sys

from ingestion import config
from ingestion.common import ref_sync_engine as engine
from ingestion.common.catalog import get_catalog

_BASE = "abfss://databricks@stdlg2commondbrickspeu2.dfs.core.windows.net/delta"


def _t(db: str, table: str) -> dict:
    """Source Delta path -> iceberg.<db>.<table> (target schema = source database name)."""
    return {"delta_path": f"{_BASE}/{db}/{table}", "target_schema": db, "target_table": table}


# All 14 hive_metastore reference tables to mirror (see docs/reference_tables.md).
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

STORAGE = {"account_name": config.AZURE_ACCOUNT, "account_key": config.AZURE_KEY}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", help="sync only this target_table (default: all)")
    args = ap.parse_args()
    entries = [e for e in TABLE_MAP if not args.table or e["target_table"] == args.table]
    if not entries:
        print(f"No TABLE_MAP entry named {args.table!r}.")
        sys.exit(1)
    engine.run(get_catalog(), entries, STORAGE)


if __name__ == "__main__":
    main()
