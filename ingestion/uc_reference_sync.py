"""Reference-data sync — Unity Catalog tables: Azure ADLS Delta -> Iceberg on S3.

IDENTICAL to the hive_metastore sync (ingestion/reference_sync.py) — same shared engine
(ingestion/common/ref_sync_engine.py), same streaming + atomic reload + normalization. The ONLY
differences are the source Azure storage account (a different UC blob), the base ADLS path, and the
table map. Each source table <db>.<table> mirrors to iceberg.<db>.<table>.

Credentials come from .env (UC_AZURE_STORAGE_ACCOUNT_NAME / UC_AZURE_STORAGE_ACCOUNT_KEY); the base
path is hardcoded below (like reference_sync.py). A table resolves to {_BASE}/<db>/<table>, i.e.
.../deltas/mrdpp/<schema>/<table> — the {container_name}/deltas/mrdpp/{schema}/<table> layout from
the Databricks DDL notebooks.

Usage:
  python -m ingestion.uc_reference_sync                          # all UC reference tables
  python -m ingestion.uc_reference_sync --table creative_match_type
"""
import argparse
import sys

from ingestion import config
from ingestion.common import ref_sync_engine as engine
from ingestion.common.catalog import get_catalog

# Canonical abfss URI for the UC common blob (container before the account, dfs endpoint) — delta-rs
# needs this form, not the blob-endpoint display form. Account name/key are in .env (UC_AZURE_*),
# and UC_AZURE_STORAGE_ACCOUNT_NAME must equal the account here (vxxdbwcommonpesteu2).
_BASE = "abfss://dbwcontainer@vxxdbwcommonpesteu2.dfs.core.windows.net/deltas/mrdpp"


def _t(db: str, table: str) -> dict:
    """Source Delta path -> iceberg.<db>.<table> (target schema = source UC schema name)."""
    return {"delta_path": f"{_BASE}/{db}/{table}", "target_schema": db, "target_table": table}


# Unity Catalog reference tables to mirror into Iceberg.
# In scope for CTV Pieces 3-5 (creative dedupe + market mapping):
TABLE_MAP = [
    # reference dims (creative dedupe + market mapping)
    _t("reference", "creative_match_type"),
    _t("reference", "global_market"),
    _t("reference", "provider_global_market_map"),
    _t("reference", "media"),
    # spend averages — read by Piece 5 (raw->gold occurrence) to assign prelim_spend/impressions
    _t("spend", "digital_dmi_prelim_spend_average_by_media"),
    _t("spend", "digital_dmi_prelim_spend_average_by_property"),
    # NOTE: vx2_taxonomy.d_advertiser / d_product are MANAGED Databricks tables (USING delta, no
    # LOCATION) -> no reachable Delta path on this blob, so they can't be path-synced. They are read
    # directly from Postgres instead (their origin) via the Trino postgres catalog — see
    # dbt/models/creatives/sources.yml (source 'vx2_taxonomy').
    # Add further path-external UC reference/lookup tables here as pieces need them.
]

STORAGE = {"account_name": config.UC_AZURE_ACCOUNT, "account_key": config.UC_AZURE_KEY}


def main():
    if not (config.UC_AZURE_ACCOUNT and config.UC_AZURE_KEY):
        print("UC sync not configured: set UC_AZURE_STORAGE_ACCOUNT_NAME and "
              "UC_AZURE_STORAGE_ACCOUNT_KEY in .env.")
        sys.exit(2)
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
