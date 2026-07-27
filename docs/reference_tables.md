# Reference table inventory (Option C sync)

The reference/lookup dimensions mirrored from Databricks **hive_metastore** (Azure ADLS Delta) into
**Iceberg** on S3, by `ingestion/reference_sync.py` (daily). Each table keeps its **source database
name as the target schema** — `hive_metastore.<db>.<table>` → `iceberg.<db>.<table>` — rather than a
single shared `reference` schema. All sources live on one ADLS container/account:

```
abfss://databricks@stdlg2commondbrickspeu2.dfs.core.windows.net/delta/<db>/<table>
```

Targets are declared as dbt sources (one per schema) in `dbt/models/reference/sources.yml`.

| # | Source (hive_metastore.db.table) | ADLS path (…/delta/…) | Target (iceberg.db.table) | Reader | Deletion vectors | ~Rows (last run) |
| :-- | :-- | :-- | :-- | :-- | :-- | --: |
| 1 | km_preparation_db.adscore_provided_adservers | km_preparation_db/adscore_provided_adservers | km_preparation_db.adscore_provided_adservers | delta-rs | no | 45,002 |
| 2 | km_preparation_db.data_provider | km_preparation_db/data_provider | km_preparation_db.data_provider | delta-rs | no | 18 |
| 3 | km_preparation_db.media_property_data_provider_map | km_preparation_db/media_property_data_provider_map | km_preparation_db.media_property_data_provider_map | delta-rs | no | 71,348 |
| 4 | km_preparation_db.origin | km_preparation_db/origin | km_preparation_db.origin | delta-rs | no | 492 |
| 5 | km_preparation_db.source_channel | km_preparation_db/source_channel | km_preparation_db.source_channel | delta-rs | no | 37 |
| 6 | km_preparation_db.standard_ad_size | km_preparation_db/standard_ad_size | km_preparation_db.standard_ad_size | delta-rs | no | 13,894 |
| 7 | km_preparation_db.standard_mime_type | km_preparation_db/standard_mime_type | km_preparation_db.standard_mime_type | delta-rs | no | 111 |
| 8 | km_preparation_db.vx0_vx2_component_mapping | km_preparation_db/vx0_vx2_component_mapping | km_preparation_db.vx0_vx2_component_mapping | **DuckDB** | **yes** (+ v2Checkpoint) | 13 |
| 9 | km_preparation_gold_db.media_property_flatten | km_preparation_gold_db/media_property_flatten | km_preparation_gold_db.media_property_flatten | delta-rs | no | 43,166 |
| 10 | productcentral.company | productcentral/company | productcentral.company | **DuckDB** | **yes** | 18,244,318 |
| 11 | productcentral.product_flatten | productcentral/product_flatten | productcentral.product_flatten | **DuckDB** | **yes** | 6,602,262 |
| 12 | productcentral.productmap | productcentral/productmap | productcentral.productmap | **DuckDB** | **yes** | 11,228,493 |
| 13 | productcentral.vx0_vx2_advertiser_mapping | productcentral/vx0_vx2_advertiser_mapping | productcentral.vx0_vx2_advertiser_mapping | **DuckDB** | **yes** | 1,017 |
| 14 | productcentral.vx0_vx2_mattress_product_mapping | productcentral/vx0_vx2_mattress_product_mapping | productcentral.vx0_vx2_mattress_product_mapping | delta-rs | no | 127 |

Notes for developers:
- **Target schema = source database.** The three schemas created in Iceberg: `km_preparation_db`
  (8 tables), `km_preparation_gold_db` (1), `productcentral` (5). The sync creates each namespace on
  demand.
- **Reader** is chosen automatically per table: **delta-rs** is primary; tables with **deletion
  vectors / v2Checkpoint** (5 of 14) can't be read by delta-rs's PyArrow-dataset path and fall back to
  **DuckDB** (which applies the deletion vectors). Row counts for DuckDB tables are the DV-applied
  (live) counts.
- **Schema is derived on read**, per run, from the source Delta schema (see `reference_sync.py` and
  `docs/runbook.md` §3). Normalization applied: binary columns → base64 strings; timestamps → UTC
  **with time zone** (Iceberg `timestamptz`), matching Databricks `TIMESTAMP`. Additive source-column
  changes auto-evolve the Iceberg table; type/removed-column changes fail loudly.
- To inspect an actual column schema in Trino:
  `docker exec -i trino trino --execute "DESCRIBE iceberg.<db>.<table>"`.
