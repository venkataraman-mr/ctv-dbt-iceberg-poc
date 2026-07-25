# Reference table inventory (Option C sync)

The reference/lookup dimensions mirrored from Databricks **hive_metastore** (Azure ADLS Delta) into
**Iceberg** on S3 under the `reference` schema, by `ingestion/reference_sync.py` (daily). All sources
live on one ADLS container/account:

```
abfss://databricks@stdlg2commondbrickspeu2.dfs.core.windows.net/delta/<db>/<table>
```

Target for every row: `iceberg.reference.<table>` (Trino) — declared as dbt sources in
`dbt/models/reference/sources.yml`.

| # | Source (hive_metastore.db.table) | ADLS path (…/delta/…) | Target (iceberg.reference.*) | Reader | Deletion vectors | ~Rows (last run) |
| :-- | :-- | :-- | :-- | :-- | :-- | --: |
| 1 | km_preparation_db.adscore_provided_adservers | km_preparation_db/adscore_provided_adservers | adscore_provided_adservers | delta-rs | no | 45,002 |
| 2 | km_preparation_db.data_provider | km_preparation_db/data_provider | data_provider | delta-rs | no | 18 |
| 3 | km_preparation_db.media_property_data_provider_map | km_preparation_db/media_property_data_provider_map | media_property_data_provider_map | delta-rs | no | 71,348 |
| 4 | km_preparation_db.origin | km_preparation_db/origin | origin | delta-rs | no | 492 |
| 5 | km_preparation_db.source_channel | km_preparation_db/source_channel | source_channel | delta-rs | no | 37 |
| 6 | km_preparation_db.standard_ad_size | km_preparation_db/standard_ad_size | standard_ad_size | delta-rs | no | 13,894 |
| 7 | km_preparation_db.standard_mime_type | km_preparation_db/standard_mime_type | standard_mime_type | delta-rs | no | 111 |
| 8 | km_preparation_db.vx0_vx2_component_mapping | km_preparation_db/vx0_vx2_component_mapping | vx0_vx2_component_mapping | **DuckDB** | **yes** (+ v2Checkpoint) | 13 |
| 9 | km_preparation_gold_db.media_property_flatten | km_preparation_gold_db/media_property_flatten | media_property_flatten | delta-rs | no | 43,166 |
| 10 | productcentral.company | productcentral/company | company | **DuckDB** | **yes** | 18,244,318 |
| 11 | productcentral.product_flatten | productcentral/product_flatten | product_flatten | **DuckDB** | **yes** | 6,602,262 |
| 12 | productcentral.productmap | productcentral/productmap | productmap | **DuckDB** | **yes** | 11,228,493 |
| 13 | productcentral.vx0_vx2_advertiser_mapping | productcentral/vx0_vx2_advertiser_mapping | vx0_vx2_advertiser_mapping | **DuckDB** | **yes** | 1,017 |
| 14 | productcentral.vx0_vx2_mattress_product_mapping | productcentral/vx0_vx2_mattress_product_mapping | vx0_vx2_mattress_product_mapping | delta-rs | no | 127 |

Notes for developers:
- **Reader** is chosen automatically per table: **delta-rs** is primary; tables with **deletion
  vectors / v2Checkpoint** (5 of 14) can't be read by delta-rs's PyArrow-dataset path and fall back to
  **DuckDB** (which applies the deletion vectors). Row counts for DuckDB tables are the DV-applied
  (live) counts.
- **Schema is derived on read**, per run, from the source Delta schema (see `reference_sync.py` and
  `docs/runbook.md` §3). Normalization applied: binary columns → base64 strings; tz-aware timestamps →
  UTC wall-clock without a zone (Iceberg `timestamp`). Additive source-column changes auto-evolve the
  Iceberg table; type/removed-column changes fail loudly.
- To inspect an actual column schema in Trino:
  `docker exec -i trino trino --execute "DESCRIBE iceberg.reference.<table>"`.
- The 3 source databases: `km_preparation_db` (8 tables), `km_preparation_gold_db` (1), `productcentral` (5).
