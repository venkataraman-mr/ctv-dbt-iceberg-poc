# Reference table inventory (Option C sync)

The reference/lookup dimensions mirrored from Databricks (Azure ADLS Delta) into **Iceberg** on S3 by
one shared engine (`ingestion/common/ref_sync_engine.py`). Each table keeps its **source database name
as the target schema** — `<db>.<table>` → `iceberg.<db>.<table>`. There are two syncs (thin configs
over the engine), differing only in the source storage account/base path:

- **hive** (`reference_sync.py`) — 14 tables from `abfss://databricks@stdlg2commondbrickspeu2.dfs.core.windows.net/delta/<db>/<table>`.
- **Unity Catalog** (`uc_reference_sync.py`) — 6 tables from `abfss://dbwcontainer@vxxdbwcommonpesteu2.dfs.core.windows.net/deltas/mrdpp/<db>/<table>`.

Targets are dbt sources (one per schema) in `dbt/models/reference/sources.yml`. The `vx2_taxonomy`
dims (`d_advertiser`, `d_product`) are MANAGED Databricks tables with no reachable Delta path, so they
are read directly from Postgres (`postgres.vx2_taxonomy.*`), **not synced** — see
`dbt/models/creatives/sources.yml`.

### Hive sync — 14 tables

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

### Unity Catalog sync — 6 tables (`abfss://dbwcontainer@vxxdbwcommonpesteu2…/deltas/mrdpp/<db>/<table>`)

| # | Source (uc db.table) | delta path (…/deltas/mrdpp/…) | Target (iceberg.db.table) | Reader | ~Rows (last run) |
| :-- | :-- | :-- | :-- | :-- | --: |
| 1 | reference.creative_match_type | reference/creative_match_type | reference.creative_match_type | **DuckDB** | 3 |
| 2 | reference.global_market | reference/global_market | reference.global_market | **DuckDB** | 243 |
| 3 | reference.provider_global_market_map | reference/provider_global_market_map | reference.provider_global_market_map | **DuckDB** | 40,443 |
| 4 | reference.media | reference/media | reference.media | **DuckDB** | 10 |
| 5 | spend.digital_dmi_prelim_spend_average_by_media | spend/digital_dmi_prelim_spend_average_by_media | spend.digital_dmi_prelim_spend_average_by_media | **DuckDB** | 7 |
| 6 | spend.digital_dmi_prelim_spend_average_by_property | spend/digital_dmi_prelim_spend_average_by_property | spend.digital_dmi_prelim_spend_average_by_property | **DuckDB** | 9,396 |

All six UC tables carry deletion vectors / v2Checkpoint, so all load via the DuckDB fallback. Used by:
`reference.*` (creative dedupe + market mapping) in Pieces 4–5; `spend.*` averages read by Piece 5
(`raw→gold`) to assign `prelim_spend`/`prelim_impressions`.

Notes for developers:
- **Target schema = source database.** Iceberg schemas created on demand: hive → `km_preparation_db`
  (8), `km_preparation_gold_db` (1), `productcentral` (5); UC → `reference` (4), `spend` (2). 20 total.
- **Reader** is chosen automatically per table: **delta-rs** is primary; tables with **deletion
  vectors / v2Checkpoint** (5 of 14) can't be read by delta-rs's PyArrow-dataset path and fall back to
  **DuckDB** (which applies the deletion vectors). Row counts for DuckDB tables are the DV-applied
  (live) counts.
- **DuckDB→Arrow 2 GB string-buffer cap (2026-08-17).** A large UC table with a wide string column
  (the spend QC report `unified_ott_ctv_spend_qc_report_w_occ_ids_nk`, added to the UC entries)
  overflows Arrow's regular-string-buffer cap (2^31-1 bytes) while streaming; the engine sets
  `SET arrow_large_buffer_size=true` (64-bit `large_string`) to handle it. It's a per-buffer limit,
  so lowering `REF_SYNC_BATCH_ROWS` also relieves it. (Confirm whether the pipeline actually consumes
  that QC report — if not, drop it from `uc_reference_sync.py` rather than mirror a multi-GB table.)
- **Schema is derived on read**, per run, from the source Delta schema (see `reference_sync.py` and
  `docs/runbooks/nessie/runbook.md` §3). Normalization applied: binary columns → base64 strings; timestamps → UTC
  **with time zone** (Iceberg `timestamptz`), matching Databricks `TIMESTAMP`. Additive source-column
  changes auto-evolve the Iceberg table; type/removed-column changes fail loudly.
- To inspect an actual column schema in Trino:
  `docker exec -i trino trino --execute "DESCRIBE iceberg.<db>.<table>"`.
