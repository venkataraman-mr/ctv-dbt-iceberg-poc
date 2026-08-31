# VXC / MRDPP Platform — Architecture Map & dbt+Iceberg Migration Notes

> Living checkpoint document. Grounded, high-level map of the existing
> PySpark/Databricks repo so future sessions never re-explain the pipeline.
> Companion deep-dive: **Digital_Flow_DeepDive** (CTV/Digital family, piece by piece).
>
> Last updated: 2026-07-20 — Digital/CTV deep-dive complete (Pieces 1–5). **PLAN CHANGE:
> ALL 5 pieces are now being implemented in the new Iceberg+dbt stack** (full end-to-end).
> Creative tables are **produced by the pipeline (no prod seeding)**; Piece 3/4 Postgres
> reads/writes target **temp/test tables** (real `creatives.*` untouched). Parallel-job
> concurrency remains a top migration coupling.

## Change log
- **2026-07-19** — Initial high-level architecture map created from read-only repo scan.
- **2026-07-20** — Added CTV PoC scope; corrected Digital "no silver" → has a silver
  holding/staging buffer; recorded design decisions (UCA API, creative_unique_urls);
  linked the Digital_Flow_DeepDive companion (Pieces 1–3 done).
- **2026-07-20 (later)** — Piece 4 (creative sync-back) documented. **PoC scope refined:**
  **only Piece 4 is deferred** (occurrence-gate creative tables seeded from production);
  **Piece 3 stays in scope as a dbt→Postgres write-path validation that writes to temp/test
  tables** (not real `creatives.*`). PoC = CTV ingestion + Piece 3 validation + occurrence
  flow. Recorded the VX1 gold gate + holding-buffer mechanics for later use.
- **2026-07-20 (later 2)** — **Piece 5 (raw→gold occurrence) documented — Digital/CTV
  deep-dive complete (Pieces 1–5).** Captured the occurrence gate (`occurrence_hold_flag`),
  the `silver.digital_staging_occurrence` holding buffer (park/release), and Half-B reactive
  updates from `gold.creative` CDF. Added parallel-job concurrency as a top migration
  coupling (Iceberg snapshot isolation + optimistic concurrency).
- **2026-07-20 (later 3)** — **PLAN CHANGE: all 5 pieces now in scope for implementation**
  in the new Iceberg+dbt stack (no longer an occurrence-only PoC). Creatives are produced
  end-to-end by the migrated pipeline (**Piece 4 no longer deferred; no prod seeding**);
  Piece 3/4 Postgres reads/writes stay on **temp/test tables**. UCA + `creative_unique_urls`
  decisions now apply to the active build, not "later."

---

## 0. Current focus — CTV end-to-end migration (all 5 pieces)

The active work is a **dbt+Iceberg migration of the CTV workflow end to end — full 5-piece
build.** CTV (AVOD, BIS-CTV provider) shares the entire Digital-family flow **post-bronze**,
so CTV = CTV-specific ingestion + the common post-bronze pieces, and it **skips product
auto-mapping** (Digital/Social only). CTV is essentially the "PlayON-shaped" path.

**Scope = ALL 5 pieces, implemented end-to-end in the new Iceberg+dbt stack** (plan
changed from an occurrence-only PoC). The full creative subsystem now runs in the new
stack — **creative tables (`gold.creative`, `silver.creative_dedupe_map`,
`gold.creative_first_seen`, `bronze.creative_unique_urls`) are produced by the migrated
pipeline (Pieces 3 + 4), NOT seeded from production.** The **Postgres side stays on
temp/test tables** for both the Piece 3 push and the Piece 4 sync-back (the real
`creatives.*` classification/serving tables are not touched yet) — so the full flow and
the dbt↔Postgres round-trip are exercised without affecting production Postgres.

**In scope:** all 5 pieces — CTV ingestion (bz2→staging→bronze) · Piece 3 creative
generation → Postgres (temp/test tables) · Piece 3 first-seen → Postgres (temp/test) ·
Piece 4 creative sync-back → `gold.creative` (VX1 gold gate, from temp/test Postgres) ·
Piece 5 raw→gold occurrence (creative-gated, silver holding buffer).
**Still out of scope:** product auto-mapping (Digital/Social only, N/A for CTV);
source-matched creatives & occurrence summary (UI audit); **TV, Live Sports, Print**;
Event Hubs streaming ingestion.

Design decisions (now applied in the active build):
- **UCA API (global creative_id generator) is a hard dependency — preserved unchanged.**
  Stays a Python step; creative_id is not regenerated.
- **`bronze.creative_unique_urls` is retained as-is** in Iceberg+dbt (same
  register→push→flip `is_staged` state machine). Because of UCA + this state machine, the
  **creative-staging step is a Python/Spark model, not pure SQL dbt.**

---

## 1. What this repo is

The VXC / MRDPP data-preparation platform (package `streaming_jobs`, Kantar/MediaRadar
lineage). It ingests advertising **occurrence** and **creative** data across several
media, runs it through a **medallion architecture** (bronze → silver → gold) on
**Azure Databricks + PySpark + Unity Catalog + Delta**, and serves results to
**Postgres** for ML/UI teams.

Catalog is env-swapped: `mrdpp_catalog = get_catalog_name(os.environ["ENV_CODE"])`
→ `mrdpp_dev / test / uat / prod`. Medallion layers are **Unity Catalog schemas**
(`bronze`, `silver`, `gold`, plus `reference`, `spend`, `archive`, `cdc_staging`,
`vx2_taxonomy`, `jobwork`, `tempwork`, `printingestion`), NOT table-name prefixes.

**UC Volumes = Azure Blob (ADLS) storage** mounted into Unity Catalog. All "file" landing
(PlayON TSV, streaming checkpoints, taxonomy files) is physically blob storage. Migration
re-points these to ADLS/S3 directly; it's a path change, not a data move.

## 2. Orchestration pattern (job → notebook → class)

Jobs are **config-driven**: `{mrdpp_catalog}.silver.workflows_configuration` (Delta) →
`workflows/notebook_files/jobcluster_job_create.py` builds Jobs 2.1 JSON via
`JobOnJobClusterUtility.generate_job_json` → `DatabrickAPIUtil.create_job` (REST). At
runtime each **notebook** (`.../notebook_files/*.py`, a thin launcher) sets `sys.path`,
reads `dbutils.widgets` (`env_code_param`, `ma_app_config_url_param`), imports its **class
file** (`.../class_files/*.py`), and calls one `process_*`/`sync_*` method. `%run` is not
used; class files build their own `SparkSession` + `DBUtils`.

## 3. Shared creative workflow (same for ALL media): Bronze → Postgres

Creatives are the **unique ads** identified from occurrence data, then pushed to Postgres
for ML/UI classification. Reusable machinery in `creatives/common/`; media folders add
source-specific extraction. Detail for the Digital/CTV family is in the companion
**Digital_Flow_DeepDive** (Piece 3). Postgres transport is a two-step upsert
(`common/psql_utility.py` → Spark `df.write.jdbc` to a temp table, then `psycopg2` calls a
Postgres stored proc). dbt does not push to Postgres — this serving-sync layer stays
outside dbt.

## 4. Per-media occurrence flows (Bronze → Silver → Gold)

| Media | Provider(s) | Bronze | Silver | Gold | Shape |
|---|---|---|---|---|---|
| **Digital** | BIS | `bronze.digital_raw_occurrence` | holding/staging buffer only* | digital gold | shared trio |
| **Social** | BIS | `bronze.digital_raw_occurrence` (US)/`_ca` | buffer only* | same as digital | **= digital** |
| **AVOD / CTV** | CTV + PlayON (OTT) | `bronze.digital_raw_occurrence` | buffer only* | same as digital | **= digital** |
| **TV** | DeepListen / FasterTV / Vivvix-EWS | `bronze.tv_raw_occurrence` | `silver.tv_silver_occurrence` (+`_syndication`) | `gold.tv_gold_occurrence` | distinct |
| **Live Sports** | DeepListen | `bronze.tv_raw_occurrence` (shared w/ TV) | `silver.live_events_silver_occurrence` | `gold.live_events_gold_occurrence` | mostly distinct |
| **Print** — MR | `printingestion.raw_ad_occurrence` (not medallion) | `printingestion.ad_match` | Postgres / DPCS | fully distinct |

\* **Digital-family silver correction:** the Digital/Social/AVOD/CTV flow has **no silver
*transformation* layer**, but it DOES use a **silver-style holding/staging table** in the
raw→gold step. Each gold run reads `bronze.digital_raw_occurrence` CDF **plus the holding
table**; an occurrence only moves to gold once its **creative is classified and
product-tagged**, otherwise it stays in the holding buffer and is retried next run. So
gold is a **join-gated release with a persistent pending set**, not a simple dedup —
important for the Iceberg incremental design (detail in Digital_Flow_DeepDive Piece 5).

### Digital = Social = AVOD/CTV (confirmed)
All converge on `bronze.digital_raw_occurrence` (US) / `_ca` (Canada), disambiguated by
`provider_code`/`source_channel`. Provider-specific ingestion differs only in staging.
CTV & PlayON are the same shape from bronze onward (CTV skips product mapping). See
Digital_Flow_DeepDive for the full CTV path.

## 5. Ingestion reality: streaming today, files are the migration target
Current raw ingestion mixes **Azure Event Hubs streaming** (BIS digital display; deferred
per scope), **Storage Queue + .bz2 blob** (BIS CTV, BIS Social US), and **TSV files on UC
Volume** (PlayON — permanent). File/batch ingestion is the migration priority; streaming
is future/low-priority.

## 6. Shared infrastructure = highest-leverage migration surface
`common/` is imported by nearly everything. Key modules: `constants.py` (catalog/env,
hardcoded ADLS root), `common_functions.py` (Delta commit-version/timestamp **watermark
engine**, config REST client, secrets), `psql_utility.py` (Spark→Postgres upsert),
`sql_server_utility.py` + `cdc_ingestion/` (SQL Server CDC→Delta + SCD2),
`vx2_taxonomy/` (reference/taxonomy sync from UC Volumes; touches legacy
`hive_metastore.*`). **All DDL** lives in `db_scripts/notebook_files/table_ddl/` (one file
per UC schema: bronze/silver/gold/spend/reference/archive/cdc_staging/jobwork/
printingestion/vx2_taxonomy/custom) — the schema source of truth.

Reference/lookup tables (Stargate portal → ADF → `hive_metastore`, plus UC `reference`):
`media_property_flatten`, `media_property_data_provider_map`, `data_provider`,
`source_channel`, `standard_mime_type`, `standard_ad_size`, `adscore_provided_adservers`,
`reference.global_market`, `reference.provider_global_market_map`. Properties = where the
ad is seen (Netflix/OTT, WaPo/digital, Fox/TV). Treat all as read-only **dbt sources**;
the ADF load is upstream and not migrated.

## 7. dbt + Iceberg mapping (target)
Medallion retained: **bronze = dbt sources**, **silver = incremental models**, **gold =
table-materialized marts**. Top couplings needing redesign:
1. **Incremental engine on Delta commit-version/timestamp time-travel + watermark tables**
   (`silver.watermark_control`, `cdc_staging.water_mark_table`) — no Iceberg drop-in.
   **The crux; both version- and timestamp-based watermarks appear.**
2. **~314 `MERGE INTO`/upsert + SCD2** → dbt incremental / snapshots; Postgres stored-proc
   upserts stay outside dbt.
3. **Delta-only table features** (`VARIANT`, CDF, deletion vectors, row tracking,
   IDENTITY, `CLUSTER BY`, `OPTIMIZE/ZORDER/VACUUM`) → Iceberg mapping/redesign; VARIANT
   has no Iceberg equivalent (map to string/JSON for the migration).
4. **Databricks runtime APIs** (`dbutils`, `%run`, `display()`, `spark._jvm` EH
   encryption, `VectorSearchClient`).
5. **Event Hubs streaming** + hardcoded ADLS/UC-Volume paths.
6. **Dual-catalog world**: `mrdpp_*` UC + residual `hive_metastore.*`.
7. **Config via REST (`MA_APP_CONFIG_URL`) + secret scope `kvl-foundation-all-eu2-sc`.**
8. **Parallel-job concurrency** — jobs run in parallel at different times and today rely on
   CDF + per-job watermarks + Delta's concurrent-write handling to decouple readers/writers.
   Iceberg must preserve this via **snapshot isolation + optimistic concurrency**; concurrent
   MERGEs to shared tables (`gold.digital_gold_occurrence`, `silver.digital_staging_occurrence`,
   deployment-chain dims, `gold.creative`) will need commit-conflict retry/backoff.

## 8. Suggested migration order (CTV, all 5 pieces)
1. **Foundations:** port bronze DDL → Iceberg; design the Iceberg-native incremental/
   watermark strategy to replace the Delta commit-version/timestamp engine (prototype on
   one table). Settle VARIANT→string and `CLUSTER BY`→partition+sort.
2. **CTV ingestion** (bz2→staging→bronze) — staging→raw as incremental model; queue/bz2
   landing stays a Python pre-step.
3. **Creative generation → Postgres (Piece 3)** — Python/Spark model (UCA +
   `creative_unique_urls` state machine + Postgres push); Postgres writes → **temp/test
   tables**. Also first-seen → Postgres (temp/test).
4. **Creative sync-back → `gold.creative` (Piece 4)** — reads classified creatives from
   temp/test Postgres, VX1 gold gate + `creative_mapping_translation_hold`; produces
   `gold.creative`, `silver.creative_dedupe_map`, `gold.creative_first_seen` end-to-end.
5. **Raw → gold occurrence (Piece 5)** — the creative-gated release with the silver
   holding buffer; reads the pipeline-produced `gold.creative`.
6. Still deferred: product mapping, source-matched, occ-summary, streaming, TV/Live/Print.

## 9. Key files to read first (migration)
`common/common_functions.py`, `common/constants.py`, `common/psql_utility.py`,
`common/cdc_ingestion/class_files/cdc_ingestion_processor.py`,
`db_scripts/notebook_files/table_ddl/` (esp. `bronze.py`). CTV path detail:
Digital_Flow_DeepDive.
