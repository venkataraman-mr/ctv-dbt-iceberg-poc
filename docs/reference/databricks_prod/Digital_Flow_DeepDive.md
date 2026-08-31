# Digital Flow — Deep Dive (Digital + Social + AVOD)

> Detailed, code-grounded documentation of the Digital family workflow, built piece
> by piece for the dbt+Iceberg migration. Scope = **US only** (CA paths noted at high
> level, not detailed). Companion to CLAUDE.md.
>
> Pieces: (1) Ingestion · (2) Raw occurrence inserts · (3) Creative generation → Postgres ·
> (4) Creative sync back · (5) Occurrence raw → gold.
>
> Last updated: 2026-07-20 — Pieces 1–5 documented. **PLAN CHANGE: all 5 pieces are being
> implemented end-to-end in the new Iceberg+dbt stack** (no longer occurrence-only).
> Creative tables are **produced by the pipeline (Pieces 3+4, no prod seeding)**; Piece 3/4
> Postgres reads/writes stay on **temp/test tables** (real `creatives.*` untouched). Team
> key note: jobs run in parallel — correctness relies on CDF + per-job watermarks +
> concurrent-safe writes (→ Iceberg snapshot isolation + optimistic concurrency).
> **Appendix added: Table dependencies by piece (hive → reference → other mrdpp_prod).**

---

## Platform note: Unity Catalog Volumes = Azure Blob storage

In the current Databricks setup, **UC Volumes are Azure Blob (ADLS) containers mounted
into Unity Catalog** — i.e. every `/Volumes/{catalog}/{schema}/{volume}/...` path is
backed by Azure Blob storage. So all "file" landing (PlayON TSV, streaming checkpoints,
vx2 taxonomy reference files) is physically blob storage surfaced through the UC Volume
abstraction. **Migration implication:** the underlying blob storage stays; only the UC
Volume abstraction goes away. On EMR/K8s + Iceberg, the same blob/object storage is
accessed directly (ADLS path or S3), and Iceberg tables/warehouse live on that object
store. This is a re-pointing of paths, not a data move.

---

## Shared target: `bronze.digital_raw_occurrence`

All four ingestion paths (PlayON, BIS CTV, BIS Social US, BIS digital stream) converge
on one canonical table, **`mrdpp_prod.bronze.digital_raw_occurrence`** (US); CA rows go
to `digital_raw_occurrence_ca`. Providers are disambiguated by `provider_code` /
`source_channel`. Canonical column set (~40 cols) each path maps to:

`country_iso_2_code, provider_code, source_channel, provider_occurrence_id,
provider_creative_id, provider_source_id, capture_date, capture_month,
capture_timestamp, eventhub_enqueued_timestamp, created_timestamp, region_dma_id/name,
region_country_code, region_city_id/name, region_state_id/name, creative_type,
creative_mime_type, publisher_id, publisher_domain, creative_width/height/duration,
creative_url, creative_url_hash, retransmit, provider_campaign_id/product_id/
advertiser_id/name/product_name/advertiser_name/description/landing_page,
occurrence_description, occurrence_link_url, daisy_chain (VARIANT), purchase_method_id,
ad_insertion_point, raw_json (VARIANT)`.

`creative_url_hash` = `xxhash64(concat(split_part(url,'.',1),'.',split_part(url,'.',2),
'.',split_part(url,'.',-1)))` for CTV/Social; `xxhash64(url)` for PlayON. This hash is
the join key to creatives downstream.

**Migration boundary:** the *landing* mechanisms (Storage Queue, Blob .bz2, Volume TSV,
Event Hubs) are external EL and sit **outside dbt**. The *staging → raw* transforms
(CDF read + dedup + map to canonical schema + anti-join insert) are the **dbt-model
candidates**. Every path uses the Delta **commit-version watermark** engine
(`get/set_watermark_commit_version_from_delta`) — the piece that needs an Iceberg-native
redesign.

---

## Piece 1 — Ingestion (four provider paths)

### 1. PlayON / AVOD — Job `AVOD_PLAYON_INGESTION`
Single notebook `occurrences/digital/notebook_files/ott_ingestion_from_blob_to_raw_occ.py`
runs two classes in series. **PlayON is a permanent TSV-file path — it will NOT move to
the queue+bz2 pattern.** It is the reference template for file-first ingestion.

**A. `OTTIngestionFromBlobToStaging.process_ott_ingestion_to_staging()`**
(`ott_ingestion_from_blob_to_staging.py`) — **FILE-based (TSV).** Lists files in UC Volume
`/Volumes/{cat}/bronze/ott-playon-ingestion/ingestion` (= an ADLS blob container), reads
each as tab-delimited, header, UTF-8, multiline CSV against an explicit 50-field
`StructType`, appends to `bronze.digital_ott_staging`, then `dbutils.fs.mv` the file to
the `/archive` volume.

**B. `OTTIngestionToRawOccurrenceCDF.move_to_raw_occurrence()`** (`ott_staging_to_raw_occ.py`)
— reads Delta **CDF** of `digital_ott_staging` between watermark commit-versions,
dedups `ROW_NUMBER() PARTITION BY (mr_ad_id, vx_source_channel_id)`, filters
`country=US OR (vx_source_channel_id=15 AND country IS NULL)`, maps to canonical schema
(`provider_code=source_playon_code`, `creative_type=video`, `mime=mp4`), joins
`hive_metastore.km_preparation_db.source_channel` for `source_channel`. Idempotency:
LEFT ANTI JOIN vs raw rows from the **last 5 hours** on
`(provider_source_id, source_channel)`, then INSERT. `scrapper_channel_id = 15`.

### 2. BIS CTV / AVOD — Job `BIS_CTV_BZ2FILE_TO_RAW_OCC_CRTV_STAGING` (2 steps, serial)

**Step 1 `bis_ctv_bz2file_to_staging_nb` → `BZ2FileToStagingCtvIngestion.process_batch()`**
— **Azure Storage Queue + Blob .bz2.** Reads queue (`IngestionCtvQueueName`) for newly
arrived file names, downloads `.bz2` JSON blobs from the CTV blob container, decompresses
& extracts JSON (helper class `AzureBZ2JsonProcessor`), lands to
`jobwork.digtial_raw_occurrence_ctv_staging_daily_file`, then
`INSERT ... SELECT parse_json(json_data) ...` into
`bronze.digtial_raw_occurrence_ctv_staging` [**note: table name is misspelled
"digtial" in code**], truncates jobwork, deletes queue messages. Secrets scope
`kvl-foundation-all-eu2-sc`.

**Step 2 `bis_ctv_us_staging_to_raw_occurrence_nb` → `StagingToRawOccurrenceBisCtvUS.process_batch()`**
— CDF of the ctv_staging table, `from_json` against a large explicit occurrence STRUCT,
dedup `ROW_NUMBER() PARTITION BY occurrence.id`, `provider_code=source_bis_ctv_code`.
**Filters: `creative_type=video`, `mime=mp4`, and `publisher_id` in a hardcoded
whitelist of 11 CTV publisher ids** (32734360, 33434701, 2945144, …). Idempotency: LEFT
ANTI JOIN vs target on `(provider_occurrence_id, capture_month)`. INSERT into
`digital_raw_occurrence`.

### 3. BIS Social US — Job `BIS_SOCIAL_US_BZ2FILE_TO_DIGITAL_RAW_OCC` (4 steps, serial)
*Social's path to bronze is "a bit different" — it adds an auto-chaff gate, and the job
also triggers the metadata-product-mapping step (documented under Piece 3).*

**Step 1 `bis_social_us_bz2file_to_staging_nb` → `BZ2FileToStagingBISSocialUS.process_batch()`**
— same Storage-Queue + `.bz2` pattern (secrets `IngestionBISSocialUS*`), applies an
explicit social schema (incl. `socialPageLink`, `socialCampaignText`, carousel/image
fields) via `from_json`, lands to jobwork then INSERT into staging `bronze.bis_social_us`.

**Step 2 `bis_social_us_staging_to_raw_occurrence_nb` → `StagingToRawOccurrenceBISSocialUS.move_to_raw_occurrence()`**
— **the auto-chaff step (the main divergence).** CDF of `bronze.bis_social_us`, maps to
canonical + social-specific columns, joins reference tables
`reference.social_us_foreign_country_extension`, `reference.social_us_non_taggable_person`,
`reference.social_us_tiktok_foreign_country_extension`. Applies four rule sections that
flag junk ("chaff"): (1) social_page_link rules, (2) advertiser-name rules incl. an
all-emoji/icon **UDF** + English/non-English language detection, (3) social_campaign_text
rules incl. non-English/non-USD-currency detection with emoji-removal & Unicode-
normalization **UDFs**, (4) campaign_landing_page foreign-country-extension rules.
Chaffed rows `MERGE` into `bronze.creative_autochaff_social_us`; clean rows
(`is_auto_chaff=False`) INSERT into `digital_raw_occurrence`. Idempotency: LEFT ANTI JOIN
vs target on `(provider_occurrence_id, capture_month)`. *This step is UDF-heavy and
regex-heavy — a real porting risk (see migration notes).*

**Step 3 (→ documented under Piece 3)** `creatives/digital/.../bis_social_us_metadata_product_mapping_nb`
→ `MetadataProductMappingBISSocialUS.process_mappings()`. This is
**creative/metadata → Postgres (Piece 3)** work that runs inside the Social ingestion job
for scheduling. Summary: reverse-syncs PSQL `mapping.bis_social_metadata_product_map` →
`silver` via `MERGE`, then reads new raw-occ CDF (`provider=bis_social`, US,
`retransmit=false`), builds `mapping_surrogate_key` via `SHA2` over
advertiser_domain/social_page_link/categories/landing_page, splits **priority** (ref
`bis_social_vx_priority_advertisers`) vs **non-priority**, upserts to PSQL
`mapping.bis_social_metadata_product_map` (+ `_staging`),
`mapping.provider_metadata_creative_map`, `mapping.provider_metadata_occurrence_summary`
via stored procedures, and maintains `silver.bis_social_vx_unknown_priority_advertisers`.
Full detail deferred to the Piece 3 section.

**Step 4 `alerts/digital/.../digital_social_mapping_table_sync_alert`** — data-quality
alert on the mapping-table sync (no data movement). Out of dbt scope.

### 4. BIS Digital display — Job `EVENTHUB_OCCURRENCE_STREAM_JOB` (STREAMING)
Notebook `eh_to_raw_occ_stream_digital` → `EventhubToRawOccurrenceStreamBIS.job_executor()`
— Spark **Structured Streaming** `readStream.format("eventhubs")` (conn from
`get_configuration("IngestionCrtvMetadataEventHub", encrypted)`), `foreachBatch
→ process_micro_batch`. Per micro-batch it does more than the file paths: joins
`hive_metastore.km_preparation_gold_db.media_property_flatten` +
`media_property_data_provider_map` (BIS media-property mapping), `data_provider`,
`source_channel`, and **`{cat}.gold.creative`** (to read `classification_type`), computes
an `archive_flag` (true when provider/source_channel unmapped, classification is
NonAd/BadAd, retransmit, media property inactive, or creative_url null), then routes:
clean → `bronze.digital_raw_occurrence`, flagged → `bronze.digital_raw_occurrence_archive`,
CA → `bronze.digital_raw_occurrence_ca`. Checkpoint on a UC Volume (= ADLS blob).
**Streaming = deferred per project scope.** Note it applies a provider/media-property
enrichment + archive gate at *ingestion* that the batch/file paths don't — a semantic
difference to reconcile if the file paths become the primary route.

### Ingestion summary table

| Path | Job | Landing mechanism | Staging table | Provider code | Special logic |
|---|---|---|---|---|---|
| PlayON/AVOD | AVOD_PLAYON_INGESTION | **TSV files** on UC Volume (permanent) | `digital_ott_staging` | playon | 5-hr anti-join dedup; channel 15 |
| BIS CTV/AVOD | BIS_CTV_BZ2FILE_TO_RAW_OCC_CRTV_STAGING | Storage Queue + **.bz2** blob | `digtial_raw_occurrence_ctv_staging` | bis_ctv | 11-publisher whitelist; video/mp4 only |
| BIS Social US | BIS_SOCIAL_US_BZ2FILE_TO_DIGITAL_RAW_OCC | Storage Queue + **.bz2** blob | `bis_social_us` | bis_social | auto-chaff (UDF/regex) + product mapping (Piece 3) + alert |
| BIS Digital | EVENTHUB_OCCURRENCE_STREAM_JOB | **Event Hubs** (stream) | — (direct) | bis | media-property join + archive gate (deferred) |

### Migration notes — Piece 1
- **In-scope for dbt:** the `staging → digital_raw_occurrence` transforms (PlayON step B,
  CTV step 2, Social step 2). Model each as an **incremental model** keyed on
  `(provider_occurrence_id, capture_month)` (or provider-specific key), replacing the
  Delta-CDF read + commit-version watermark with Iceberg incremental logic.
- **Out of dbt (EL/landing):** queue/blob/.bz2 decompression, TSV Volume reads, Event
  Hubs streaming, file archival. Keep as Spark/Python pre-steps feeding Iceberg staging
  tables. Storage stays on Azure Blob (today via UC Volumes → tomorrow direct ADLS/S3).
- **Porting risks:** Social's auto-chaff (Python **UDFs** for emoji/icon detection,
  regex, Unicode normalization) is not pure SQL — it won't drop into a dbt SQL model
  cleanly; likely a Python model or a pre-dbt Spark step. Hardcoded CTV publisher
  whitelist should become a reference/seed table. Misspelled staging table name
  `digtial_raw_occurrence_ctv_staging` should be fixed on the way over.
- **Reference tables consumed here:** `hive_metastore.km_preparation_db.source_channel`,
  `data_provider`, `media_property_flatten`, `media_property_data_provider_map`
  (Stargate → ADF loaded); UC `reference.social_us_*`. All are read-only **dbt sources**.
- **Idempotency pattern** (ROW_NUMBER/QUALIFY dedup + LEFT ANTI JOIN vs target) maps
  naturally to dbt incremental `unique_key` + `is_incremental()` filters.

---

## Piece 2 — Bronze `digital_raw_occurrence` table (schema, DDL, storage)

### Where DDL lives
All Unity Catalog table DDL is under **`db_scripts/notebook_files/table_ddl/`**, one file
per UC schema: `bronze.py`, `silver.py`, `gold.py`, `spend.py`, `reference.py`,
`archive.py`, `cdc_staging.py`, `jobwork.py`, `printingestion.py`, `vx2_taxonomy.py`,
`vx2_taxonomy_ca.py`, `custom.py`. Each is a Databricks notebook that runs
`spark.sql("CREATE TABLE IF NOT EXISTS ...")` per table. This is the **schema source of
truth** to port into dbt models / Iceberg DDL.

### The CTV-relevant bronze tables (all in `bronze.py`)

**1. `digtial_raw_occurrence_ctv_staging`** [sic] — CTV ingestion landing table (Piece 1
step 1 target). Minimal schema: `json_data VARIANT`, `record_index INT`,
`blob_name STRING`, `source_filename STRING`, `created_timestamp TIMESTAMP`. Raw JSON is
kept as **VARIANT** and only parsed (via `from_json` against an explicit STRUCT) at the
staging→raw step. No clustering.

**2. `digital_raw_occurrence`** — the shared canonical bronze occurrence table (the CTV
staging→raw target, and the source for all post-bronze pieces). ~40 columns (documented
in the "Shared target" section above), fully commented in the DDL. Key physical config:
- `USING delta`, `LOCATION '{container_name}/deltas/mrdpp/bronze/digital_raw_occurrence'`
  (an ADLS blob path — see Volumes note).
- **`CLUSTER BY (capture_month, provider_occurrence_id)`** — liquid clustering; these two
  cols are also the dedup/anti-join key used throughout ingestion.
- VARIANT columns: `daisy_chain`, `raw_json`.
- `created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()`.

**3. `digital_raw_occurrence_archive`** — identical schema to `digital_raw_occurrence`,
same clustering; holds rows the stream's `archive_flag` (or housekeeping) diverts out of
the main raw table. (Also `digital_raw_occurrence_archive` DDL duplicated in `archive.py`.)

Also in `bronze.py` and relevant to the family: `digital_ott_staging` (PlayON staging;
uses `occurrence_id BIGINT GENERATED BY DEFAULT AS IDENTITY`, `CLUSTER BY (occurrence_id,
capture_date)`), `creative_unique_urls` (`CLUSTER BY (creative_url_hash)`, identity
feature), `bis_social_us` (social staging), `creative_autochaff_social_us` (chaff archive).

### Delta table features in use (and Iceberg mapping)
Every bronze table carries this `TBLPROPERTIES` block. These are the concrete
Databricks/Delta couplings Piece 2 hands to the migration:

| Delta feature (TBLPROPERTIES / DDL) | Purpose | Iceberg / dbt equivalent |
|---|---|---|
| `USING delta` + `LOCATION abfss://…` | Delta table on ADLS blob | Iceberg table (format-v2) on same ADLS/S3 warehouse |
| `delta.enableChangeDataFeed=true` | **CDF** — the incremental read engine used everywhere | No CDF; use Iceberg incremental/changelog scans + redesigned watermark. **Biggest item.** |
| `CLUSTER BY (...)` (liquid clustering) | data skipping / layout | Iceberg hidden **partitioning** (e.g. by `capture_month`) + sort order (`provider_occurrence_id`); maintained via `rewrite_data_files` |
| `delta.enableDeletionVectors=true` | merge-on-read deletes | Iceberg v2 positional deletes (MoR) |
| `delta.enableRowTracking=true` | row lineage (supports CDF) | No direct equivalent; typically dropped |
| `VARIANT` columns (`json_data`,`daisy_chain`,`raw_json`) | semi-structured JSON | **No Iceberg VARIANT type** — map to `string` (JSON text) or a typed `struct`. Decision needed for CTV: staging `json_data` and raw `raw_json`/`daisy_chain`. |
| `GENERATED BY DEFAULT AS IDENTITY` (ott staging) | auto surrogate id | No Iceberg identity; use sequence/hash/monotonic id. *(CTV staging does not use identity — lower risk for the migration.)* |
| `... DEFAULT CURRENT_TIMESTAMP()` + `allowColumnDefaults` | column defaults | Set in the model/INSERT; Iceberg default support is engine-dependent |
| `delta.autoOptimize.autoCompact`, `checkpointPolicy=v2` | auto file compaction / checkpoints | Iceberg maintenance procedures (`rewrite_data_files`, `expire_snapshots`) on a schedule |

### Migration notes — Piece 2
- **Good news for CTV:** the CTV staging table is trivial (VARIANT + a few cols) and uses
  **no identity column**, so the two hardest DDL features (identity, complex clustering)
  are mostly a `digital_raw_occurrence`-level concern, not CTV-staging-level.
- **VARIANT decision is the key schema choice.** Simplest is to land JSON as
  `string` in the Iceberg staging table and parse with `from_json`/`get_json_object` in
  the model — mirrors what the current code already does at staging→raw. Revisit typed
  structs later if query patterns need them.
- **Partitioning:** replace `CLUSTER BY (capture_month, provider_occurrence_id)` with
  Iceberg partitioning on `capture_month` (already the natural time grain and anti-join
  key) plus a sort order on `provider_occurrence_id`.
- **CDF replacement** is shared with every other piece — settle the Iceberg incremental/
  watermark strategy once (foundations step) and reuse for CTV staging→raw and raw→gold.

---

## Piece 3 — Creative generation → Postgres (two jobs)

> **SCOPE (2026-07-20, updated): IN scope — implemented end-to-end in the new stack.**
> Job 1 Step 1 (creative staging → Postgres) and Job 2 Step A (first-seen → Postgres) run
> fully — **UCA API id reservation**, **`creative_unique_urls` (Iceberg) registry writes**,
> and the **Postgres push** — with the **Postgres targets pointed at temp/test tables**
> (real `creatives.*` untouched). This is now **part of the real end-to-end flow**: it
> hands creatives to classification (on the temp/test Postgres), which **Piece 4 syncs back
> to build `gold.creative`**, which **Piece 5 gates on**. (Previously scoped as a standalone
> validation; now wired end-to-end.) Full detail below.

The creative process is split across two Databricks jobs. The in-scope
paths are **Job 1 Step 1** (creative staging → Postgres) and **Job 2 Step A** (creative
first-seen → Postgres). Job 1 Step 2 (source-matched) and Job 2 Step B (occ summary) are
UI/audit-only and **out of scope** (documented for completeness). Product
auto-mapping is Digital/Social-only and N/A for CTV.

### New entities introduced in Piece 3
- **`bronze.creative_unique_urls`** — the **creative registry / dedup + state machine**.
  One row per unique `creative_url_hash`, holding the assigned `creative_id`, `is_staged`
  flag, `first_seen_media_id`, `first_seen_provider_id`. This is the "have we seen this
  creative before, and has it been pushed to Postgres?" table. `CLUSTER BY
  (creative_url_hash)`, identity feature enabled.
- **`bronze.creative_autochaff`** — registry of creatives judged junk (chaff), so they're
  never re-processed.
- **Postgres `creatives.creative_staging`** — where creatives land for ML +
  classification (the hand-off to the ML/UI teams).
- **Postgres `creatives.creative_first_seen`** — first-seen metadata (when/where/on what
  property a creative was first observed) for UI classification display.
- **Universal Creative API (UCA)** — an external HTTP service
  (`UCAglobalidgeneratorUrl`, secrets scope `kvl-ucapi-all-directheat`) that **reserves a
  block of global `creative_id`s**. Called mid-pipeline via `get_api_response`.

### Job 1 — `RAW_OCCS_TO_CREATIVE_STAGING`

**Step 1 (in scope) — `raw_occ_to_crtv_staging_to_firstseen` →
`DigitalRawocctoCrtvStaging.process_to_crtv_staging()`**
(`occurrences/digital/class_files/raw_occs_to_creative_staging.py`). Identifies unique
new ads/creatives and pushes them to Postgres. Watermark
`DIGITAL_RAW_OCC_TO_CRTV_STAGING` (commit-version) on `bronze.digital_raw_occurrence`.
Sequence:
1. **CDF read** (insert-only, US, `creative_url` not null, `retransmit=false`), dedup
   `ROW_NUMBER() PARTITION BY creative_url_hash ORDER BY capture_timestamp ASC` (keep
   earliest = first sighting), then **ANTI JOIN vs `creative_unique_urls` (is_staged=true)
   and vs `creative_autochaff`** → only genuinely new, unstaged, non-chaff creatives.
2. **`map_media_market_details_to_occurrence`** — heavy enrichment: derives
   `creative_mime_type` (big CASE by source_channel/mime/file-extension), and joins
   `hive_metastore.km_preparation_gold_db.media_property_flatten` +
   `media_property_data_provider_map` (→ media_property_id/name/category, market_id),
   `reference.global_market` + `reference.provider_global_market_map` (market via
   `provider_dma_city_name`), `standard_mime_type`, `source_channel`, `data_provider`.
   Dedup by `creative_url_hash` earliest → `tmp_digital_raw_occ_media`.
3. **`get_auto_chaff_records` / `exclude_auto_chaff_records`** — a scoring model
   (ad_size, creative_type, ad_server, href, creative_url, click-through scores; video
   duration 2–220s; threshold ≥315). **BIS-display only.** The CASE only scores
   `provider_code = bis`; for everything else `is_auto_chaff=false`. **So for CTV
   (and PlayON) this step is effectively a no-op — all CTV creatives pass.** (BIS Social
   was already chaffed at ingestion.)
4. **`get_new_creatives`** — ANTI JOIN vs `creative_unique_urls` for truly-new hashes,
   then calls the **UCA API** to reserve a block of ids, and `generate_internal_id`
   assigns `creative_id = row_number() + (min_id-1)` and builds a `first_seen_metadata`
   JSON struct.
5. **`get_previous_creatives`** — rows in `creative_unique_urls` with `is_staged=false`
   (previous run pushed to registry but failed to reach Postgres) — retried, reusing the
   existing `creative_id`.
6. **`combine_creatives`** — union new + previous → Delta temp
   `jobwork.tmp_digital_raw_occ_to_crtv_staging`.
7. **`persist_to_auto_chaff`** (MERGE → `creative_autochaff`), **`persist_to_unique_url`**
   (MERGE new → `creative_unique_urls` with `is_staged=false`, sets creative_id +
   first_seen_media/provider).
8. **`insert_to_crtv_staging_first_seen_psql`** — the actual **push to Postgres**
   `creatives.creative_staging` via stored proc
   `creatives.sp_dbx_digital_insert_crtv_staging_first_seen`.
9. **`update_flag_creative_unique_url`** — MERGE `is_staged=true` (only after the Postgres
   push succeeds — this is the idempotency/at-least-once guard), then set watermark.

*Idempotency & "push once" logic* lives entirely in `creative_unique_urls.is_staged`: a
creative is registered (staged=false) → pushed to Postgres → flipped to staged=true.
If already staged, step 1's anti-join skips it → **no action if already pushed**.

CTV-specific bits: `media_id = ctv_media_id` (vs digital) when
`provider_code = bis_ctv`; auto-chaff scoring skipped; otherwise identical to PlayON/BIS.

**Step 2 (out of scope, audit) — `digital_source_matched_creatives_to_sql_nb` →
`DigitalSourceMatchedCreativesToSql`.** Finds creatives whose `creative_url_hash` already
exists in `creative_unique_urls` but arrived from a *different* media/provider, and pushes
them to Postgres `creatives.source_matched_creatives` for a UI audit view. No pipeline
impact. Notable detail worth keeping: it **caps its CDF upper bound at the version already
processed by `DIGITAL_RAW_OCC_TO_CRTV_STAGING`** (reads `silver.watermark_control`) to
avoid the two-task watermark gap — a good example of an inter-job watermark dependency.

### Job 2 — `CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY` (two steps, parallel)

**Step A (in scope) — `occurrences/common/notebook_files/raw_occ_to_crtv_firstseen` →
`RawocctoDigitalCrtvFirstseen.process_digital_first_seen()`.** Loads Postgres
`creatives.creative_first_seen` (when a creative was first seen, on what
property/media/market, first-seen timestamp) so the UI team can present creatives for
classification. Uses a **commit-TIMESTAMP watermark**
(`DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE`) — note this differs from the version-based
watermark used elsewhere. CDF from timestamp → dedup by creative_url_hash earliest → join
`creative_unique_urls` (for creative_id) + media-property + market → upsert Postgres via
`creatives.sp_dbx_digital_update_raw_occ_to_crtv_first_seen`. `media_id=ctv` for CTV.
*Scheduling note:* the notebook runs **Print first-seen always**, and **Digital
first-seen only when `datetime.now().hour % 6 == 0`** (every 6 hours); TV is commented
out.

**Step B (out of scope, audit) — `occurrences/common/notebook_files/raw_occ_to_occ_summary`
→ `CrtvOccSummary.process_occurrence_to_psql_and_staging()`** (+ Print summary). Builds an
occurrence-count summary per creative/market/media/property/date and upserts Postgres
`creatives.creative_occurrence_summary` for a UI audit view. Uses a holding table
`bronze.missing_digital_occurrence_for_summary` (UNION with CDF, then MERGE: delete when
creative_id resolved, insert when still unknown) — a pending buffer for occurrences whose
creative_id hasn't been assigned yet. Not used by any downstream pipeline.

### Migration notes — Piece 3
- **CTV is the simplest creative path:** auto-chaff scoring is skipped for CTV, and there's
  no product auto-mapping — so the CTV creative flow is essentially CDF → enrich → assign
  id → register → push to Postgres.
- **`creative_unique_urls` — DECIDED: retained as-is in Iceberg+dbt.** Stays the creative
  registry of record with the same register(`is_staged=false`)→push→flip(`is_staged=true`)
  state-machine semantics, ported to an Iceberg table. Because it's a multi-step MERGE
  state machine with the Postgres push in the middle, it is **not a pure dbt incremental** —
  implement as a Python/Spark model (or dbt incremental + post-hook) that preserves the
  "flip staged only after Postgres success" guarantee. No redesign of the table/semantics.
- **External UCA API for creative_id — DECIDED: hard dependency, preserved unchanged.** The
  mid-pipeline call to `UCAglobalidgeneratorUrl` to reserve global creative_ids stays as-is
  (it's the global id authority across systems). It remains a **Python step** inside the
  creative process; `creative_id` is NOT regenerated via sequence/hash. This confirms the
  creative-staging step is a Python/Spark model, not a pure SQL dbt model.
- **Postgres pushes** (`creative_staging`, `creative_first_seen`) via stored procs remain
  **outside dbt** — a Spark/Python serving step after the models run. **In the migration
  these target temp/test Postgres tables** (real `creatives.*` untouched).
- **Two watermark styles** appear here: version-based (staging) and **timestamp-based**
  (first-seen). The Iceberg incremental redesign must cover both.
- **Reference/enrichment tables** (all read-only sources): hive `media_property_flatten`,
  `media_property_data_provider_map`, `data_provider`, `source_channel`,
  `standard_mime_type`, `standard_ad_size`, `adscore_provided_adservers`; UC
  `reference.global_market`, `reference.provider_global_market_map`.

---

## Piece 4 — Creative sync-back (Postgres → Databricks) — Job `SYNC_CREATIVES_TO_DATABRICKS`

> **SCOPE (2026-07-20, updated): IN scope — implemented in the new stack.** Plan changed:
> the creative subsystem is now built end-to-end, so this sync-back **produces
> `gold.creative`, `silver.creative_dedupe_map`, `gold.creative_first_seen`,
> `bronze.creative_unique_urls`** in the new stack (**no prod seeding**). It **reads
> classified creatives from the temp/test Postgres** tables Piece 3 wrote to (real
> `creatives.*` untouched). The VX1 gold gate + `creative_mapping_translation_hold` below
> are live; the `gold.creative` produced here is exactly what Piece 5's occurrence gate
> reads. Core tasks 1a/1b/2/3 are essential; tasks 4/5/6a/6b are enrichment/maintenance
> (lower priority, built as needed).

Single job, **8 tasks**. This is where classified/product-tagged creatives come **back**
from Postgres into `gold.creative`, and where the **creative-level gold gate** (VX1
mandatory) lives. All watermarks here are **timestamp-based** (in
`silver.watermark_control`), off either PSQL `updated_timestamp` or Delta `_commit_timestamp`.

Task graph:
- **1a + 1b (parallel):** first-seen sync · dedupe-map sync
- **2:** creatives → `gold.creative` (the gate)
- **3:** update first-seen info · **4:** update first-seen occurrence_id · **5:** update
  last-seen info
- **6a + 6b (parallel):** product-translation resync · component-coding sync

> **CTV note:** none of the 8 tasks carry CTV-specific branches — CTV participates
> generically as one media/provider. The one place media identity matters is the
> **dedupe map** (task 1b), where any of CTV/TV/PlayON/Digital can be parent or child.

### Task 2 — `sync_psql_creatives_to_databricks` → `PsqlCrtvSync.process_batch()` (the gold gate)
**This is the crux of "a creative only reaches gold if classified + product-tagged."**
1. Reads changed creatives from **PSQL `creatives.creative`** via stored proc
   `creatives.sp_dbx_creative_get_changes_for_databricks` (two watermarks:
   `SYNC_PSQL_CRTVS_TO_GOLD_CRTV` for live, `SYNC_PSQL_ARCHIVE_CRTVS_TO_GOLD_CRTV` for
   archived; each with a **5-minute lookback** safety buffer). Also re-pushes anything
   currently parked in `silver.creative_mapping_translation_hold` back into the batch.
2. `schema_process` shapes columns and joins `data_provider` / `source_channel` for codes.
3. **`rev_translate_creatives` → `get_reverse_translation(...)`** (shared fn in
   `common/common_functions.py`) does the **vx0 → vx1 / vx2 product taxonomy translation**
   against `hive_metastore.productcentral.productmap`. It then computes a **`holding_flag`**:
   `TRUE` when `classification_type = Advert` **AND VX1 product mapping is missing**
   (`vx1_product_id IS NULL`, or vx1 present but vx1 secondaries missing while secondaries
   exist). **The VX2 branch of the filter is commented out** — so **only VX1 is mandatory**
   for gold (matches your description; VX2 is loaded when available but not gating).
4. **`persist_gold_creative`** — `MERGE INTO gold.creative` with **both the matched-UPDATE
   and not-matched-INSERT gated on `holding_flag = False`.** So creatives missing a VX1
   product mapping **do not enter gold**. `gold.creative` is loaded with **all three
   taxonomies**: `primary_product_id` (vx0), `vx1_product_id`, `vx2_product_id` (+ their
   secondary-product variants), plus full classification/attribution/first-seen/print cols.
5. **`persist_translation_hold_creative`** — `MERGE INTO silver.creative_mapping_translation_hold`:
   INSERT when `holding_flag=True` (park the creative), DELETE when `holding_flag=False`
   (release once it becomes translatable). **This is the creative-level holding buffer** —
   analogous to, but separate from, the occurrence-level silver holding buffer in Piece 5.
6. Also mirrors the hold state to PSQL `creatives.creative_classification_engine_holding`,
   logs changes to `silver.gold_creative_change_log`, then advances watermarks.

**So the two-level gate is:** creative needs VX1 → enters `gold.creative` (task 2); then
(Piece 5) an occurrence needs its creative present/classified in `gold.creative` → enters
gold occurrence. Held creatives sit in `creative_mapping_translation_hold` and are retried
every run.

### Task 1b — `sync_creative_dedupe_map_to_databricks` → `SyncCreativeDedupMapFromPsql` (cross-media parent/child)
Syncs **PSQL `creatives.creative_dedupe_map` → `silver.creative_dedupe_map`**. This is the
ML/UI-populated **parent↔child creative mapping across media**: columns
`child_creative_id/url_hash/provider_code/creative_type/subtype` and
`parent_creative_*` equivalents, plus `match_type` (from PSQL `reference.creative_match_type`),
`is_auto_mapped`, `video_score`, `audio_score`, `json_response`. Any of CTV/TV/PlayON/
Digital can be parent or child; **Print & Social are excluded upstream** (never in the
PSQL map). Two watermarks: upserts (`SYNC_CREATIVE_DEDUP_TO_DATABRICKS`) and deletes
(`SYNC_DELETED_CREATIVES_FROM_DEDUPED_MAP`). This table drives the first-seen "family"
logic in task 3.

### Task 1a — `sync_creative_first_seen_from_psql_to_databricks` → `SyncCreativeFirstSeenFromPsql`
PSQL `creatives.creative_first_seen` (the same table Piece 3 Job 2 populated —
`provider_id IN (2,11,13,16,18)`) → `gold.creative_first_seen`, MERGE on
`creative_url_hash`. Watermark `SYNC_CRTV_FIRST_SEEN_FROM_PSQL_TO_GOLD`.

### Task 3 — `update_creative_first_seen_info` → `UpdateCreativeFirstSeenInfo`
Reads CDF of `gold.creative_first_seen` + `gold.creative`, resolves each creative to its
**dedupe parent** (`creative_dedupe_map` where `match_type='Map'`), gathers the whole
family, picks earliest `occurrence_timestamp`, and MERGEs ~22 `first_seen_*` columns onto
`gold.creative` **at the deduped-parent/family level**. Enriched by `reference.media` and
`hive_metastore.km_preparation_db.data_provider`. Two commit-timestamp watermarks.

### Task 4 — `update_crtv_first_seen_occurrence_id` (notebook-only, no class)
Backfills `gold.creative.first_seen_occurrence_id` (internal id) from the provider
occurrence id, joining `gold.creative` to `gold.digital_gold_occurrence` +
`gold.tv_gold_occurrence`. **No watermark — uses a hardcoded date filter (`'2025-10-13'`)
+ rolling 1-month occurrence window.** Per the team: this is a **recently-added,
permanently-running step that is deliberately date-bounded** so it only backfills the
internal `first_seen_occurrence_id` for newer creatives (not older data). The hardcoded
date is intentional scoping, not a one-off backfill.

### Task 5 — `update_creative_last_seen_info` → `UpdateCreativeLastSeenInfo`
Gated to **04:00 UTC**. CDF of three gold occurrence tables (`tv_`, `digital_`,
`live_events_gold_occurrence`), takes the latest `capture_timestamp` per creative, MERGEs
`last_seen_timestamp` onto `gold.creative`. Three commit-timestamp watermarks. (Keyed on
changed `creative_id` directly — no dedupe-parent resolution.)

### Task 6a — `product_translation_resync_process` (notebook-only)
Gated to **02:00 UTC**. When products change in `hive_metastore.productcentral.productmap`
(after `taxonomyLaunchDate 2024-05-23`), re-derives `vx1/vx2_product_id` (+secondaries) via
`get_reverse_translation` and MERGEs the refreshed taxonomy ids onto `gold.creative`
(LEFT ANTI JOIN excludes creatives currently in `creative_mapping_translation_hold`).
Logs to `silver.creative_product_translation_resync_log`. Periodic taxonomy-maintenance.

### Task 6b — `sync_component_coding_to_databricks` → `PsqlComponentSync`
Syncs PSQL `creatives.component_coding` (per-creative component/attribute annotations) →
`gold.component_coding`, with its own vx0→vx2 reverse-translation (via
`productcentral.productmap`, `vx0_vx2_advertiser_mapping`,
`km_preparation_db.vx0_vx2_component_mapping`, `productcentral.vx0_vx2_mattress_product_mapping`,
and `vx2_taxonomy.d_product` / `d_advertiser`) and **its own holding buffer**
`silver.component_coding_translation_hold` (same park/release pattern as task 2).
Watermark `SYNC_COMPONENT_CODING_PSQL`.

### Migration notes — Piece 4
- **`get_reverse_translation` (vx0→vx1/vx2 via `productcentral.productmap`) is central
  shared logic** used by tasks 2, 6a, 6b. Porting it once (as a shared macro / Python
  helper) covers all three. `productmap` is a hive reference source.
- **Two creative-level holding buffers** (`creative_mapping_translation_hold` for the main
  gate, `component_coding_translation_hold` for components) — plus the occurrence-level
  buffer in Piece 5. All follow the same **park-when-unresolved / release-when-resolved**
  MERGE pattern; design one reusable Iceberg pattern for all.
- **Task 2 is a Python/Spark model, not pure SQL dbt** — it pulls from Postgres via
  stored proc, calls the translation helper, does a conditional MERGE, and writes back a
  PSQL holding table. The gate (`holding_flag` on VX1) can be a model column; the
  register/hold/release is the stateful part.
- **VX1 is the only mandatory taxonomy for gold** (VX2 filter commented out). Confirmed
  behavior to preserve.
- **Postgres round-trips** (stored procs, jdbc temp tables, `creative_classification_engine_holding`)
  stay outside dbt as Spark/Python serving steps — targeting **temp/test Postgres** in the
  migration.
- **DECIDED (updated) — Piece 4 is IN scope and implemented.** Core tasks 1a/1b/2/3 build
  `gold.creative` + `silver.creative_dedupe_map` + `gold.creative_first_seen` end-to-end
  from temp/test Postgres (no prod seeding). Tasks 4/5/6a/6b are enrichment/maintenance,
  lower priority. The VX1 gold gate and holding-buffer semantics must be preserved.

---

## Piece 5 — Occurrence raw → gold — Job `DIGITAL_RAW_TO_GOLD_OCCS` ⭐ (the most complex piece)

`occurrences/digital/notebook_files/raw_occurrence_to_gold_occurrence` →
`DigitalRawocctoGoldocc.process_raw_occ_stg_occ_data()`
(`occurrences/digital/class_files/raw_occ_to_gold_occ.py`). **The most involved piece of
the migration to reproduce on dbt+Iceberg.** It has two halves driven by two commit-version watermarks:
**(A)** new occurrences → gold (creative-gated, with the holding buffer), and **(B)**
reactive updates to gold when `gold.creative` changes.

### Key tables
- **Source:** `bronze.digital_raw_occurrence` CDF (watermark `DIGITAL_RAW_OCC_TO_GOLD_OCC`).
- **Holding buffer:** `silver.digital_staging_occurrence` — the "waiting on creative
  classification" pending set.
- **Gate inputs (produced by Piece 4 in the new stack):** `gold.creative` (classification +
  `primary_product_id`), `silver.creative_dedupe_map` (cross-media parent/child).
- **Target:** `gold.digital_gold_occurrence`.
- **Dimension/side tables (MERGE-upserted):** `gold.digital_deployment_chain`,
  `digital_deployment_chain_role`, `digital_deployment_chain_mediator`; spend enrichment
  from `spend.digital_dmi_prelim_spend_average_by_property/_by_media` and
  `gold.digital_spend_availability`.
- **Second source:** `gold.creative` CDF (watermark `DIGITAL_CRTV_CHANGES_TO_GOLD_OCC`).

### Notebook-level scheduling optimization (the holding-buffer reprocess window)
The notebook sets `staging_query_addon`: during **US-Eastern hours 18:00–02:00** it
reprocesses the **entire** `digital_staging_occurrence` (no filter); otherwise it limits
to `capture_month >= (now − 12 months)`. So the full pending set is drained in off-peak
windows; peak runs only retry the last ~year. This is a cost/perf lever to carry over.

### Half A — new occurrences → gold (or hold)
1. **`get_new_raw_occurrence_data_cdf`** — CDF of `digital_raw_occurrence` (insert, US,
   `retransmit=false`), dedup by `(country, provider_code, provider_occurrence_id)` latest
   → `jobwork.tmp_raw_occ_for_gold`.
2. **`persist_roles_mediator` / `persist_deployment_chain`** — explode `daisy_chain`,
   upsert roles/mediators and the deployment-chain dimension (md5 hash of purchase-method +
   sorted chain) into the `gold.digital_deployment_chain*` tables.
3. **`combine_raw_occ_stg_occ_adclassification` — THE GATE.** UNION of new raw occurrences
   (`source_flag='1-RawOccurrence'`) **+ the existing holding buffer**
   (`source_flag='2-IntermediateStaging'` from `silver.digital_staging_occurrence`,
   filtered by `staging_query_addon`). Dedup per occurrence. Enrich with media-property /
   market / source-channel / data-provider. Join `creative_dedupe_map` (match_type=Map) to
   get the **parent** creative hash, then LEFT JOIN `gold.creative` on the parent (and a
   child join on the original hash). Compute **`occurrence_hold_flag`**:
   - **'Not Hold'** when the occurrence's creative is present in `gold.creative` as
     `classification_type=Advert` **AND `primary_product_id` is set (≠ -1)** — i.e. the
     creative is classified **and product-tagged**. For BIS/BIS-Social with a daisy chain
     it additionally requires a resolved `deployment_chain_id` and the child creative to
     exist. (For **CTV**, the simple branch applies: creative present as Advert +
     `primary_product_id` set → Not Hold; no deployment-chain requirement.)
   - **'Hold'** otherwise.
   Also derives `delete_flag` (creative is NonAd/BadAd), `is_house_ad` (creative product
   vs media-property parent company), `market_id`, `mediator_chain`. → `jobwork.tmp_occ_dedup_result`.
4. **`persist_to_gold_occ`** — filter `occurrence_hold_flag='Not Hold'`, add prelim
   spend/impression averages, **`MERGE ... WHEN NOT MATCHED INSERT` into
   `gold.digital_gold_occurrence`** (matched on capture_date/country/capture_month/
   provider_occurrence_id, **partition-pruned by the batch's `capture_month` list**).
5. **`persist_to_intermediate_staging` — holding-buffer management.** `MERGE INTO
   silver.digital_staging_occurrence`: **INSERT** when a *new* occurrence is `Hold`
   (park it); **DELETE** when a *previously-held* (`2-IntermediateStaging`) occurrence is
   now `Not Hold` (it reached gold, so release it). This is exactly the "park until the
   creative is classified, then release" mechanic.
6. `update_spend_availability`; then set watermark `DIGITAL_RAW_OCC_TO_GOLD_OCC`.

### Half B — reactive updates when `gold.creative` changes
Driven by a second CDF read of `gold.creative` (watermark `DIGITAL_CRTV_CHANGES_TO_GOLD_OCC`):
- **`merge_statement_creative_match_unmatch_update`** — when dedupe-map parent/child
  mapping changes, re-point `gold.digital_gold_occurrence.creative_id` /
  `provider_parent_creative_url_hash`.
- **`update_gold_occ_delete_flag`** — when a creative flips to NonAd/BadAd, set the gold
  occurrence `delete_flag=true`.
- **`update_house_ad_flag`** — recompute `is_house_ad` from creative product changes within
  the spend-availability window.
- Set watermark `DIGITAL_CRTV_CHANGES_TO_GOLD_OCC`.

> Half B is why held occurrences eventually surface: once a (Piece 4-produced) `gold.creative`
> row becomes an Advert with a product, the next run's Half A re-reads the held occurrence
> from `digital_staging_occurrence`, passes the gate, and releases it to gold — while Half
> B fixes up already-published gold rows whose creative changed.

### Migration notes — Piece 5 (the crux of the migration)
- **This is a stateful incremental, not a plain dbt incremental.** It combines: two CDF
  sources (raw occurrences + creative changes) · a **persistent holding buffer** with
  park/release MERGE semantics · a **join-gated release** against the Piece 4-produced
  `gold.creative` + `creative_dedupe_map` · and **reactive back-updates** to already-written
  gold rows.
  Design target: dbt models for the enrichment/gate + an explicit Iceberg **holding-buffer
  model** and **incremental MERGE**; the gate is a join, the park/release and Half-B
  updates are stateful MERGEs (likely dbt incremental + Python/Spark, not pure SQL).
- **PARALLEL-JOB CONCURRENCY (per the team's key note): jobs run in parallel at different
  times; correctness relies on CDF + per-job watermarks to decouple readers/writers, and
  the design already handles concurrent UC-table reads/writes.** On Iceberg this must be
  preserved via **snapshot isolation + optimistic concurrency**: multiple jobs MERGE-ing
  the same tables (`gold.digital_gold_occurrence`, `silver.digital_staging_occurrence`,
  the deployment-chain dims, and — in prod — `gold.creative`) will hit commit conflicts and
  need retry/backoff. Replacing the Delta commit-version watermark with an
  Iceberg-native incremental token is the shared prerequisite (same engine as Pieces 1–3).
- **Partition/prune:** the gold MERGE prunes on `capture_month`; keep `capture_month` as
  the Iceberg partition on `gold.digital_gold_occurrence` and the staging buffer.
- **VARIANT:** `provider_raw_json`, `daisy_chain` (→ string/JSON for the migration).
- **CTV simplification:** the gate's daisy-chain/deployment-chain branch is BIS/BIS-Social;
  CTV uses the simple "creative present as Advert + product-tagged" branch — one fewer
  dependency to satisfy.
- **Reference/side inputs:** `media_property_flatten(_vx0_vw)`, `media_property_data_provider_map`,
  `data_provider`, `source_channel`, `origin`, `productcentral.product_flatten`, UC
  `reference.provider_global_market_map`, `spend.*`.

---

## Appendix — Table dependencies by piece (CTV migration)

> Sorted per piece: **hive_metastore → mrdpp_prod.reference → other mrdpp_prod**. Read-only
> lookups vs. pipeline-owned tables are marked; `jobwork.*` temp tables and
> `silver.watermark_control` are **pipeline-internal** (created/managed by the jobs).
> Postgres tables (`creatives.*`, `mapping.*`, PSQL `reference.*`) are listed separately —
> they are **temp/test** in the migration. `productcentral.productmap` / `product_flatten`
> enter via the shared `get_reverse_translation` in `common/common_functions.py`.

### Piece 1 — CTV ingestion (`BIS_CTV_BZ2FILE_TO_RAW_OCC_CRTV_STAGING`)
- **hive_metastore:** none
- **mrdpp_prod.reference:** none
- **other mrdpp_prod:**
  - `bronze.digtial_raw_occurrence_ctv_staging` [sic] — staging (write, then CDF read)
  - `bronze.digital_raw_occurrence` — raw target + anti-join dedup
  - `jobwork.digtial_raw_occurrence_ctv_staging_daily_file` (temp)
  - `silver.watermark_control` (control)
- *External:* Azure Storage Queue + Blob (source files) — no tables.

### Piece 2 — Bronze DDL
Not a runtime job; defines `bronze.digtial_raw_occurrence_ctv_staging`,
`bronze.digital_raw_occurrence` (+ `_archive`). No reads.

### Piece 3 — Creative generation → Postgres (`RAW_OCCS_TO_CREATIVE_STAGING`, first-seen)
- **hive_metastore:**
  - `km_preparation_gold_db.media_property_flatten`
  - `km_preparation_db.media_property_data_provider_map`
  - `km_preparation_db.data_provider`
  - `km_preparation_db.source_channel`
  - `km_preparation_db.standard_mime_type`
  - `km_preparation_db.standard_ad_size`
  - `km_preparation_db.adscore_provided_adservers`
- **mrdpp_prod.reference:**
  - `reference.global_market`
  - `reference.provider_global_market_map`
- **other mrdpp_prod:**
  - `bronze.digital_raw_occurrence` (CDF source)
  - `bronze.creative_unique_urls` (registry; anti-join + MERGE)
  - `bronze.creative_autochaff` (anti-join + MERGE)
  - `jobwork.tmp_digital_raw_occ_to_crtv_staging`, `jobwork.tmp_digital_raw_occ_to_crtv_firstseen` (temp)
  - `silver.watermark_control` (control)
- *Postgres (temp/test):* `creatives.creative_staging`, `creatives.creative_first_seen` (targets).

### Piece 4 — Creative sync-back (`SYNC_CREATIVES_TO_DATABRICKS`, 8 tasks)
- **hive_metastore:**
  - `km_preparation_db.data_provider` (task 2, 3)
  - `km_preparation_db.source_channel` (task 2)
  - `km_preparation_db.vx0_vx2_component_mapping` (task 6b)
  - `productcentral.productmap` (reverse-translation — tasks 2, 6a, 6b)
  - `productcentral.product_flatten` (reverse-translation, via `common_functions`)
  - `productcentral.vx0_vx2_advertiser_mapping` (task 2, 6b)
  - `productcentral.vx0_vx2_mattress_product_mapping` (task 6b)
- **mrdpp_prod.reference:**
  - `reference.media` (task 3)
- **other mrdpp_prod:**
  - `gold.creative` (task 2 target; read/updated by 3/4/5/6a; 6b)
  - `gold.creative_first_seen` (task 1a target; task 3 CDF)
  - `gold.component_coding` (task 6b target)
  - `gold.digital_gold_occurrence` (task 4, 5)
  - `gold.tv_gold_occurrence` (task 4, 5)
  - `gold.live_events_gold_occurrence` (task 5)
  - `silver.creative_dedupe_map` (task 1b target; task 3 read)
  - `silver.creative_mapping_translation_hold` (task 2, 6a — creative holding buffer)
  - `silver.component_coding_translation_hold` (task 6b holding buffer)
  - `silver.gold_creative_change_log` (task 2 log)
  - `silver.creative_product_translation_resync_log` (task 6a log)
  - `vx2_taxonomy.d_product`, `vx2_taxonomy.d_advertiser` (task 6b)
  - `jobwork.sync_creatives_to_databricks_rev_translated`, `jobwork.tmp_rev_translated_flag_creatives`, `jobwork.creative_updated_product_resync` (temp)
  - `silver.watermark_control` (control)
- *Postgres (temp/test):* `creatives.creative`, `creatives.creative_first_seen`,
  `creatives.creative_dedupe_map`, `creatives.component_coding`,
  `creatives.creative_classification_engine_holding`, PSQL `reference.creative_match_type`,
  plus `jobwork.*` temps.

### Piece 5 — Raw → gold occurrence (`DIGITAL_RAW_TO_GOLD_OCCS`)
- **hive_metastore:**
  - `km_preparation_gold_db.media_property_flatten`
  - `km_preparation_gold_db.media_property_flatten_vx0_vw`
  - `km_preparation_db.media_property_data_provider_map`
  - `km_preparation_db.data_provider`
  - `km_preparation_db.source_channel`
  - `km_preparation_db.origin`
  - `productcentral.product_flatten` (house-ad logic)
- **mrdpp_prod.reference:**
  - `reference.provider_global_market_map`
- **other mrdpp_prod:**
  - `bronze.digital_raw_occurrence` (CDF source)
  - `silver.digital_staging_occurrence` (occurrence holding buffer)
  - `silver.creative_dedupe_map` (parent/child join)
  - `gold.creative` (gate + 2nd CDF source)
  - `gold.digital_gold_occurrence` (target)
  - `gold.digital_deployment_chain`, `gold.digital_deployment_chain_role`, `gold.digital_deployment_chain_mediator` (dims)
  - `gold.digital_spend_availability`
  - `spend.digital_dmi_prelim_spend_average_by_property`, `spend.digital_dmi_prelim_spend_average_by_media`
  - `jobwork.tmp_raw_occ_for_gold`, `jobwork.tmp_occ_dedup_result`, `jobwork.tmp_creative_changes` (temp)
  - `silver.watermark_control` (control)
