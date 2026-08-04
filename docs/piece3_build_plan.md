# Piece 3 — Creative push: validated build plan (RESUME HERE)

> Working handoff doc. Design is **validated by Venkat**; next action is to **build the code**.
> This is not a Drive checkpoint — it's the resume note for the Piece 3 build. Read this top-to-bottom
> and you can start building without re-reading the Databricks source.

## Where we are
- Studied the Databricks creative-push flow end-to-end (Job A + Job B). Design agreed.
- **Approach: Trino/dbt-native, NO Python.** Postgres stored procs are called through Trino's
  `postgres.system.execute(query => '…')` passthrough; the creative-id sequence is read through the
  `postgres.system.query(...)` table function. Both confirmed in current Trino (476) PostgreSQL
  connector docs.
- Nothing has been written to the repo yet. Next step = build (task #4).

## The two jobs (independent; different schedules, different watermarks)
- **Job A — `DIGITAL_RAW_OCC_TO_CRTV_STAGING`** — every **20 min**, **version** watermark. Pushes new
  creatives to `creative_staging` (+ seeds `creative_first_seen`) and maintains `creative_unique_urls`.
- **Job B — `DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE`** — every **1 hr**, **timestamp** watermark.
  UPDATE-only: pulls each creative's `creative_first_seen` back to its earliest occurrence.

Both read new inserts from `bronze.digital_raw_occurrence` (Iceberg). They do **not** depend on each
other. Two separate end-to-end runnable flows.

## Validated decisions (all locked)
1. **Clone targets, empty, in `tempwork`, names end `_ctv_poc`. Do NOT seed from prod.** Every CTV
   creative is therefore net-new (fine — PoC proves Iceberg→Postgres movement). 26B sequence start
   guarantees no id collision.
2. **No triggers** on the clones (we're not testing PG-side downstream, only data movement).
3. **Sequence** `tempwork.creative_id_seq_ctv_poc`, `START 26000000000`. Orchestrator (dbt macro)
   **reserves a contiguous block per run** — OK'd.
4. **Job B watermark = timestamp style** (same as Databricks). Trino `table_changes` takes snapshot
   ids, so resolve the stored `start_timestamp` → snapshot range via the Iceberg `$snapshots`
   metadata table (`committed_at`). Faithful analog of legacy `table_changes(table,'<ts>')`.
5. **`reference.data_provider`** (prod PG) is read directly (read-only lookup) — the insert proc joins
   it to map `provider_code` → `provider_id`. Not cloned.
6. **Postgres calls stay in dbt (not Python).** `CALL postgres.system.execute(...)`.
7. **Autochaff is a no-op for CTV** (legacy scoring only runs for `provider_code='BIS'`; CTV is
   `'AVOD BISCTV'`). Keep `bronze.creative_autochaff` + the anti-join for parity; do **not** port the
   scoring SQL.
8. **Duplication / id-burn on retry is acceptable** — matches current Databricks behavior. So we don't
   need extra machinery beyond the legacy ordering + `ON CONFLICT DO NOTHING`.
9. Fail-safe model accepted (see "Fail-safe" below). Optional bash/Python wrapper for explicit
   retry/logging is a future nicety, not required now.

## Naming (Postgres, all in `tempwork`)
| Object | Name |
| :-- | :-- |
| Clone: creative_staging | `tempwork.creative_staging_ctv_poc` |
| Clone: creative_first_seen | `tempwork.creative_first_seen_ctv_poc` |
| Clone proc (Job A insert) | `tempwork.sp_dbx_digital_insert_crtv_staging_first_seen_ctv_poc` |
| Clone proc (Job B update) | `tempwork.sp_dbx_digital_update_raw_occ_to_crtv_first_seen_ctv_poc` |
| Sequence | `tempwork.creative_id_seq_ctv_poc` (START 26000000000) |
| Temp table (Job A) | `tempwork.tmp_digital_raw_occ_to_crtv_staging_ctv_poc` |
| Temp table (Job B) | `tempwork.tmp_digital_raw_occ_to_crtv_firstseen_ctv_poc` |

Clone procs = the uploaded proc bodies **verbatim**, except: target `tempwork.*_ctv_poc` instead of
`creatives.*`; keep the `reference.data_provider` join as-is; triggers dropped (they live on the real
tables, not our clones, so nothing to remove — just don't create them).

## Job A — step order (mirrors legacy `process_to_crtv_staging`, fail-safe)
Runs as **one `dbt build`**. Transform steps are models; imperative steps are ordered post-hooks /
run-operations on the terminal model.

1. **Watermark begin** (pre): read last **SUCCEEDED** `last_commit_version` for
   `DIGITAL_RAW_OCC_TO_CRTV_STAGING` from `silver.watermark_control`; pin current snapshot as end.
   (Reuse Piece 1's two-phase begin/finish macros — begin must read last *SUCCEEDED* version so a
   failed InProgress run doesn't lose the start point.)
2. **Candidate model (Iceberg):** `table_changes(digital_raw_occurrence, start+1, end)` where
   `_change_type='insert'`; filter `country_iso_2_code='US'`, `creative_url is not null`,
   `coalesce(retransmit,false)=false`; dedup earliest `capture_timestamp` per `creative_url_hash`
   (`rnum=1`); **anti-join** `creative_unique_urls` where `is_staged=true`; **anti-join**
   `creative_autochaff`. Then derive `creative_mime_type` (the big source_channel / mimetype /
   url-extension CASE) and join media/market dims → `media_property_id`, `market_id`, `market_name`,
   `mime_type_id`, `occurrence_creative_url` (= `raw_json:$.occurrence.creativeUrl`), etc. Split:
   **net-new** (not in `creative_unique_urls` at all) vs **previously-unstaged** (`is_staged=false`,
   reuse their existing `creative_id`). Build `first_seen_metadata` JSON (exact keys below).
3. **Reserve ids (macro):** count N net-new; `SELECT nextval('tempwork.creative_id_seq_ctv_poc') FROM
   generate_series(1,N)` via `postgres.system.query`; stamp `creative_id = reserved` onto net-new by
   `row_number() over (order by created_timestamp)` (legacy ordering key).
4. **Final candidate (Iceberg):** UNION previously-unstaged (existing id) + net-new (stamped id) →
   the "combined_df" equivalent, 26-col contract (below).
5. **Insert net-new into `bronze.creative_unique_urls`** (Iceberg), `is_staged=false`, guarded by
   anti-join so re-runs never double-insert. (Legacy MERGE WHEN NOT MATCHED.)
6. **Write Postgres temp table:** dbt model targeting the postgres catalog, or DROP+CTAS hook →
   `tempwork.tmp_digital_raw_occ_to_crtv_staging_ctv_poc`. Cast timestamps to `timestamp(6)` WITHOUT
   tz (UTC wall clock) so PG `timestamp` columns don't shift; `first_seen_metadata` as text (proc
   casts to jsonb).
7. **CALL insert proc:** `CALL postgres.system.execute(query => 'CALL
   tempwork.sp_dbx_digital_insert_crtv_staging_first_seen_ctv_poc(''tempwork.tmp_digital_raw_occ_to_crtv_staging_ctv_poc'')')`.
   Proc = one PG transaction; both inserts `ON CONFLICT (creative_url_hash) DO NOTHING`.
8. **Flip `creative_unique_urls.is_staged = true`** for the pushed hashes (idempotent).
9. **Watermark finish** (last): promote to SUCCEEDED + advance version.

Autochaff persist step: skipped for CTV (empty set).

## Job B — step order (mirrors `process_digital_first_seen`)
1. **Watermark begin (timestamp):** read `start_timestamp`/`end_timestamp` for
   `DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE`; resolve start_timestamp → start snapshot via
   `$snapshots.committed_at`; end = latest snapshot.
2. **Candidate model:** `table_changes` over that snapshot range, `_change_type='insert'`, US,
   creative_url not null, retransmit false, dedup earliest per hash; **inner join**
   `creative_unique_urls` to attach `creative_id`; join media/market dims; produce the occurrence-level
   first-seen payload (32-col contract below). `occurrence_timestamp_local` = null; `provider_id` =
   `data_provider_id` from the hive `data_provider` join; `due_timestamp = capture_timestamp + 4h`.
3. **Write Postgres temp** `tempwork.tmp_digital_raw_occ_to_crtv_firstseen_ctv_poc`.
4. **CALL update proc** via `postgres.system.execute`. Proc UPDATEs `creative_first_seen` only where
   `trg.occurrence_timestamp > tmp.occurrence_timestamp` (keep earliest).
5. **Watermark finish (timestamp):** set start=old end, end=new max.

Note: on a fresh empty clone, Job A **seeds** `creative_first_seen` (its proc inserts from
`first_seen_metadata`); Job B only ever UPDATEs existing rows to earlier occurrences.

## Fail-safe (accepted)
dbt stops on the first failing hook and does **not** run later hooks or advance the watermark. With the
ordering above + idempotent guards (Iceberg anti-joins; PG `ON CONFLICT DO NOTHING`; proc = single PG
transaction), no run advances the watermark past unpushed data, and every retry is idempotent. Not
cross-engine atomic — a failed run can leave a creative at `is_staged=false`, which the next run
self-heals via the "previously-unstaged" branch (same reserved id). This matches legacy Databricks
behavior. Duplication/id-burn on retry is acceptable per decision #8.

## Postgres contracts (from the uploaded DDL/procs)
**`creative_staging` cols** (temp table must match, in this order): creative_id, legacy_creative_id,
country_iso_2_code, provider_code, source_channel, provider_creative_id, capture_month,
capture_timestamp, creative_type, mime_type_id, media_id, media_property_id, publisher_domain,
creative_width, creative_height, creative_duration, creative_url, creative_url_hash,
**creative_machine_learning_response** (proc casts→`creative_machine_learning_payload` jsonb),
creative_url_override, creative_payload, record_status, first_seen_metadata, suggested_vx0_product_id,
created_timestamp, updated_timestamp.
- Insert proc: created/updated = `clock_timestamp() AT TIME ZONE 'UTC'`; `ON CONFLICT
  (creative_url_hash) DO NOTHING`; then inserts `creative_first_seen` from `first_seen_metadata ->>`
  keys, joining `reference.data_provider` on `provider_code=data_provider_code` → provider_id.
- `media_id`: CTV (`provider_code='AVOD BISCTV'`) → **30** (`ctv_media_id`); else 5 (`digital_media_id`).

**`first_seen_metadata` JSON keys** (must match exactly — proc reads them): occurrence_id,
occurrence_timestamp, provider_code, media_property_id, media_property_name, media_category_id,
media_category_code, provider_creative_link_url, provider_publisher_id, provider_publisher_domain,
provider_campaign_id, provider_campaign_name, provider_advertiser_id, provider_advertiser_name,
provider_product_id, provider_product_name, due_timestamp, market_id, market_name, daypart_id,
daypart_name, affiliate_id, affiliate_name, occurrence_description, provider_campaign_description(*),
social_campaign_text(*), provider_campaign_landing_page, adclarity_url (= occurrence_creative_url).
(*) present in legacy struct; null/absent for CTV. Timestamps in the JSON must be
`::timestamp`-castable strings — format explicitly, e.g. `format_datetime(ts,'yyyy-MM-dd HH:mm:ss.SSS')`.

**`creative_first_seen` temp cols (Job B)**: creative_id, creative_url_hash, provider_creative_id,
country_iso_2_code, media_id, occurrence_id, occurrence_timestamp, occurrence_timestamp_local,
provider_id, media_property_id, media_property_name, media_category_id, media_category_code,
provider_creative_link_url, provider_publisher_id, provider_publisher_domain, provider_campaign_id,
provider_campaign_name, provider_advertiser_id, provider_product_id, provider_product_name,
provider_advertiser_name, due_timestamp, market_id, market_name, daypart_id, daypart_name,
affiliate_id, affiliate_name, created_timestamp, updated_timestamp, provider_campaign_landing_page.
Update proc SETs everything except creative_id/creative_url_hash/created_timestamp, WHERE
`creative_url_hash` matches AND `trg.occurrence_timestamp > tmp.occurrence_timestamp`.

## Reference/media joins (all already in Iceberg or read via postgres catalog)
- Iceberg (synced): `km_preparation_gold_db.media_property_flatten`,
  `km_preparation_db.media_property_data_provider_map`, `km_preparation_db.source_channel`,
  `km_preparation_db.standard_mime_type`, `km_preparation_db.data_provider`,
  `reference.provider_global_market_map`, `reference.global_market`.
- Autochaff-only dims (`standard_ad_size`, `adscore_provided_adservers`) — **not needed** (CTV skip).
- `reference.data_provider` — **prod Postgres**, read via the `postgres` catalog (read-only).

## Key constants (from Databricks `common/constants.py`)
`source_bis_ctv_code='AVOD BISCTV'`, `source_bis_code='BIS'`, `source_bis_social_code='BISSocial'`,
`source_playon_code='PlayOn'`, `country_code_US='US'`, `status_flag_active='ACTIVE'`,
`bc_active_status_id=92`, `ctv_media_id=30`, `digital_media_id=5`, `record_status_O='O'`.
`market_id` fallback → **302**, `market_name` fallback → `'~NOT SPECIFIED'` (see the CASE in the
Databricks `map_media_market_details_to_occurrence`). `provider_dma_city_name` for CTV
(`source_bis_ctv_code`) = `concat(region_city_name,', ',region_state_name)`.

## Available raw columns (from dbt/models/bronze/digital_raw_occurrence.sql)
42-col canonical incl. `raw_json` (full JSON retained → extract `$.occurrence.creativeUrl` for
adclarity_url). `occurrence_description` and `occurrence_link_url` are NULL for CTV (as in source).
`creative_width`/`height` null; `creative_duration` in seconds. `capture_timestamp`,
`created_timestamp` are `timestamp(6) with time zone` (UTC).

## Files to create (build task #4)
- `ddl/postgres/piece3_tempwork_ctv_poc.sql` — CREATE SCHEMA IF NOT EXISTS tempwork; 2 clone tables
  (no triggers); 2 clone procs (retargeted); sequence START 26000000000. **User runs once on PG.**
- `dbt/macros/` — `crtv_reserve_ids` (sequence block via `system.query`); reuse/extend watermark
  macros (add timestamp begin/finish variants if not present); a `pg_execute(call_sql)` helper wrapping
  `postgres.system.execute`.
- Job A models: e.g. `dbt/models/creatives/crtv_staging_candidate.sql` (+ new-ids + final + pg-temp
  model), terminal model carrying the ordered post-hooks (steps 5–9).
- Job B models: e.g. `dbt/models/creatives/crtv_firstseen_candidate.sql` (+ pg-temp + terminal hooks).
- `ddl/03` watermark seed rows: `DIGITAL_RAW_OCC_TO_CRTV_STAGING` (version) +
  `DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE` (timestamp).
- Schedules: `scripts/cron/` — Job A */20min, Job B hourly.
- Docs: `docs/ctv_creative_push.md` (Piece 3); update README + `docs/ctv_dbt_iceberg_poc.md` +
  `docs/runbook.md`. (Long Drive checkpoint later, on the checkpoint formatter.)

## Verify during build (don't assume)
- `postgres.system.execute` + `postgres.system.query` actually work against our wired `postgres`
  catalog on the VM (Trino 476).
- Trino cross-catalog CTAS Iceberg→Postgres type mapping (esp. bigint hash, timestamps, text/jsonb).
- `$snapshots.committed_at` timestamp→snapshot resolution for Job B.
- `first_seen_metadata` JSON timestamp strings are `::timestamp`-castable by the proc.

## Source files (read-only Databricks copy, for reference)
- `occurrences/digital/class_files/raw_occs_to_creative_staging.py` (Job A)
- `occurrences/common/class_files/raw_occ_to_digital_crtv_firstseen.py` (Job B)
- `occurrences/digital/notebook_files/raw_occ_to_crtv_staging_to_firstseen.py` (Job A executor)
- `common/psql_utility.py` (upsert pattern: write temp → CALL proc)
- Uploaded PG scripts: `creative_staging.sql`, `creative_first_seen.sql`,
  `sp_dbx_digital_insert_crtv_staging_first_seen.sql`, `sp_dbx_digital_update_raw_occ_to_crtv_first_seen.sql`
