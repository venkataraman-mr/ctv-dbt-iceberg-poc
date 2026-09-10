# CTV daily pipeline runbook — POLARIS (reference sync → ingestion → Pieces 1–5)

Operator runbook for running the **CTV occurrence flow daily** on the **Apache Polaris** Iceberg stack
(Trino 483 / dbt-trino / PyIceberg). It sequences every job in dependency order and gives the exact run +
verification commands. This is the *pipeline* runbook; for how the Polaris copy was built (and the **v3 + VARIANT
limitations you must know** — chiefly the loss of `sorted_by` on VARIANT tables) see
**`docs/runbooks/polaris/polaris_pipeline_runbook.md`** (esp. its **Key learnings** section). Per-piece design
detail is the same catalog-agnostic logic as Nessie — see the linked `docs/pipeline/*.md` in each step.

> **This is the Polaris twin of `docs/runbooks/nessie/ctv_daily_runbook.md`.** Same jobs, same order, same
> verification — the only differences are the **catalog (`polaris`)**, the **`dbt_polaris` / `ingestion_polaris`
> services**, the **`_ctv_poc_pol` Postgres clones**, the **`ddl/polaris/` DDL**, and the **`landing_polaris/` S3
> prefix**. Runs side-by-side with the Nessie pipeline on the same VM with no interference.

All five jobs are dbt **tags** named after their Databricks job, so each runs with one `--select tag:<JOB>`:

| Order | Piece | dbt tag | Writes |
|------|-------|---------|--------|
| 0 | Reference dims (hive 14 + UC 6) | — (Python) | `polaris.<db>.*` reference schemas |
| 1 | Ingestion (land + staging→raw) | `BIS_CTV_BZ2FILE_TO_RAW_OCC` | `polaris.bronze.digital_raw_occurrence` |
| 2 | Piece 3 Job A — creative push | `RAW_OCCS_TO_CREATIVE_STAGING` | Postgres `tempwork.creative_staging_ctv_poc_pol` / `creative_first_seen_ctv_poc_pol` |
| 3 | Piece 3 Job B — first-seen + occ summary | `CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY` | Postgres `creative_first_seen_ctv_poc_pol` / `creative_occurrence_summary_ctv_poc_pol` |
| 4 | Piece 4 — sync-back (seed → sync) | `SYNC_CREATIVES_TO_ICEBERG` | `polaris.gold.creative` / `gold.creative_first_seen` / `silver.creative_dedupe_map` / `gold.component_coding` |
| 5 | Piece 5 — gold occurrence | `DIGITAL_RAW_OCC_TO_GOLD_OCC` | `polaris.gold.digital_gold_occurrence` / `silver.digital_staging_occurrence` |

**Dependency order is fixed:** creatives must be pushed (2) and classified/synced (4) before the occurrence
gate (5) can classify occurrences against `gold.creative`. Run the steps in this order every day.

> `psql` is **not** on the VM. Steps that say "Postgres (SQL client)" run against prod Postgres from a SQL
> client (DBeaver etc.) and need membership in `tempwork_admin_role`. Everything else runs on the VM.

> **Trino catalog flag.** These commands pass `--catalog polaris` explicitly. If your Trino session already
> defaults to `polaris` you can drop the flag; keeping it is safe and unambiguous next to a Nessie (`iceberg`) run.

---

## Part 1 — One-time setup (run once per environment)

Do this once when standing up the Polaris stack (after the Polaris catalog + services exist — see
`polaris_pipeline_runbook.md` §2a/§3). Skip on daily runs.

**1a. Iceberg tables + watermark seeds (Trino).** Creates every persistent Polaris table and seeds the Piece-1 /
Piece-3 version watermarks (`ddl/polaris/03` + `05`), the Piece-4 timestamp watermarks (`ddl/polaris/08`), and the
Piece-5 watermarks (`ddl/polaris/11`). `ddl/polaris/03` also partitions `silver.watermark_control` by
`watermark_name` (concurrency) **and seeds the ingestion watermark**. The reference-sync view is created after the
reference sync (Step 0). **Every pipeline table is v3 + `variant`; VARIANT tables carry NO `sorted_by` (§ Key
learnings, limitation ①).**

```bash
# schemas + all pipeline DDL (00..11). Order matters (silver/gold before the models that write them).
for f in ddl/polaris/0[0-9]_*.sql ddl/polaris/1[01]_*.sql; do echo "== $f =="; docker exec -i trino trino --catalog polaris -f /dev/stdin < "$f"; done
# NOTE: unlike Nessie, the gold.creative* landing_page columns are already in ddl/polaris/06 — no ALTER retrofit needed.
# product-resync timestamp watermark must NOT start at 1900 (else full-productmap sweep -> OOM); init to max(change_dt):
docker exec -i trino trino --catalog polaris --execute "UPDATE polaris.silver.watermark_control SET start_timestamp = end_timestamp, end_timestamp = cast((SELECT max(change_dt) FROM polaris.productcentral.productmap) as timestamp(6) with time zone), transaction_status='INIT', updated_timestamp=cast(current_timestamp as timestamp(6) with time zone) WHERE watermark_name='CTV_PRODUCT_RESYNC'"
```

**1b. Postgres objects (SQL client; needs `tempwork_admin_role`).** All objects are `tempwork.*_ctv_poc_pol`
**clones** — separate from the Nessie `_ctv_poc` objects, so the two runs never collide. Real `creatives.*` are
untouched.

```sql
\i ddl/postgres/polaris/piece3_tempwork_ctv_poc_pol.sql   -- Piece 3 clones + insert/first-seen/occ-summary procs + creative_id sequence (26B)
\i ddl/postgres/polaris/piece4_seed_tempwork_ctv_poc_pol.sql -- Piece 4 read-side clones + the two-mode seeding proc
\i ddl/postgres/polaris/piece4_sync_procs_ctv_poc.sql     -- the two cloned/retargeted get_changes procs (proc names are *_ctv_poc_pol)
\i ddl/postgres/polaris/piece5_occ_id_seq_ctv_poc.sql     -- occurrence_id sequence (START 75,000,000,000) + block table + reserve proc (*_ctv_poc_pol)
```

**1c. Reference sync — UC env + schedule (once).** The UC sync needs the `UC_*` keys in `.env`, then a container
recreate. `ingestion_polaris` reuses the Nessie image but has its own Polaris REST/OAuth2 env — after any `.env`
change, recreate it so it re-reads the env (a plain `exec` won't).

```bash
docker compose up -d --force-recreate ingestion_polaris    # pick up POLARIS_OAUTH2_CREDENTIAL / UC_* env
# (optional) schedule the daily hive sync via cron, pointed at ingestion_polaris.
```

---

## Part 2 — Daily run (end-to-end)

Copy-paste the whole sequence, or run step by step and check each verify block. Run dbt with **≥2 threads**
(set in `dbt_polaris/profiles.yml`) so the parallel branches inside Pieces 4 and 5 run concurrently.

### TL;DR — the daily sequence

```bash
# 0. reference dims (or rely on the daily cron)
docker compose exec ingestion_polaris python -m ingestion_polaris.reference_sync
docker compose exec ingestion_polaris python -m ingestion_polaris.uc_reference_sync
# 1. ingest  (first, on your LOCAL Windows machine: upload the day's *.bz2 to the POLARIS landing prefix)
#    powershell -File scripts\upload_ctv_sample.ps1 -Prefix "landing_polaris/ctv/ingestion"
docker compose exec ingestion_polaris python -m ingestion_polaris.ctv_ingestion
docker compose exec dbt_polaris dbt run --select tag:BIS_CTV_BZ2FILE_TO_RAW_OCC
# 2. Piece 3 Job A
docker compose run --rm dbt_polaris dbt run --select tag:RAW_OCCS_TO_CREATIVE_STAGING
# 3. Piece 3 Job B
docker compose run --rm dbt_polaris dbt run --select tag:CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY
# 4. Piece 4 — seed (ALL) on Postgres, then sync-back
#    (SQL client) CALL tempwork.sp_seed_creative_clones_ctv_poc_pol('ALL');
docker compose run --rm dbt_polaris dbt run --select tag:SYNC_CREATIVES_TO_ICEBERG
# 5. Piece 5 — gold occurrence
docker compose run --rm dbt_polaris dbt run --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC
```

The detailed steps below add the verification queries for each.

### Step 0 — Reference dims (hive 14 + UC 6)

Refreshes the reference/lookup dimensions the creative + occurrence joins depend on. Detail:
`polaris_pipeline_runbook.md` §5 Step 1, `docs/pipeline/reference_tables.md`.

> **Polaris note:** the reference/spend mirrors are **v2** (PyIceberg 0.11.1 can't write v3, and these tables have
> no VARIANT — § Key learnings, limitation ②). In production this data is read directly from Databricks via Trino,
> not synced; the sync is a PoC convenience.

```bash
docker compose exec ingestion_polaris python -m ingestion_polaris.reference_sync      # all 14 hive tables
docker compose exec ingestion_polaris python -m ingestion_polaris.uc_reference_sync   # all 6 UC tables
# create/refresh the real Iceberg view (after the sync — its base tables must exist):
docker exec -i trino trino --catalog polaris -f /dev/stdin < ddl/polaris/views/media_property_flatten_vx0_vw.sql
# verify:
docker exec -i trino trino --catalog polaris --execute "SELECT count(*) FROM polaris.km_preparation_db.data_provider"
docker exec -i trino trino --catalog polaris --execute "SELECT count(*) FROM polaris.km_preparation_gold_db.media_property_flatten_vx0_vw"
```

### Step 1 — Ingestion (Piece 1): land files → bronze staging → raw

Lands `.bz2`/plain-JSON from `s3://…/landing_polaris/ctv/ingestion/` into bronze staging (archiving processed
files to `landing_polaris/ctv/archive/`), then the dbt-trino incremental transforms staging →
`bronze.digital_raw_occurrence` (reads only new inserts via `system.table_changes` after the first full load).
`daisy_chain` / `raw_json` are landed as **string** by PyIceberg and CAST to **`variant`** in this dbt model
(§ Key learnings, limitation ②). Detail: `docs/pipeline/ctv_ingestion.md`.

**1a. Place the day's `.bz2` files into the POLARIS S3 prefix (local Windows machine, has AWS creds).** Polaris and
Nessie must **not** share a landing prefix (the landing step *archives/moves* files, so they'd steal each other's).
Upload to `landing_polaris/ctv/ingestion` via the `-Prefix` flag:

```powershell
# on your local Windows machine:
powershell -File scripts\upload_ctv_sample.ps1 -Prefix "landing_polaris/ctv/ingestion"
# or a specific folder / single file (still pass -Prefix):
#   powershell -File scripts\upload_ctv_sample.ps1 -Path "C:\ctv\landing" -Prefix "landing_polaris/ctv/ingestion"
```

**1b. Land → raw (on the VM).**

```bash
docker compose exec ingestion_polaris python -m ingestion_polaris.ctv_ingestion       # land -> bronze staging
docker compose exec dbt_polaris dbt run --select tag:BIS_CTV_BZ2FILE_TO_RAW_OCC        # staging -> raw (single incremental model)
# verify:
docker exec -i trino trino --catalog polaris --execute "SELECT count(*) FROM polaris.bronze.digtial_raw_occurrence_ctv_staging"
docker exec -i trino trino --catalog polaris --execute "SELECT count(*), min(capture_month), max(capture_month) FROM polaris.bronze.digital_raw_occurrence"
# confirm VARIANT round-trips (read with SUBSCRIPT + CAST or CAST(col AS json); NEVER json_parse/json_query on a variant):
docker exec -i trino trino --catalog polaris --execute "SELECT provider_occurrence_id, CAST(raw_json['occurrence']['id'] AS varchar) AS occ_id, CAST(daisy_chain AS json) AS daisy_chain_json FROM polaris.bronze.digital_raw_occurrence WHERE daisy_chain IS NOT NULL LIMIT 3"
```

### Step 2 — Piece 3 Job A: raw occ → creative staging (Postgres)

Pushes new CTV creatives from `bronze.digital_raw_occurrence` into the Postgres `_ctv_poc_pol` clones (creative
staging + first-seen seed). Reads the variant `raw_json` via `cast(raw_json as json)` (not `json_parse`). Detail:
`docs/pipeline/ctv_creative_push.md`.

```bash
docker compose run --rm dbt_polaris dbt run --select tag:RAW_OCCS_TO_CREATIVE_STAGING
# verify:
docker exec -i trino trino --catalog polaris --execute "SELECT count(*) FROM postgres.tempwork.creative_staging_ctv_poc_pol"
docker exec -i trino trino --catalog polaris --execute "SELECT count(*) FROM postgres.tempwork.creative_first_seen_ctv_poc_pol"
docker exec -i trino trino --catalog polaris --execute "SELECT count(*) FILTER (WHERE is_staged) FROM polaris.bronze.creative_unique_urls"
docker exec -i trino trino --catalog polaris --execute "SELECT last_commit_version, transaction_status FROM polaris.silver.watermark_control WHERE watermark_name='DIGITAL_RAW_OCC_TO_CRTV_STAGING'"
```

### Step 3 — Piece 3 Job B: first-seen update + occurrence summary

First-seen update + occurrence summary (CDF ∪ parked buffer → upsert proc → park/release MERGE), version-watermarked
and parallel-safe. Detail: `docs/pipeline/ctv_creative_push.md`.

```bash
docker compose run --rm dbt_polaris dbt run --select tag:CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY
# -- OR Job A + Job B together in ONE parallel command (no ref() edge between them; watermark_control is
# -- partitioned by watermark_name so their writes don't collide):
#    docker compose run --rm dbt_polaris dbt run --select tag:RAW_OCCS_TO_CREATIVE_STAGING tag:CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY --threads 4
# verify:
docker exec -i trino trino --catalog polaris --execute "SELECT count(*) FROM postgres.tempwork.creative_occurrence_summary_ctv_poc_pol"
docker exec -i trino trino --catalog polaris --execute "SELECT count(*) FROM polaris.bronze.missing_digital_occurrence_for_summary"
```

### Step 4 — Piece 4: seed classified creatives (ALL) → sync-back to gold

Two parts, in order: **(a)** run the seeding proc in **`ALL`** mode on Postgres (the PoC stand-in for the external
classification engine; Mode 1 new + Mode 2 updates, watermark-driven). **(b)** run the sync-back tag, which reads
the seeded `tempwork.*_ctv_poc_pol` clones and MERGEs into Polaris `gold.*`/`silver.*` in the reconciled Databricks
DAG order. VARIANT is written natively in dbt (`CAST(... AS variant)`); the gold/silver tables are v3 with no
`sorted_by`. Detail: `docs/pipeline/ctv_creative_seed.md`, `docs/pipeline/ctv_creative_sync_plan.md`.

```sql
-- (a) Postgres (SQL client; needs tempwork_admin_role): seed only the day's changes
CALL tempwork.sp_seed_creative_clones_ctv_poc_pol('ALL');
-- inspect the seed high-water marks:
SELECT watermark_name, table_tx_start, table_tx_end, tx_status, tx_message, tx_datetime FROM tempwork.watermark_control_ctv_poc_pol;
```

```bash
# (b) sync-back — the whole 8-task job in DAG order:
docker compose run --rm dbt_polaris dbt ls  --select tag:SYNC_CREATIVES_TO_ICEBERG --output name    # membership (18 models)
docker compose run --rm dbt_polaris dbt run --select tag:SYNC_CREATIVES_TO_ICEBERG                   # end-to-end
# verify:
docker exec -i trino trino --catalog polaris --execute "SELECT count(*) FROM polaris.gold.creative"
docker exec -i trino trino --catalog polaris --execute "SELECT count(*) FROM polaris.silver.creative_dedupe_map"
docker exec -i trino trino --catalog polaris --execute "SELECT watermark_name, end_timestamp, transaction_status FROM polaris.silver.watermark_control WHERE watermark_name='CTV_SYNC_CREATIVE'"
```

> **Note on occurrence-id + last-seen (inside this tag).** These two tasks update `gold.creative` from
> `gold.digital_gold_occurrence`, so on any given day they reflect the gold-occurrence state **as of now** (the
> *previous* day's Piece 5). Today's occurrences reach them on tomorrow's Step 4 (Piece 5 is gated on
> `gold.creative`, so Piece 4 must run first). For same-day catch-up, optionally re-run just those two after Step 5:
> `docker compose run --rm dbt_polaris dbt run --select crtv_occid_update crtv_lastseen_update`.

### Step 5 — Piece 5: raw occ → gold occurrence

The occurrence gate: new raw occ ∪ hold buffer → deployment chains → enrich → classify against `gold.creative`
(Not Hold / Hold) → reserve `occurrence_id` (75 B Postgres `_ctv_poc_pol` sequence) → MERGE
`gold.digital_gold_occurrence`; park/release the hold buffer; Half B applies creative-change updates.
`provider_raw_json` / `daisy_chain` are VARIANT. Detail: `docs/pipeline/ctv_occurrence_gold_plan.md`.

```bash
docker compose run --rm dbt_polaris dbt ls  --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC --output name   # 6 models
docker compose run --rm dbt_polaris dbt run --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC                  # Half A (A1->{A2,A3}->A4->A5) then Half B
# verify:
docker exec -i trino trino --catalog polaris --execute "SELECT count(*), min(occurrence_id), max(occurrence_id) FROM polaris.gold.digital_gold_occurrence"
docker exec -i trino trino --catalog polaris --execute "SELECT count(*) FROM polaris.silver.digital_staging_occurrence"   -- held (parked) occurrences
docker exec -i trino trino --catalog polaris --execute "SELECT watermark_name, current_commit_version, transaction_status FROM polaris.silver.watermark_control WHERE watermark_name IN ('DIGITAL_RAW_OCC_TO_GOLD_OCC','DIGITAL_CRTV_CHANGES_TO_GOLD_OCC')"
```

Expect **parity with the Nessie baseline** on the same input (Nessie first validated run: 811,764 raw →
746,245 gold occurrences [`occurrence_id` 75,000,000,000 …] + 65,519 held). Polaris `occurrence_id` uses its own
`*_ctv_poc_pol` 75 B sequence, so the id range starts at 75 B independently of the Nessie run.

---

## Part 3 — Operating notes

- **Incremental by design.** Every step is watermark-driven, so a daily run processes only new/changed data (the
  first run of each is a one-time full load). Ingestion + Half A use **version** watermarks
  (`system.table_changes` on append-only bronze); the Piece-4 sync and Piece-5 Half B use **timestamp** watermarks
  on `updated_timestamp` (their gold sources are MERGE-written, so version-CDF is invalid).
- **Scratch self-cleans.** Each tagged job drops its bronze scratch (`digital_occ_*`, `crtv_sync_*`,
  `crtv_staging_*`, …) on a successful `on-run-end`. A **failed** run keeps that tag's scratch for debugging; add
  `--vars 'keep_<TAG>_tables: true'` to keep intermediates on a clean run.
- **Threads.** Run dbt with ≥2 threads so the parallel branches (Piece 4: dedup ∥ first-seen, component ∥
  product-resync; Piece 5: A2 ∥ A3) run concurrently.
- **Concurrency.** `polaris.silver.watermark_control` is partitioned by `watermark_name` so concurrent watermarked
  writers don't collide; still, run the pipeline steps in the order above (they have real data dependencies).
- **Failure handling.** Steps are idempotent MERGEs — re-running a failed step is safe (the watermark only advances
  on success). Fix the cause, re-run that step, then continue.
- **Polaris-specific error signatures** (see § Key learnings in `polaris_pipeline_runbook.md`): `Unsupported Hive
  type: variant` = a stray `sorted_by` on a VARIANT table; `Type not supported for Iceberg: smallint` = a missed
  `SMALLINT→INTEGER`; `ICEBERG_CATALOG_ERROR "Failed to create transaction"` = a body-variant model missing
  `format_version=3`. All are build-time issues (already resolved) — flagged here for future model changes.
- **Parallel with Nessie.** This whole flow runs beside the Nessie daily run with no interference: different Trino
  catalog (`polaris` vs `iceberg`), different Postgres clones (`_ctv_poc_pol` vs `_ctv_poc`), different S3 landing
  prefix (`landing_polaris/` vs `landing/`), and per-catalog watermark tables.
- **VM stand-up / infra / how the Polaris copy was built:** `docs/runbooks/polaris/polaris_pipeline_runbook.md`.
