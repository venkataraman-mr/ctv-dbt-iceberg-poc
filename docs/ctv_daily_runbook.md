# CTV daily pipeline runbook — end-to-end (reference sync → ingestion → Pieces 1–5)

Operator runbook for running the **CTV occurrence flow daily** on the Trino/dbt/Iceberg stack. It sequences
every job in dependency order and gives the exact run + verification commands. This is the *pipeline* runbook;
for VM stand-up (Docker, Nessie, Trino, smoke test) see **`docs/runbook.md` §0–2**, and for per-piece design
detail see the linked piece docs in each step.

All five jobs are dbt **tags** named after their Databricks job, so each runs with one `--select tag:<JOB>`:

| Order | Piece | dbt tag | Writes |
|------|-------|---------|--------|
| 0 | Reference dims (hive 14 + UC 6) | — (Python) | `iceberg.<db>.*` reference schemas |
| 1 | Ingestion (land + staging→raw) | `BIS_CTV_BZ2FILE_TO_RAW_OCC` | `bronze.digital_raw_occurrence` |
| 2 | Piece 3 Job A — creative push | `RAW_OCCS_TO_CREATIVE_STAGING` | Postgres `tempwork.creative_staging_ctv_poc` / `creative_first_seen_ctv_poc` |
| 3 | Piece 3 Job B — first-seen + occ summary | `CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY` | Postgres `creative_first_seen` / `creative_occurrence_summary_ctv_poc` |
| 4 | Piece 4 — sync-back (seed → sync) | `SYNC_CREATIVES_TO_ICEBERG` | `gold.creative` / `gold.creative_first_seen` / `silver.creative_dedupe_map` / `gold.component_coding` |
| 5 | Piece 5 — gold occurrence | `DIGITAL_RAW_OCC_TO_GOLD_OCC` | `gold.digital_gold_occurrence` / `silver.digital_staging_occurrence` |

**Dependency order is fixed:** creatives must be pushed (2) and classified/synced (4) before the occurrence
gate (5) can classify occurrences against `gold.creative`. Run the steps in this order every day.

> `psql` is **not** on the VM. Steps that say "Postgres (SQL client)" run against prod Postgres from a SQL
> client (DBeaver etc.) and need membership in `tempwork_admin_role`. Everything else runs on the VM.

---

## Part 1 — One-time setup (run once per environment)

Do this once when standing up the stack (after `docs/runbook.md` §0–2). Skip on daily runs.

**1a. Iceberg tables + watermark seeds (Trino).** Creates every persistent table and seeds the Piece-1 /
Piece-3 version watermarks (ddl/03), the Piece-4 timestamp watermarks (ddl/08), and the Piece-5 watermarks
(ddl/09). `ddl/03` also partitions `silver.watermark_control` by `watermark_name` (concurrency).

```bash
for f in ddl/0[0-7]_*.sql; do echo "== $f =="; docker exec -i trino trino --catalog iceberg -f /dev/stdin < "$f"; done
docker exec -i trino trino --catalog iceberg -f /dev/stdin < ddl/08_silver_watermark_control_piece4.sql
docker exec -i trino trino --catalog iceberg -f /dev/stdin < ddl/09_silver_watermark_control_piece5.sql
# gold columns the sync writes (metadata-only add on already-created tables):
docker exec -i trino trino --execute "ALTER TABLE iceberg.gold.creative_first_seen ADD COLUMN provider_campaign_landing_page VARCHAR"
docker exec -i trino trino --execute "ALTER TABLE iceberg.gold.creative ADD COLUMN first_seen_provider_campaign_landing_page VARCHAR"
# ingestion (staging->raw) version watermark — seed if not already present (NULL last_commit_version => first run is a full load):
docker exec -i trino trino --execute "INSERT INTO iceberg.silver.watermark_control (watermark_name, start_timestamp, end_timestamp, last_commit_version, current_commit_version, transaction_status, created_timestamp, updated_timestamp) VALUES ('BIS_CTV_US_INGESTION_STG_TO_RAW_OCC', NULL, NULL, NULL, NULL, 'SUCCEEDED', current_timestamp, current_timestamp)"
# product-resync timestamp watermark must NOT start at 1900 (else full-productmap sweep -> OOM); init to max(change_dt):
docker exec -i trino trino --execute "UPDATE iceberg.silver.watermark_control SET start_timestamp = end_timestamp, end_timestamp = cast((SELECT max(change_dt) FROM iceberg.productcentral.productmap) as timestamp(6) with time zone), transaction_status='INIT', updated_timestamp=cast(current_timestamp as timestamp(6) with time zone) WHERE watermark_name='CTV_PRODUCT_RESYNC'"
```

**1b. Postgres objects (SQL client; needs `tempwork_admin_role`).** All objects are `tempwork.*_ctv_poc`
**clones** — real `creatives.*` are untouched.

```sql
\i ddl/postgres/piece3_tempwork_ctv_poc.sql      -- Piece 3 clones + insert/first-seen/occ-summary procs + creative_id sequence
\i ddl/postgres/piece4_seed_tempwork_ctv_poc.sql -- Piece 4 read-side clones + the two-mode seeding proc
\i ddl/postgres/piece4_sync_procs_ctv_poc.sql    -- the two cloned/retargeted get_changes procs
\i ddl/postgres/piece5_occ_id_seq_ctv_poc.sql    -- occurrence_id sequence (START 75,000,000,000) + block table + reserve proc
```

**1c. Reference sync — UC env + schedule (once).** The UC sync needs the `UC_*` keys in `.env`, then a
container recreate; schedule the daily hive sync via cron.

```bash
docker compose up -d --force-recreate ingestion            # pick up UC_* env
crontab scripts/cron/reference_sync.cron                   # daily 02:15 UTC; confirm: crontab -l
```

---

## Part 2 — Daily run (end-to-end)

Copy-paste the whole sequence, or run step by step and check each verify block. Run dbt with **≥2 threads**
(set in `dbt/profiles.yml`) so the parallel branches inside Pieces 4 and 5 run concurrently.

### TL;DR — the daily sequence

```bash
# 0. reference dims (or rely on the 02:15 cron)
docker compose exec ingestion python -m ingestion.reference_sync
docker compose exec ingestion python -m ingestion.uc_reference_sync
# 1. ingest  (first, on your LOCAL Windows machine: place the day's *.bz2 into the landing folder and upload+remove)
#    powershell -File scripts\upload_ctv_sample.ps1
docker compose exec ingestion python -m ingestion.ctv_ingestion
docker compose exec dbt dbt run --select tag:BIS_CTV_BZ2FILE_TO_RAW_OCC
# 2. Piece 3 Job A
docker compose run --rm dbt dbt run --select tag:RAW_OCCS_TO_CREATIVE_STAGING
# 3. Piece 3 Job B
docker compose run --rm dbt dbt run --select tag:CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY
# 4. Piece 4 — seed (ALL) on Postgres, then sync-back
#    (SQL client) CALL tempwork.sp_seed_creative_clones_ctv_poc('ALL');
docker compose run --rm dbt dbt run --select tag:SYNC_CREATIVES_TO_ICEBERG
# 5. Piece 5 — gold occurrence
docker compose run --rm dbt dbt run --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC
```

The detailed steps below add the verification queries for each.

### Step 0 — Reference dims (hive 14 + UC 6)

Refreshes the reference/lookup dimensions the creative + occurrence joins depend on. Usually the daily
`02:15 UTC` cron; run manually to refresh on demand. Detail: `docs/runbook.md` §3/§3b, `docs/reference_tables.md`.

```bash
docker compose exec ingestion python -m ingestion.reference_sync                 # all 14 hive tables
docker compose exec ingestion python -m ingestion.uc_reference_sync              # all 6 UC tables
# tune batch size for a big table: REF_SYNC_BATCH_ROWS=200000 docker compose exec ingestion python -m ingestion.reference_sync
# verify:
docker exec -i trino trino --execute "SELECT count(*) FROM iceberg.km_preparation_db.data_provider"
docker exec -i trino trino --execute "SELECT table_schema, table_name FROM iceberg.information_schema.tables WHERE table_schema IN ('km_preparation_db','km_preparation_gold_db','productcentral') ORDER BY 1,2"
```

### Step 1 — Ingestion (Piece 1): land files → bronze staging → raw

Lands `.bz2`/plain-JSON from `s3://…/landing/ctv/ingestion/` into bronze staging (archiving processed files),
then the dbt-trino incremental transforms staging → `bronze.digital_raw_occurrence` (reads only new inserts via
`system.table_changes` after the first full load). Detail: `docs/ctv_ingestion.md`.

**1a. Place the day's `.bz2` files into S3 (local Windows machine, has AWS creds).** Drop the day's
`.bz2` files into the local landing folder (default `C:\Users\venkata.adapa\Downloads\ctv_landing`), then
run the upload script — it uploads every `*.bz2` to the ingestion prefix and **removes each local file once
its upload succeeds** (`aws s3 mv`). Pass `-KeepLocal` to copy without deleting; `-Path` to point at another
folder or a single file.

```powershell
# on your local Windows machine:
powershell -File scripts\upload_ctv_sample.ps1
# or a specific folder / single file:
#   powershell -File scripts\upload_ctv_sample.ps1 -Path "C:\ctv\landing"
#   powershell -File scripts\upload_ctv_sample.ps1 -Path "C:\path\to\daily_US_CTV_YYYYMMDD.bz2"
```

**1b. Land → raw (on the VM).** The landing step reads that prefix into bronze staging (archiving processed
files in S3), then the dbt-trino incremental transforms staging → `bronze.digital_raw_occurrence`.

```bash
docker compose exec ingestion python -m ingestion.ctv_ingestion                          # land -> bronze staging
docker compose exec dbt dbt run --select digital_raw_occurrence                          # by model
docker compose exec dbt dbt run --select tag:BIS_CTV_BZ2FILE_TO_RAW_OCC                   # by job tag (same model, whole job)
# verify:
docker exec -i trino trino --execute "SELECT count(*) FROM iceberg.bronze.digtial_raw_occurrence_ctv_staging"
docker exec -i trino trino --execute "SELECT count(*), min(capture_month), max(capture_month) FROM iceberg.bronze.digital_raw_occurrence"
```

### Step 2 — Piece 3 Job A: raw occ → creative staging (Postgres)

Pushes new CTV creatives from `bronze.digital_raw_occurrence` into the Postgres clones (creative staging +
first-seen seed), Trino/dbt-native. Detail: `docs/ctv_creative_push.md`.

```bash
docker compose run --rm dbt dbt run --select +crtv_staging_final                          # by model (whole Job A DAG)
docker compose run --rm dbt dbt run --select tag:RAW_OCCS_TO_CREATIVE_STAGING             # by job tag (same models, whole job)
# verify:
docker exec -i trino trino --execute "SELECT count(*) FROM postgres.tempwork.creative_staging_ctv_poc"
docker exec -i trino trino --execute "SELECT count(*) FROM postgres.tempwork.creative_first_seen_ctv_poc"
docker exec -i trino trino --execute "SELECT count(*) FILTER (WHERE is_staged) FROM iceberg.bronze.creative_unique_urls"
docker exec -i trino trino --execute "SELECT last_commit_version, transaction_status FROM iceberg.silver.watermark_control WHERE watermark_name='DIGITAL_RAW_OCC_TO_CRTV_STAGING'"
```

### Step 3 — Piece 3 Job B: first-seen update + occurrence summary

Two version-watermarked sub-pipelines (parallel-safe after the watermark-table partitioning): first-seen
update (`CALL` the update proc → earliest occurrence) + occurrence summary (CDF ∪ parked buffer → upsert proc
→ park/release MERGE). Detail: `docs/ctv_creative_push.md`.

```bash
docker compose run --rm dbt dbt run --select crtv_firstseen crtv_occ_summary_candidate crtv_occ_summary_final   # by model
docker compose run --rm dbt dbt run --select tag:CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY                            # by job tag
# verify:
docker exec -i trino trino --execute "SELECT count(*) FROM postgres.tempwork.creative_occurrence_summary_ctv_poc"
docker exec -i trino trino --execute "SELECT count(*) FROM iceberg.bronze.missing_digital_occurrence_for_summary"   -- parked-unresolved
```

### Step 4 — Piece 4: seed classified creatives (ALL) → sync-back to gold

Two parts, in order: **(a)** run the seeding proc in **`ALL`** mode on Postgres — this is the PoC stand-in for
the external classification engine; `ALL` runs Mode 1 (new inserts) then Mode 2 (creative + parent-attribute
updates), both watermark-driven, so **only changed rows are picked and processed**. **(b)** run the sync-back
tag, which reads the seeded `tempwork.*_ctv_poc` clones and MERGEs into Iceberg `gold.*`/`silver.*` in the
reconciled Databricks DAG order (dedup ∥ first-seen → creative → first-seen-info → occurrence-id → last-seen →
component ∥ product-resync). Detail: `docs/ctv_creative_seed.md`, `docs/ctv_creative_sync_plan.md`.

```sql
-- (a) Postgres (SQL client; needs tempwork_admin_role): seed only the day's changes
CALL tempwork.sp_seed_creative_clones_ctv_poc('ALL');
-- inspect the seed high-water marks:
SELECT watermark_name, table_tx_start, table_tx_end, tx_status, tx_message, tx_datetime FROM tempwork.watermark_control_ctv_poc;
```

```bash
# (b) sync-back — the whole 8-task job in DAG order:
docker compose run --rm dbt dbt ls  --select tag:SYNC_CREATIVES_TO_ICEBERG --output name    # membership (18 models)
docker compose run --rm dbt dbt run --select tag:SYNC_CREATIVES_TO_ICEBERG                   # end-to-end
# verify:
docker exec -i trino trino --execute "SELECT count(*) FROM iceberg.gold.creative"
docker exec -i trino trino --execute "SELECT count(*) FROM iceberg.silver.creative_dedupe_map"
docker exec -i trino trino --execute "SELECT watermark_name, end_timestamp, transaction_status FROM iceberg.silver.watermark_control WHERE watermark_name='CTV_SYNC_CREATIVE'"
```

> **Note on occurrence-id + last-seen (inside this tag).** These two tasks update `gold.creative` from
> `gold.digital_gold_occurrence`, so on any given day they reflect the gold-occurrence state **as of now**
> (i.e. the *previous* day's Piece 5). Today's occurrences reach them on tomorrow's Step 4 — this is inherent
> (Piece 5 is gated on `gold.creative`, so Piece 4 must run first). For same-day catch-up you may optionally
> re-run just those two after Step 5: `docker compose run --rm dbt dbt run --select crtv_occid_update crtv_lastseen_update`.

### Step 5 — Piece 5: raw occ → gold occurrence

The occurrence gate: new raw occ ∪ hold buffer → deployment chains → enrich → classify against `gold.creative`
(Not Hold / Hold) → reserve `occurrence_id` (75 B Postgres sequence) → MERGE `gold.digital_gold_occurrence`;
park/release the hold buffer; Half B applies creative-change updates. Detail: `docs/ctv_occurrence_gold_plan.md`.

```bash
docker compose run --rm dbt dbt ls  --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC --output name   # 6 models
docker compose run --rm dbt dbt run --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC                  # Half A (A1->{A2,A3}->A4->A5) then Half B
# verify:
docker exec -i trino trino --execute "SELECT count(*), min(occurrence_id), max(occurrence_id) FROM iceberg.gold.digital_gold_occurrence"
docker exec -i trino trino --execute "SELECT count(*) FROM iceberg.silver.digital_staging_occurrence"   # held (parked) occurrences
docker exec -i trino trino --execute "SELECT watermark_name, current_commit_version, transaction_status FROM iceberg.silver.watermark_control WHERE watermark_name IN ('DIGITAL_RAW_OCC_TO_GOLD_OCC','DIGITAL_CRTV_CHANGES_TO_GOLD_OCC')"
```

First validated full run (2026-08-11): 811,764 raw → **746,245 gold occurrences** (`occurrence_id`
[75,000,000,000 … 75,000,746,244]) + **65,519 held**; deployment chains match prod.

---

## Part 3 — Operating notes

- **Incremental by design.** Every step is watermark-driven, so a daily run processes only new/changed data
  (the first run of each is a one-time full load). Ingestion + Half A use **version** watermarks
  (`system.table_changes` on append-only bronze); the Piece-4 sync and Piece-5 Half B use **timestamp**
  watermarks on `updated_timestamp` (their gold sources are MERGE-written, so version-CDF is invalid).
- **Scratch self-cleans.** Each tagged job drops its bronze scratch (`digital_occ_*`, `crtv_sync_*`,
  `crtv_staging_*`, …) on a successful `on-run-end`. A **failed** run keeps that tag's scratch for debugging;
  to inspect a chained job's intermediates on a clean run, add `--vars 'keep_<TAG>_tables: true'`.
- **Threads.** Run dbt with ≥2 threads so the parallel branches (Piece 4: dedup ∥ first-seen, component ∥
  product-resync; Piece 5: A2 ∥ A3) run concurrently.
- **Concurrency.** `silver.watermark_control` is partitioned by `watermark_name` so concurrent watermarked
  writers don't collide; still, run the pipeline steps in the order above (they have real data dependencies).
- **Failure handling.** Steps are idempotent MERGEs — re-running a failed step is safe (the watermark only
  advances on success). Fix the cause, re-run that step, then continue.
- **VM stand-up / infra** (Docker, Nessie, Trino, smoke test, Postgres reachability): `docs/runbook.md`.
