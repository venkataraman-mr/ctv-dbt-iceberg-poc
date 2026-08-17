# Piece 3 — Creative push (Job A + Job B) — VALIDATED (2026-08-05)

Piece 3 pushes new CTV creatives from `bronze.digital_raw_occurrence` into Postgres, and maintains the
bronze creative registries. It is the dbt-trino port of the Databricks `DigitalRawocctoCrtvStaging`,
`RawocctoDigitalCrtvFirstseen`, and `CrtvOccSummary` classes. Both jobs are built and validated
end-to-end on the VM:
- **Job A** (`DIGITAL_RAW_OCC_TO_CRTV_STAGING`) — creative staging + first-seen seed (this doc's main body).
- **Job B** — two independent version-watermarked sub-pipelines run together: first-seen update
  (`DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE`) + occurrence summary (`DIGITAL_RAW_OCC_SUMMARY_PSQL`).
  See "Job B" below.

## Design — Trino/dbt-native, no Python

Unlike Databricks (PySpark writes a DataFrame to a Postgres temp table via JDBC, then `CALL`s a stored
proc), the PoC does the whole push **inside dbt/Trino, no Python process**. Two Trino features on the
PostgreSQL connector make it possible:

- `postgres.system.execute(query => '…')` — runs a statement in Postgres that returns **no rows**
  (a `CALL`, `INSERT`, `UPDATE`, DDL). This is how we invoke the cloned stored procedure.
- `postgres.system.query(query => '…')` — runs a **read-only** `SELECT` in Postgres and returns rows.
  (Read-only: `nextval()` and other writes are rejected here — see the id-reservation note.)

Everything else is native Trino: reading Iceberg via `system.table_changes`, the media/market joins,
writing the Postgres temp table via cross-catalog `CREATE TABLE postgres.… AS SELECT …`, and the
`creative_unique_urls` / `creative_autochaff` maintenance.

## Full-fidelity port

The dbt models are a **1:1 transliteration** of the PySpark SQL — same CTEs, same column list, same
order, keeping columns even where nothing downstream consumes them (`capture_date`, `provider_source_id`,
the `region_*` set, `retransmit`, `source_channel_id`, `occurrence_creative_url_domain`, `domain_url`,
`primary_language_code`, `media_property_status_id`). The **auto-chaff path is ported in full** even
though it is a no-op for CTV (the scoring filters `provider_code='BIS'`; CTV is `'AVOD BISCTV'`, so the
BIS input set is empty). Only unavoidable Spark→Trino syntax swaps were applied:

| Spark (Databricks) | Trino (here) |
| :-- | :-- |
| `table_changes(tbl, v1, v2)` | `system.table_changes(schema, table, start_snap, end_snap)` + version-watermark first-run/no-change branches |
| `ANTI JOIN` | `LEFT JOIN … WHERE key IS NULL` |
| `raw_json:occurrence:x` | `json_extract_scalar(try(json_parse(raw_json)), '$.occurrence.x')` |
| `RLIKE` | `regexp_like` · `Nvl`→`coalesce` · `Regexp_extract`→`regexp_extract` |
| `SUBSTRING_INDEX(...)` | `substring_index()` macro (`macros/sql_compat.sql`) |
| `POSITION(x, y)` | `strpos(y, x)` |
| `to_json(struct(...))` | `json_object_str()` macro — JSON-object string of varchar values (proc reads via `->>`+cast) |
| `boolean('false')` / `IS TRUE` | `false` / `= true` |
| `/*+ BROADCAST(..) */` | dropped (Trino uses its cost-based optimizer) |

## The models (dbt DAG, all `schema='bronze'`, `materialized='table'`)

Run order is the `ref()` DAG; the push happens in the final model's post-hooks.

1. **`crtv_staging_candidate`** = `get_new_raw_occurrence_data_cdf` + `map_media_market_details_to_occurrence`.
   Reads new `digital_raw_occurrence` inserts via `table_changes` (version watermark begin), filters
   US / `creative_url` not null / `retransmit=false`, dedups earliest-capture per `creative_url_hash`,
   anti-joins already-staged urls (`is_staged=true`) + `creative_autochaff`, derives `creative_mime_type`
   and joins the media/market dims → `tmp_digital_raw_occ_media` (all 41 columns, `rnum=1`).
2. **`crtv_autochaff`** = `get_all_ad_sizes` + `get_all_known_ad_servers` + `get_auto_chaff_records`.
   The full scoring (`ad_size`/`ad_server`/`creative_type`/`href`/`creative_url`/`click_through`) →
   `is_auto_chaff` → `tmp_digital_occ_auto_chaff` (0 rows for CTV).
3. **`crtv_autochaff_records`** = `persist_to_auto_chaff`'s `cte_auto_chaff_data` (candidate ⋈ scored),
   the full `creative_autochaff` insert record with the auto-chaff `first_seen_metadata` JSON.
4. **`crtv_staging_excluded`** = `exclude_auto_chaff_records` (candidate ANTI-JOIN auto-chaff on
   `creative_url_hash + provider_code`).
5. **`crtv_staging_final`** = `get_new_creatives` / `get_previous_creratives` / `combine_creatives` +
   `generate_internal_id`. Classifies new (not in `creative_unique_urls`) vs previously-unstaged
   (`is_staged=false`, reuse `creative_id`); assigns `creative_id = block_start + row_number()-1` to new
   rows; builds the 26-column staging contract + staging `first_seen_metadata`. **Post-hooks (ordered as
   `process_to_crtv_staging`, watermark last):**
   1. insert `crtv_autochaff_records` → `bronze.creative_autochaff` (anti-join)
   2. insert new rows → `bronze.creative_unique_urls` (`is_staged=false`, joining `data_provider` for `first_seen_provider_id`)
   3. `drop` + 4. `create` the Postgres temp table `tempwork.tmp_digital_raw_occ_to_crtv_staging_ctv_poc` (cross-catalog CTAS)
   5. `pg_call` → `tempwork.sp_dbx_digital_insert_crtv_staging_first_seen_ctv_poc` (inserts `creative_staging` + seeds `creative_first_seen`)
   6. flip `creative_unique_urls.is_staged=true`
   7. `watermark_version_finish`

Fail-safe: the watermark advances only in the last hook, so a failed push leaves it un-advanced and the
next run reprocesses. Idempotency: Iceberg anti-joins + the proc's `ON CONFLICT (creative_url_hash) DO
NOTHING` + the proc running as one Postgres transaction. Matches legacy behavior (duplication/id-burn on
retry is acceptable, as in Databricks).

## Scratch cleanup (jobwork analog)

The five dbt models are materialized as **tables** (the native Nessie connector has no views), so they're the analog of the
Databricks `jobwork` temp tables — refreshed every run. They're dropped at the end of a **successful**
run by an `on-run-end` hook (`dbt_project.yml` → `cleanup_tagged_models(results, 'RAW_OCCS_TO_CREATIVE_STAGING',
'crtv_staging_final')`, in `macros/cleanup.sql`): it drops every model tagged `RAW_OCCS_TO_CREATIVE_STAGING` (all five) but
only when `crtv_staging_final` succeeded — a **failed** run keeps them for debugging. Opt out with
`--vars 'keep_RAW_OCCS_TO_CREATIVE_STAGING_tables: true'` to inspect the combined push record after a good run. The persistent
tables (`creative_unique_urls`, `creative_autochaff`, `silver.watermark_control`) aren't `RAW_OCCS_TO_CREATIVE_STAGING` models,
so they're never touched. (Gotcha found here: a Jinja `{% set %}` inside a `{% for %}` is loop-scoped —
the gate flag must live on a `namespace()` to escape the loop.)

After a clean run the intermediates are gone, so verify results from the persistent outputs
(`postgres.tempwork.*`, `creative_unique_urls`, the watermark), not `crtv_staging_final`.

## Macros

- `macros/crtv_push.sql` — `pg_call(inner_sql)` (tunnels a `CALL`/DML via `system.execute`);
  `reserve_creative_ids(n)` (reserves the id block — see below).
- `macros/sql_compat.sql` — `substring_index()` (Spark `SUBSTRING_INDEX`); `json_object_str(pairs)`
  (JSON-object string; Trino has no `json_object`).
- `macros/watermark.sql` — `snapshot_range_since_ts()` (generic timestamp→snapshot resolver, for Job B).

## Creative-id reservation (replaces the Universal Creative API)

Databricks called a global-id API returning a `[min, max]` block. Here a **Postgres sequence**
(`tempwork.creative_id_seq_ctv_poc`, start 26,000,000,000) plays that role, reserved via a procedure:
`sp_reserve_creative_ids_ctv_poc(n)` atomically pops `n` values off the sequence and records the block
into `tempwork.creative_id_block_ctv_poc`. `reserve_creative_ids(n)` calls it via `system.execute`
(writable), then reads the newest block row via `system.query` (read-only) — two calls, because Trino
can't both write and return in one. The block table is the API's response log (one row per run).

## Postgres side (clone objects — `ddl/postgres/piece3_tempwork_ctv_poc.sql`, run once)

Everything lives in the `tempwork` schema with a `_ctv_poc` suffix so the PoC never touches the real
`creatives.*` tables/procs. Clones start **empty** (no prod seed); **no triggers** (we only prove
Iceberg→Postgres movement). Objects: `creative_staging_ctv_poc`, `creative_first_seen_ctv_poc` (clone
tables, PK + `unique(creative_url_hash)` for the proc's `ON CONFLICT`), the two clone procs (bodies
verbatim, retargeted; the insert proc still reads the real read-only `reference.data_provider`),
`creative_id_seq_ctv_poc` (sequence), `creative_id_block_ctv_poc` + `sp_reserve_creative_ids_ctv_poc`.
The temp tables (`tmp_digital_raw_occ_to_crtv_staging_ctv_poc`, `…firstseen_ctv_poc`) are dropped/created
by the dbt run, not here.

## Watermark

Job A uses the **version** watermark (like Piece 1): begin pins `digital_raw_occurrence`'s end snapshot
in `current_commit_version` (`InProgress`); the final model's post-hook promotes it to
`last_commit_version` (`SUCCEEDED`) after the push. Seed row `DIGITAL_RAW_OCC_TO_CRTV_STAGING` in
`ddl/03` (Job B's `DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE` timestamp seed is there too).

## Run & verify

```bash
# once, on Postgres: run ddl/postgres/piece3_tempwork_ctv_poc.sql
# once, on Trino: seed the Job A watermark row (ddl/03 DIGITAL_RAW_OCC_TO_CRTV_STAGING)
docker compose run --rm dbt dbt run --select +crtv_staging_final          # by model
docker compose run --rm dbt dbt run --select tag:RAW_OCCS_TO_CREATIVE_STAGING          # by job tag (same models, whole job)
# verify
docker exec -i trino trino --execute "SELECT count(*) FROM postgres.tempwork.creative_staging_ctv_poc"
docker exec -i trino trino --execute "SELECT count(*) FROM postgres.tempwork.creative_first_seen_ctv_poc"
docker exec -i trino trino --execute "SELECT count(*) FILTER (WHERE is_staged) FROM iceberg.bronze.creative_unique_urls"
docker exec -i trino trino --execute "SELECT last_commit_version, transaction_status FROM iceberg.silver.watermark_control WHERE watermark_name='DIGITAL_RAW_OCC_TO_CRTV_STAGING'"
```
First validated run: **26,592** creatives staged (all net-new), `creative_first_seen` seeded 1:1,
`creative_autochaff` 0 (CTV), watermark `SUCCEEDED`.

Reset for a clean re-test:
```bash
docker exec -i trino trino --execute "DELETE FROM postgres.tempwork.creative_staging_ctv_poc"
docker exec -i trino trino --execute "DELETE FROM postgres.tempwork.creative_first_seen_ctv_poc"
docker exec -i trino trino --execute "DELETE FROM iceberg.bronze.creative_unique_urls"
docker exec -i trino trino --execute "UPDATE iceberg.silver.watermark_control SET last_commit_version=NULL, current_commit_version=NULL, transaction_status='SUCCEEDED' WHERE watermark_name='DIGITAL_RAW_OCC_TO_CRTV_STAGING'"
docker exec -i trino trino --execute "SELECT * FROM TABLE(postgres.system.query(query => 'SELECT setval(''tempwork.creative_id_seq_ctv_poc'', 26000000000, false)'))"
```

## Job B — first-seen update + occurrence summary (2026-08-05)

Two independent sub-pipelines, both **version-watermarked** (Job B was consolidated onto version
watermarks — the timestamp macros stay for Pieces 4–5), reading `digital_raw_occurrence` separately and
run together as one Job B. All Postgres targets are `tempwork.*_ctv_poc` clones.

**First-seen update** (`crtv_firstseen`, watermark `DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE`) — port of
`RawocctoDigitalCrtvFirstseen`. One model: reads new inserts, `INNER JOIN creative_unique_urls` for the
`creative_id`, joins the media/market dims, builds the occurrence-level payload; post-hooks write the PG
temp table and `pg_call` the cloned update proc, which pulls each `creative_first_seen` row back to its
EARLIEST occurrence (`UPDATE … WHERE trg.occurrence_timestamp > tmp.occurrence_timestamp`). Job A seeds
`creative_first_seen`; Job B only updates. `REPLACE(x,'\0','')` → `replace(x, chr(0), '')` (Spark
`\0` is the NUL char here, unlike the Postgres proc's literal-`\0` strip).

**Occurrence summary** (`crtv_occ_summary_candidate` → `crtv_occ_summary_final`, watermark
`DIGITAL_RAW_OCC_SUMMARY_PSQL`) — port of `CrtvOccSummary` (minus `PrintCreativeOccurrenceSummary`).
Candidate = the CDF read **UNION the parked buffer** `bronze.missing_digital_occurrence_for_summary`,
deduped latest per `(provider_code, provider_occurrence_id)`, joined to resolve `creative_id` / market /
media and anti-joined against `creative_autochaff`. Final = the aggregate (rows with `creative_id` +
`media_property_id`) grouped with `min/max capture_timestamp` + `occ_cnt`. Post-hooks: PG temp CTAS →
`pg_call` the upsert proc (accumulates `occurrence_count` + `first_run`/`last_run` + a 7-day count) →
**park/release `MERGE`** into the buffer (`WHEN MATCHED AND creative_id resolved THEN DELETE`;
`WHEN NOT MATCHED AND creative_id null THEN INSERT`) → watermark finish. This is the one place using an
Iceberg `MERGE` with `DELETE` (works on Nessie; fall back to `DELETE`+`INSERT` if it regresses).

New objects: PG clone `tempwork.creative_occurrence_summary_ctv_poc` + `sp_dbx_digital_upsert_to_crtv_occ_summary_ctv_poc`
(`ddl/postgres/piece3_tempwork_ctv_poc.sql`); `capture_timestamp` added to `bronze.missing_digital_occurrence_for_summary`
(ddl/04, Databricks MRVXVC-11059); watermark seeds `DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE` +
`DIGITAL_RAW_OCC_SUMMARY_PSQL` (ddl/03). Both jobs tagged `CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY`; the generalized `on-run-end` cleanup
drops the `CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY` scratch after a clean run.

Run (parallel is safe after the watermark-table partitioning fix below):
```bash
docker compose run --rm dbt dbt run --select crtv_firstseen crtv_occ_summary_candidate crtv_occ_summary_final          # by model
docker compose run --rm dbt dbt run --select tag:CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY          # by job tag (same models, whole job)
docker exec -i trino trino --execute "SELECT count(*) FROM postgres.tempwork.creative_occurrence_summary_ctv_poc"
docker exec -i trino trino --execute "SELECT count(*) FROM iceberg.bronze.missing_digital_occurrence_for_summary"   -- parked-unresolved
```

## Concurrency — watermark_control partitioned by watermark_name (2026-08-05)

Job B's two sub-pipelines (and, on schedule, Job A vs Job B) write `silver.watermark_control`
concurrently. As one tiny **unpartitioned** table it was a single data file, so any two concurrent
writers rewrote the same file → `ICEBERG_COMMIT_ERROR "Found conflicting files"`. This is a
serializable-isolation row-conflict that Trino does **not** retry — `max_commit_retry` didn't help (the
run failed in ~1s, no retry). Fix: **partition `watermark_control` by `watermark_name`** (ddl/03), so
each process's row is its own partition = its own file; different processes rewrite different files and
Iceberg's conflict check prunes non-matching partitions. Now Job B runs at full `threads=4` cleanly, and
any concurrent mix of watermarked jobs is safe. `max_commit_retry=20` kept as a cheap safety margin.
Retrofit needs a recreate (partition spec can't be added to existing files) — back up rows, recreate
partitioned, restore (commands in the runbook / this file's history).

## Build learnings (Trino/dbt gotchas found on the VM — so they are not rediscovered)

- **`IS TRUE` unsupported** in Trino (`IS` accepts only `NULL`/`NOT`/`DISTINCT`) → use `= true`.
- **Jinja comment whitespace** (`{#- … -#}`) between `with` and the first CTE glued them into one token
  (`withcdf_source`) → use plain SQL `--` comments there.
- **`nextval()` under `system.query` fails** ("cannot execute nextval() in a read-only transaction") →
  reserve the block via `system.execute` (writable) + a Postgres procedure; read it back via `system.query`.
- **Parse-time `this` schema** — `post_hook` strings built in `{% set %}` render at parse (before the
  in-file `config(schema='bronze')`), so `{{ this }}` froze to the profile default schema (`silver`).
  Fix: reference the model in hooks by an explicit relation (`this.database ~ '.bronze.' ~ this.identifier`).
- **`Relation` objects in post-hooks degrade on re-render** — `source()`/`ref()` stored in `{% set %}`
  and embedded in a post-hook string re-render to the model's own relation (`silver.<model>`) at run
  time. Fix: build every hook relation as a **plain literal string**; preserve DAG order with a
  `-- depends_on:` comment.
- **Watermark finish froze to a no-op** — `watermark_version_finish` returns `"select 1"` when
  `execute` is False; capturing it via `{% set %}` at parse baked the no-op. Fix: pass it as a run-time
  template string `"{{ watermark_version_finish('…') }}"` in the hook list (renders at run, `execute=True`).
- **Jinja loop scope (cleanup macro)** — a `{% set %}` inside a `{% for %}` is loop-scoped and doesn't
  escape, so the cleanup gate flag stayed false and nothing dropped. Fix: hold flags on a `namespace()`.
- **Concurrent watermark writers** — two Job B sub-pipelines (and Job A vs Job B on schedule) writing the
  single-file `watermark_control` collide (`ICEBERG_COMMIT_ERROR "Found conflicting files"`); Trino does
  not retry that serializable conflict (`max_commit_retry` didn't help). Fix: partition the table by
  `watermark_name` so each process writes its own file. Then `threads=4` is safe.
- **Postgres event trigger on `tempwork`** — a DBA event trigger (`dba.trg_create_set_owner`) reassigns
  every new `tempwork` table to `tempwork_admin_role`; the login must be a MEMBER of that role
  (`GRANT tempwork_admin_role TO <login>;`) or `CREATE TABLE` fails with "must be able to SET ROLE".

## Source (Databricks, read-only)

`occurrences/digital/class_files/raw_occs_to_creative_staging.py` (Job A);
`occurrences/common/class_files/raw_occ_to_digital_crtv_firstseen.py` (Job B);
`common/psql_utility.py` (temp-table + `CALL` pattern). Uploaded PG scripts: `creative_staging.sql`,
`creative_first_seen.sql`, `sp_dbx_digital_insert_crtv_staging_first_seen.sql`,
`sp_dbx_digital_update_raw_occ_to_crtv_first_seen.sql`.
