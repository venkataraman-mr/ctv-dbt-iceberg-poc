# Piece 4 — creative sync-back: high-level flow & build plan

Status: **COMPLETE (built) — all 8 tasks ported; the whole job runs end-to-end in one command in the
Databricks DAG order (2026-08-10).** This is the port of the Databricks `SYNC_CREATIVES_TO_DATABRICKS` job
(8 tasks) to our Trino/dbt/Iceberg stack, reading the seeded `tempwork.*_ctv_poc` clones and writing Iceberg
`gold.*` / `silver.*`. Companion to the seed doc (`docs/pipeline/ctv_creative_seed.md`).

Validation status: **tasks 1 (creative), 2 (first-seen), 3 (dedup), 5 (first-seen-info) VALIDATED** on real
CTV data; **occurrence-id + task 6 (last-seen) VALIDATED (2026-08-11)** once Piece 5 populated
`gold.digital_gold_occurrence` — occurrence-id resolved **13,333** `first_seen_occurrence_id`s and last-seen
refreshed **21,750** creatives; **task 4 (component)** near-empty for CTV (smoke-tested) and **task 8
(product-resync)** a no-op with the watermark at `max(change_dt)` (needs productmap churn to exercise). The
full job = `dbt run --select tag:SYNC_CREATIVES_TO_ICEBERG` (see §11).

## 1. Shape of the job

The Databricks job DAG (from the job JSON):

```
(dedup ∥ first-seen) → creative → first-seen-info → occurrence-id → last-seen → (component ∥ product-resync)
```

The Postgres half is two unchanged stored procs that build `jobwork.*_forsync_tmp`; the lakehouse half
(what we port) reads those, reverse-translates products, applies two hold loops, and MERGEs into gold.

Engine: **Trino/dbt-native, no Spark/Python** (same pattern as Piece 3). Procs are `CALL`ed via
`postgres.system.execute`; `forsync` outputs are read via the Trino `postgres` catalog; all transforms and
MERGEs run in dbt/Trino against Iceberg.

## 2. Cross-cutting decisions (locked)

- **CDF → column watermark.** Databricks reads Delta `table_changes` (`_change_type IN
  ('insert','update_postimage')`) on MERGE-written gold tables. Trino's `system.table_changes` only works on
  append-only snapshots (it errors on delete-file snapshots, which every Iceberg MERGE/UPDATE produces), so
  those reads become **column-watermark scans** (`WHERE updated_timestamp > :wm`). Semantically equivalent for
  what these tasks consume (they only want the current state of inserted/updated rows to derive the changed
  creative set).
- **UTC + no-miss discipline** (applies to every watermarked read):
  - Trino session `time_zone = 'UTC'`; Iceberg timestamps `timestamp(6) with time zone` (UTC); watermarks
    stored UTC in `silver.watermark_control`.
  - Prod Postgres `updated_timestamp` is tz-*naive* but written UTC — watermark predicates pushed to Postgres
    render the mark as a **naive-UTC literal** so there's no implicit local-tz shift on either side.
  - **Inclusive lower bound + safety lag** (**1 min**, per Venkat) so a boundary/clock-skew row is never skipped.
  - Watermark advances to **max(updated_timestamp) actually processed − lag**, never `now()`.
  - All target writes are **idempotent MERGE on stable keys** → over-read is safe (we bias to over-read).
- **VARIANT/JSONB → VARCHAR (JSON string).** Iceberg has no VARIANT/JSON type; `gold.creative` etc. store
  those columns as VARCHAR JSON strings (Trino `json_format`/`json_parse` in place of Databricks
  `to_json`/`parse_json`).
- **Archive parked.** No `creative_archive` clone; task 1 uses a single creative watermark (no
  `SYNC_PSQL_ARCHIVE_*`). Flagged to revisit at end of PoC (§7).
- **Piece-5-dependent tasks — now VALIDATED (2026-08-11).** Tasks 6 + occurrence-id read
  `gold.digital_gold_occurrence` (Piece 5 output). Once Piece 5 populated that table they validated end-to-end:
  occurrence-id resolved 13,333 `first_seen_occurrence_id`s, last-seen refreshed 21,750 creatives.

## 3. New Postgres objects to add (proc clones)

Same clone pattern as Piece 3 — bodies verbatim from prod, retargeted to `tempwork.*_ctv_poc`, `jobwork.*` →
`tempwork` scratch, `reference.*`/`config.*` left as prod reads:

1. **`tempwork.sp_dbx_creative_get_changes_for_databricks_ctv_poc(text)`** → builds
   `tempwork.creative_forsync_tmp_ctv_poc`; reads `creatives_advert_hold_tmp` clone; unchanged hold-flag logic.
2. **`tempwork.sp_dbx_component_get_changes_for_databricks_ctv_poc(timestamp)`** → builds
   `tempwork.component_coding_forsync_tmp_ctv_poc`; reads `component_hold_creative_tmp` clone.

The base tables both procs read are already seeded (incl. `component_coding`). The `*_forsync_tmp` outputs and
the `*_advert_hold_tmp`/`*_hold_creative_tmp` inputs are runtime scratch (created per run, like Piece 3's temps).

## 4. Per-task build plan (Trino/dbt-native)

| # | Task | Iceberg target | Read (source → watermark) | Key logic to port |
|---|---|---|---|---|
| 3 | Dedup-map sync | `silver.creative_dedupe_map` | `tempwork.creative_dedupe_map_ctv_poc` + `reference.creative_match_type`; delete pass off `creative_ctv_poc` | upsert MERGE on (child_creative_id, child_url_hash); delete pass for children removed from the map |
| 2 | First-seen sync | `gold.creative_first_seen` | `tempwork.creative_first_seen_ctv_poc` (provider filter + `updated_timestamp`) | straight MERGE on `creative_url_hash` |
| 1 | Creative sync | `gold.creative` (+ `silver.gold_creative_change_log`, `silver.creative_mapping_translation_hold`) | push translation-hold → `creatives_advert_hold_tmp` clone; `CALL` proc-1; read `creative_forsync_tmp_ctv_poc` | provider/source-channel code join; collapse product/competitor/celebrity to arrays; **reverse-translate vx1/vx2**; **2 hold loops**; MERGE holding_flag=false → gold |
| 5 | First-seen-info update | `gold.creative` | column-watermark on `gold.creative_first_seen` + `gold.creative` `updated_timestamp`; `silver.creative_dedupe_map` | resolve parent→family; earliest-occurrence first-seen per parent; MERGE first-seen fields into gold.creative |
| — | First-seen occurrence-id | `gold.creative` | `gold.creative` + `gold.digital_gold_occurrence` **(Piece 5)** | match provider_occurrence_id+url_hash → set first_seen_occurrence_id |
| 6 | Last-seen-info update | `gold.creative` | column-watermark on `gold.digital_gold_occurrence` **(Piece 5)**; digital only (tv/live dropped) | max capture_timestamp per creative → last_seen_timestamp |
| 4 | Component sync | `gold.component_coding` (+ `silver.component_coding_translation_hold`) | push hold → `component_hold_creative_tmp` clone; `CALL` proc-2; read `component_coding_forsync_tmp_ctv_poc` | component reverse-translation (vx2 via productmap + advertiser/mattress/component maps + `vx2_taxonomy` from Postgres); MERGE holding_flag=false → gold; near-empty for CTV |
| 8 | Product-translation resync | `gold.creative` (+ `silver.creative_product_translation_resync_log`) | column-watermark on `productcentral.productmap.change_dt` | find gold.creative rows whose products remapped; re-run reverse-translation; MERGE vx1/vx2 |

## 5. Watermark inventory (all in `silver.watermark_control`, timestamp cols, UTC, partitioned by name)

| Watermark name | Source column (UTC) | Task |
|---|---|---|
| `CTV_SYNC_CREATIVE` | prod `creatives.creative.updated_timestamp` | 1 |
| `CTV_SYNC_FIRST_SEEN` | prod `creative_first_seen.updated_timestamp` | 2 |
| `CTV_SYNC_DEDUP_UPSERT` | prod `creative_dedupe_map.updated_timestamp` | 3 |
| `CTV_SYNC_DEDUP_DELETE` | prod `creative.updated_timestamp` | 3 (delete pass) |
| `CTV_FSINFO_FROM_FIRSTSEEN` / `CTV_FSINFO_FROM_CREATIVE` | Iceberg `gold.creative_first_seen` / `gold.creative` `updated_timestamp` | 5 |
| `CTV_LAST_SEEN_DIGITAL` | Iceberg `gold.digital_gold_occurrence` (col TBD w/ Piece 5) | 6 |
| `CTV_SYNC_COMPONENT` | prod `component_coding.modified` | 4 |
| `CTV_PRODUCT_RESYNC` | ref `productcentral.productmap.change_dt` | 8 |

(Archive watermark intentionally omitted.)

## 6. Port notes / fidelity risks to validate

- **Reverse-translation md5 must byte-match prod.** `get_reverse_translation` builds
  `Upper(md5(concat(primary_product_id, system_id, concat_ws('|', sort_array(transform(sec, x->x.product_id))))))`.
  Trino port: `upper(to_hex(md5(to_utf8( concat(cast(...as varchar), ...) ))))`, with `array_sort` for
  `sort_array`, `array_join(...,'|')` for `concat_ws`, explicit casts on ints. This is the same class of risk as
  the Piece-1 xxhash64 seed-42 finding — **validate the hash equals prod** on a sample before trusting it.
- **Two hold loops (task 1):** (a) CE hold — the proc computes `hold_flag`; after the gold MERGE we replay the
  Databricks Step-7 MERGE into `tempwork.creative_classification_engine_holding_ctv_poc` from the proc's
  `crtv_sync_...holding_flag` scratch (via `system.execute`). (b) Reverse-translation hold — park/release
  `silver.creative_mapping_translation_hold` (Iceberg MERGE) + push it to `creatives_advert_hold_tmp` before the
  `CALL`. Both ported faithfully.
- **JSON handling:** array-collapse (`collect_set` over window) → Trino `array_agg` over window; struct→JSON via
  `json_format(cast(... as json))`; store as VARCHAR.
- **`QUALIFY ROW_NUMBER()`** (tasks 5/6/8) → **NOT supported on this Trino build** — rewritten as a
  `row_number() over (...)` subquery filtered `rn = 1` (see §11).
- **`vx2_taxonomy.d_advertiser/d_product`** read from the Postgres catalog at sync time (not reference-synced —
  managed Delta didn't sync); component task only.

## 7. Open flags (documented risks, per your calls)

- **Archive parked** — `creatives.creative_archive` not cloned; task 1 syncs current creatives only. Revisit at
  end of PoC if archived-creative sync is needed.
- **Piece-5 dependency (RESOLVED 2026-08-11)** — tasks 6 + occurrence-id read `gold.digital_gold_occurrence`;
  it was empty until Piece 5, which populated it and let both validate end-to-end: occurrence-id resolved 13,333
  `first_seen_occurrence_id`s, last-seen refreshed 21,750 creatives (`CTV_LAST_SEEN_DIGITAL` advanced off 1900).
  No longer an open flag.
- **Reverse-translation hash fidelity** — must be validated against prod before task 1/4/8 are trusted.

## 8. Build order + validation status (all BUILT)

1. **Proc clones** (creative + component) — **BUILT & VALIDATED** (`ddl/postgres/nessie/piece4_sync_procs_ctv_poc.sql`), pglast-parse-clean; run once on Postgres.
2. **Task 2 (first-seen)** — **VALIDATED** (`crtv_sync_first_seen.sql`: MERGE → `gold.creative_first_seen`, watermark advanced; **no lag** — see §12). Model: `crtv_sync_first_seen`.
3. **Task 3 (dedup)** — **VALIDATED** (`crtv_sync_dedupe_map` upsert + `crtv_sync_dedupe_map_delete` delete pass; `match_type` from Iceberg `reference.creative_match_type`; `json_format(json_response)`; **no lag**, `> start`).
4. **Task 1 (creative)** — **VALIDATED** (`crtv_sync_creative_forsync` → `_raw` → `_revxlate` → `crtv_sync_creative`): reverse-translation vx1/vx2 match prod; 69 adverts held (expected); both hold loops; 107-col gold MERGE; change log; watermark read (stage-1 proc call) → advance (stage-3, non-held max). Full reprocess (33,407) **and** incremental re-run both confirmed.
5. **Task 5 (first-seen-info)** — **VALIDATED** (`crtv_fsinfo_update`): parent→family earliest first-seen → `gold.creative`; two gold-internal watermarks; `<>`-guarded update-only MERGE; validated (1,796 parents on the incremental run).
6. **Occurrence-id** (`crtv_occid_update`) — **VALIDATED (2026-08-11)** — NO watermark (null-check + `updated_timestamp` floor); matches `gold.digital_gold_occurrence`; resolved 13,333 `first_seen_occurrence_id`s once Piece 5 populated gold.
7. **Task 6 (last-seen)** (`crtv_lastseen_update`) — **VALIDATED (2026-08-11)** — digital-only; `CTV_LAST_SEEN_DIGITAL`; latest `capture_timestamp` → `gold.creative`; refreshed 21,750 creatives once Piece 5 populated gold.
8. **Task 4 (component)** — **BUILT, near-empty for CTV** (`comp_sync_forsync` → `_explode` → `_revxlate` → `comp_sync`): component proc + `attribute_response` vx2 reverse-translation via the vx2_taxonomy + component/mattress maps; gold MERGE + hold + `CTV_SYNC_COMPONENT`. Smoke-tested; the vx2 rebuild + lookup column names need real component data (§10).
9. **Task 8 (product-resync)** — **BUILT** (`crtv_product_resync_affected` → `_prim` ∥ `_sec` → `crtv_product_resync`): productmap `change_dt` watermark; reuses stage-2b md5; MERGE vx1/vx2 + resync log. **Split into 4 models** and **productmap streamed** (never hashed) to fit Trino's memory + 150-stage limits; watermark **initialized to `max(change_dt)`** (no backlog to resync).

**DAG (reconciled to the Databricks job, 2026-08-10):**
```
(crtv_sync_dedupe_map → crtv_sync_dedupe_map_delete)  ∥  crtv_sync_first_seen
        → crtv_sync_creative_forsync → _raw → _revxlate → crtv_sync_creative
        → crtv_fsinfo_update → crtv_occid_update → crtv_lastseen_update
        → ( comp_sync_forsync → _explode → _revxlate → comp_sync )
          ∥ ( crtv_product_resync_affected → _prim ∥ _sec → crtv_product_resync )
```
Each task's ENTRY model `-- depends_on:` the upstream task's EXIT model; dbt runs the whole thing in this
order (roots and leaves run in parallel with ≥2 threads). All models tagged `SYNC_CREATIVES_TO_ICEBERG`.

## 9. Model pattern + build learnings

The Piece-4 sync model shape (each task follows it): read the watermark (`watermark_ts_begin`, or the
stage-1 proc-call pre-hook for the proc-driven tasks) → model body reads the source with
`updated_timestamp > start` (UTC; **no lag** — see §12) and casts to the gold schema, materializing a
**candidate** table (`schema='bronze'`, tag `SYNC_CREATIVES_TO_ICEBERG`) → **post-hooks** MERGE the
candidate into the gold target and advance the watermark → the candidate is dropped by the `on-run-end`
`SYNC_CREATIVES_TO_ICEBERG` cleanup (re-enabled 2026-08-10; opt out with
`--vars 'keep_SYNC_CREATIVES_TO_ICEBERG_tables: true'`).

- **`schema='bronze'` is the candidate's location, not the target.** The gold target is written by the
  post-hook MERGE (target named explicitly); dbt only "materializes" the staging batch.
- **post_hook is captured at PARSE, so run-time values can't be conditionally appended.** The watermark
  advance must be an always-registered **run-time template string** (`watermark_ts_finish_from_relation`,
  which reads `max(updated_timestamp)` off the candidate at run time). An earlier version appended the
  watermark hook only when `execute` was true → it never registered → the watermark never advanced. Fixed.
- **UTC predicate:** the watermark (tz-aware) is stringified/truncated to a naive-UTC literal to compare
  against the tz-naive Postgres `updated_timestamp` — no implicit local-tz shift.
- **Type casts to the gold schema matter** (e.g. `gold.creative_first_seen.media_id` is VARCHAR; ids sized;
  naive Postgres timestamps → `timestamp(6) with time zone`). Reconcile each gold target's columns before
  writing its MERGE — this surfaced the missing `provider_campaign_landing_page` column on
  `gold.creative_first_seen` (added to ddl/nessie/06 + ALTER on the VM).
- **Provider filter** kept faithful: `provider_id in (2,11,13,16,18)` via var `p4_first_seen_provider_ids`.

Each step: dbt models + any watermark seed rows, run on the VM, verify counts/keys, then commit (commands
provided for manual push per your workflow).

## 10. Task-1 (creative) build learnings (2026-08-10)

Task 1 is staged into four dbt models — `crtv_sync_creative_forsync` (stage 1: rebuild the advert-hold
clone from `silver.creative_mapping_translation_hold`, then `CALL` the proc), `_raw` (stage 2a: schema
collapse to one row per creative, all 8 jsonb cols → VARCHAR via `json_format`), `_revxlate` (stage 2b:
vx0→vx1/vx2 reverse translation + competitor vx2 + `holding_flag`), and `crtv_sync_creative` (stage 3:
107-col gold MERGE + change log + both hold loops + watermark advance, all as post-hooks). Findings, most
of which apply to every remaining sync task:

- **The watermark READ for task 1 is the stage-1 proc call, not a `watermark_ts_begin`.** Task 1 filters
  server-side in the Postgres proc, so `p4_creative_proc_call('CTV_SYNC_CREATIVE')` reads the watermark's
  `end_timestamp` and passes it into the proc as the lower bound (`updated_timestamp >= flag`). First-seen
  needs `watermark_ts_begin` only because it filters in the model body. Loop: read (stage 1) → advance
  (stage 3 `watermark_ts_finish_from_relation`, over NON-held rows only, mirroring Databricks
  `get_max_timestamp`'s anti-join). Validated with a full-history reprocess then an idempotent re-run.
- **In a dbt post-hook, reference relations as LITERAL strings — never `ref()`/`source()`/`this`.**
  Post-hooks are stored as templates and re-rendered at RUN; on that re-render `ref()`/`source()` degrade
  to `this`, which is frozen to the profile DEFAULT schema (`silver`), so `ref('crtv_sync_creative_revxlate')`
  resolved to `iceberg.silver.crtv_sync_creative` (TABLE_NOT_FOUND). Fix: hard-code
  `iceberg.bronze.crtv_sync_creative_revxlate` in every hook (gold MERGE, change log, translation hold,
  watermark). Keep `ref()` only in the model body + a `-- depends_on:` comment to anchor the DAG. (Same
  family as the Piece-3 silver-default trap.)
- **Trino MERGE `UPDATE SET` target columns must be UNQUALIFIED** — `SET col = s.col`, not `SET t.col =`.
  The RHS may reference the target alias (`t.first_seen_occurrence_id`).
- **`json_object` can't serialize a json-typed value without `FORMAT JSON`.** `json_parse(x)` inside the
  change-log `json_object(...)` produced json-typed values that Trino tried to cast to varchar (fails on
  arrays). Since our json columns are already VARCHAR JSON strings, pass them straight through (they embed
  as escaped strings — fine for the audit log). Timestamps must be `cast(... as varchar)` too — Trino can't
  cast a timestamp to json.
- **A jinja comment `{# … #}` cannot live inside a `{{ config(...) }}` argument list** — it parses as a
  stray `#`. Put rationale in the header docstring.
- **`on_table_exists='drop'`** on this hook-heavy model — rebuild by drop+create (correctly schema-qualified)
  rather than the rename/backup swap, which on this Nessie catalog resolved to the wrong (default) schema.
- **107-col gold MERGE ported faithfully:** exact Databricks UPDATE-SET *subset* so immutable/identity cols
  (`creative_id`, `provider_code`, `created_timestamp`, `occurrence_description`, `historical_creative_md5`,
  `keywords`, …) are preserved on match; `updated_timestamp := current_timestamp` (MRVXVC-14938);
  `first_seen_occurrence_id` keeps the existing value when incoming is null; `mr_secondary_company_ids` = null.
  Reconcile-before-MERGE surfaced a second missing gold column — `first_seen_provider_campaign_landing_page`
  on `gold.creative` (added to ddl/nessie/06 + ALTER on the VM), the same class of gap as first-seen's.
- **69 adverts held is expected, not a bug.** Only `classification_type='Advert'` rows whose product didn't
  reverse-translate are held; ~1,071 creatives have a null vx1 but the non-advert ones flow through. Held
  rows are parked in `silver.creative_mapping_translation_hold`, kept out of gold this run, and re-read next
  run via the stage-1 advert-hold pushback.

## 11. Tasks 5 / occ-id / 6 / 8 / 4 + DAG + platform-limit learnings (2026-08-10)

- **`QUALIFY` is not supported on this Trino build.** Every per-partition "pick one" (first-seen-info earliest,
  last-seen latest, occ-id/product-resync dedup) uses a `row_number() over (...)` in a subquery filtered `rn = 1`.
- **Two gold-internal watermarks with a new advance macro.** First-seen-info reads changes from BOTH
  `gold.creative_first_seen` and `gold.creative`; last-seen from `gold.digital_gold_occurrence`. Advance via
  `watermark_ts_advance_from_source(name, rel, ts_col)` — reads `max(ts_col)` from the source filtered by the
  watermark's OWN stored end (no start literal to escape into the hook); runs post-MERGE (source unmodified by
  that MERGE → exact changed-set max; for the self-written source it sweeps past this run's own writes, intended).
- **Occurrence-id has NO watermark.** The real job self-limits on `first_seen_occurrence_id IS NULL` (+ an
  `updated_timestamp` date floor) — once set, a creative is never reprocessed. `CTV_FIRST_SEEN_OCC_ID` is seeded
  but UNUSED (kept for inventory symmetry).
- **The `<>` MERGE guard is NULL-unsafe (faithful).** First-seen-info / last-seen only update when the incoming
  value differs; a row whose target field is NULL won't update — same as Databricks.
- **Trino's 150-STAGE limit → split heavy models.** Trino doesn't materialize CTEs, so a CTE referenced N times
  re-plans its subtree N times. The single-model product-resync hit 284 stages. Fix: **split into staged models**
  (`_affected` / `_prim` / `_sec` / writer for task 8; `forsync` / `explode` / `revxlate` / writer for task 4),
  each a small query reading physical bronze tables.
- **The huge `productmap` must be STREAMED, never hashed.** A `LEFT JOIN productmap` builds a ~3.6 GB hash on the
  (huge) build side → `EXCEEDED_LOCAL_MEMORY_LIMIT`. Fix: `productmap` is only ever (a) a filtered scan for change
  detection and (b) a semi-join `where mapping_hash in (<needed hashes>)` (`pm_small`) — Trino streams it and
  hashes the small side. Same pattern used for `d_product` in the component task.
- **Product-resync watermark must start at `max(change_dt)`, not 1900.** A fresh deploy has no resync backlog
  (task 1 already translated everything against the current productmap), and `change_dt >= 1900` would select the
  ENTIRE productmap → memory blow-up. One-time init in ddl/nessie/08.
- **`json_object` vs `map` for building JSON objects.** To emit a JSON *object* (named keys) that array-aggregates
  cleanly, build each element as `cast(map(array[keys], array[values]) as json)` (component vx2 rebuild). Trino
  can't cast a timestamp to json (change-log), and `json_parse(x)` inside `json_object` yields json-typed values
  it then fails to serialize — so json string columns are passed as VARCHAR.
- **DAG reconciled to the Databricks job.** Each task's entry model `-- depends_on:` the upstream task's exit
  model (see §8). `-- depends_on: {{ ref('x') }}` is a real dependency (dbt evaluates `ref()` at render even
  inside a `--` comment) with no effect on the emitted SQL — it's how a model whose only real reads are
  hook-written/literal tables still gets ordered.
- **Tag + cleanup.** Tag renamed `p4_sync` → `SYNC_CREATIVES_TO_ICEBERG` (job naming); the `on-run-end` scratch
  cleanup is re-enabled (drops the bronze candidates after a clean run — the `jobwork` DROP analog).

## 12. Run commands (single-task + full DAG)

**Full job** (all 8 tasks in DAG order; roots/leaves parallelize with ≥2 threads):
```
docker compose run --rm dbt dbt run --select tag:SYNC_CREATIVES_TO_ICEBERG
```
Prereqs: `CTV_PRODUCT_RESYNC` initialized to `max(change_dt)` (ddl/nessie/08 one-time UPDATE); the two Postgres proc
clones + the tempwork seed already applied.

**Single task** — chained tasks must run their whole chain (the scratch is dropped each run); single-model tasks
read the already-built gold tables so the final model alone suffices:
```
# creative (task 1):     dbt run --select crtv_sync_creative_forsync crtv_sync_creative_raw crtv_sync_creative_revxlate crtv_sync_creative
# first-seen (task 2):   dbt run --select crtv_sync_first_seen
# dedup (task 3):        dbt run --select crtv_sync_dedupe_map crtv_sync_dedupe_map_delete
# first-seen-info (5):   dbt run --select crtv_fsinfo_update
# occurrence-id:         dbt run --select crtv_occid_update
# last-seen (task 6):    dbt run --select crtv_lastseen_update
# component (task 4):    dbt run --select comp_sync_forsync comp_sync_explode comp_sync_revxlate comp_sync
# product-resync (8):    dbt run --select crtv_product_resync_affected crtv_product_resync_prim crtv_product_resync_sec crtv_product_resync
```
For dev inspection of a chained task's intermediates, add `--vars 'keep_SYNC_CREATIVES_TO_ICEBERG_tables: true'`
so the `on-run-end` cleanup doesn't drop them. (`+model` also works but pulls cross-task ancestors via the DAG.)

## Decisions (resolved 2026-08-09)

- **Safety lag = 1 min** (Venkat).
- **Watermark storage** — reuse `silver.watermark_control`'s `start_timestamp`/`end_timestamp` columns (already
  partitioned by `watermark_name`, concurrency-safe).
- Plan approved; build proceeding in the §8 order.
