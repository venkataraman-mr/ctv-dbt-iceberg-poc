# Piece 4 prerequisite — seeding production creative data into the clones

## Why this exists

Piece 4 is the creative **sync-back**: the unchanged Postgres proc
`creatives.sp_dbx_creative_get_changes_for_databricks` builds `jobwork.creative_forsync_tmp`, the
payload Databricks (in the new stack, dbt/Trino) pulls into `gold.creative`. For the PoC we run that
proc **as-is** but pointed at `tempwork.*_ctv_poc` **clones** instead of the real `creatives.*` tables.
The proc's logic does not change — only the tables it reads.

That leaves one gap. Piece 3 populated three clones (`creative_staging_ctv_poc`,
`creative_first_seen_ctv_poc`, `creative_occurrence_summary_ctv_poc`) with our PoC's newly-pushed CTV
creatives, but the sync-back proc reads a whole **family** of creative tables. This step clones the rest
of that family and **seeds them from production**, so Piece 4 has realistic input. Nothing is pushed
through ML — we copy what production already resolved. Real `creatives.*` / `ml_results.*` are only ever
**read**; every write lands in `tempwork`.

Deliverable: `ddl/postgres/piece4_seed_tempwork_ctv_poc.sql` (clone DDL + the seeding proc). Run once on
prod Postgres via a SQL client (DBeaver — `psql` isn't on the VM). Idempotent.

## The production creative lifecycle (what we're modelling)

Understanding the seed requires the Postgres-side state machine that produces a `creatives.creative` row:

1. **Staging + ML.** ML reads `creative_staging`, runs fingerprinting + deduplication, and stamps
   `record_status`: **D** (matched → this is a duplicate/child of an existing creative), **O** (original),
   or **E/F** (corrupt/failed). `record_status` lives on staging and is Piece 3's concern — out of scope here.
2. **Duplicates (D).** A D creative flows to `creatives.creative`, and its parent's attributes are copied
   into the child's dependent rows (`creative_product`, `creative_celebrity`, …). Before the status flips
   to D, ML writes the parent↔child pair — both ids **and** both url-hashes — into `creative_dedupe_map`.
3. **Originals (O) → Classification Engine.** An O creative gets a row in
   `ml_results.creative_ai_classification_staging_vx0`; CE reads it and returns a suggested product.
   - **CE responds successfully** → the creative is inserted into `creatives.creative` **and** a row is
     placed in `creative_classification_engine_holding` (id + `inserted_at`). Holding = "classified but
     awaiting user QA." While it sits in holding it must **not** sync downstream. When a user verifies/QAs,
     the holding row is deleted and the creative becomes syncable.
   - **CE gives no response** → the creative is **not** inserted into `creative` or holding at all.
   - **CE fails** → the creative is inserted into `creative` **without product info**, no holding row; it
     stays out of Databricks naturally because unclassified data isn't pulled.
4. **Dedup map is the live source of truth.** Users can later delete a mapping, move a child to another
   parent, or tag a parent under another parent. The graph is strictly **two-level** (no A→B→C hierarchy):
   a parent has one or more children; a child has exactly one parent. `updated_timestamp` is bumped on
   **both** parent and child whenever a mapping combination changes.

So the two things the sync-back proc leans on that we must reproduce faithfully are the **dedup graph**
(hold-flag propagates parent→child) and the **CE gate** (holding / staging_vx0 decide what's held back).

## Table-role catalog

| Table | Role in the seed | Ownership |
|---|---|---|
| `creative_ctv_poc` | **New clone.** The creative master; seeded from prod (anchors + parents). | Seed owns |
| `creative_product_ctv_poc` | **New clone.** Per-creative product mapping. | Seed owns |
| `creative_celebrity_ctv_poc` | **New clone.** Per-creative celebrity tags. | Seed owns |
| `creative_competitor_ctv_poc` | **New clone.** Per-creative competitor tags. | Seed owns |
| `creative_dedupe_map_ctv_poc` | **New clone.** Parent↔child map (rows where child ∈ our creatives). | Seed owns |
| `creative_classification_engine_holding_ctv_poc` | **New clone.** CE QA gate. PK added on `creative_id` for upsert. | Seed owns |
| `creative_ai_classification_staging_vx0_ctv_poc` | **New clone.** CE request/response log. | Seed owns |
| `watermark_control_ctv_poc` | **New clone** of `config.watermark_control`; two seed marks. | Seed owns |
| `creative_staging_ctv_poc` | Piece 3 Job A. **Not seeded** — the Mode 1 driver and reserved-id authority. Parents reference prod `creatives.creative_staging` directly. | Piece 3 |
| `creative_first_seen_ctv_poc` | Job B owns rows for our creatives. Seed **adds external-parent (prod-id) rows only**. | Job B + seed (ext. parents) |
| `creative_occurrence_summary_ctv_poc` | Job B owns rows for our creatives. Seed **adds external-parent (prod-id) rows only**. | Job B + seed (ext. parents) |

**Read-only at Piece 4 runtime, never cloned:** `reference.*`, `config.*` (except the watermark table),
`productcentral.*`. **Deliberately skipped:** `creatives.creative_archive` — parents older than 3 years are
extremely rare, ML only looks back 6 months for a parent, and CTV has only existed in prod since Jan 2025,
so effectively every parent lives in `creatives.creative`.

## The hybrid id model

`creative_url_hash` is **unique per creative** in both prod and our PoC — it is the cross-system link key,
and `creative_staging_ctv_poc` is the reserved-id authority. For **every** creative the seed loads (anchors
*and* one-hop parents), `clone_creative_id` is resolved the same way — by matching `creative_url_hash`
against clone staging:

- **In staging → one of our creatives:** use the **reserved PoC `creative_id`** (≥ 26 B), unchanged. This
  covers this-run anchors, prior-run creatives, and — importantly — a dedup **parent that is itself one of
  our creatives**. (The earlier bug stamped such a parent with its prod id and re-loaded it as a duplicate;
  resolving against staging fixes it.)
- **Not in staging → external parent:** keep the **exact prod `creative_id`** (< 26 B; prod ids sit below
  ~25 B and won't cross soon — which is why 26 B was chosen as the reserved start).

Everything is remapped through a single per-run id map (`prod_creative_id → clone_creative_id`, plus an
`is_ours` flag). `dedupe_map` carries clone ids on **both** child and parent (reserved or prod as resolved).
`first_seen` / `occ_summary` are seeded **only for external parents** (`is_ours = false`); rows for our own
creatives there are Job-B-owned and never touched — and because our creatives always carry a reserved id
(≥ 26 B) while external-parent rows carry a prod id, the two never collide.

## Clone-sourced descriptor fields (accuracy for our creatives)

A few descriptor columns are sourced from **our clone** staging/first_seen rather than copied straight from
prod, so the seed reflects our pipeline's view of our own creatives. The pattern is
`COALESCE(clone-value, prod-value)`: for our creatives the clone row supplies the value; for external parents
(no clone row) it falls back to prod — with a final fallback that keeps every NOT NULL column satisfied.

- **`creative_dedupe_map_ctv_poc`** — the child-side joins are **INNER**, so only rows whose child is one of
  our CTV clone creatives are loaded (a non-CTV child of a CTV parent is excluded, not kept with prod
  fallbacks). `child_provider_code` / `child_creative_type` come straight from clone staging;
  `child_creative_subtype` from `reference.media.display_n` via the child's clone `first_seen.media_id`. The
  `parent_*` equivalents come from clone staging/first_seen when the parent is ours, else from prod
  `creatives.creative_staging` / `creative_first_seen`; `parent_creative_id` / `parent_creative_url_hash` are
  `COALESCE(clone, prod)`, and each parent descriptor falls back to the prod dedupe_map denormalized value.
- **`creative_ai_classification_staging_vx0_ctv_poc`** — `provider_code` / `creative_type` from clone staging
  (else prod vx0).
- **`creative_ctv_poc`** — `provider_id`, `media_id`, `market_id`, `occurrence_timestamp` from clone first_seen;
  `provider_creative_id`, `creative_mime_type_id`, `creative_width`, `creative_height`, `creative_duration`
  from clone staging (else prod). `creative_type_id`
  is resolved from the clone staging `creative_type` name via `reference.creative_type`; `source_channel_id`
  from the clone staging `source_channel` via `reference.source_channel.short_desc` (both else prod).

**Sourced straight from prod** (no clone equivalent): `creative_ai_classification_staging_vx0.creative_source_type`
and `creative_ctv_poc.occurrence_description`.

## Two-mode seeding

One Postgres proc, `tempwork.sp_seed_creative_clones_ctv_poc(p_mode)`, run adhoc/manually. It has two
phases sharing one internal footprint loader (`sp_seed_load_footprint_ctv_poc`). Two independent marks live
in `watermark_control_ctv_poc`, each storing its high-water in `table_tx_end`.

The loader writes the clones in **production insertion order**: `first_seen → dedupe_map → occ_summary →
staging_vx0 → creative → product/celebrity/competitor → holding`. (`creative_staging` is never written —
it's the driver/authority.) Since the clones have no FKs, the order is for fidelity, not referential
correctness.

**Mode 1 — new inserts.** Watermarked on `creative_staging_ctv_poc.updated_timestamp` (`CTV_POC_SEED_NEW`).
Reads staging rows inserted since the last mark, takes their url-hashes as the anchor set, and loads the
full footprint: match each hash to its prod creative (skipping any hash prod hasn't resolved yet — Mode 2
catches those later), stamp the reserved id, upsert/copy the creative + dependents + CE tables, pull in any
missing one-hop parent (reserved id if it's ours, else prod id) and its dependents, and add external-parent
rows to `first_seen` / `occ_summary`.

**Mode 2 — creative updates.** Watermarked on prod `creatives.creative.updated_timestamp`
(`CTV_POC_SEED_UPDATE`). Grabs prod creatives changed since the last mark and rebuilds the affected
footprints. Two ways a prod change touches us, unioned into one anchor set:

- **(a) a child changed** — one of our creatives (url-hash in clone staging) was updated (e.g. 100 of
  10,000 global changes are ours). Rebuild it.
- **(b) a parent changed** — a *pure parent-attribute* change (title, provider, classification…) does **not**
  bump the child's `updated_timestamp`, so it would otherwise be missed. We detect it by joining the changed
  prod creatives to `creative_dedupe_map_ctv_poc.parent_creative_url_hash`, and rebuild the affected child.
  Rebuilding the child re-pulls its `dedupe_map` row (refreshing the denormalized `parent_*` fields) and
  re-pulls the parent creative itself via the loader's one-hop closure.

Rebuild = merge the one-row-per-creative tables, delete-and-reinsert the multi-row ones. A *mapping* change
(a child re-pointed to a new parent) bumps the child and is caught by (a); a pure parent-attribute change is
caught by (b). `creative_staging` is never written (so the Mode 1 watermark is never disturbed); every other
clone — including `staging_vx0` and `holding` — is refreshed, scoped to our creatives and their parents.

`p_mode` is `'NEW'`, `'UPDATE'`, or `'ALL'` (default; Mode 1 then Mode 2). Rebuild is idempotent, so a
creative caught by both modes in one `ALL` run is harmless.

## How to run

Everything is Postgres-native — run on prod Postgres via a SQL client (DBeaver; `psql` is not on the VM).
Requires membership in `tempwork_admin_role` (same as the Piece 3 clones).

```sql
-- 1) once: create the clones + watermark clone + seeding procs (idempotent)
\i ddl/postgres/piece4_seed_tempwork_ctv_poc.sql

-- 2) seed — adhoc / manual (later scheduled once daily ingestion starts)
CALL tempwork.sp_seed_creative_clones_ctv_poc('ALL');       -- Mode 1 (new) then Mode 2 (updates); default
-- or a single phase:
CALL tempwork.sp_seed_creative_clones_ctv_poc('NEW');       -- new inserts only
CALL tempwork.sp_seed_creative_clones_ctv_poc('UPDATE');    -- creative updates only

-- 3) inspect the two watermarks (high-water in table_tx_end)
SELECT watermark_name, table_tx_start, table_tx_end, tx_status, tx_message, tx_datetime
FROM tempwork.watermark_control_ctv_poc;
```

The proc raises `NOTICE`s per phase (candidate/affected anchor counts and the watermark it advanced from),
so `RAISE NOTICE` output in the client log is the quickest read on what a run did.

## Resolved decisions (so we don't relitigate)

- Clones carry **no** indexes, partitioning, triggers, or foreign keys — matching the Piece 3 pattern.
  PK / unique / check constraints are kept. `serial` surrogates become plain ints and are **copied**
  verbatim from prod (not regenerated) — the one exception is `creative_occurrence_summary_ctv_poc.summary_row_id`
  (a Job-B `bigserial`), which auto-generates so imported-parent rows coexist with Job B's rows.
- `creative_product` keeps the `creatives.product_map_type_enum` / `product_map_sub_type_enum` **enum types**
  so `map_type`/`map_sub_type` copy 1:1 and the proc's `jsonb_build_object` keeps working.
- `creative` drops prod's range-partitioning and composite PK → plain table, PK `creative_id`.
- `holding` gets a PK on `creative_id` (prod has none) so the seed can upsert it.
- **Write strategy** (one proc serves both new-insert and update cases): `creative`, `staging_vx0`, and
  `holding` are one-row-per-creative → **MERGE / upsert** (`ON CONFLICT (creative_id)`). `product`,
  `celebrity`, `competitor`, `dedupe_map`, and the external-parent `first_seen` / `occ_summary` rows are
  multi-row → **delete-in-scope + insert**, so prod *removals* (an unmapped product, a remapped child)
  propagate.
- Load happens in production insertion order (`first_seen → dedupe_map → occ_summary → staging_vx0 →
  creative → product/celebrity/competitor → holding`).
- Ghost parents (a parent orphaned by a remap, no CTV child left) are **left in place**; we don't garbage-
  collect them — we only keep `dedupe_map` truthful.
- One-hop closure only (child → parent). We never expand to a parent's other children.
- **Scope gate — our CTV clones + related parents only.** Every load driven by `_seed_idmap` restricts to
  creatives that are ours (`is_ours` = in clone staging) **or** are a parent of one of *this run's* in-scope
  children — the clone dedupe_map is joined back to `_seed_idmap` on the child side
  (`... creative_dedupe_map_ctv_poc d JOIN _seed_idmap ci ON ci.clone_creative_id = d.child_creative_id`) so
  it matches only the current batch's related parents, not every parent ever recorded. A non-CTV creative
  that is merely a child of a CTV parent never gets loaded. Applied to `creative`,
  `staging_vx0`, `holding`, `product`/`celebrity`/`competitor`, and `occ_summary` (all run after dedupe_map,
  so the clone map is populated). `dedupe_map` itself enforces this via INNER child-side joins. `first_seen`
  is the one exception — it runs before dedupe_map in the prod insertion order and only inserts external-parent
  rows (our creatives' rows are already Job-B-owned), so it needs no gate. The delete-in-scope steps stay
  broad, so any previously over-included rows are cleaned out and only the gated set is re-inserted.

## Assumptions & risks to watch on first run

- **`creative_first_seen` / `creative_occurrence_summary` column parity.** The parent-row inserts select the
  clone's column list by name from prod. That assumes prod's columns match the clones (true — the clones
  were generated verbatim from prod in Piece 3, including `occurrence_timestamp_local`, `edition_*`,
  `section_*`). If prod's schema has since drifted, the parent insert is where it would surface.
- **Parents assumed present in `creatives.creative`.** Parents that exist only in `creative_archive` are
  skipped (the seed guards parent insertion with `EXISTS (… creatives.creative …)`); their dedup-map row
  still records the prod parent id, but no parent creative row is seeded. Expected to be effectively never,
  per the archive-skip decision.
- **CE hold behaviour is faithful, not stubbed.** Because we copy real `holding` / `staging_vx0` rows, the
  sync-back proc's `hold_flag` can genuinely evaluate TRUE for CTV creatives (`provider_id IN (13,18)`), so
  some seeded creatives may legitimately be held back from the forsync — that's correct, not a bug.
- **`holding` uses upsert, so prod *removals* don't retract.** If prod deletes a holding row (a QA release
  that should make the creative syncable), the merge won't remove the stale clone row — the creative stays
  held in the clone until manually cleared. Accepted for the PoC; switch that table to delete-in-scope +
  insert if release propagation becomes necessary.
- **Mode 2 scans prod `creative` by `updated_timestamp`.** There's a prod index on that column, so both the
  `max()` and the `> watermark` filter are index-driven; the intersection to our ~26 K staging rows bounds
  the work.

## Validation status

**Mode 1 (new data) — VALIDATED** end-to-end on the clone tables (run by the user on prod Postgres): new
creatives from `creative_staging_ctv_poc` seed `creative` + dependents + CE tables + dedupe_map, with one-hop
parents pulled in, all correctly scope-gated to our CTV clones and their related parents.

**Mode 2 (creative-level updates) — pending.** To be exercised once daily ingestion is running (so there are
real prod `creatives.creative` changes to pick up). The logic is in place (changed-child and changed-parent
detection unioned into the anchor set); it just hasn't been run against a live change stream yet.

Static checks (kept for regression): the file parses as valid Postgres grammar (libpg_query via `pglast`),
and every column-remap insert has exact INSERT/SELECT parity (`creative` 94/94, `staging_vx0` 48/48,
`first_seen` 36/36, `dedupe_map` 22/22, `product`/`celebrity`/`competitor` 10/7/7, `occ_summary` 10/10), with
the two upsert `DO UPDATE SET` lists at 93 and 47 non-PK columns.
