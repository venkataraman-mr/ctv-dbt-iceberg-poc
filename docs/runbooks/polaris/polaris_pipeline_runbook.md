# Polaris pipeline runbook — parallel PoC (Nessie → Polaris)

**Status: IN PROGRESS.** Base structure + **Step 1 (reference sync)** and **Step 2 (ingestion → raw occurrence)**
built; Steps 3–6 pending (see §5 Build progress). This runbook is both the build + validation plan and the running
log. It stands the **whole CTV pipeline** (reference sync → ingestion → Pieces 1–5) up on **Apache Polaris**,
running **in parallel** to the working Nessie pipeline on the same VM, so we can prove parity before the AWS build.

- Catalog decision: **Polaris** (leads confirmed for the AWS non-prod build) — see
  [`../../catalog/iceberg_catalog_evaluation.md`](../../catalog/iceberg_catalog_evaluation.md).
- Catalog stand-up (already done on the VM): [`../../catalog/catalog_poc_runbook.md`](../../catalog/catalog_poc_runbook.md).
- The Nessie pipeline (reference / source of truth for the logic): [`../nessie/runbook.md`](../nessie/runbook.md)
  and [`../nessie/ctv_daily_runbook.md`](../nessie/ctv_daily_runbook.md).

> **Why a clone, not a repoint.** We keep the Nessie pipeline **untouched** as the comparison baseline and build a
> **separate copy** for Polaris. Full isolation → the working pipeline is never at risk, and we get an
> apples-to-apples diff (row counts, id ranges, watermarks) at every piece. At cutover we retire the Nessie copy.
>
> **What's actually changing.** All 5 pieces are **already built and validated** in the Nessie `dbt/` PoC — the
> logic and "brain" (models, watermarks, MERGEs, Postgres push, gates) is **reused as-is**. The **only substantive
> change for Polaris is v3 + VARIANT** (data-type parity with the source Databricks tables). Expect only **minor**
> dbt tweaks (catalog binding, the VARIANT read/write patterns) — the core logic is intact. This is a
> catalog + data-type migration, **not** a re-implementation.

---

## 0. HARD REQUIREMENT — data-type parity with the source Databricks tables

The Nessie PoC deliberately **dropped v3/VARIANT** (VARIANT→VARCHAR/JSON workaround, binary→base64) because
Nessie can't host v3. **Polaris must NOT carry that workaround.** Every Polaris Iceberg table — pipeline **and**
reference-sync — must **match the source Databricks table's data types exactly**, with **VARIANT preserved** (no
VARCHAR/JSON/base64 substitution), so the AWS Iceberg tables are drop-in equivalents of today's Databricks Delta
tables. This is the business hard requirement driving the whole migration.

**Schema source of truth — the authoritative Databricks table DDL:**
`@master_readonly_copy_venkat/db_scripts/notebook_files/table_ddl/{bronze,silver,gold,custom,spend,archive}.py`
(one `CREATE TABLE` per UC table; `Digital_Flow_DeepDive.md` calls this "the schema source of truth"). **Derive
`ddl/polaris/` from THESE, not from `ddl/nessie/`** (which encodes the workaround).

> `Digital_Flow_DeepDive.md` + `claude.md` (copied into `docs/reference/databricks_prod/`) document the
> **running Databricks *production* pipeline** — they are the authority for the source **data types** (v3+VARIANT)
> and business **semantics**. They are **NOT** the dbt implementation to rebuild: the dbt+Iceberg logic already
> exists in `dbt/` (Nessie PoC). Use these docs only to (a) get the exact VARIANT/source schemas and (b)
> sanity-check semantics — the *code* to port is the existing `dbt/`, not the Databricks prod code.

> **⚠️ Supersede the deep-dive's VARIANT guidance.** `Digital_Flow_DeepDive.md` (2026-07-20, **pre-Polaris**)
> says *"No Iceberg VARIANT type → map to string"* — that is precisely the Nessie-era workaround, and it is
> **superseded**: Polaris was chosen *because* it serves v3+VARIANT. Keep VARIANT as VARIANT; ignore the
> map-to-string note wherever it appears in the deep-dive.

**Governing rule — the Databricks `table_ddl` is the source of truth for *every* column type.** For each Polaris
table, match the source column type exactly (STRING→VARCHAR, INT→INTEGER, BIGINT→BIGINT, DATE→DATE,
TIMESTAMP→`timestamp(6) with time zone`, BOOLEAN→BOOLEAN, **VARIANT→`variant`**). Never deviate from source; if
it's VARIANT in Databricks it's `variant` in Polaris. VARIANT columns are **table-specific** — check the actual
`CREATE TABLE`, don't carry a column onto a table that doesn't have it.

> **The one forced exception: `SMALLINT`/`TINYINT` → `INTEGER`.** Iceberg has no 8/16-bit int, and the Polaris
> Iceberg REST connector rejects them (`NOT_SUPPORTED "Type not supported for Iceberg: smallint"`). Map any
> `SMALLINT`/`TINYINT` source column to `INTEGER` (values fit). This is a physical Iceberg-spec limitation, not a
> choice — the only place we can't match the source type exactly. (Nessie's *native* connector accepted `smallint`,
> so `ddl/nessie/*` used it; the Polaris REST connector does not.)

**VARIANT columns to preserve — confirmed per table from the authoritative `table_ddl`:**
- **`bronze.digital_raw_occurrence` (CTV bronze):** exactly **two** — `daisy_chain`, `raw_json`. *(No `json_data` on
  this table — that's the whole-object text blob in the staging table `digtial_raw_occurrence_ctv_staging`, which
  stays VARCHAR.)* `ddl/polaris/02` = the Nessie DDL with these two flipped VARCHAR→`variant` + `format_version=3`.
- **Other occurrence tables (silver staging / gold / non-CTV digital):** may add `json_data`, `provider_raw_json`,
  `live_events_tag`, `gpt_response` — **verify against each table's own `CREATE TABLE`** before adding.
- **Creative (bronze `creative_unique_urls` / `gold.creative` / silver):** `creative_payload`,
  `creative_machine_learning_payload` (a.k.a. `machine_learning_payload`), `first_seen_metadata`,
  `attribute_response`(+`_vx2`), `secondary_products` (+`vx1_`/`vx2_`), `mr_secondary_company_ids`,
  `attribution_competitor`(+`_vx2`), `attribution_celebrity`, `custom_attributes`, `print_matching_ads`,
  `print_ad_images`, `json_response` (fingerprint dedupe), `json_log`.
- **Reference sync — no VARIANT (sorted).** Verified against both sync `TABLE_MAP`s and the `table_ddl`: the
  UC-synced tables (`reference.creative_match_type` / `global_market` / `provider_global_market_map` / `media`, the
  two `spend.digital_dmi_prelim_spend_average_by_*`, and the `tempwork.*` spend QC tables) and the hive-synced
  `reference.*` dims have **no** VARIANT columns. The `source_json VARIANT` in `spend.py` belongs to
  `spend.tv_rates_availability` / `tv_rates_raw` — **TV rates tables, out of scope and NOT in either sync map**.
  (And `hive_metastore` Delta can't hold VARIANT anyway.) So reference/spend sync needs **no** VARIANT handling —
  it can keep the existing shared-engine behavior unchanged.

*(Line numbers for every VARIANT decl are in the `table_ddl` files above — use them to map exact per-table
schemas when writing `ddl/polaris/`.)*

**The key technical risk — VARIANT on the WRITE path (occurrence only).** The VARIANT that ingestion actually
lands is on **`bronze.digital_raw_occurrence`** (`json_data`, `raw_json`, `daisy_chain`, `provider_raw_json`, …).
Trino 483 / dbt write VARIANT natively (`CAST(JSON… AS VARIANT)`), but the **PyIceberg landing writer does not** —
**no released PyIceberg writes VARIANT** (confirmed; V3 `VariantType` still open). **Chosen path:** ingestion lands
those columns as **string/JSON text** (PyIceberg handles today), then the **first dbt model `CAST`s to `variant`**
(materializes in Iceberg via Trino). Creative VARIANT columns are produced *in dbt* (Pieces 3/4), so they write
VARIANT natively. **Reference/spend sync needs none of this** (no VARIANT — see above). Validate the occurrence
land-as-string → CAST round-trip **early** on one table.

Because of this, **v3 + VARIANT is the target from the first table** — there is no VARCHAR-first phase. A
v2/VARCHAR copy is only an *optional per-table diagnostic* to isolate catalog mechanics (§5); the deliverable is
source-type parity.

---

## 1. Approach — reuse Nessie code, add v3+VARIANT, build in daily-runbook order

**The motto.** *Reuse the code we already wrote for Nessie; verify each table against the current Databricks
`table_ddl`; bring in `variant` + `format_version=3` for every table we create. Do **not** change the dbt logic we
already validated — the only edits are the VARIANT pieces (table types + the `CAST`s that produce them).*

> **v3 scope.** "v3 for every table" applies to every **pipeline** table we build (occurrence + creative — all
> Trino/dbt-written). The **one exception is the reference/spend sync** (Step 1): PyIceberg 0.11.1 can't write v3,
> those tables have no VARIANT, and **in production this data isn't synced — it's read directly from Databricks via
> Trino** — so the PoC reference mirrors stay v2. See §5 Build progress.

- **Parallel clone.** New folders `dbt_polaris/`, `ingestion_polaris/`, `ddl/polaris/`, `ddl/postgres/polaris/`,
  all targeting the **`polaris`** Trino catalog. Nessie's `dbt/`, `ingestion/`, `ddl/nessie/`,
  `ddl/postgres/nessie/` stay as-is.
- **Side-by-side.** Both catalogs live in one Trino (`iceberg` = Nessie, `polaris` = Polaris). Run each step on
  Polaris, then compare against the Nessie run (row counts, id ranges, watermarks) and — for VARIANT columns —
  against the **source UC** values.
- **Single pass, v3+VARIANT from the start.** No VARCHAR-first phase. Each table is created **v3** with real
  `variant` columns wherever the source `table_ddl` has VARIANT (§0). *(A VARCHAR/v2 copy is available only as an
  optional one-table diagnostic if catalog mechanics ever need isolating — see the note at §5's end.)*
- **Build in the Nessie daily-runbook order** (§5) — the same dependency order operators already run:
  reference sync → ingestion → creative push/first-seen → seed → sync-back → gold occurrence.

What changes vs. Nessie, per table: (1) the `ddl/polaris/*` table is v3 + `variant` (not VARCHAR); (2) the dbt
model's `SELECT` `CAST`s those columns `AS variant` instead of leaving them string. **Nothing else in the model
logic changes.**

---

## 2. Naming & isolation conventions (decide once, stick to it)

| Thing | Nessie (existing) | Polaris (new) |
| :-- | :-- | :-- |
| Trino catalog | `iceberg` | `polaris` |
| dbt project | `dbt/` (profile `catalog: iceberg`) | `dbt_polaris/` (profile `catalog: polaris`) |
| dbt model folders | `bronze/`, `occurrences/`, `creatives/`, `reference/`, `spend/` | **`occurrences/` + `creatives/` only** (see rule below) |
| Ingestion code | `ingestion/` | `ingestion_polaris/` |
| Trino DDL | `ddl/nessie/` | `ddl/polaris/` |
| Postgres DDL | `ddl/postgres/nessie/` | `ddl/postgres/polaris/` |
| Iceberg schemas | `bronze` / `silver` / `gold` (+ ref schemas) | **same names** — no collision (`polaris.bronze.*` vs `iceberg.bronze.*` are different namespaces) |
| S3 prefix | `s3://…/warehouse/` | `s3://…/polaris/` (already set on the Polaris catalog) |
| Postgres clone tables | `tempwork.*_ctv_poc` | **`tempwork.*_ctv_poc_pol`** (separate objects) |
| Postgres id sequences | existing `creative_id` / `occurrence_id` (75B) sequences | **separate sequences / id-block tables** so id ranges don't interleave |
| Watermark control | `iceberg.silver.watermark_control` | `polaris.silver.watermark_control` (its own) |

**Postgres is the one real collision risk.** Separate tempwork tables handle the data; give the Polaris run its
**own creative_id / occurrence_id sequences** (or id-block tables) too, or the two runs' ids interleave and the
parity diff gets muddy. Shared S3 bucket is safe (different prefixes). Watermarks are per-catalog (own tables).

### RULE — dbt model folders are by DOMAIN, not medallion layer

`dbt_polaris/models/` has **exactly two folders that hold models: `occurrences/` and `creatives/`.**

- **`occurrences/`** = **every** occurrence model, across all layers — the raw occurrence (staging→raw), the silver
  staging/hold models, and the gold occurrence models — **plus** the spend update (`avod_ctv_spend_update`, Piece 5),
  since it feeds the occurrence flow. We do **not** split occurrence models into separate `bronze/` and `gold/`
  folders — they all live together because they're all about occurrences.
- **`creatives/`** = every creative model (Piece 3 push + first-seen/summary, Piece 4 sync-back).
- **The medallion layer is preserved by `config(schema='bronze'|'silver'|'gold')` inside each model — NOT by the
  folder.** So a model in `occurrences/` still writes to `polaris.bronze.digital_raw_occurrence` etc. Folder =
  domain; `schema=` = layer.
- The `bronze/`, `gold/`, `reference/` folders are kept **empty** (no models). `silver/` isn't used.

> This differs from the Nessie layout (which keeps `bronze/digital_raw_occurrence.sql`, `reference/…`, `spend/…`
> as separate folders). When cloning a Nessie model into `dbt_polaris/`, place it by **domain** per the rule above.
> *(Edge model `avod_ctv_spend_update` is defaulted to `occurrences/`; move if a later step shows it's
> creative-domain.)*

**Views: real Iceberg views, not ephemeral models.** Nessie's native connector can't hold Iceberg views, so the
Nessie build faked view-shaped objects as **ephemeral dbt models** (e.g. `media_property_flatten_vx0_vw`). **Polaris
supports Iceberg views**, so on Polaris we create such objects as **real views via `ddl/polaris/*` Trino DDL** in
their proper schema (e.g. `polaris.km_preparation_gold_db.media_property_flatten_vx0_vw`) and declare them as
**sources** in `sources.yml` — downstream models `source(...)` them (not `ref()`). This restores the original
Databricks shape and lets non-dbt tools query them.

---

## 2a. Container topology (Docker)

Two **new** compose services, added alongside the existing ones — the Nessie `dbt` and `ingestion` services (and
their images) are **never touched**. The two pieces bind to the catalog differently, so they're handled
differently.

| Service | Image | Mounts | Catalog binding | Reuse Nessie image? |
| :-- | :-- | :-- | :-- | :-- |
| `dbt_polaris` | **reuse** `infra/Dockerfile.dbt` | `./dbt_polaris` | dbt→**Trino** only; profile `database: polaris` | **Yes** — same image |
| `ingestion_polaris` | **reuse** `infra/Dockerfile.ingestion` | `./ingestion_polaris` | direct **Polaris REST** (OAuth2 + `ctv_poc` warehouse) | **Yes** — same image |

**dbt → reuse the image.** dbt never talks to the catalog; Trino does. The `dbt_polaris` service is the same
`Dockerfile.dbt` with `./dbt_polaris` mounted and a profile pointing `database: polaris`. No image change, no new
libraries — the only difference from Nessie is which Trino catalog the profile names.

**ingestion → same image, separate container (decided).** Ingestion writes to the catalog **REST endpoint
directly** (PyIceberg), not through Trino. But because the VARIANT it lands is handled by **land-as-string → `CAST`
in dbt** (no PyIceberg VARIANT write needed — §8), `ingestion_polaris` needs **no new libraries** — its
`requirements.txt` is unchanged from Nessie. So it **reuses `Dockerfile.ingestion`** as a **separate compose
service** with `./ingestion_polaris` mounted and Polaris env (OAuth2 REST + `ctv_poc` warehouse). The Nessie
`ingestion` service is untouched, and the two run side-by-side.

> **Fallback:** if a future write path *does* force a library change (e.g. a Spark-based VARIANT writer), split off
> `infra/Dockerfile.ingestion.polaris` + a separate `requirements.txt` then — cheap to do later, and the runbook
> already anticipates it. Not needed for the land-as-string plan.

> **Images vs services.** "Separate container" = separate compose *service* (own name, own mount, own env), reusing
> the Nessie *image* for both dbt and ingestion. Purely additive — `docker compose up` for the Nessie stack is
> unchanged.

---

## 3. Prerequisites (verify before starting)

1. **Polaris is up and wired in Trino** (catalog PoC done): `docker exec -i trino trino --execute "SHOW CATALOGS"`
   lists `polaris`; `SHOW SCHEMAS FROM polaris` works. If not, follow
   [`../../catalog/catalog_poc_runbook.md`](../../catalog/catalog_poc_runbook.md) Part A.
2. **`.env`** has `POLARIS_OAUTH2_CREDENTIAL` (the `trino_poc` client_id:secret) so the `polaris` Trino catalog
   authenticates.
3. **Feature baseline** already validated on Polaris (v3 + VARIANT + DML + views over REST) — see the runbook
   matrix. So the catalog can host what the v3+VARIANT build needs.

---

## 4. Build the clones (one-time)

> Keep the clones **thin** — copy, then change only the catalog binding and the VARIANT columns (table types +
> the `CAST`s that produce them). Don't diverge the transform logic; it must stay comparable to Nessie.

> **✅ Base structure DONE (2026-08-31).** Scaffolding is in place; per-step work adds the actual models/scripts:
> - `ddl/polaris/00_create_schemas.sql` — all 8 Polaris schemas (bronze/silver/gold + reference/km_*/productcentral/spend).
> - `ingestion_polaris/` — `config.py` (Polaris REST+OAuth env), `common/catalog.py` (repointed to Polaris),
>   `common/{ref_sync_engine,spark_hash}.py` + `requirements.txt` copied verbatim. Piece scripts (`reference_sync`,
>   `uc_reference_sync`, `ctv_ingestion`) added per step.
> - `dbt_polaris/` — `dbt_project.yml` (`ctv_occurrence_polaris`), `profiles.yml` (`database: polaris`), `macros/`
>   copied verbatim (all catalog refs go through `source()` — clean), and a **single consolidated `models/sources.yml`**
>   (all 10 sources; Iceberg→`polaris`, tempwork clones→`_ctv_poc_pol`). Model folders: **`occurrences/` +
>   `creatives/`** (domain-organized per §2 rule); `bronze/`/`gold/`/`reference/` kept empty. Models added per step.
> - `docker-compose.yml` — `dbt_polaris` + `ingestion_polaris` services (reuse the Nessie images).
>
> **⚠️ Model catalog literals — handle per step.** The Nessie models hardcode the catalog name **69×** as
> literal `iceberg.<schema>.<table>` (e.g. `merge into iceberg.gold.creative`, `set rel = 'iceberg.bronze.…'`) —
> these are **not** covered by `sources.yml`. When cloning each model into `dbt_polaris/`, replace `iceberg.` →
> `polaris.` in the model body (a mechanical catalog-binding swap, not a logic change). The `sources.yml`
> consolidation only handles the ~10 `source()`-referenced tables; the literals are the bulk of the catalog coupling.

1. **`dbt_polaris/`** — copy `dbt/` and change the profile catalog to `polaris`:
   ```bash
   cp -r dbt dbt_polaris
   # in dbt_polaris/profiles.yml: database: polaris   (was iceberg)
   ```
   Add a **`dbt_polaris` compose service reusing `infra/Dockerfile.dbt`** with `./dbt_polaris` mounted (§2a, §7).
   Keep schema names (`bronze`/`silver`/`gold`) — they don't collide across catalogs. `sources.yml` `database:`
   entries flip from `iceberg` to `polaris`.
2. **`ingestion_polaris/`** — copy `ingestion/` and point the catalog at Polaris REST (same image, new service):
   ```bash
   cp -r ingestion ingestion_polaris
   ```
   Add an **`ingestion_polaris` compose service reusing `infra/Dockerfile.ingestion`**, mounting `./ingestion_polaris`
   with Polaris env (§2a) — **no image or `requirements.txt` change** (VARIANT is handled as land-as-string → CAST
   in dbt, so PyIceberg needs nothing new). In `ingestion_polaris/common/catalog.py`: use the **Polaris REST**
   endpoint (`uri http://polaris:8181/api/catalog`, `warehouse ctv_poc`, OAuth2 `credential`/`scope`, own S3 keys via
   `fs`/`S3FileIO`) instead of Nessie's `/iceberg/`. `ctv_ingestion.py` lands `bronze.digital_raw_occurrence`
   VARIANT columns as **string**; `reference_sync.py` (hive) and `uc_reference_sync.py` (UC) just change the catalog
   they write to (no VARIANT in those — §0). **Don't touch the Nessie `ingestion/` files** — the clone keeps it frozen.
3. **`ddl/polaris/`** — **regenerate from the source Databricks table DDL (§0), NOT from `ddl/nessie/`** (which
   encodes the VARCHAR workaround). Target: **v3** (`WITH (format_version = 3)`) with real **`variant`** columns
   matching the source (§0 inventory), Spark→Trino mapped (STRING→VARCHAR, **keep `variant`**, INT→INTEGER,
   FLOAT→REAL, TIMESTAMP→`timestamp(6) with time zone`, CLUSTER BY→partition+sort). *(Optional debugging
   diagnostic only: a v2/VARCHAR copy of `ddl/nessie/` with the catalog swapped, to isolate catalog mechanics.)*
4. **`ddl/postgres/polaris/`** — copy `ddl/postgres/nessie/piece3/4/5*.sql`; rename the clone objects to
   `*_ctv_poc_pol` and create **separate** creative_id / occurrence_id sequences + id-block procs.

---

## 5. Implementation order (mirrors the Nessie daily runbook)

Build in the **same dependency order operators run daily** on Nessie
([`../nessie/ctv_daily_runbook.md`](../nessie/ctv_daily_runbook.md)). For **each** step: clone the Nessie code →
verify every table's column types against the Databricks `table_ddl` → create the table **v3 + `variant`** (variant
only where source has it, §0) → in the dbt model, `CAST(... AS variant)` on those columns → run on Polaris →
compare to Nessie (counts / ids / watermarks) and to **source UC** for VARIANT values. **Do not touch the model
logic** beyond the VARIANT column edits.

| # | Step | Nessie job tag / cmd | `dbt_polaris/` folder | Iceberg tables created (→ where VARIANT applies) |
| :-- | :-- | :-- | :-- | :-- |
| **1** | **Reference sync** | `ingestion.reference_sync` (hive 14) + `uc_reference_sync` (UC 6) | — (Python + Trino view `ddl/polaris/01`) | reference/lookup schemas — **no VARIANT** (§0); v2 (see below); + view `km_preparation_gold_db.media_property_flatten_vx0_vw` |
| **2** | **Ingestion** (land + staging→raw) | `ctv_ingestion` + `tag:BIS_CTV_BZ2FILE_TO_RAW_OCC` | **`occurrences/`** | `bronze.digtial_raw_occurrence_ctv_staging` (`json_data` = raw text, **VARCHAR**) → `bronze.digital_raw_occurrence` (**variant:** `daisy_chain`, `raw_json`) |
| **3** | **Creative push + first-seen/occ summary** (Piece 3 A+B) | `tag:RAW_OCCS_TO_CREATIVE_STAGING`, `tag:CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY` | **`creatives/`** | Postgres `tempwork.*_ctv_poc_pol` clones + `bronze.creative_unique_urls`, `bronze.missing_digital_occurrence_for_summary` — **verify VARIANT per each table's DDL** |
| **4** | **Seed production data** (Piece 4a) | `CALL tempwork.sp_seed_creative_clones_ctv_poc_pol('ALL')` | — (Postgres proc) | Postgres-only (seeding proc); no Iceberg tables. Creative payloads live in Postgres as JSON/text here |
| **5** | **Creative sync-back** (Piece 4b) | `tag:SYNC_CREATIVES_TO_ICEBERG` | **`creatives/`** | `gold.creative`, `gold.creative_first_seen`, `silver.creative_dedupe_map`, `gold.component_coding` — **VARIANT-heavy** (`creative_payload`, `machine_learning_payload`, `first_seen_metadata`, `attribute_response`, `secondary_products`, …); produced in dbt → `CAST AS variant` natively |
| **6** | **Raw → gold occurrence** (Piece 5) | `tag:DIGITAL_RAW_OCC_TO_GOLD_OCC` | **`occurrences/`** | `gold.digital_gold_occurrence`, `silver.digital_staging_occurrence` (+ spend update `avod_ctv_spend_update`) — **verify VARIANT per DDL** (`daisy_chain`, `provider_raw_json`, …) |

**Per-step exit criterion:** the step runs green on Polaris; counts / id ranges / watermarks match the Nessie
baseline on the same input; and every VARIANT column **round-trips** (write → read → extract a field) and matches
the source UC value. Only then move to the next step (they have real data dependencies — creatives must be pushed
(3) and synced (5) before the occurrence gate (6)).

**VARIANT write mechanics (all steps).** Ingestion lands VARIANT-origin columns as **string** (PyIceberg can't
write variant); the first dbt model does `CAST(JSON <text> AS variant)`. dbt-produced tables (Pieces 3–5) write
variant natively in-model. Reading: `col['key']` / `CAST`. This replaces the Nessie models' `json_format(...)`
/ VARCHAR conversions **only on the VARIANT columns** — every other line of the model is unchanged.

> **Optional diagnostic only.** If a step fails and you can't tell whether it's a catalog issue or a v3/VARIANT
> issue, temporarily create that one table as **v2/VARCHAR** (a copy of `ddl/nessie/`) and run the unmodified
> Nessie model against Polaris to isolate catalog mechanics — then switch it back to v3+variant. This is a
> debugging aid, not a required phase.

### Build progress

**✅ Step 1 — Reference sync (built 2026-09-01).**
- `ingestion_polaris/reference_sync.py` + `uc_reference_sync.py` — verbatim clones (imports repointed to
  `ingestion_polaris`; the catalog binding is Polaris via `common/catalog.py`). The shared `ref_sync_engine.py` is
  reused unchanged. Same `TABLE_MAP`s / Azure sources as Nessie.
- `ddl/polaris/views/media_property_flatten_vx0_vw.sql` — a **real Iceberg view** `polaris.km_preparation_gold_db.
  media_property_flatten_vx0_vw` (Polaris supports views, so we dropped the Nessie ephemeral-model workaround — see
  §2 "Views"). Declared as a source in `sources.yml`; downstream models `source()` it. Run **after** the reference
  sync (its base tables must exist).
- **Reference/spend tables are created at Iceberg v2, not v3 — DECIDED (2026-09-01), the one exception to
  v3-everywhere.** PyIceberg 0.11.1 **cannot write v3 at all** (proven: the guard is on
  `TableMetadataV3.model_dump_json` → `NotImplementedError` on *any* commit that serializes v3 metadata, so it
  blocks create **and** the daily delete+append reload — even "Trino pre-creates v3, PyIceberg appends" fails).
  Making these v3 would require moving the sync's writer off PyIceberg to Trino. **We are not doing that**, because:
  (1) these tables have **no VARIANT** so v3 buys nothing here, and (2) **in production this data is not synced at
  all — reference and creative data will be read directly from Databricks via Trino** (the sync is a PoC
  convenience only). So the reference sync stays the verbatim v2 clone. Data-type parity is still honored (engine
  maps Delta→Iceberg types: binary→base64 string, TIMESTAMP→timestamptz-UTC, as in Nessie). **v3-everywhere still
  holds for every PIPELINE table we build (occurrence + creative), which are Trino/dbt-written.**
- **Run (on the VM):**
  ```bash
  docker exec -i trino trino -f /dev/stdin < ddl/polaris/00_create_schemas.sql   # once: create polaris.* schemas
  docker compose up -d ingestion_polaris                                          # build/start the service
  docker compose exec ingestion_polaris python -m ingestion_polaris.reference_sync      # 14 hive tables
  docker compose exec ingestion_polaris python -m ingestion_polaris.uc_reference_sync   # 6 UC tables (needs UC_* env)
  docker exec -i trino trino -f /dev/stdin < ddl/polaris/views/media_property_flatten_vx0_vw.sql  # after sync
  # verify: docker exec -i trino trino --execute "SELECT count(*) FROM polaris.km_preparation_db.data_provider"
  #         docker exec -i trino trino --execute "SELECT count(*) FROM polaris.km_preparation_gold_db.media_property_flatten_vx0_vw"
  #         docker exec -i trino trino --execute "SELECT table_schema, table_name FROM polaris.information_schema.tables ORDER BY 1,2"
  ```
  > **Env reload gotcha.** `docker compose exec` runs in the *already-running* container — it does **not** re-read
  > `.env`. After changing `.env` (e.g. `POLARIS_OAUTH2_CREDENTIAL`), run `docker compose up -d --force-recreate
  > <service>` (or `restart`) so the container picks it up. (Symptom seen: PyIceberg OAuth 500 "Failed to retrieve
  > principal secrets" while Trino worked — Trino had been restarted, the ingestion container had not.)

**✅ Step 2 — Ingestion: land → staging → raw (built 2026-09-01).**
- `ingestion_polaris/ctv_ingestion.py` — verbatim clone (imports repointed). Lands `.bz2`/JSON from S3 into
  `bronze.digtial_raw_occurrence_ctv_staging` via PyIceberg (append).
- `dbt_polaris/models/occurrences/digital_raw_occurrence.sql` — **single incremental model** (the "initial way"),
  logic identical to the Nessie staging→raw model, with the variant `CAST`s inline (`daisy_chain` =
  `cast(json_extract(j,'$.occurrence.unifiedChain') as variant)`, `raw_json` = `cast(json_parse(raw_json_text) as
  variant)`), `properties={format_version:'3', partitioning:[capture_month]}` and **no sorted_by**. Only other
  change vs. Nessie: `purchase_method_id` `SMALLINT→INTEGER` (§0). Watermark + idempotency guard unchanged.
  *(An earlier VARCHAR-staging + run-operation-promote workaround was removed — it was only needed because of the
  sorted_by red herring; a plain model works.)*
- DDL: `ddl/polaris/01_..._ctv_staging.sql` (**v2** — PyIceberg appends here), `02_bronze_digital_raw_occurrence.sql`
  (**v3 + variant** on `daisy_chain`/`raw_json`, `purchase_method_id` INTEGER, types matched to source `bronze.py`),
  `03_silver_watermark_control.sql` (**v3**, partitioned by `watermark_name`) + seeds the ingestion watermark.
- **Staging is the second PyIceberg-written v2 exception** (like reference sync): PyIceberg can't write v3, and the
  staging landing has no VARIANT (json_data is raw text). The v3+variant lands one hop later, in the dbt-written
  `bronze.digital_raw_occurrence`.
- **Separate S3 landing prefix — `landing_polaris/` (not `landing/`).** The landing step *archives* (moves) each
  processed file, so Polaris and Nessie must not share a prefix or they'd steal each other's files. `config.py`
  defaults to `s3://<bucket>/landing_polaris/ctv/{ingestion,archive}`. Create the folders once, and upload the day's
  `.bz2` to the Polaris ingestion prefix (pass `-Prefix` to the shared upload script):
  ```bash
  # one-time: create the Polaris landing folders in S3 (folder markers)
  aws s3api put-object --bucket dataplatformpoc-venketa --key landing_polaris/ctv/ingestion/
  aws s3api put-object --bucket dataplatformpoc-venketa --key landing_polaris/ctv/archive/
  ```
  ```powershell
  # on your local Windows machine — upload the day's *.bz2 to the POLARIS prefix:
  powershell -File scripts\upload_ctv_sample.ps1 -Prefix "landing_polaris/ctv/ingestion"
  ```
- **Run (on the VM):**
  ```bash
  # one-time DDL (structural tables):
  for f in ddl/polaris/0[1-3]_*.sql; do echo "== $f =="; docker exec -i trino trino -f /dev/stdin < "$f"; done
  # land the day's files (upload to landing_polaris first, see above), then staging->raw (single model):
  docker compose exec ingestion_polaris python -m ingestion_polaris.ctv_ingestion
  docker compose exec dbt_polaris dbt run --select digital_raw_occurrence        # or tag:BIS_CTV_BZ2FILE_TO_RAW_OCC
  # verify:
  docker exec -i trino trino --execute "SELECT count(*) FROM polaris.bronze.digtial_raw_occurrence_ctv_staging"
  docker exec -i trino trino --execute "SELECT count(*), min(capture_month), max(capture_month) FROM polaris.bronze.digital_raw_occurrence"
  # confirm VARIANT round-trips: read a variant with SUBSCRIPT + CAST (NOT json_query — that errors
  # "Cannot read input of type variant as JSON"). Feature-test syntax: col['key'] then CAST, or CAST(col AS json).
  docker exec -i trino trino --execute "SELECT provider_occurrence_id, CAST(raw_json['occurrence']['id'] AS varchar) AS occ_id, CAST(daisy_chain AS json) AS daisy_chain_json FROM polaris.bronze.digital_raw_occurrence WHERE daisy_chain IS NOT NULL LIMIT 3"
  ```
  > **✅ Step 2 validated (2026-09-04): 811,764 raw rows — exact Nessie parity — with `daisy_chain`/`raw_json` as
  > real `variant`.** The v3+VARIANT approach is proven end-to-end; the two rules below make Steps 3–6 mechanical.
  > Read variants with `col['key']` + `CAST` (or `CAST(col AS json)`), never `json_query`.

> ## ⚠️ VARIANT on Polaris — the ONE rule that matters (settled the hard way in Step 2)
>
> **RULE — a table with a `variant` column MUST NOT use `sorted_by`.** Trino's sort-on-write serializes **every**
> column (including the variant) through its legacy Hive-type mapping, which doesn't know `variant` →
> `NOT_SUPPORTED "Unsupported Hive type: variant"` on **any** write (INSERT, CTAS, autocommit or not).
> **Proven:** the identical `INSERT … SELECT … CAST(… AS variant)` of 811,764 rows **succeeds** into a
> `partitioning`-only v3 table and **fails** the instant `sorted_by` is present (even sorting by a non-variant
> column). `partitioning` is completely fine. → **Drop `sorted_by` from every table that has a variant column.**
> The legacy Databricks CLUSTER BY becomes partitioning-only (perf-only loss; correctness/pruning intact).
>
> > **This is a TRINO limitation, not a Polaris one.** Polaris (the REST catalog) only stores metadata and serves
> > v3+variant fine — the sort-on-write happens inside Trino's Iceberg connector before anything reaches the catalog.
> > So it's **not catalog-specific** (Lakekeeper/Glue via the same Trino would hit it too) and it's
> > **version-dependent** — a newer Trino that handles `variant` in the sorted-write path would let `sorted_by` return.
>
> **A normal single dbt model writes `variant` fine** — once `sorted_by` is gone. Proven: an incremental model
> with `properties={format_version:'3', partitioning:[...]}` and `cast(... as variant)` builds on both the first
> (CTAS) and subsequent (`__dbt_tmp`) runs. dbt **does** apply `format_version=3` to its `__dbt_tmp` intermediate,
> so the intermediate is v3 and holds variant. The earlier "dbt can't stage variant / needs a VARCHAR staging +
> run-operation promote" theory was **wrong** — every one of those failures was actually the `sorted_by` on the
> target. So: keep the model as a plain incremental model with the variant `CAST`s inline; just **omit `sorted_by`**
> on any table that has a variant column.
>
> **Step 2 reference impl:** `models/occurrences/digital_raw_occurrence.sql` (single incremental model, variant
> casts inline, `format_version=3` + `partitioning` only) → `bronze.digital_raw_occurrence` (pre-created by
> `ddl/polaris/02`, **no sorted_by**). Reads variants with `col['key']` + `CAST` (or `CAST(col AS json)`), never
> `json_query`. Same shape reused for the creative/gold variant models in Steps 5–6.

---

## 7. Running the Polaris dbt project

Use the dedicated **`dbt_polaris`** compose service (added in §2a) — its own image mount + `database: polaris`
profile, runs side-by-side with the Nessie `dbt` service:
```bash
docker compose exec dbt_polaris dbt run --select <model|tag>     # e.g. tag:BIS_CTV_BZ2FILE_TO_RAW_OCC
docker compose run --rm dbt_polaris dbt run --select <tag>       # for the parallel-branch jobs (Pieces 4/5, ≥2 threads)
docker compose exec dbt_polaris dbt ls --select tag:<JOB> --output name    # membership check
```

---

## 8. Gotchas to watch (from the catalog PoC)

- **PyIceberg VARIANT writes (ingestion + reference sync) — the §0 make-or-break.** ⚠️ **Confirmed (Aug 2026):
  no released PyIceberg supports VARIANT writes** — `VariantType` is still open under the V3 tracking issue
  ([#1819](https://github.com/apache/iceberg-python/issues/1819)), pending PyArrow/Parquet support; 0.11.0 (Feb
  2026) shipped ORC read + REST improvements, no VARIANT. So **bumping PyIceberg won't fix this — no version has
  it.** The write path is therefore **land VARIANT columns as string/JSON via PyIceberg (works today) → `CAST(... AS
  variant)` in the first dbt model** (Trino 483 + dbt *can* write VARIANT). Consequence: `ingestion_polaris` keeps
  the **same libraries and image** as Nessie (it writes strings, same as now) and the VARIANT materialization lives
  in dbt/Trino. This applies **only to `bronze.digital_raw_occurrence`** (the VARIANT ingestion lands); creative
  VARIANT is produced in dbt directly. Validate the occurrence round-trip **before** building all pieces.
- **Reference/spend sync — no VARIANT, nothing to change.** Verified against both sync `TABLE_MAP`s (§0): none of
  the synced `reference.*` / `spend.digital_dmi_prelim_*` / `tempwork.*` tables use VARIANT (the `source_json
  VARIANT` in `spend.py` is on out-of-scope `tv_rates_*`, not synced; hive Delta can't hold VARIANT anyway). Leave
  the shared-engine reference sync behavior **as-is**.
- **MERGE / UPDATE / DELETE on v3 tables (Pieces 4 & 5).** These pieces lean on `MERGE` heavily and Trino's v3 is
  still "experimental" — validate row-level DML on v3 Polaris tables **early** (it passed in the feature test, but
  confirm at pipeline scale). Note VARIANT columns sit alongside the MERGE keys — confirm MERGE works on v3+VARIANT
  tables, not just v3.
- **Views on Polaris (decided).** Nessie couldn't hold Iceberg views, so view-shaped reference objects were faked as
  **ephemeral dbt models** and dbt ran with `views_enabled: false`. **Polaris supports Iceberg views**, so
  view-shaped objects are now **real views created via `ddl/polaris/*` Trino DDL** and consumed as `source()`s (see
  §2 "Views"; first instance: `media_property_flatten_vx0_vw`). The dbt-project `+views_enabled: false` is a
  *separate* dbt-trino setting (it controls dbt's own temp relations, not these standalone views) — kept `false` for
  now to mirror the validated Nessie behavior; it can likely be relaxed on Polaris, but verify incremental models
  still land as **tables** before flipping it.
- **Trino `unique-table-location`.** New Polaris tables get a UUID suffix on their S3 path (Trino connector
  default) — cosmetic; documented in `polaris.properties`. Uncomment `iceberg.unique-table-location=false` if you
  want clean paths.
- **DROP on Polaris** needs `DROP_WITH_PURGE_ENABLED` (already set) to purge S3 files on drop.
- **Postgres id sequences.** Use the Polaris run's **own** sequences (per §2) or ids interleave with the Nessie run.

---

## 9. Cutover & consolidation

The clone is a **temporary** migration artifact. Once Polaris passes the §5 build (all 6 steps green + VARIANT
verified) and the AWS build is ready:
- Polaris becomes the pipeline of record; retire the Nessie `dbt/` / `ingestion/` / `ddl/nessie/` copy (or archive
  it), and the `_polaris` suffixes become the norm.
- Provision an **IAM role** to switch Polaris from own-keys to real **credential vending** (§ catalog runbook).
- Move the catalog metadata **Postgres → RDS** for the AWS deployment.
- The Databricks cross-cloud read (first/last-seen) uses the validated **PyIceberg-on-UC** path — see
  [`../../crosscloud/crosscloud_read_databricks_design.md`](../../crosscloud/crosscloud_read_databricks_design.md).

---

## 10. Open decisions (fill during the build)
- ~~**VARIANT write path**~~ **DECIDED:** occurrence VARIANT lands as **string** → `CAST` to `variant` in the
  first dbt model (no released PyIceberg writes VARIANT). Reference/spend sync = no VARIANT. Creative VARIANT is
  produced in dbt. Still to do: **validate the occurrence land-as-string → CAST round-trip on one table first.**
- ~~**Container split**~~ **DECIDED (§2a):** `dbt_polaris` and `ingestion_polaris` are separate compose services
  that **reuse the Nessie images** (no `requirements.txt` change). Split off a separate ingestion image only if a
  future write path forces a library change.
- Build cadence: all 6 steps in one pass, or stop-and-verify after each (recommended — they're dependency-ordered).
- Postgres: dedicated sequences vs an offset block for the Polaris run's ids.
- Whether to stand up `ingestion_polaris` reference sync for **all** synced tables or just the ones the pieces read.
