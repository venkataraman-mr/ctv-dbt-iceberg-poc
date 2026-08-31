# Polaris pipeline runbook — parallel PoC (Nessie → Polaris)

**Status: DRAFT / plan.** The Polaris pipeline is **not built yet**; this runbook is the build + validation plan.
It stands the **whole CTV pipeline** (reference sync → ingestion → Pieces 1–5) up on **Apache Polaris**, running
**in parallel** to the working Nessie pipeline on the same VM, so we can prove parity before the AWS build.

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

**VARIANT columns to preserve — confirmed from the authoritative `table_ddl`:**
- **Occurrence (bronze/gold/silver/archive):** `json_data` (CTV staging), `daisy_chain`, `raw_json`,
  `provider_raw_json`, `live_events_tag`, `gpt_response`.
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

Because of this, **v3 + VARIANT is the target end-state, not a deferred Phase 2** — Phase 1 below (VARCHAR parity)
is only an *optional diagnostic* to isolate catalog mechanics; the deliverable is source-type parity.

---

## 1. Approach — parallel, side-by-side, two phases

- **Parallel clone.** New folders `dbt_polaris/`, `ingestion_polaris/`, `ddl/polaris/`, `ddl/postgres/polaris/`,
  all targeting the **`polaris`** Trino catalog. Nessie's `dbt/`, `ingestion/`, `ddl/nessie/`,
  `ddl/postgres/nessie/` stay as-is.
- **Side-by-side.** Both catalogs are live in one Trino (`iceberg` = Nessie, `polaris` = Polaris). Run each piece
  on Polaris, then compare against the Nessie run.
- **Two phases — do NOT combine:**
  - **Phase 1 — catalog swap (same schema).** Keep today's v2 tables and the `VARIANT → VARCHAR/JSON` workaround.
    Goal: prove every piece **runs on Polaris and matches Nessie**. Isolates catalog issues from schema changes.
  - **Phase 2 — v3 + VARIANT to full source parity (the requirement; see §0).** Tables are **v3** with real
    **`variant`** columns **matching the source Databricks schema** (§0 inventory); the VARCHAR workaround is
    retired. Comparison shifts to "does VARIANT round-trip and match the source UC values." **Per §0, this is the
    real target** — Phase 1 is just an optional catalog-mechanics smoke test.

Cloning is what makes Phase 2 easy: `dbt_polaris/` models can adopt VARIANT without touching the Nessie baseline.

---

## 2. Naming & isolation conventions (decide once, stick to it)

| Thing | Nessie (existing) | Polaris (new) |
| :-- | :-- | :-- |
| Trino catalog | `iceberg` | `polaris` |
| dbt project | `dbt/` (profile `catalog: iceberg`) | `dbt_polaris/` (profile `catalog: polaris`) |
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
   matrix. So the catalog can host what Phase 2 needs.

---

## 4. Build the clones (one-time)

> Keep the clones **thin** — copy, then change only the catalog binding (and, in Phase 2, the VARIANT columns).
> Don't diverge the transform logic; it must stay comparable to Nessie.

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
   FLOAT→REAL, TIMESTAMP→`timestamp(6) with time zone`, CLUSTER BY→partition+sort). *(Optional Phase-1 diagnostic
   only: a v2/VARCHAR copy of `ddl/nessie/` with the catalog swapped, to smoke-test catalog mechanics.)*
4. **`ddl/postgres/polaris/`** — copy `ddl/postgres/nessie/piece3/4/5*.sql`; rename the clone objects to
   `*_ctv_poc_pol` and create **separate** creative_id / occurrence_id sequences + id-block procs.

---

## 5. Phase 1 — catalog swap (prove parity on Polaris, same schema)

Run each piece on Polaris, then compare to the Nessie result. Create the Polaris schemas + tables first:

```bash
# create schemas + all persistent tables on Polaris (Phase 1 = v2, same as Nessie)
for f in ddl/polaris/0*.sql; do docker exec -i trino trino -f /dev/stdin < "$f"; done
docker exec -i trino trino --execute "SHOW TABLES FROM polaris.bronze"   # repeat silver/gold
# seed the Polaris watermarks (same rows as Nessie, in polaris.silver.watermark_control)
```

Then, piece by piece (mirror [`../nessie/ctv_daily_runbook.md`](../nessie/ctv_daily_runbook.md), swapping the
catalog):

| Piece | Nessie run | Polaris run | Parity check |
| :-- | :-- | :-- | :-- |
| Reference sync | `python -m ingestion.reference_sync` / `uc_reference_sync` | `python -m ingestion_polaris.reference_sync` / `uc_reference_sync` | 20 tables loaded; row counts per table match |
| Piece 1 ingestion | `ingestion.ctv_ingestion` + `dbt run --select tag:BIS_CTV_BZ2FILE_TO_RAW_OCC` | `ingestion_polaris.ctv_ingestion` + `dbt_polaris … tag:BIS_CTV_BZ2FILE_TO_RAW_OCC` | staging + raw row counts match; `creative_url_hash` identical |
| Piece 3 Job A | `tag:RAW_OCCS_TO_CREATIVE_STAGING` | same on `dbt_polaris` (writes to `tempwork.*_ctv_poc_pol`) | creatives staged count matches |
| Piece 3 Job B | `tag:CREATIVE_FIRST_SEEN_AND_OCCS_SUMMARY` | same | first-seen + occ-summary counts match |
| Piece 4 sync-back | `tag:SYNC_CREATIVES_TO_ICEBERG` | same (reads the Polaris-seeded clones) | per-task counts match; watermarks advance |
| Piece 5 gold occ | `tag:DIGITAL_RAW_OCC_TO_GOLD_OCC` | same | 811,764 raw → 746,245 gold + 65,519 held (or same deltas on the same input) |

**Parity is the exit criterion for Phase 1:** every piece runs green on Polaris and the row counts / id ranges /
watermark values match the Nessie baseline on the same input.

---

## 6. Phase 2 — v3 + VARIANT (the business requirement)

Only after Phase 1 parity. This is the **dbt VARIANT guardrail** (see the evaluation doc §6): wherever a source UC
table has a **VARIANT** column, the Polaris target must also be **VARIANT** — retire the `VARIANT → VARCHAR/JSON`
workaround.

1. **DDL:** recreate the Polaris tables as v3 (`WITH (format_version = 3)`) with real `variant` columns where the
   workaround used VARCHAR/JSON (e.g. `json_data`, `daisy_chain`, `raw_json`).
2. **dbt models (`dbt_polaris/`):** write VARIANT with `CAST(JSON '…' AS VARIANT)` (Trino) and read with
   `payload['key']` + `CAST`; drop the `json_format(...)`/`VARCHAR` conversions the Nessie models use.
3. **Re-run** the pieces and validate the VARIANT columns round-trip (write → read → extract fields), plus the
   same counts hold.

Comparison shifts here from "counts match Nessie" to "VARIANT stored and read correctly" (Nessie can't hold v3, so
there's no VARIANT baseline to diff against — validate against the **source UC** VARIANT values instead).

---

## 7. Running the Polaris dbt project

Two options for the dbt container:
- **Reuse the `dbt` service** pointing at the clone: `docker compose run --rm -w /dbt_polaris dbt dbt run …`
  (mount `./dbt_polaris` into the container, or add `- ./dbt_polaris:/dbt_polaris` to the `dbt` service volumes).
- **Add a `dbt_polaris` compose service** (a second dbt container with `DBT_PROJECT_DIR=/dbt_polaris`) if you want
  both projects runnable independently. Decide during the build; the reuse option is lighter.

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
- **dbt incremental materialization.** Nessie forced `views_enabled: false` (its native connector can't create
  views). **Polaris supports views**, so re-check the temp-relation behavior on `dbt_polaris` — you may be able to
  leave `views_enabled` default, but verify incremental models still land as tables.
- **Trino `unique-table-location`.** New Polaris tables get a UUID suffix on their S3 path (Trino connector
  default) — cosmetic; documented in `polaris.properties`. Uncomment `iceberg.unique-table-location=false` if you
  want clean paths.
- **DROP on Polaris** needs `DROP_WITH_PURGE_ENABLED` (already set) to purge S3 files on drop.
- **Postgres id sequences.** Use the Polaris run's **own** sequences (per §2) or ids interleave with the Nessie run.

---

## 9. Cutover & consolidation

The clone is a **temporary** migration artifact. Once Polaris passes Phase 1 + Phase 2 and the AWS build is ready:
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
- Phase-1 scope: all 5 pieces at once, or a representative subset first for speed.
- Postgres: dedicated sequences vs an offset block for the Polaris run's ids.
- Whether to stand up `ingestion_polaris` reference sync for **all** synced tables or just the ones the pieces read.
