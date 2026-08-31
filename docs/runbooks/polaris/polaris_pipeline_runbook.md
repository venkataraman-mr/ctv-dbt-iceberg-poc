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
  - **Phase 2 — v3 + VARIANT (the business requirement).** Recreate the tables as **v3** with real **`variant`**
    columns; retire the VARCHAR workaround in the models. Now the comparison shifts to "does VARIANT round-trip."

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
   # in dbt_polaris/profiles.yml: catalog: polaris   (was iceberg)
   # add a dbt/ingestion service or reuse the dbt container with DBT_PROJECT_DIR=/dbt_polaris (see §7)
   ```
   Keep schema names (`bronze`/`silver`/`gold`) — they don't collide across catalogs. `sources.yml` `database:`
   entries flip from `iceberg` to `polaris`.
2. **`ingestion_polaris/`** — copy `ingestion/` and point the catalog at Polaris REST:
   ```bash
   cp -r ingestion ingestion_polaris
   ```
   In `ingestion_polaris/common/catalog.py`: use the **Polaris REST** endpoint (`uri
   http://polaris:8181/api/catalog`, `warehouse ctv_poc`, OAuth2 `credential`/`scope`, own S3 keys via
   `fs`/`S3FileIO`) instead of Nessie's `/iceberg/`. Reference sync (`reference_sync.py` hive,
   `uc_reference_sync.py` UC) and CTV landing (`ctv_ingestion.py`) all just change the catalog they write to.
3. **`ddl/polaris/`** — copy `ddl/nessie/00–09*.sql`; change `iceberg.` → `polaris.` in the CREATEs. **Phase 1:**
   keep v2 + VARCHAR (straight copy with the catalog swapped). **Phase 2:** add `WITH (format_version = 3)` and
   change the workaround columns to real `variant`.
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

- **MERGE / UPDATE / DELETE on v3 tables (Pieces 4 & 5).** These pieces lean on `MERGE` heavily and Trino's v3 is
  still "experimental" — validate row-level DML on v3 Polaris tables **early** (it passed in the feature test, but
  confirm at pipeline scale).
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
- dbt clone: reuse the `dbt` container vs a dedicated `dbt_polaris` service (§7).
- Phase-1 scope: full 20-table reference sync + all 5 pieces, or a representative subset first for speed.
- Postgres: dedicated sequences vs an offset block for the Polaris run's ids.
- Whether to also stand up `ingestion_polaris` reference sync for **all** 20 tables or just the ones the pieces read.
