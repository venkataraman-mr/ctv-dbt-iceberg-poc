# Productionization design — cross-cloud breaking-creative flow & last-seen update

**Status: design finalized (2026-08-19).** This captures the agreed productionization design for the CTV/AVOD
occurrence pipeline as it splits across two platforms — the new open-source stack on **AWS
(Trino · dbt · Iceberg · Airflow, on Kubernetes)** and **Azure Databricks (Unity Catalog managed Iceberg)** —
for two specific flows: the **breaking-creative** flow and the **last-seen/first-seen** creative update. It also
records the implementation and testing considerations to carry into the build. Engine-capability basis for the
design: `docs/uc_managed_iceberg_trino_write_capabilities.md` (validated: Trino can **read + append** UC-managed
Iceberg but **cannot** `UPDATE`/`DELETE`/`MERGE`; Databricks can do full DML).

---

## 1. What runs where (finalized split)

**Stays in Azure Databricks (Delta → converted to UC-managed Iceberg so Trino can read):**

- **Reference dimensions.** Read live from UC by the new stack via the Iceberg REST Catalog (no more Option-C
  S3 sync). Cross-cloud egress accepted; jobs don't full-scan reference per run.
- **Creative tables** (incl. `creative_unique_urls`) and the **SYNC Creatives** job.
- **Piece 4** creative work. The PoC's clone-seeding step is **not needed** in production — the sync targets the
  real production Postgres tables directly.

**Moves to the AWS new stack (Trino/dbt/Iceberg/Airflow/K8s):**

- **Occurrence flow, ingestion → gold** (Pieces 1 & 5) for CTV/AVOD.
- **Breaking-creative Job 1** (raw occurrences → `creative_unique_urls` inserts) for CTV/AVOD.

**Media scope:** the SYNC Creatives job reads gold occurrence for **three** media — AVOD/CTV, Digital, TV — but
only **AVOD/CTV** gold occurrence lives in Iceberg on AWS; Digital and TV remain on Databricks.

**Storage/auth note:** Trino → UC-managed Iceberg uses an explicit Azure storage-account key (Trino 483 has no
ADLS credential vending — `trinodb/trino` #23238); see the capability doc / `uc_iceberg_rest_access_request.md`.

---

## 2. Breaking-creative flow — split into two jobs

The single `RAW_OCCURRENCE_TO_CRTV_STAGING` job is split so each operation runs on the engine that can perform
it: **inserts on Trino** (the only op Trino can do on UC-managed Iceberg), **id-generation + updates on
Databricks** (the MERGE/UPDATE only it can do). `creative_unique_urls` becomes the cross-cloud hand-off table
and gains additional columns to carry the Postgres-push payload.

### Job 1 — platform-specific inserts (AWS: Trino/dbt cron on Airflow, for CTV/AVOD)

- Reads raw occurrences, identifies breaking creatives (new `url_hash`).
- **Insert-if-not-exists** into `creative_unique_urls`: anti-join the batch's `url_hash` against the current
  table, `INSERT` only the missing ones with **`stage = false`** and **no `creative_id`**. If the `url_hash`
  already exists, no action. (Append-only — no `MERGE` — which is what Trino can do to UC-managed Iceberg.)
- Different media can run their own Job 1 on their own platform; this instance is CTV/AVOD on the new stack.

### Job 2 — id generation + Postgres push + finalize (Azure Databricks, single global job)

Runs on Databricks (which can UPDATE/MERGE UC-managed Iceberg). Ordered for crash-safety:

1. Generate `creative_id` — **only for rows where `creative_id IS NULL`** (single shared sequence; single point
   of id generation across all media).
2. **Update `creative_unique_urls` with the `creative_id`** (stage still `false`).
3. **Push to `creative_staging` (Postgres)** — the proc is insert-if-no-conflict, so a re-push is a no-op.
4. **Update `creative_unique_urls` `stage = true`.**

Writing the id onto the row *before* the Postgres push is the key: a failure at/after the push re-drives with the
**same** id (step 1 skips non-null rows), and the idempotent Postgres proc prevents a duplicate creative. Net
effect: exactly one id per `url_hash`, no duplicate pushes, safe re-runs.

### Why this shape

- Trino does only appends; Databricks owns all id-generation and updates → matches validated engine limits.
- Single point of id generation and finalize (Job 2) keeps ids globally consistent across media.
- Per-media Job 1 lets each medium run breaking-creative ingestion on its own platform while sharing one
  finalize path.

### creative_id timing (decoupling)

`creative_id` is now assigned later (Job 2), so a breaking creative exists briefly as `stage=false` / no id.
This is absorbed by the existing **silver staging "waiting area"**: occurrences whose creative isn't yet
available (no id / not yet classified in UI/ML) park there and are picked up once ready. The AWS flow resolves
`creative_id` by reading `creative_unique_urls` from UC (cross-cloud, bounded to the waiting set).

---

## 3. Last-seen / first-seen update — cross-cloud, incremental

The SYNC Creatives job (Databricks) updates first-seen/last-seen on the creative table from gold occurrences of
all three media. For AVOD/CTV, gold occurrence now lives in **Iceberg on AWS**, so Databricks must read it
cross-cloud.

**Approach:** the Spark SYNC process maintains its own **watermark** and pulls only the **incremental changes**
from the AWS Iceberg gold-occurrence table per batch (never a full-table read), then MERGEs first-seen/last-seen
into the creative table — the same incremental pattern the current Databricks code already uses.

**Mechanism: timestamp-watermark filtered scan** — `WHERE updated_timestamp > :last_watermark` — **not** an
Iceberg incremental-snapshot / changelog read. Rationale:

- Gold occurrence is **MERGE-written** (last-seen updates are UPDATEs), producing overwrite/deletion-vector
  snapshots. An Iceberg incremental-append/changelog read chokes on those (the same limitation that blocks
  Trino's `table_changes`); a plain filtered `SELECT` is **immune** to snapshot type / delete files / DVs.
- The current incremental read is already timestamp-based, so the logic ports directly — only the *source*
  swaps from a Delta table to the AWS Iceberg table.

**Completeness guaranteed by two data guarantees (confirmed):** gold tables are **soft-delete only**
(`delete_flag = true`, never hard delete) and **`updated_timestamp` is always bumped on any change**. So the
timestamp filter captures inserts, updates, *and* deletes (a delete is a row whose `delete_flag` flipped and
whose timestamp moved). The downstream MERGE is idempotent on re-processed windows.

**Cross-cloud attach (validated in the catalog PoC):** a **non-UC Databricks cluster** with a manual Spark
Iceberg REST attach to the AWS new-stack catalog (**Polaris/Lakekeeper**, not Nessie) + AWS S3 creds. Unity
Catalog **Lakehouse Federation to a generic REST catalog is not available** (only Glue/HMS/Snowflake Horizon), so
the manual attach is the path — proven for full CRUD incl. v3+VARIANT on DBR 18 LTS
(`databricks/README_catalog_crosscloud.md`). Egress is bounded because the watermark filter returns only changed
rows.

---

## 4. Implementation & testing considerations (carry into the AWS K8s build)

Ordered by risk. These are the things to validate during development, not open design questions.

1. **Cross-engine concurrent writers on `creative_unique_urls` (the genuinely new risk).** Today it's one
   Databricks job; in production it's Trino appending (AWS) and Databricks MERGE-updating (Azure) the **same**
   UC table. UC serializes commits (no corruption), but commit **conflicts → retries** are possible, and Trino
   was observed *not* to retry serializable commit conflicts gracefully on Nessie (`max_commit_retry`
   ineffective). **Test Trino's IRC commit-conflict behavior** against a table Databricks just committed to, and
   default to **non-overlapping schedule windows** for Job 1 (Trino inserts) vs Job 2 (Databricks updates) per
   media so they don't contend.
2. **Uniqueness can't be enforced on the Iceberg table, and Trino can't MERGE to dedup.** Uniqueness rests on
   Job 1's non-atomic anti-join. Guard with: **single-writer Job 1 per media** (non-overlapping runs) **and**
   **Job 2 dedups on `url_hash` before assigning ids** (one id per distinct `url_hash`) — otherwise a duplicate
   append yields two ids, Postgres keeps one, and the other row carries a phantom id.
3. **Table growth & maintenance.** `creative_unique_urls` grows unbounded and is MERGE-updated every batch
   (accruing delete files / deletion vectors). Rely on UC **Predictive Optimization** for compaction and
   consider **partitioning** (e.g., by media or insert-date) so Job 1's anti-join stays cheap over time.
4. **Trino read-after-Databricks-update correctness.** Job 1's anti-join must see Databricks-updated rows (DV
   applied on read) so it doesn't re-insert an already-finalized `url_hash`. Plain `SELECT` applying deletion
   vectors is core read behavior (our v3 read test passed), but it's load-bearing here — verify explicitly on a
   DV-updated table.
5. **Job 2 idempotency invariant.** Generate an id **only when `creative_id IS NULL`**, and keep the Postgres
   proc insert-if-no-conflict, so partial-failure re-runs reuse the persisted id and never double-push.
6. **Last-seen watermark discipline.** Preserve the existing `>` vs `>=` boundary + small UTC lag so rows
   written mid-batch aren't missed or double-counted; the MERGE is idempotent on overlap. Confirm
   `updated_timestamp` is bumped on **every** change (incl. `delete_flag` flips) and validate Iceberg→Spark type
   mapping (timestamptz, any binary→base64) on first read.
7. **Cross-cloud reads.** (a) The AWS flow resolves `creative_id` by reading `creative_unique_urls` from UC
   cross-cloud (bounded to the waiting set). (b) `creative_id` stays a **single shared sequence** driven only by
   Job 2. (c) Storage auth for Trino→UC is the explicit Azure account key until Trino ships ADLS vending (#23238).

---

## 5. Deferred / to-confirm during development

- Exact **additional columns** on `creative_unique_urls` for the Postgres push, and whether the schema is
  media-generic (nullable per-media fields) so one shared table serves CTV/AVOD/Digital/TV.
- Whether `creative_unique_urls` dedup scope is **global** or **per-media** (drives whether the same `url_hash`
  can arrive from two platforms' Job 1s, and how Job 2 dedups).
- ~~The cross-cloud **attach method** for the SYNC job's AWS Iceberg read.~~ **Resolved:** non-UC DBR 18 manual
  Spark REST attach against the chosen Polaris/Lakekeeper catalog (UC federation to a generic REST catalog isn't
  available). Validated in the catalog PoC.
- Trino **IRC commit-conflict/retry** behavior (item 4.1) — benchmark early.

---

### Related docs

- `docs/uc_managed_iceberg_trino_write_capabilities.md` — engine-capability basis (read/append vs. mutate; CDC).
- `docs/uc_iceberg_rest_access_request.md` — UC access, endpoint, Azure-key config for the Trino→UC path.
- `docs/ctv_dbt_iceberg_poc.md` — project checkpoint (§8.11 UC interop, §8.8/8.9 CDC & deletes).
