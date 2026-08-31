# External Trino → Databricks UC-managed Iceberg: write & CDC capability reference

**Scope.** This document steps *outside* the CTV PoC scope. The CTV flow only needs to **read** Databricks
reference/creative data and, in one case (`creative_unique_urls`), **append** to it — so read + INSERT is
enough there. But future media workflows may need to **UPDATE / DELETE / MERGE** data in, or **consume change
data (CDC)** from, UC-managed Iceberg tables via our external Trino/dbt engine. This is a full, **hands-on
validated** exploration of what an external Trino engine can and cannot do against Databricks Unity Catalog
**managed Iceberg** tables (Trino 483), why, and what the options are. It is a decision reference for per-media
cutovers, not a statement of the current CTV design.

**Status: testing complete.** The capability matrix below was validated end-to-end against real UC-managed
tables on **both Iceberg v2 and v3**, using our scratch `unity_catalog` catalog → UC Iceberg REST Catalog
(IRC) endpoint, OAuth2 client-credentials auth, and an explicit Azure storage-account key for ADLS (see §5).
Test tables in `mrdpp_prod.tempwork` (`trino_interop_test` v2, `trino_interop_v3` v3), data on
`abfss://dbwcontainer@vxxdbwcommonpesteu2.dfs.core.windows.net/…/__unitystorage/…`.

**Headline:** Trino is an excellent **read + append + append-CDC** engine over UC-managed Iceberg. It is **not**
a row-level mutation engine and **not** a full-CDC (update/delete) reader for these tables — and **the Iceberg
format version (v2 vs v3) changes neither of those limits.**

---

## 1. First, don't confuse the two external-write mechanisms

Databricks exposes **two different** paths for writing UC-managed tables from outside Databricks. They are
easy to conflate because both are "external writes coordinated by Unity Catalog," but they use different
substrates and different engines:

**(a) Iceberg REST Catalog (IRC)** — the path *we* use. Any Iceberg-REST-compatible engine (Trino,
Spark-with-Iceberg, Flink, Dremio) talks the Iceberg REST protocol to UC's
`/api/2.1/unity-catalog/iceberg-rest` endpoint. UC returns table metadata and (meant to) vends short-lived
storage credentials; the engine reads/writes the Parquet + Iceberg metadata files directly in ADLS/S3. This
is the mechanism behind `unity_catalog.*` in our Trino.

**(b) Catalog Commits + Delta Kernel** — a *separate* Beta covering managed **Delta** tables (not Iceberg).
Spark, DuckDB, Flink, Starburst, and StreamNative write managed Delta tables via the Delta Kernel library,
with UC serializing every commit. This is what the widely-shared Databricks "DML from external engines" blog
demonstrates — and it is **not** the Trino-Iceberg path. If someone cites that blog as proof Trino can MERGE
into UC Iceberg, they're mixing the two: that blog is Delta + Spark/DuckDB/Flink, not Iceberg + Trino.

Both share the important governance property: **UC serializes commits across engines**, so Databricks and an
external engine can write the same table without corrupting the log — for the operations the external engine
can actually perform (for Trino-Iceberg that's read + append, per §2).

---

## 2. Capability matrix — Trino 483 against UC-managed Iceberg (validated v2 **and** v3)

| Operation | v2 | v3 | Notes |
| :-- | :--: | :--: | :-- |
| `SELECT` (read) | ✅ | ✅ | Reads metadata via IRC, data files via ADLS. |
| `INSERT` / append | ✅ | ✅ | Writes new data files + an appended snapshot; UC serializes the commit. Confirmed on both. |
| `UPDATE` | ❌ | ❌ | `NoClassDefFoundError: org/apache/hadoop/fs/Path` on the merge-on-read delete-file write path. **v3 does not fix it.** See §3. |
| `DELETE` (row-level) | ❌ | ❌ | Same delete-writer path as UPDATE. Confirmed failing on v3. |
| `MERGE` | ❌ | ❌ | Same path. |
| `CREATE TABLE` / CTAS from Trino | ⚠️ | ⚠️ | Trino `CREATE TABLE` returned **"Failed to create transaction"**; tables were created on Databricks. Likely a UC-side managed-storage / grant / preview limitation. Not re-cleared. |
| `table_changes` — **append** snapshots | ✅ | ✅ | Trino reads inserts as change rows. Works on both. See §4. |
| `table_changes` — **delete / overwrite / DV** snapshots | ❌ | ❌ | "Table uses features which are not yet supported by the table_changes function." **v3 + row tracking does not fix it.** See §4. |
| `ALTER TABLE` (schema evolution) | ⚠️ | ⚠️ | Untested against a UC-managed table. |
| Maintenance (`optimize`, `expire_snapshots`, …) | — | — | UC's job. Predictive Optimization runs `OPTIMIZE` itself (observed in history); don't drive it from Trino. |

The clean line, on **every** format version tested: **Trino can read and append; it cannot mutate; and it can
follow inserts but not updates/deletes in CDC.**

---

## 2b. Table creation & placement — no `LOCATION` for UC Iceberg tables

You **cannot point a UC Iceberg table at a specific storage path.** Databricks does not support the `LOCATION`
clause for Iceberg tables in Unity Catalog — an Iceberg table must be either a **managed** table (UC picks the
path) or a **foreign** table read through catalog federation (read-only). `CREATE … USING ICEBERG LOCATION
'…'` errors ("path-based external Iceberg tables aren't supported in UC").

**Why:** an Iceberg table's current state is a **metadata pointer that changes on every commit**, so a fixed
storage path can't reliably identify "the table" — unlike Delta, whose `_delta_log` lives at a stable
location. This is why existing Databricks DDLs that use `LOCATION` are **Delta / external** tables; that
pattern does **not** port to Iceberg. Migrating those tables to UC-managed Iceberg means dropping `LOCATION`
and using `CREATE TABLE … USING ICEBERG` (no path).

**Controlling placement (the supported lever):** you can't set a per-table `LOCATION`, but you *can* set a
**managed storage location at the catalog or schema level**, so every managed table in that catalog/schema is
written under a storage account/container you designate. UC still appends its own
`…/schemas/<guid>/tables/<guid>` layout underneath (exactly the `data_location` shape we observed on the test
table: `…/__unitystorage/schemas/2f063d0f…/tables/…`).

**This constraint actually protects the interop:** managed Iceberg (no `LOCATION`) is the **only** externally
writable path — it's what gets read/write via the IRC plus credential vending. A path/foreign Iceberg table is
**read-only** from outside Databricks *and* gets **no credential vending** (the external client must supply its
own storage creds). So letting UC own the path is what preserves the Trino read + append capability in §2;
going location-based would forfeit both.

**Migration guidance (per-media cutover):** rewrite `LOCATION`-based Databricks DDLs as managed
`CREATE TABLE … USING ICEBERG` (no path); use a schema/catalog managed location if you need to steer the
storage account. If you have existing Iceberg data already sitting at a path that Databricks should see, that's
foreign-catalog federation (read-only) or a `CTAS` into a managed table when Databricks/external writes are
needed.

---

## 3. Why UPDATE / DELETE / MERGE fails (and why v3 doesn't help)

Row-level mutation on an Iceberg table is **merge-on-read**: instead of rewriting whole data files, the engine
writes a **delete representation** (v2: position-delete files; v3: deletion vectors) plus new rows. INSERT
never touches that delete-writer path — it only appends — which is exactly why INSERT succeeds and UPDATE
doesn't.

On UPDATE/DELETE, Trino 483 throws `java.lang.NoClassDefFoundError: org/apache/hadoop/fs/Path` — a
**class-loading failure** in the delete-file/manifest write path, an Iceberg-library code path reaching for a
Hadoop `Path` class not on Trino's Iceberg-plugin classpath for that operation. It is in the same family as
long-standing Trino issues where writing Iceberg **delete files** trips over missing Hadoop classes.

**Confirmed empirically: the format version is not the lever.** We re-ran the test on a true v3 table (row
tracking + deletion vectors on) and Trino UPDATE/DELETE failed identically. Changing v2→v3 changes the delete
*representation* (position deletes → deletion vectors) but not the write-path class-loading bug, so the failure
is unchanged. Contributing factors we did not fully isolate: `object_store_layout_enabled = true` on the
Databricks-authored tables, and the OSS Trino 483 plugin packaging (commercial Starburst carries a fuller
dependency set — §6).

---

## 4. CDC via `table_changes` — what Trino can and cannot follow

Trino's `table_changes` reads the change history of an Iceberg table between two snapshots. Validated
behaviour against UC-managed tables mutated by Databricks:

**Works: append (insert) snapshots.** Trino returns the added rows as `insert` change rows across the range.
Confirmed on v2 and v3.

**Fails: delete / overwrite / deletion-vector snapshots.** The moment the range includes a snapshot produced
by a Databricks `UPDATE`/`DELETE`/`MERGE` (which write deletion vectors on v3, position deletes on v2), Trino
throws *"Table uses features which are not yet supported by the table_changes function."* Enabling **row
tracking** on the v3 table did **not** change this — row tracking enriches Databricks' *own* CDF, but does not
give Trino 483 a reader for those snapshots.

**Also breaks the feed: background `OPTIMIZE`.** UC Predictive Optimization compacts the table on its own,
producing a rewrite/replace snapshot. That snapshot type is likewise unreadable by `table_changes`, so a Trino
CDC feed can break from routine UC housekeeping alone — even with no user updates.

**Representation difference (even where it works).** Trino and Databricks describe an update differently:

| Engine | How it reports an UPDATE of one row |
| :-- | :-- |
| Databricks Change Data Feed | `update_preimage` (old) + `update_postimage` (new) |
| Trino `table_changes` | `delete` (old) + `insert` (new), same commit (`_change_version_id` / `_change_ordinal`) |

Trino has only `insert` and `delete` change types — no update type — so an update surfaces as a delete+insert
pair you must recombine (group by commit id + business key). Same information, different shape.

**Range boundary difference (a gotcha, not a bug).** Databricks `table_changes(name, start, end)` is inclusive
of the range; Trino `table_changes(start_snapshot_id, end_snapshot_id)` is **start-exclusive** — you only see
changes *after* the start snapshot. To include the first insert, start from the snapshot before it.

**Net:** Trino-side CDC over UC-managed Iceberg is limited to **append-only** change capture, and even that is
fragile because UC compaction introduces snapshots Trino can't read. For update/delete CDC, use Databricks'
Change Data Feed (the reliable path) or another engine.

---

## 5. The cross-cloud credential constraint (applies to *every* Azure-backed media workflow)

Independent of DML/CDC: **Trino 483 implements IRC vended credentials for S3 only, not Azure ADLS**
(`trinodb/trino` #23238, open). With vending on, UC returns a per-table SAS token but Trino ignores the
`adls.sas-token.*` config and falls back to `DefaultAzureCredential`, which on our AWS EC2 VM has no
`AZURE_CLIENT_ID` and no Azure IMDS → the file read/write dies with "Error processing metadata for table."

Our workaround, which unblocked read + append: **disable vending and give Trino an explicit Azure
storage-account key** (`azure.auth-type=ACCESS_KEY`, `azure.access-key=${ENV:UC_AZURE_STORAGE_ACCOUNT_KEY}` —
the same account/key `uc_reference_sync` already uses). This works **only because** the managed tables happen
to live in the *same* storage account as our reference-sync key (`vxxdbwcommonpesteu2`); a managed table in a
different account would 403 until we supply that account's key or an Azure SP with `Storage Blob Data
Contributor` on it.

Implication: the clean "UC vends short-lived, per-table creds; no standing secrets" story is **not available on
open-source Trino for Azure today**. Any Azure-backed UC table accessed from Trino needs either a shared
account key or an Entra SP secret — a long-lived credential to manage — until #23238 lands. This is a
cross-cutting constraint, not a CTV-specific one.

---

## 6. The v3 / deletion-vector dimension (forward-looking)

Databricks has moved UC-managed Iceberg toward **Iceberg v3 GA** — deletion vectors, row lineage, VARIANT — and
v3 deletion vectors are the default acceleration path for updates/merges/deletes on new managed tables. New
Databricks-created managed tables increasingly won't be v2.

Meanwhile **open-source Trino is not v3-ready** as of 2026: deletion-vector read/write is partial and row
lineage is incomplete. Our hands-on results match this exactly — Trino **reads** v3 data fine and **appends**
fine, but cannot write DV-based mutations and cannot read DV-based change snapshots. So the trajectory is
unfavorable for OSS Trino as a writer/CDC-reader: as more managed tables become v3 and are mutated with
deletion vectors, more of the change history becomes opaque to Trino.

Note the fork: **Starburst Enterprise (476-e+) / Galaxy** — commercial, Trino-based — already advertise Iceberg
v3 including binary deletion vectors. That's the open-source-vs-proprietary tradeoff in sharp relief: the
capability exists in the commercial Trino distribution before the OSS one, at the cost of licensing and
lock-in.

---

## 7. Options to actually get UPDATE / DELETE / MERGE (or full CDC) from an external engine

Ranked, each with the open-source-vs-proprietary and cost/lock-in read the project cares about:

**Option A — Keep the writer-of-record in Databricks; Trino reads (and appends only).** Zero new
infrastructure, zero new failure mode. Row-level mutation of governed tables — and update/delete CDC via
Databricks CDF — stay in Databricks/Spark where they work; Trino consumes. This is the current CTV stance and
the safest default for any read-mostly media workflow. Downside: you can't retire Databricks compute for those
write/CDC paths.

**Option B — Use external Apache Spark (open source) for the write / full-CDC workloads, not Trino.**
Spark-with-Iceberg speaks the same IRC and can do full `MERGE`/`UPDATE`/`DELETE` into UC-managed Iceberg, and
the Delta-Kernel / catalog-commits path (§1b) is explicitly built and tested by Databricks for
Spark/DuckDB/Flink. If a media workflow genuinely needs external mutation or DV-aware CDC, an external Spark
job is the open-source answer that works today, at the cost of standing up and operating Spark alongside
Trino/dbt (splits the engine story: Trino for SQL transforms, Spark for mutation/CDC).

**Option C — Adopt Starburst Enterprise/Galaxy (commercial Trino).** Fuller Iceberg v3 + deletion-vector DML in
a Trino-compatible engine, so dbt-trino and existing SQL mostly carry over. Buys the capability now but
reintroduces a proprietary dependency and licensing cost — the exact lock-in the migration is trying to
reduce. Evaluate only if external Trino-side mutation/CDC is a hard, broad requirement.

**Option D — Wait on open-source Trino.** Track #23238 (Azure vending), the delete-writer Hadoop-class bug
(§3), and v3 / DV-CDC support. Costs nothing but time; delivers nothing until upstream ships.

---

## 8. Concurrency & governance (the part that already works)

When an external write path *does* work (read, append today; more later), the multi-writer story is sound:
**UC serializes commits across engines**, so Databricks and Trino writing the same managed table won't corrupt
the transaction log — UC checks schema and commit sequence before applying, every commit is attributed to its
engine (we saw `Kernel-4.4.0/Iceberg REST Catalog` for Trino writes vs `Databricks-Runtime/…` for Databricks),
and every operation is auditable. The governance gate is three things on the Databricks side: metastore
**external data access = enabled**, the principal granted **`EXTERNAL USE SCHEMA`** (plus
`USE CATALOG`/`USE SCHEMA` and `SELECT`/`MODIFY`), and **M2M OAuth** (service-principal client credentials) or
a PAT.

---

## 9. Decision guide per media workflow

- **Read-only, or read + occasional append** (the CTV shape): **Trino IRC works today.** Use an explicit Azure
  key/SP for storage (§5). No Databricks compute needed on the hot path.
- **Needs row-level UPDATE / DELETE / MERGE into UC-managed Iceberg:** do **not** make OSS Trino the writer —
  it fails on every format version. Route those writes to **Databricks** (Option A) or an **external Spark**
  job (Option B); evaluate Starburst (Option C) only if broad Trino-side mutation is unavoidable.
- **Needs to consume update/delete CDC from a UC-managed table:** OSS Trino can only follow **appends**, and UC
  compaction can break even that. Use **Databricks Change Data Feed**, or Spark, for full change capture.
- **Table is (or will be) Iceberg v3 / deletion-vector-backed:** assume OSS Trino write and DV-CDC are out
  (confirmed); reads and appends still work. Plan mutation/CDC on Databricks/Spark; re-test Trino when upstream
  v3 support matures.

The through-line: **Trino is an excellent open read/append engine over UC-managed Iceberg; it is not a
row-level mutation engine nor a full-CDC reader for those tables today, on any format version.** Design
cutovers so mutation and update/delete CDC stay where they work and Trino does what it's good at.

---

## 10. Open items (remaining after this round of testing)

1. Isolate the §3 root cause (capture the full delete-writer stack at DEBUG; test a UC-managed table **without**
   `object_store_layout_enabled` to see if that property alone is the trigger).
2. Retest Trino `CREATE TABLE` / CTAS after grants are confirmed (the "Failed to create transaction" case).
3. Prove Option B end-to-end: an external OSS **Spark** `MERGE` into a UC-managed Iceberg table via IRC, and
   Spark reading DV-based CDC.
4. Re-test the whole matrix when Trino ships **Azure vended credentials** (#23238) and fuller **v3 / DV** support.

*Resolved this round:* UPDATE/DELETE/MERGE fail on v2 **and** v3; `table_changes` reads appends but not
delete/overwrite/DV snapshots on v2 **and** v3; explicit Azure key unblocks read + append; row tracking does
not enable Trino DV-CDC.

---

### Sources
- Trino #23238 — Iceberg REST vended credentials for Azure (open): https://github.com/trinodb/trino/issues/23238
- Trino #19716 — Hadoop class not found on Iceberg delete files: https://github.com/trinodb/trino/issues/19716
- Databricks — Access Databricks tables from Apache Iceberg clients (IRC read/write): https://docs.databricks.com/aws/en/external-access/iceberg
- Databricks — UC managed tables (no LOCATION for Iceberg): https://docs.databricks.com/aws/en/tables/managed
- Databricks — Specify a managed storage location in Unity Catalog: https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/managed-storage
- Databricks Community — Create external table using Iceberg not working: https://community.databricks.com/t5/data-engineering/create-external-table-using-iceberg-not-working/td-p/140797
- Databricks — Use Apache Iceberg v3 features: https://docs.databricks.com/aws/en/iceberg/iceberg-v3
- Databricks — Row tracking: https://docs.databricks.com/aws/en/tables/features/row-tracking
- Databricks — Deletion vectors: https://docs.databricks.com/aws/en/tables/features/deletion-vectors
- Databricks blog — Advancing Apache Iceberg on Databricks (v3 GA): https://www.databricks.com/blog/unity-catalog-and-next-era-apache-icebergtm
- Databricks community — DML on UC-managed tables from external engines (Delta + Catalog Commits): https://community.databricks.com/t5/technical-blog/performing-dml-operations-on-unity-catalog-managed-tables-from/ba-p/156308
- Starburst — Iceberg v3 (deletion vectors in Starburst Enterprise/Galaxy): https://www.starburst.io/blog/iceberg-v3/
- Trino 483 — Iceberg connector docs: https://trino.io/docs/current/connector/iceberg.html
