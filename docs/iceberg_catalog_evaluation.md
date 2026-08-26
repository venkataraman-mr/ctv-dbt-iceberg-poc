# Iceberg catalog evaluation & decision — v3/VARIANT + cross-cloud Databricks access

**Status: PoC COMPLETE — both open-source candidates pass all hard requirements (2026-08-26).** Apache **Polaris
1.7.0** and **Lakekeeper 0.13.3** were stood up on the AWS VM and tested end-to-end. **Both satisfy every hard
requirement** — R1 (v3 + VARIANT over REST), R2 (external Trino/Spark **and Databricks** read/write), R3
(open-source). **Nessie is ruled out** (no v3 over REST). The remaining choice between Polaris and Lakekeeper is
now a soft/operational one (see §5). Stand-up steps: `docs/catalog_poc_runbook.md`; Databricks cross-cloud:
`databricks/README_catalog_crosscloud.md`.

**PoC results at a glance:**

| Hard requirement | Polaris 1.7.0 | Lakekeeper 0.13.3 | Nessie |
| :-- | :-- | :-- | :-- |
| R1 — v3 + VARIANT over REST | ✅ | ✅ | ❌ (v2 only) |
| R2 — Trino R/W | ✅ | ✅ | ✅ (v2) |
| R2 — Databricks CRUD incl. v3+VARIANT | ✅ | ✅ | ❌ |
| R3 — open-source | ✅ Apache | ✅ Apache-2.0 | ✅ |

Companion findings: `docs/uc_managed_iceberg_trino_write_capabilities.md` (engine capabilities),
`docs/crosscloud_read_databricks_design.md` (Databricks read path), the Trino/Databricks test scripts under
`databricks/` and `scripts/` (`catalog_feature_tests*.sql`, `*_crossengine_verify.sql`).

---

## 1. Requirements

**Hard — non-negotiable (business):**
- **R1 — v3 + VARIANT over REST.** Iceberg **v3** with the **VARIANT** type; JSON/semi-structured columns must
  stay VARIANT (no VARCHAR/string substitution). Served over the **Iceberg REST** protocol so all engines can use it.
- **R2 — External-engine read/write.** Trino/Spark and (phased) **Databricks** must read/write the tables over
  an open protocol.
- **R3 — Open-source catalog.** The catalog itself must be **open-source** (no managed/paid catalog lock-in).
  Data is already open Iceberg on S3; the *catalog* must be open too.

**Soft — relaxable:** Iceberg **views**, **branch/tag isolation**. (RBAC and operational overhead are
considerations, not gates.)

---

## 2. What we validated about Nessie (our current catalog)

| Capability | Nessie catalog type | Result / additional details |
| :-- | :-- | :-- |
| v3 + VARIANT | **REST** | ❌ **v2 works; v3 NOT supported yet.** `CREATE … format-version=3` silently makes **v2**; `ALTER … =3` → **500 "Implement format version update"**. VARIANT requires v3 → unavailable over REST. v3 is "still under active development" in Nessie; latest release doesn't add it. |
| v3 + VARIANT | **Native** | ⚠️ Trino can create/write/read/mutate v3+VARIANT via `/api/v2` (validated) — but **Trino-only, not cross-engine** (these tables aren't readable over REST). |
| External CRUD by other engines (e.g. Databricks) | **Native** | ❌ **Hard block.** The native Nessie API isn't consumable by external engines — Databricks can't read/write it (no federation); only our own Trino/PyIceberg use it. |
| External read/write by Iceberg REST clients | **REST** | ✅ v2 read/write works for REST clients. ❌ But **Databricks still can't federate to Nessie REST** (generic Iceberg REST catalogs aren't a supported UC foreign-catalog source; the fragile manual OSS attach can't parse VARIANT). |
| Views | **Native** | ❌ Not supported → pipeline runs `views_enabled=false`. |
| Views | **REST** | ✅ Supported (validated 2026-08-17). |
| Snapshots / time-travel / CDC | Both | ⚠️ Nessie exposes a **single Iceberg snapshot** by design (history lives in its git commit log) → no snapshot-based time-travel or `table_changes`; must use **timestamp watermarks**. |
| RBAC | Both | ❌ No table/schema-level grants like UC; rely on Trino-level RBAC (Ranger/OPA) + Nessie OAuth2. |
| Branch / tag isolation | Both | ✅✅ Git-like branching, tags, atomic multi-table commits — Nessie's signature strength, which the alternatives lack. |

**Two headline Nessie limitations:** (1) **Native** — external Databricks CRUD is a **hard block** (not reachable/operable by Databricks); (2) **REST** — **v2 works but v3 is not supported yet** (so no VARIANT).

**Net:** Nessie fails **both** hard requirements — no v3/VARIANT over REST (R1) and no Databricks access (R2).
Its unique value (branching) doesn't offset a mandatory-requirement miss.

---

## 3. Catalog landscape & comparison

Every Iceberg catalog worth evaluating, split by open-source (meets R3) vs managed/paid (fails R3, fallback
only). Legend: ✅ yes · ⚠️ partial / verify · ❌ no · **TEST** = must be validated empirically (v3+VARIANT
end-to-end is bleeding-edge and uneven across catalogs — see §4).

### 3a. Open-source catalogs (meet R3)

| Catalog | v3 + VARIANT over REST | External R/W (Trino/Spark) | Databricks access | Views | Branching | RBAC / cred vending | Hosting / notes |
| :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| **Apache Polaris** | ✅ **VALIDATED (1.7.0)** — v3 CREATE + VARIANT create/insert/read over REST | ✅ REST (Trino + Databricks) | ✅ **validated** — manual DBR-18 attach, full CRUD incl. v3+VARIANT | ✅ | ❌ | ✅ RBAC + cred vending | JVM service; self-host on K8s. Apache TLP (Feb 2026). Also core of Snowflake Open Catalog / Dremio. |
| **Lakekeeper** | ✅ **VALIDATED (0.13.3)** — v3 CREATE + VARIANT create/insert/read over REST | ✅ REST (Trino + Databricks) | ✅ **validated** — manual DBR-18 attach, full CRUD incl. v3+VARIANT | ✅ (+ stats) | ❌ | ✅ OpenFGA + OPA/Trino + cred vending (S3/GCS/ADLS+STS) | **Rust** single binary, ms startup, K8s helm/HA. Apache-2.0. Lean, authz-strong. |
| **Apache Gravitino** | ❌/**TEST** — variant create → **HTTP 406** on JDBC backend (Iceberg 1.10.1); end-to-end variant incomplete | ✅ REST | manual attach | ⚠️ | ❌ | ✅ | Federated metadata lake (unifies Hive/PG/Kafka/Iceberg/…). 1.2.0 (Mar 2026). |
| **Nessie — REST** | ❌ **v2 only; v3 not supported** (500 on format-version update) | ✅ REST (v2) | manual attach (v2) | ✅ | ✅✅ | ❌ (Trino RBAC/OPA + OAuth2) | Self-host. Our current catalog. |
| **Nessie — Native** (our PoC) | v3 **Trino-only** (not cross-engine) | ❌ external CRUD blocked | ❌ | ❌ | ✅✅ | ❌ | Self-host. |
| **Hive Metastore** | ❌ (legacy; no native REST, no v3) | ⚠️ via bridge | ⚠️ | ❌ | ❌ | ❌ | Legacy; not a fit. |
| **JDBC catalog / reference REST fixture** (`iceberg-rest-fixture`) | ❌/**TEST** — JDBC-backed; likely same variant gap as Gravitino | ✅ (dev) | manual attach | ⚠️ | ❌ | ❌ | **Dev/CI only, not production.** What our local MinIO PoC used. |

### 3b. Managed / paid catalogs (fail R3 — fallback only, needs business approval)

| Catalog | v3 + VARIANT | External R/W | Databricks access | Views | RBAC | Notes |
| :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| **AWS S3 Tables** | ✅ **v3 GA** | ✅ REST (Trino/Spark) | ✅ via Glue / SageMaker Lakehouse federation | ⚠️ verify | ✅ (LF/SageMaker) | AWS-native, managed/paid. **Strongest paid fallback** (data stays open Iceberg on S3). |
| **AWS Glue** | ❌ REST create v1/v2 only | ✅ REST | ✅ UC federates to Glue | ⚠️ | ✅ (Lake Formation) | Managed; fails v3. |
| **Unity Catalog** | ✅ v3 GA | ⚠️ Trino read+append only | ✅ native | ✅ | ✅✅ | Databricks/Azure-managed (OSS core exists); reverse cloud. |
| **Snowflake Open Catalog** | ✅ (managed **Polaris**) | ✅ REST | manual attach | ✅ | ✅ | Snowflake-hosted Polaris; paid. |
| **Google BigLake Metastore** | ⚠️ verify | ✅ REST | ⚠️ | ⚠️ | ✅ (IAM) | GCP-native — **wrong cloud** for an AWS shop. |
| **Dremio (Open) Catalog** | ✅ (Polaris core) | ✅ REST | manual attach | ✅ | ✅ | Managed Dremio + OSS Polaris core; paid tier. |

*(⚠️ = supported-but-verify or partial. "manual attach" = Databricks non-UC-cluster Spark REST attach — access
without UC governance; see §5.)*

### 3c. How each catalog stores its metadata (the pointer store)

Reminder — two layers of metadata: **Iceberg table metadata** (`metadata.json`, manifests) always lives in
**S3** next to the data and is self-describing; the **catalog metadata** below is the small *pointer store*
(table → current-metadata pointer, namespaces, and for feature-rich catalogs also views / roles / grants /
principals). This pointer store is the stateful component you operate and back up. Losing it does **not** lose
data (tables are re-registerable from their S3 metadata), but you'd lose the governance model.

| Catalog | Metadata-store backend | Durability / ops note |
| :-- | :-- | :-- |
| **Apache Polaris** | Relational **JDBC — Postgres** (persistent); in-memory H2/EclipseLink (dev) | In-memory is ephemeral → use Postgres for real; back it up (RDS in prod). No embedded/RocksDB option. |
| **Lakekeeper** | **Postgres ≥ 15 (required)** | No embedded option at all. Postgres on a durable volume / managed RDS. |
| **Apache Gravitino** | **JDBC — H2 / MySQL / Postgres** | Its Iceberg REST server uses a JDBC catalog backend. |
| **Nessie** | **Pluggable version store**: **RocksDB** (embedded), JDBC/Postgres, DynamoDB, MongoDB, Cassandra/BigTable, in-memory | Our PoC = **RocksDB on the VM's EBS** — single-node (SPOF) unless externalized to RDS/DynamoDB. |
| **Hive Metastore** | **RDBMS — Postgres / MySQL / Derby** | Classic HMS relational backend. |
| **JDBC catalog / reference REST fixture** | **SQLite / JDBC** (or in-memory) | Dev/CI only. |
| **AWS Glue** | **AWS-managed** (no DB you run) | Serverless; AWS operates it. |
| **AWS S3 Tables** | **AWS-managed** | Serverless; AWS operates it (auto-maintenance included). |
| **Unity Catalog** | **Databricks-managed** control plane (OSS UC core = RDBMS: Postgres/MySQL) | Managed in the Databricks product. |
| **Snowflake Open Catalog** | **Snowflake-managed** (Polaris under the hood) | Managed Polaris. |
| **Google BigLake Metastore** | **GCP-managed** | Serverless; GCP operates it. |
| **Dremio (Open) Catalog** | **Dremio-managed** (Polaris core) | Managed. |

Takeaway for our PoC: **Nessie** is the only one offering embedded RocksDB-on-EBS; **Polaris** and **Lakekeeper**
both want **Postgres** (Lakekeeper mandatory, Polaris for persistence). So the catalog PoC adds a small Postgres
(one container, a database each) on an EBS-backed volume — the durability analog of Nessie's RocksDB — moving to
managed RDS for production.

---

## 4. Analysis against the three hard requirements (R1 v3+VARIANT · R2 external read/write · R3 open-source)

**R3 (open-source) makes the managed catalogs fallbacks only** (S3 Tables, Unity Catalog, Glue, Snowflake Open
Catalog, BigLake, Dremio). Among the **open-source** options:

- **Nessie** — fails R1 (REST no v3) and the Databricks-CRUD side of R2. Out.
- **Hive Metastore / JDBC / reference REST fixture** — no v3 or dev-only. Out.
- **Apache Gravitino** — open & federated, but VARIANT create currently **fails (HTTP 406, JDBC backend)** →
  fails R1 today; re-test on a newer build.
- **Apache Polaris** and **Lakekeeper** — the two viable open-source REST catalogs: open (R3 ✅), external
  Trino/Spark read/write over REST (R2 ✅), v3 that "rides on the engine." **Both were tested and PASS R1**
  (v3+VARIANT over REST) — see the PoC results banner at the top and the runbook matrix.

**The critical caveat — resolved by the PoC.** The concern was that v3+VARIANT is bleeding-edge and end-to-end
support through a catalog backend was unproven (Gravitino's JDBC-backed REST server returns HTTP 406 on a variant
create; Nessie doesn't do v3 at all), so it had to be **tested empirically** rather than asserted from docs. It
was: **both Polaris 1.7.0 and Lakekeeper 0.13.3 create/insert/read v3+VARIANT over REST** (Trino and Databricks),
so the paid AWS S3 Tables fallback is **not needed**. (Trino 483 needs no special handling for VARIANT beyond the
`variant` type; both catalogs used Trino/Spark own S3 keys since no IAM role was provisioned for vending.)

Databricks caveat (resolved): no open-source catalog is UC-*federatable*, but Databricks *accesses* both catalogs
via the manual non-UC Spark attach (§5) — validated on **DBR 18 LTS**, full CRUD incl. v3+VARIANT. The earlier
"Databricks can't read v3+VARIANT" results were against **Nessie, which never served v3** — invalid for a real v3
catalog, and now superseded by the passing Polaris/Lakekeeper tests.

---

## 5. Recommendation

**Both open-source REST catalogs — Apache Polaris and Lakekeeper — passed the full hard-requirement gate in the
PoC.** Each does v3 CREATE + VARIANT create/insert/read over REST (R1), external Trino/Spark **and** Databricks
read/write (R2), and is open-source (R3); both also do row-level DML and Iceberg views (soft). The paid AWS S3
Tables fallback is **not** needed. Branching is given up (soft, relaxed — the pipeline uses a single `main`
branch). **The decision is now Polaris vs Lakekeeper on soft/operational grounds — recorded neutrally here
pending the team/business pick.**

**Operational differentiators observed (none block a hard requirement):**

| Dimension | Apache Polaris | Lakekeeper |
| :-- | :-- | :-- |
| Language / footprint | JVM/Quarkus service | **Rust** single binary, ms startup, lean |
| Metadata store | Postgres (JDBC) | Postgres ≥15 (required) |
| Storage auth (PoC, no IAM role) | `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` + own keys | remote signing (not consumed by Trino 483) → own keys |
| DROP cleanup | needs `DROP_WITH_PURGE_ENABLED` | background purge task |
| Cross-cloud networking | **relative paths — zero extra config** for internal Trino + external Databricks | advertises single `BASE_URI` → must be public for Databricks (Trino keeps internal) |
| AuthZ model | Polaris principals / roles / grants | OpenFGA / Cedar / OPA |
| Ecosystem | Apache TLP; core of Snowflake Open Catalog & Dremio (managed escape hatch) | focused OSS project, active feature set (stats, generic tables) |
| Table paths | clean | clean (Trino adds a UUID suffix via its own connector default — cosmetic, both catalogs) |

Quick read: **Polaris** = broader ecosystem + simplest cross-cloud networking + a managed escape hatch later;
**Lakekeeper** = leaner Rust/K8s footprint + strong pluggable authz. Re-checking **Gravitino** is optional (its
variant gap may have closed) but unnecessary now that two catalogs pass.

**Writer engine.** Trino/dbt handles SQL transforms and did v3/VARIANT writes fine in the PoC (Trino 483). For
production high-volume v3/VARIANT writes, **Spark (AWS EMR/K8s)** remains the safer choice (Trino's v3 is marked
experimental); reads from Trino/dbt are fine.

**Databricks read — validated (no longer an open item).** Databricks can't *UC-federate* to a generic REST
catalog, but the **manual non-UC Spark attach on DBR 18 LTS** does full CRUD incl. v3+VARIANT against **both**
Polaris and Lakekeeper (tested). The earlier "Databricks can't read v3+VARIANT" results were against Nessie
(never served v3) and are superseded. Remaining fallbacks if ever needed: external Spark (EMR/K8s, OSS Iceberg)
or a VARIANT-free projection handoff — neither is required today.

**The one trade to flag to leads:** *no* open-source catalog is natively UC-federatable — Databricks federation
only supports **managed** catalogs (Glue / Snowflake Horizon / UC). So "open-source catalog" and "native
Databricks UC federation" are mutually exclusive; since open-source is the hard requirement, Databricks accesses
the catalog via the manual Spark attach (validated) rather than UC governance.

---

## 6. dbt-model implication (design guardrail)

Once on a v3 catalog, the transforms must **preserve VARIANT end-to-end**: wherever a source Unity Catalog
table has a **VARIANT** column, the target Iceberg table must also be **VARIANT** (not VARCHAR/JSON). This
applies across every piece that touches such columns — **ingestion, breaking-creative, creative-sync (future),
and the gold occurrence flow**. Concretely: retire the current `VARIANT → VARCHAR/JSON` workaround; dbt models
declare `variant` columns and write with `CAST(JSON '…' AS VARIANT)` (Trino) / native VARIANT (Spark), and read
with `payload['key']` + `CAST`. This is only possible on a catalog that serves v3+VARIANT over REST (i.e., the
catalog decision gates the dbt change).

---

## 7. Evaluation & feature-test plan (open-source PoC)

Purpose: decide by **evidence**, not docs. Test each open-source candidate against the feature checklist; pick
the one that passes the hard requirements; if none do, escalate for the paid fallback. **Candidates: Apache
Polaris, Lakekeeper, and (re-check) Gravitino.** Use the reference REST fixture first as a fast pre-check of the
v3+VARIANT question.

### 7a. Feature checklist (run on every candidate)

| # | Feature (→ req) | How to test | Pass = |
| :-- | :-- | :-- | :-- |
| 1 | **v3 CREATE** (R1) | `CREATE TABLE … WITH (format_version=3)`; check it's really v3 | not silently downgraded to v2 |
| 2 | **VARIANT column** (R1) | table with a `variant` column; `INSERT CAST(JSON… AS VARIANT)`; read `payload['k']` | create + insert + read all succeed |
| 3 | **Trino R/W** (R2) | run `scripts/test_trino_v3_variant.sql` against the catalog | all steps pass |
| 4 | **Spark R/W** (R2) | Spark 4.x + Iceberg 1.11 → create/insert/read v3+VARIANT | round-trips |
| 5 | **Databricks read** (R2) | manual non-UC Spark attach on **DBR 18** → read a v3+VARIANT table | rows return |
| 6 | **Row-level DML** | UPDATE / DELETE / MERGE on a v3 table | succeed |
| 7 | **Views** (soft) | `CREATE VIEW` + read | works |
| 8 | **RBAC / credential vending** | define a grant; vend scoped S3 creds | enforced |
| 9 | **Deploy on AWS / K8s + HA** | run on target infra | runs; HA option exists |
| 10 | **Auth (OAuth2)** | secure the endpoint | works |

Hard-requirement gate = **#1, #2, #3/#4** (and #5 or a read fallback per §5). #6–#10 are tie-breakers.

### 7b. Per-candidate quick-start
- **Reference fixture (pre-check, ~10 min):** run `apache/iceberg-rest-fixture` + MinIO, point Trino at it, run
  `test_trino_v3_variant.sql`. If VARIANT create fails here (JDBC backend), expect the same on other
  JDBC-backed catalogs — a cheap early signal.
- **Apache Polaris:** self-host (Docker/Helm) over S3 → Trino + Spark → run #1–#8.
- **Lakekeeper:** single Rust binary / Helm over S3 → Trino (via OPA) + Spark → run #1–#8.
- **Apache Gravitino:** run its Iceberg REST server over S3 → focus #2 (the variant-406) on a current build.

### 7c. Results matrix (PoC complete, 2026-08-26 — Polaris 1.7.0, Lakekeeper 0.13.3, Trino 483)

| Feature | Polaris | Lakekeeper | Gravitino | S3 Tables (paid ref) |
| :-- | :-- | :-- | :-- | :-- |
| 1 v3 CREATE | ✅ | ✅ | not tested | ✅ |
| 2 VARIANT | ✅ | ✅ | ❌ (406 earlier) | ✅ |
| 3 Trino R/W | ✅ | ✅ | — | ✅ |
| 4 Spark R/W | ✅ (via Databricks DBR 18) | ✅ (via Databricks DBR 18) | — | ✅ |
| 5 Databricks CRUD incl v3+VARIANT | ✅ | ✅ | — | ✅ (federation) |
| 6 DML (UPDATE/DELETE/MERGE) | ✅ | ✅ | — | ✅ |
| 7 Views | ✅ | ✅ | ⚠️ | ⚠️ |
| 8 RBAC / cred vending | ⏳ not tested (no IAM role) | ⏳ not tested | — | ✅ |

Gate (#1–#4, #5) **passes for both Polaris and Lakekeeper.** #8 (RBAC/vending) untested because no IAM role was
provisioned in the PoC — a production follow-up, not a blocker. Standalone (non-Databricks) Spark on EMR/K8s not
separately run; Databricks (Spark 4/Iceberg) already exercises the Spark path.

### 7d. Decision rule — outcome
- **≥1 open-source catalog passes the gate (#1–#4, #5)** → adopt it. **Met: BOTH Polaris and Lakekeeper pass**,
  so the open-source requirement is satisfied and the paid S3 Tables fallback is not triggered. Tie-break between
  the two is on ops/ecosystem/authz (§5 table) — pending the team pick.
- Follow-through (post-decision): migrate Nessie → chosen catalog + dbt catalog config; apply the dbt VARIANT
  guardrail (§6); Databricks read via the validated manual Spark attach (§5); provision an IAM role to test
  credential vending (#8); productionize the metadata Postgres → RDS.

---

### Sources
- Nessie — Iceberg REST guide (v3 under development) / releases: https://projectnessie.org/guides/iceberg-rest/ · https://projectnessie.org/releases/
- Databricks — foreign Iceberg / Lakehouse Federation (Glue, HMS, Snowflake Horizon): https://docs.databricks.com/aws/en/iceberg/
- AWS — Glue Iceberg REST CreateTable is v1/v2 only: https://repost.aws/questions/QU-obVH8nqSpCjnMruhZt_QQ/support-for-iceberg-table-version-3-in-glue-data-catalog-createtable-api
- AWS — S3 Tables support Iceberg v3: https://bigdataboutique.com/blog/apache-iceberg-on-aws
- AWS — Access S3 Iceberg tables from Databricks via Glue Iceberg REST / SageMaker Lakehouse: https://aws.amazon.com/blogs/big-data/access-amazon-s3-iceberg-tables-from-databricks-using-aws-glue-iceberg-rest-catalog-in-amazon-sagemaker-lakehouse/
- Apache Polaris — v3, views, credential vending (1.4, 2026): https://dev.to/alexmercedcoder/the-state-of-apache-iceberg-catalogs-in-june-2026-265e
- Lakekeeper — Rust Iceberg REST catalog (authz, cred vending, views, K8s): https://github.com/lakekeeper/lakekeeper
- Apache Gravitino — VARIANT create fails HTTP 406 on JDBC backend (Iceberg 1.10.1): https://github.com/apache/gravitino/issues/10994
- Catalog landscape / picks by use case (2026): https://dev.to/alexmercedcoder/the-best-data-lakehouse-tools-for-apache-iceberg-in-2026-a-complete-breakdown-5fd
- Trino 483 — Iceberg VARIANT / v3 experimental: https://trino.io/docs/current/connector/iceberg.html
