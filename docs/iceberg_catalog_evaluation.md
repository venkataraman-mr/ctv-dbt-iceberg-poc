# Iceberg catalog evaluation & decision — v3/VARIANT + cross-cloud Databricks access

**Status: decision needed (key blocker).** Two production requirements have surfaced that our current catalog
(**Nessie**) cannot meet, so the catalog choice is now a first-order decision for the productionization plan.
This doc states the requirements, what we validated about Nessie, a comparison of the realistic alternatives,
and a recommendation.

Companion findings: `docs/uc_managed_iceberg_trino_write_capabilities.md` (engine capabilities),
`docs/crosscloud_read_databricks_design.md` (Databricks read path), and the Trino/Databricks test scripts under
`databricks/` and `scripts/test_trino_v3_variant.sql`.

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

## 3. Catalog comparison

| Catalog | R1: v3+VARIANT over REST | R2: Databricks access | Views | Trino | Spark | RBAC | Branching | Model (open-source / managed / paid) |
| :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| **Nessie — Native** (our PoC) | n/a (not REST); v3 is **Trino-only** | ❌ external CRUD **blocked** (no federation) | ❌ | ✅ | ✅ | ❌ | ✅✅ | **Fully open-source** (self-hosted) |
| **Nessie — REST** | ❌ **v2 only; v3 not supported yet** | ❌ (no federation) | ✅ | ✅ | ✅ | ❌ | ✅✅ | **Fully open-source** (self-hosted) |
| **AWS Glue** (Iceberg REST) | ❌ create v1/v2 only (can *read* v3 made elsewhere) | ✅ UC federates to Glue | ⚠️ limited | ✅ | ✅ | ✅ (Lake Formation) | ❌ | **Managed** (AWS; pay per request + storage) |
| **AWS S3 Tables** | ✅ **v3 GA** | ✅ via Glue / SageMaker Lakehouse federation | ⚠️ verify | ✅ (REST) | ✅ | ✅ (LF / SageMaker) | ❌ | **Managed / paid** (AWS service; data stays open Iceberg on S3) |
| **Apache Polaris** | ✅ v3 (1.4, 2026) + view federation + cred vending | ❌ (no UC federation to Polaris) | ✅ | ✅ | ✅ | ✅ | ❌ | **Open-source** (Apache TLP); managed options exist (e.g. Snowflake Open Catalog, paid) |
| **Unity Catalog** | ✅ v3 GA | ✅ native (it *is* UC) | ✅ | ⚠️ read+append only (no update/delete) | ✅ | ✅✅ | ❌ | **Managed / paid** (Databricks); OSS core (open-source Unity Catalog) with fewer features |

*(⚠️ = supported-but-verify or partial.)*

---

## 4. Analysis against the three hard requirements (R1 v3+VARIANT · R2 external read/write · R3 open-source)

Adding **R3 (open-source)** eliminates the managed catalogs, which reshapes the answer:

- **AWS S3 Tables** — meets R1 + R2, but it's an **AWS-managed/paid** service → **fails R3**. Out.
- **Unity Catalog** — meets R1, but managed/paid → **fails R3** (and external Trino write is append-only). Out.
- **AWS Glue** — managed → **fails R3** (and REST create is v1/v2 → also fails R1). Out.
- **Nessie** — open-source (R3 ✅) but **fails R1** (REST has no v3) and **fails R2** (native blocks external
  CRUD; no Databricks federation). Out.
- **Apache Polaris** — **open-source (Apache TLP → R3 ✅)**, **v3 + VARIANT over REST (R1 ✅)**, and
  **external Trino/Spark read/write over REST (R2 ✅ for open engines)**. The **only** option that clears all
  three. Views ✅ (soft bonus); no branching (soft, relaxed).

The one caveat on Polaris is the **Databricks** slice of R2: Databricks can't *UC-federate* to a generic REST
catalog (Nessie or Polaris). But that's a Databricks-federation limitation, not a Polaris capability gap —
external Spark reads Polaris v3+VARIANT fine, and Databricks can still read it by other means (see §5).
Importantly, **our earlier "Databricks can't read v3+VARIANT" tests were run against Nessie, which never served
v3** — so they don't prove anything about reading a *real* v3 catalog like Polaris. That must be re-tested.

---

## 5. Recommendation

**Adopt Apache Polaris as the AWS Iceberg catalog** (replacing Nessie for the production path). It is the
**only** evaluated catalog that satisfies all three hard requirements: **open-source** (Apache top-level
project), **v3 + VARIANT over REST**, and **external Trino/Spark read/write**. It also keeps Iceberg **views**
(soft bonus); the only thing given up vs Nessie is **git-like branching**, a relaxed soft requirement (the
pipeline uses a single `main` branch today).

**Writer engine.** Keep Trino/dbt for SQL transforms, but for the **v3/VARIANT writes** prefer **Spark
(AWS EMR/K8s)** — Trino's v3 support is experimental. Validate whether Trino-on-Polaris v3 write is reliable
before relying on it; reads from Trino/dbt are fine.

**Databricks read — the one open item, and how to solve it (all open-source-compatible):**
Databricks can't *UC-federate* to Polaris (generic REST catalog), so use one of:
1. **Re-test the manual Spark attach on DBR 18 against Polaris.** Our previous Databricks v3+VARIANT failures
   were against **Nessie, which never served v3** — invalid for this question. DBR 18's bundled Iceberg is
   v3/VARIANT-aware, so a manual `SparkCatalog` REST attach to *Polaris* may read v3+VARIANT. Test before
   assuming it can't.
2. **External Spark (EMR/K8s, OSS Iceberg 1.11).** A non-Databricks Spark reads Polaris v3+VARIANT cleanly
   (reference implementation, no DBR classpath shadowing). Run the read-side workload there.
3. **VARIANT-free projection / handoff.** The AWS stack emits a Databricks-consumable copy of only the columns
   Databricks needs (e.g. last-seen: ids/timestamps — no VARIANT). Databricks never touches v3+VARIANT; the
   variant data stays AWS-internal. Sidesteps the issue entirely.

**The one real trade to flag to leads:** *no* open-source catalog is natively UC-federatable — Databricks
federation only supports **managed** catalogs (Glue / Snowflake Horizon / UC). So "open-source catalog" and
"native Databricks UC federation" are mutually exclusive today. Since **open-source is the hard requirement**,
Databricks reads via one of the three approaches above rather than UC federation. (Only if native UC federation
were later deemed more important than open-source would the fallback be a managed catalog — S3 Tables.)

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

## 7. Open items / next steps (short validation PoC)

1. **Stand up Apache Polaris** (self-hosted on AWS); point Trino + Spark at it via the Iceberg REST catalog.
2. **v3+VARIANT write PoC:** create a v3+VARIANT table in Polaris; write from **Spark** and from **Trino**;
   confirm which writer is reliable for v3.
3. **Databricks read PoC (the decisive one):** manual Spark attach on **DBR 18** to Polaris → read a v3+VARIANT
   table (the test we could never validly run against Nessie). If it works, Databricks-direct is solved; if
   not, fall back to external Spark or the VARIANT-free projection.
4. **Migration assessment:** Nessie → Polaris (dbt profile/catalog config, table re-register/recreate); confirm
   no dependence on Nessie branching.
5. **RBAC / governance / views:** confirm Polaris RBAC + Iceberg view support meet the production bar.

---

### Sources
- Nessie — Iceberg REST guide (v3 under development) / releases: https://projectnessie.org/guides/iceberg-rest/ · https://projectnessie.org/releases/
- Databricks — foreign Iceberg / Lakehouse Federation (Glue, HMS, Snowflake Horizon): https://docs.databricks.com/aws/en/iceberg/
- AWS — Glue Iceberg REST CreateTable is v1/v2 only: https://repost.aws/questions/QU-obVH8nqSpCjnMruhZt_QQ/support-for-iceberg-table-version-3-in-glue-data-catalog-createtable-api
- AWS — S3 Tables support Iceberg v3: https://bigdataboutique.com/blog/apache-iceberg-on-aws
- AWS — Access S3 Iceberg tables from Databricks via Glue Iceberg REST / SageMaker Lakehouse: https://aws.amazon.com/blogs/big-data/access-amazon-s3-iceberg-tables-from-databricks-using-aws-glue-iceberg-rest-catalog-in-amazon-sagemaker-lakehouse/
- Apache Polaris — v3, views, credential vending (1.4, 2026): https://dev.to/alexmercedcoder/the-state-of-apache-iceberg-catalogs-in-june-2026-265e
- Trino 483 — Iceberg VARIANT / v3 experimental: https://trino.io/docs/current/connector/iceberg.html
