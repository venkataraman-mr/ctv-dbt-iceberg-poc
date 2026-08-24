# Iceberg catalog evaluation & decision — v3/VARIANT + cross-cloud Databricks access

**Status: decision needed (key blocker).** Two production requirements have surfaced that our current catalog
(**Nessie**) cannot meet, so the catalog choice is now a first-order decision for the productionization plan.
This doc states the requirements, what we validated about Nessie, a comparison of the realistic alternatives,
and a recommendation.

Companion findings: `docs/uc_managed_iceberg_trino_write_capabilities.md` (engine capabilities),
`docs/crosscloud_read_databricks_design.md` (Databricks read path), and the Trino/Databricks test scripts under
`databricks/` and `scripts/test_trino_v3_variant.sql`.

---

## 1. The two hard requirements

- **R1 — v3 + VARIANT, served over REST.** The business requires Iceberg **v3** with the **VARIANT** type;
  JSON/semi-structured columns must stay VARIANT (no VARCHAR/string substitution). The catalog must
  create/read/write v3+VARIANT over the **Iceberg REST** protocol (so all engines can use it), not just via a
  single engine's native path.
- **R2 — Databricks can access the catalog.** The phased plan keeps parts of the flow in **Azure Databricks**,
  which must read the AWS Iceberg tables. So Databricks needs a **supported** way to reach the catalog.

Soft criteria (tie-breakers): Iceberg **views**, cross-engine **Trino + Spark + Databricks**, **RBAC**,
**branch/tag isolation**, **open-source vs managed / lock-in**, operational overhead.

---

## 2. What we validated about Nessie (our current catalog)

| Capability | Result |
| :-- | :-- |
| v3 + VARIANT over **REST** | ❌ Not supported. `CREATE … format-version=3` silently makes **v2**; `ALTER … format-version=3` → **500 "Implement format version update"**. v3 is "still under active development" in Nessie; latest release doesn't add it. |
| v3 + VARIANT via **native** connector | ⚠️ Trino *can* create/write/read/mutate v3+VARIANT via the native Nessie API (`/api/v2`) — validated — but those tables are **not readable over REST**, so not cross-engine. |
| **Views** | Native Nessie connector: ❌ none (pipeline runs `views_enabled=false`). Nessie **REST** catalog: ✅ views work (validated 2026-08-17). |
| **Snapshots / time-travel / CDC** | Nessie exposes a **single Iceberg snapshot** by design (history lives in Nessie's git commit log). → no snapshot-based time-travel or `table_changes` CDC; must use **timestamp watermarks**. |
| **RBAC** | ❌ No table/schema-level grants like UC. Relies on Trino-level RBAC (Ranger/OPA) + Nessie OAuth2. |
| **Databricks access** | ❌ Databricks **cannot federate to Nessie** (generic Iceberg REST catalogs like Nessie/Polaris are not supported UC foreign-catalog sources). Only the fragile manual OSS `SparkCatalog` attach, which can't parse VARIANT. |
| **Branch / tag isolation** | ✅✅ Git-like branching, tags, atomic multi-table commits — Nessie's signature strength, which the alternatives don't offer. |

**Net:** Nessie fails **both** hard requirements — no v3/VARIANT over REST (R1) and no Databricks access (R2).
Its unique value (branching) doesn't offset a mandatory-requirement miss.

---

## 3. Catalog comparison

| Catalog | R1: v3+VARIANT over REST | R2: Databricks access | Views | Trino | Spark | RBAC | Branching | Open-source / managed |
| :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| **Nessie** (current) | ❌ (REST no v3) | ❌ (no federation) | ✅ REST / ❌ native | ✅ | ✅ | ❌ | ✅✅ | OSS, self-host |
| **AWS Glue** (Iceberg REST) | ❌ create is v1/v2 only (can *read* v3 made elsewhere) | ✅ UC federates to Glue | ⚠️ limited | ✅ | ✅ | ✅ (Lake Formation) | ❌ | AWS-managed |
| **AWS S3 Tables** | ✅ **v3 GA** | ✅ via Glue / SageMaker Lakehouse federation | ⚠️ verify | ✅ (REST) | ✅ | ✅ (Lake Formation / SageMaker) | ❌ | AWS-managed (data stays open Iceberg on S3) |
| **Apache Polaris** | ✅ v3 (1.4, 2026) + view federation + cred vending | ❌ (no UC federation to Polaris) | ✅ | ✅ | ✅ | ✅ | ❌ | OSS (Apache TLP) |
| **Unity Catalog** | ✅ v3 GA | ✅ native (it *is* UC) | ✅ | ⚠️ read+append only (no update/delete) | ✅ | ✅✅ | ❌ | Databricks/Azure-managed |

*(⚠️ = supported-but-verify or partial.)*

---

## 4. Analysis against the two hard requirements

Only options that clear **both** R1 and R2 are viable:

- **Nessie** — fails both. Out (unless v3 is dropped, which the business rejects).
- **AWS Glue** — Databricks-federatable (R2 ✅) but its REST CreateTable is **v1/v2 only** (R1 ❌). Good reader,
  not a v3 writer-catalog.
- **Apache Polaris** — v3-capable and open-source (R1 ✅) but **Databricks can't federate to it** (R2 ❌), so
  Databricks would be back to the manual attach that can't do VARIANT.
- **Unity Catalog** — clears both technically, but it's **Azure/Databricks-managed** and external Trino writes
  to UC-managed Iceberg are **append-only** (no update/delete/MERGE) — the wrong shape for an AWS
  writer-of-record, and it inverts the cloud topology.
- **AWS S3 Tables** — **v3 GA (R1 ✅)** and reachable from Databricks via **Glue / SageMaker Lakehouse
  federation (R2 ✅)**. The one option that meets both, with the data staying as open Iceberg on S3.

---

## 5. Recommendation

**Adopt AWS S3 Tables as the AWS Iceberg catalog for the v3/VARIANT tables** (replacing Nessie for the
production path). It is the only evaluated catalog that satisfies *both* mandatory requirements: it serves
Iceberg **v3 + VARIANT** over REST, and Databricks can read it through the supported **Glue/SageMaker Lakehouse
federation** path. Data remains open Apache Iceberg in your S3 bucket.

**Writer engine:** keep Trino/dbt for SQL transforms, but validate the v3 write path — **Trino's v3 support is
experimental**, so for v3/VARIANT writes prefer **Spark (AWS EMR/K8s)** as the writer, or confirm Trino's v3
write against S3 Tables is reliable before committing. (Reads from Trino/dbt are fine.)

**Trade-offs to accept:** S3 Tables is an **AWS-managed** catalog service (a lock-in shift from self-hosted
Nessie, though the table format stays open) and you **lose Nessie's git-like branching** — so first confirm
whether the pipeline actually depends on branch isolation (today it uses a single `main` branch, so likely
low impact).

**If a requirement could flex** (it can't today, per the business): drop v3 → Nessie stays with
VARIANT→VARCHAR; drop Databricks-access → Polaris (open-source v3) becomes ideal. Naming these makes the
trade space explicit for the leads.

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

1. **S3 Tables PoC:** create a v3+VARIANT table in S3 Tables; write from **Trino** and from **Spark**; confirm
   which writer is reliable for v3.
2. **Databricks read PoC:** register the S3 Tables / Glue Iceberg REST catalog in UC (Lakehouse Federation) and
   read a v3+VARIANT table from DBR 18 — confirm VARIANT columns come through.
3. **Migration assessment:** Nessie → S3 Tables catalog swap (dbt profile/catalog config, table re-registration
   or re-create), and confirm no dependence on Nessie branching.
4. **RBAC/governance:** confirm S3 Tables + Lake Formation/SageMaker meets the governance bar (Nessie had none).
5. **Views:** verify Iceberg view support on S3 Tables if the pipeline needs views in production.

---

### Sources
- Nessie — Iceberg REST guide (v3 under development) / releases: https://projectnessie.org/guides/iceberg-rest/ · https://projectnessie.org/releases/
- Databricks — foreign Iceberg / Lakehouse Federation (Glue, HMS, Snowflake Horizon): https://docs.databricks.com/aws/en/iceberg/
- AWS — Glue Iceberg REST CreateTable is v1/v2 only: https://repost.aws/questions/QU-obVH8nqSpCjnMruhZt_QQ/support-for-iceberg-table-version-3-in-glue-data-catalog-createtable-api
- AWS — S3 Tables support Iceberg v3: https://bigdataboutique.com/blog/apache-iceberg-on-aws
- AWS — Access S3 Iceberg tables from Databricks via Glue Iceberg REST / SageMaker Lakehouse: https://aws.amazon.com/blogs/big-data/access-amazon-s3-iceberg-tables-from-databricks-using-aws-glue-iceberg-rest-catalog-in-amazon-sagemaker-lakehouse/
- Apache Polaris — v3, views, credential vending (1.4, 2026): https://dev.to/alexmercedcoder/the-state-of-apache-iceberg-catalogs-in-june-2026-265e
- Trino 483 — Iceberg VARIANT / v3 experimental: https://trino.io/docs/current/connector/iceberg.html
