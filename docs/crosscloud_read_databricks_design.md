# Cross-cloud read — Databricks reading the AWS gold-occurrence Iceberg table

**Purpose.** In the productionized design the creative-sync job stays on **Azure Databricks**, but two of its
steps read the **AWS** new-stack `gold.digital_gold_occurrence` Iceberg table to update the creative table.
This document specifies how Databricks Spark attaches to and reads that table (via the **Nessie Iceberg REST
Catalog**), the read/merge logic, the networking/security prerequisites, and how it was tested. It's the
blueprint for the Databricks-side implementation.

> **Status (current):** we are validating **connectivity only** first — see
> `databricks/connectivity_test_crosscloud.py` (read-only) and `databricks/README.md`. The full production
> notebook (last-seen + first-seen-occurrence-id MERGEs) and its offline logic test are **deferred**; the
> logic below is the design to implement once the cross-cloud read is confirmed.

Related: `docs/ctv_productionization_crosscloud_design.md` (the overall cross-cloud plan),
`docs/uc_managed_iceberg_trino_write_capabilities.md` (engine capability findings).

---

## 1. Which reads are cross-cloud (and which are not)

The creative-sync job has three creative-update steps that touch occurrence data. Only two read the
occurrence table and therefore cross the cloud boundary:

| Step | dbt reference model | Reads occurrence? | Cross-cloud? |
| :-- | :-- | :-- | :-- |
| Last-seen update | `crtv_lastseen_update` | Yes — `digital_gold_occurrence` | **Yes** — reads AWS |
| First-seen occurrence-id | `crtv_occid_update` | Yes — `digital_gold_occurrence` | **Yes** — reads AWS |
| First-seen **info** | `crtv_fsinfo_update` | No — reads `creative_first_seen` / `creative` / `creative_dedupe_map` | No — stays in Databricks |

So this work covers the **last-seen** and **first-seen-occurrence-id** reads. First-seen-info is unaffected —
its sources are all creative-domain tables that live in Databricks.

The MERGE **target** in both cases is the Databricks UC-managed `gold.creative` table — Databricks remains the
writer-of-record; only the *source read* is remote.

---

## 2. Attach method — Nessie Iceberg REST Catalog (chosen)

Databricks Spark registers the AWS catalog as a generic **Iceberg REST catalog** pointed at Nessie's REST
endpoint, with the branch carried in the URI path and `S3FileIO` reading the data files directly:

```
spark.sql.catalog.aws_occ                    = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.aws_occ.type               = rest
spark.sql.catalog.aws_occ.uri                = https://<NESSIE_HOST>:19120/iceberg/main/   # branch 'main' in path
spark.sql.catalog.aws_occ.warehouse          = warehouse                                   # Nessie warehouse NAME, not the s3:// path
spark.sql.catalog.aws_occ.io-impl            = org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.aws_occ.s3.region          = us-east-2
spark.sql.catalog.aws_occ.s3.access-key-id     = {{secret}}   # from a Databricks secret scope
spark.sql.catalog.aws_occ.s3.secret-access-key = {{secret}}
# production: add Nessie bearer/OAuth2 auth props once the endpoint is secured
```

The occurrence table is then `aws_occ.gold.digital_gold_occurrence`. This mirrors the validated
`infra/trino/catalog/iceberg_rest.properties` (uri `.../iceberg/`, branch `main`) — same REST surface, just the
Spark client. It's the most portable choice and aligns with Databricks Lakehouse Federation for Iceberg REST
catalogs.

**Cluster prerequisites.** A **non-UC cluster** (UC clusters won't attach a foreign Iceberg catalog), with the
Iceberg Maven cluster libraries whose build **matches the runtime's Spark/Scala** (a real version, not
`undefined`): Spark 3.5 (DBR 15.4/16.4 LTS) → `iceberg-spark-runtime-3.5_2.12:1.9.1` + `iceberg-aws-bundle:1.9.1`
(recommended, stable); Spark 4.x / Scala 2.13 (DBR 18) → `iceberg-spark-runtime-4.0_2.13:1.11.0` +
`iceberg-aws-bundle:1.11.0` (Spark-4.0 build on Spark 4.1 is unverified). `iceberg-aws-bundle` provides
`S3FileIO`. For a first connectivity test, prefer the Spark-3.5 runtime. Set the catalog config in the **cluster Spark config** (preferred) or via `spark.conf.set(...)` at
the top of the notebook before first use.

**Alternatives considered** (documented in the capability doc):
- *Native Nessie Spark extensions* (`catalog-impl=org.apache.iceberg.nessie.NessieCatalog`, uri `.../api/v2`,
  `ref=main`) — works, but Nessie-specific and needs the Nessie extension jar. Chosen REST instead for
  portability.
- *Lakehouse Federation* (register the Nessie REST catalog as a UC foreign catalog) — most governed and keeps
  it on a UC cluster, but foreign Iceberg is read-only with limited platform support. Good future option; the
  read SQL here ports unchanged to it.

---

## 3. Read + merge logic

**Last-seen** (`CTV_LAST_SEEN_DIGITAL` watermark). Changed creatives = distinct `creative_id` whose occurrence
`updated_timestamp > watermark`; for each, the latest **live** (`delete_flag = false`) `capture_timestamp`;
MERGE into `creative` update-only with a NULL-unsafe `<>` guard (a creative with NULL `last_seen_timestamp` is
not updated — prod parity). The watermark is **pinned before the read** (to `max(updated_timestamp)` over the
changed rows) and advanced only after a successful MERGE, so rows written mid-run aren't skipped next time.
Because the source occurrence table is not modified by the MERGE, this is idempotent on re-run.

**First-seen occurrence-id** (no watermark). Self-limits on `first_seen_occurrence_id IS NULL` plus an
`updated_timestamp` date floor, and a recent `capture_month` window on the occurrence side; matches on
(`creative_url_hash` = `provider_original_creative_url_hash`) AND (`first_seen_provider_occurrence_id` =
`provider_occurrence_id`), deduped to one row per (creative_id, url_hash); MERGE sets
`first_seen_occurrence_id`. Idempotent — once set, a creative is never reprocessed.

The watermark lives in a small Databricks-side control table (`<uc>.control.crosscloud_watermark`).

**Why a timestamp watermark, not Iceberg CDC.** The occurrence table is MERGE-written (soft-deletes/updates),
so Iceberg incremental-append reads / `table_changes` choke on its snapshots. A plain
`WHERE updated_timestamp > :wm` filtered scan is immune to snapshot type / delete files / deletion vectors, and
soft-delete + always-bumped `updated_timestamp` means the scan captures inserts, updates and deletes. This is
the same pattern the pipeline already uses for MERGE-written tables.

**Confirmed on Databricks (2026-08-24): Nessie exposes only a single Iceberg snapshot per table** — by design,
to preserve its git-like branch/tag isolation (history lives in Nessie's commit log, not the Iceberg snapshot
log). Verified from Databricks: `.history` / `.snapshots` show one row for every table regardless of how many
commits were made. Consequences for cross-cloud reads: (1) Iceberg **snapshot-based** reads are unavailable —
no `VERSION AS OF <snapshot_id>`, no `start/end-snapshot-id` incremental read, no `create_changelog_view`;
(2) therefore the **timestamp-watermark filtered scan is the CDC-read mechanism** over a Nessie catalog (this
finding reinforces the design); (3) point-in-time / version reads are still possible, but via **Nessie refs**
(branch / tag / commit hash / timestamp), not Iceberg snapshot ids; (4) true commit-level CDC (before/after
images) would use a **Nessie commit-log diff** between two refs, not the Iceberg snapshot APIs. Write DML
(INSERT/UPDATE/DELETE/MERGE) from Databricks Spark to a Nessie Iceberg table was also confirmed working (on a
scratch table) — full DML, unlike Trino → UC-managed.

---

## 4. Networking & security prerequisites

1. **Databricks → Nessie endpoint reachability.** Azure Databricks must reach the AWS Nessie REST endpoint
   (`https://<host>:19120/iceberg/...`). In production (Nessie on Kubernetes) expose it via a private
   endpoint / peering / allow-listed ingress; today's single-VM Nessie is not exposed for external use.
2. **Databricks → S3 read.** `S3FileIO` reads the data files from `s3://dataplatformpoc-venketa/...`. Provide
   AWS credentials via a **Databricks secret scope** (an IAM user/role scoped to read that bucket/prefix), or
   an assume-role/instance-profile on the cluster. Never inline keys.
3. **Nessie auth.** For the PoC the REST endpoint is open; production should require **OAuth2 bearer** on
   Nessie (add the auth props to the catalog config) — see the RBAC limitation in the capability doc.
4. **Egress.** Cross-cloud read volume is bounded (watermark-filtered / null-self-limited), so egress scales
   with churn, not table size.

---

## 5. Testing

**Done — offline logic test** (`databricks/tests/test_crosscloud_read_logic.py`, plain PySpark, no jar/network):
validates the transformation logic with mock tables — watermark selection, latest-per-creative,
`delete_flag=false` filter, the NULL-unsafe `<>` guard, and the occid match + null-self-limit. All checks pass.
It emulates the MERGE outcome with an equi-join (offline Spark has no MERGE engine); the SELECT logic is
byte-faithful to the dbt models.

**On Databricks — to validate after access + networking are arranged:**
1. Attach the `aws_occ` REST catalog; run the smoke test (`SHOW NAMESPACES`, `count(*)`, `max(updated_timestamp)`).
2. Run Step 1 (last-seen) and confirm changed creatives' `last_seen_timestamp` updated; re-run → 0 changes
   (idempotent); confirm the watermark advanced.
3. Run Step 2 (occid) and confirm `first_seen_occurrence_id` populated for matched creatives; re-run → no-op.
4. Confirm read volume/egress is bounded and the cross-cloud read latency is acceptable per batch.

---

## 6. Open items / production hardening

- Secure the Nessie endpoint (OAuth2) and finalize the Databricks→Nessie network path.
- Decide the AWS-read credential model (scoped IAM user in a secret scope vs. assume-role).
- Confirm the Iceberg cluster-library versions against the chosen Databricks Runtime; revisit if a future DBR
  can attach the foreign catalog on a UC cluster (would remove the non-UC-cluster constraint).
- Re-evaluate Lakehouse Federation once its foreign-Iceberg support broadens — it would make this read
  UC-governed with the same SQL.
