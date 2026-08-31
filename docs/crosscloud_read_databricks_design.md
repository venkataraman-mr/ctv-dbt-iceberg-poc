# Cross-cloud read — Databricks reading the AWS gold-occurrence Iceberg table

**Purpose.** In the productionized design the creative-sync job stays on **Azure Databricks**, but two of its
steps read the **AWS** new-stack `gold.digital_gold_occurrence` Iceberg table to update the creative table. This
document specifies **how the Databricks (Unity Catalog) job reads that AWS table**, the read/merge logic, the
networking/security prerequisites, and how it was tested.

> **Catalog:** **Apache Polaris** is the go-to catalog (the catalog PoC selected it; Lakekeeper is the equally
> capable OSS alternative — see `docs/iceberg_catalog_evaluation.md`). **Nessie is ruled out** — it cannot host
> Iceberg **v3 + VARIANT** over REST, the business hard requirement. Nessie details are retained below **for
> reference only, marked *not preferred*.**
>
> **Key point:** the cross-cloud read is **catalog-agnostic** — PyIceberg / Spark talk to *any* Iceberg REST
> catalog — so this read design is the same whichever OSS catalog wins; only the endpoint/auth differ.
>
> **Status (validated 2026-08-26):** on the target **UC** cluster, a foreign Spark catalog can NOT be registered
> (see §2); the working read path is **PyIceberg on the UC cluster** (validated). The `MERGE` logic in §3 is
> unchanged from the original design. Test assets: `databricks/polaris_uc_cluster_read_test.py`,
> `scripts/catalog/polaris/polaris_uc_read_seed.sql`, `databricks/README_catalog_crosscloud.md`.

Related: `docs/ctv_productionization_crosscloud_design.md` (overall cross-cloud plan),
`docs/iceberg_catalog_evaluation.md` (catalog decision), `docs/uc_managed_iceberg_trino_write_capabilities.md`
(engine capability findings).

---

## 1. Which reads are cross-cloud (and which are not)

The creative-sync job has three creative-update steps that touch occurrence data. Only two read the occurrence
table and therefore cross the cloud boundary:

| Step | dbt reference model | Reads occurrence? | Cross-cloud? |
| :-- | :-- | :-- | :-- |
| Last-seen update | `crtv_lastseen_update` | Yes — `digital_gold_occurrence` | **Yes** — reads AWS |
| First-seen occurrence-id | `crtv_occid_update` | Yes — `digital_gold_occurrence` | **Yes** — reads AWS |
| First-seen **info** | `crtv_fsinfo_update` | No — reads `creative_first_seen` / `creative` / `creative_dedupe_map` | No — stays in Databricks |

So this work covers the **last-seen** and **first-seen-occurrence-id** reads. First-seen-info is unaffected — its
sources are all creative-domain tables in Databricks. The MERGE **target** in both cases is the Databricks
UC-managed creative table — Databricks stays writer-of-record; only the *source read* is remote.

---

## 2. How the Databricks (UC) job reads the AWS Polaris table — two options

**The constraint that shapes everything (validated 2026-08-26).** The first/last-seen job runs on a **Unity
Catalog** cluster (it MERGEs into UC-managed tables). A UC cluster **cannot register a foreign Spark REST
catalog**: setting `spark.sql.catalog.polaris …` is silently ignored — `SHOW CATALOGS` omits it and the query
fails with `NO_SUCH_CATALOG_EXCEPTION: Catalog 'polaris' not found`. This is a **UC namespace-governance limit,
not an access-mode one** — it happens on **both** Standard *and* Dedicated (single-user) access modes (tested).
The manual `SparkCatalog` REST attach therefore only works on a **non-UC** cluster. That leaves two viable
options for the UC job:

### Option A — PyIceberg read on the UC cluster  (PREFERRED — validated)

PyIceberg is a **Python library** that speaks Iceberg REST directly — it does **not** use a Spark catalog, so UC
doesn't block it. The whole job is **one task on the existing UC cluster**: PyIceberg reads the bounded
occurrence slice → Arrow/pandas → Spark DataFrame → `MERGE` into the UC creative table. Validated 2026-08-26
(read the Polaris **v3** occurrence table's non-VARIANT columns from a UC cluster).

```python
%pip install "pyiceberg[pyarrow]" boto3
from pyiceberg.catalog.rest import RestCatalog
cat = RestCatalog("polaris", **{
    "uri": "http://<VM>:8181/api/catalog", "warehouse": "ctv_poc",
    "credential": "<trino_poc_clientId>:<clientSecret>", "scope": "PRINCIPAL_ROLE:ALL",
    "s3.access-key-id": "<AWS_KEY>", "s3.secret-access-key": "<AWS_SECRET>", "s3.region": "us-east-2",
})
tbl = cat.load_table(("gold", "digital_gold_occurrence"))
arrow = tbl.scan(
    row_filter="updated_timestamp > '<watermark>'",      # BOUND the read — never scan the whole table
    selected_fields=("occurrence_id","creative_url_hash","capture_timestamp","updated_timestamp","delete_flag"),
).to_arrow()
spark.createDataFrame(arrow.to_pandas()).createOrReplaceTempView("occ")
# ... Spark MERGE from `occ` into the UC creative table (logic in §3) ...
```

- **Bound the read — critical.** PyIceberg materializes on the **driver** (single node). Always pass a
  `row_filter` (the watermark, or the waiting `creative_url_hash` set) **and** `selected_fields`. Never pull the
  full occurrence table.
- **Secrets** via a Databricks **secret scope** in production (inline only for the PoC test).
- **v3** — reads non-VARIANT columns of a v3 table fine; the sync job never reads the VARIANT payload.
- **Shape:** single UC task, no second cluster, no handoff table. Simplest and preferred.

### Option B — Two-task workflow (FALLBACK: very large slices, or PyIceberg not permitted on the UC cluster)

One Databricks Job, two tasks on different clusters (Task 2 `depends_on` Task 1):

- **Task 1 — non-UC cluster:** the Spark `SparkCatalog` REST attach to Polaris (validated on a non-UC cluster) →
  **distributed** read of the bounded slice → write a small **projected handoff table** (ids + timestamps, no
  VARIANT).
- **Task 2 — UC cluster:** read the handoff table → `MERGE` into the UC creative table.

**Where the handoff table sits.** It must be reachable by **both** a non-UC and a UC cluster, so it **cannot** be
a UC-managed table (Task 1 has no UC) and not DBFS-local-only:

- **Preferred (governed):** a **Delta table on ADLS** registered as a **UC external location + external table**.
  Task 1 writes Delta to the path using the Azure storage key (direct storage access, no UC needed); Task 2 reads
  the UC external table. Same physical files, two access mechanisms. Put it on the **Azure/ADLS** side (that's
  where the UC consumer is; Task 1 pushes the small bounded projection cross-cloud).
- **Quick (PoC only):** a **`hive_metastore`** table — both cluster types can see `hive_metastore.<db>.<table>`.
  Fewer steps, but legacy/DBFS-root with Standard-UC restrictions; not for production.
- Keep it **small and ephemeral** — projected columns only, watermark-bounded, overwritten each run.

**Pick B over A only** when the changed slice is too large to materialize on the PyIceberg driver (Spark
distributes the read in Task 1), or when org policy forbids installing PyIceberg on the UC cluster. Otherwise A.

### Spark REST attach config (used by Task 1 in Option B, and the non-UC connectivity tests)

Polaris, on a **non-UC** cluster (cluster Spark config; `spark.sql.extensions` is static so it must live here):

```
spark.sql.catalog.aws_occ                     = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.aws_occ.type                = rest
spark.sql.catalog.aws_occ.uri                 = http://<VM>:8181/api/catalog
spark.sql.catalog.aws_occ.warehouse           = ctv_poc
spark.sql.catalog.aws_occ.credential          = <trino_poc_clientId>:<clientSecret>   # OAuth2
spark.sql.catalog.aws_occ.scope               = PRINCIPAL_ROLE:ALL
spark.sql.catalog.aws_occ.io-impl             = org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.aws_occ.client.region       = us-east-2          # REQUIRED (AWS client factory reads this, not s3.region)
spark.sql.catalog.aws_occ.s3.region           = us-east-2
spark.sql.catalog.aws_occ.s3.access-key-id     = {{secret}}
spark.sql.catalog.aws_occ.s3.secret-access-key = {{secret}}
```
Occurrence table = `aws_occ.gold.digital_gold_occurrence`. Libraries: `iceberg-spark-runtime-4.0_2.13:1.11.0` +
`iceberg-aws-bundle:1.11.0` on DBR 18 (or the 3.5_2.12:1.9.1 pair on DBR 15.4/16.4). Full cross-cloud how-to:
`databricks/README_catalog_crosscloud.md`.

> **Not preferred — Nessie (reference only).** The original design attached Nessie's REST catalog
> (`uri …:19120/iceberg/main/`, `warehouse = warehouse`, branch in path, no OAuth). It reads/writes v2 fine and
> the read SQL is identical, but **Nessie is ruled out** (no v3+VARIANT over REST) and is not the productionized
> catalog. The read being catalog-agnostic means switching Nessie→Polaris only changes the endpoint/auth, not the
> logic.

---

## 3. Read + merge logic

**Last-seen** (`CTV_LAST_SEEN_DIGITAL` watermark). Changed creatives = distinct `creative_id` whose occurrence
`updated_timestamp > watermark`; for each, the latest **live** (`delete_flag = false`) `capture_timestamp`; MERGE
into the creative table update-only with a NULL-unsafe `<>` guard (a creative with NULL `last_seen_timestamp` is
not updated — prod parity). The watermark is **pinned before the read** (to `max(updated_timestamp)` over the
changed rows) and advanced only after a successful MERGE, so rows written mid-run aren't skipped next time.
Because the source occurrence table isn't modified by the MERGE, this is idempotent on re-run. In Option A the
watermark becomes the PyIceberg `row_filter`; in Option B it bounds Task 1's Spark scan.

**First-seen occurrence-id** (no watermark). Self-limits on `first_seen_occurrence_id IS NULL` plus an
`updated_timestamp` date floor, and a recent `capture_month` window on the occurrence side; matches on
(`creative_url_hash` = `provider_original_creative_url_hash`) AND (`first_seen_provider_occurrence_id` =
`provider_occurrence_id`), deduped to one row per (creative_id, url_hash); MERGE sets `first_seen_occurrence_id`.
Idempotent — once set, a creative is never reprocessed.

The watermark lives in a small Databricks-side control table (`<uc>.control.crosscloud_watermark`).

**Why a timestamp watermark, not Iceberg snapshot CDC.** The occurrence table is MERGE-written
(soft-deletes/updates), so Iceberg incremental-append reads / `table_changes` choke on its snapshots. A plain
`WHERE updated_timestamp > :wm` filtered scan is immune to snapshot type / delete files / deletion vectors, and
soft-delete + always-bumped `updated_timestamp` means the scan captures inserts, updates and deletes — the same
pattern the pipeline already uses for MERGE-written tables. This holds on **any** catalog and both read options.

> **Catalog note on snapshots.** Unlike Nessie (which exposed only a *single* Iceberg snapshot per table, so
> snapshot-based time-travel/CDC was unavailable — a 2026-08-24 finding), **Polaris uses standard Iceberg
> snapshots**, so `VERSION AS OF` / snapshot-range reads *are* available there. We still use the
> timestamp-watermark scan by choice (MERGE-written source, existing pattern) — but the Nessie single-snapshot
> limitation no longer applies.

---

## 4. Networking & security prerequisites

1. **Databricks → catalog endpoint.** The UC job (Option A) or the non-UC Task 1 (Option B) must reach the AWS
   **Polaris** REST endpoint (`http://<VM>:8181/api/catalog`). The firewall/security-group must allow the
   Databricks egress to **8181** (already opened for the PoC). Production (Polaris on Kubernetes) exposes it via
   private endpoint / peering / allow-listed ingress.
2. **Databricks → S3 read.** `S3FileIO` (Spark) / PyIceberg read the data files from
   `s3://dataplatformpoc-venketa/…`. Provide AWS creds via a **Databricks secret scope** (IAM user/role scoped to
   read that bucket/prefix), or assume-role/instance-profile. Never inline keys in production. Polaris runs
   without credential vending in the PoC, so the client uses its own keys; production may switch to vending once
   an IAM role is provisioned.
3. **Polaris auth.** OAuth2 client-credentials using the `trino_poc` principal (`credential` + `scope`).
4. **Egress.** Cross-cloud read volume is bounded (watermark-filtered / null-self-limited), so egress scales with
   churn, not table size.

---

## 5. Testing

**Done — UC-cluster read (2026-08-26).** `databricks/polaris_uc_cluster_read_test.py` on a UC cluster confirmed:
(a) the Spark foreign-catalog attach is rejected by UC (`NO_SUCH_CATALOG`), and (b) **PyIceberg reads the Polaris
occurrence table** (v3, non-VARIANT columns) and computes the first/last-seen aggregation — Option A works.
Seed: `scripts/catalog/polaris/polaris_uc_read_seed.sql`.

**Done — offline logic test** (`databricks/tests/test_crosscloud_read_logic.py`, plain PySpark): validates the
transformation logic with mock tables — watermark selection, latest-per-creative, `delete_flag=false` filter, the
NULL-unsafe `<>` guard, and the occid match + null-self-limit. All checks pass.

**To validate next (against real data, on the chosen catalog):**
1. Option A: PyIceberg-read the real occurrence slice bounded by the watermark; confirm row counts + latency.
2. Run last-seen and confirm changed creatives' `last_seen_timestamp` updated; re-run → 0 changes (idempotent);
   confirm the watermark advanced.
3. Run occid and confirm `first_seen_occurrence_id` populated for matched creatives; re-run → no-op.
4. Confirm read volume/egress is bounded and cross-cloud latency is acceptable per batch.
5. If slices are large, benchmark Option B (two-task) to compare distributed read vs the PyIceberg driver read.

---

## 6. Open items / production hardening

- **Read-option decision per volume:** default **Option A (PyIceberg on UC)**; keep **Option B (two-task)** for
  very large slices or if PyIceberg can't be installed on the UC cluster. Benchmark once real volumes are known.
- **Secrets:** move the `trino_poc` creds + AWS keys into a Databricks secret scope (currently inline for tests).
- **Handoff storage (Option B):** stand up the ADLS path + UC external location/table; confirm a non-UC cluster
  can write Delta there and the UC cluster reads it.
- **Bounded-read guardrail:** enforce the `row_filter` (watermark / waiting set) so the PyIceberg driver read
  never materializes the whole table.
- **Endpoint hardening:** finalize the Databricks→Polaris network path (private/peered) and keep OAuth2 on.
- **Credential vending:** revisit once an IAM role is provisioned (would remove client-side S3 keys).
