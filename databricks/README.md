# Databricks cross-cloud read — connectivity test runbook

Goal: prove that **Azure Databricks** can read the **AWS** new-stack `gold.digital_gold_occurrence` Iceberg
table through the **Nessie Iceberg REST Catalog**. This is a **read-only** test — no writes.

The full creative-sync logic (last-seen + first-seen occurrence-id MERGEs) is deferred; the design for it is
kept in [`../docs/crosscloud_read_databricks_design.md`](../docs/crosscloud_read_databricks_design.md) for later.

## Files

| File | Run where | Purpose |
| :-- | :-- | :-- |
| `connectivity_test_crosscloud.py` | Databricks | Read-only connectivity test (attach catalog + 3 read checks). |

---

## Execution order

### Step 0 — Cluster prerequisites (one-time)

1. **Cluster type:** a **non-UC** cluster (a UC cluster won't attach a foreign Iceberg catalog). Confirmed by a
   blank "Data access" on the cluster summary.
2. **Runtime ↔ Iceberg library must match** (this is the common mistake). Pick ONE row:

   | Databricks Runtime | Spark / Scala | Iceberg Maven libraries (set a real version!) |
   | :-- | :-- | :-- |
   | **16.4 LTS or 15.4 LTS** *(recommended)* | Spark 3.5 / Scala 2.12 | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1`  +  `org.apache.iceberg:iceberg-aws-bundle:1.9.1` |
   | 18 LTS | Spark 4.1 / Scala 2.13 | `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0`  +  `org.apache.iceberg:iceberg-aws-bundle:1.11.0`  *(Spark-4.0 build on Spark 4.1 — unverified)* |

   The `3.5_2.12` libs you first attached are for Spark 3.5 — they will **not** load on DBR 18 (Spark 4.1), and
   the version must be a real number, not `undefined`. For a first test, DBR 16.4 LTS + the `3.5_2.12:1.9.1`
   libs is the stable, matched choice. This read only needs to consume an external Iceberg table, so it does
   not require DBR 18's managed-Iceberg features.
3. **AWS credentials:** no secret scope needed for the test — the notebook takes the key/secret as widgets
   (typed at runtime, not stored in code). Use a **temporary, least-privilege** key (S3 read on the warehouse
   prefix only) and rotate it afterward. Production should use a secret scope or assume-role.
4. **Network:** Databricks must reach the Nessie endpoint and S3. Nessie on the VM is **plaintext HTTP**
   (`security=NONE`), so use `http://<host>:19120/...`, not `https://`. The EC2 **security group** must allow
   inbound TCP **19120** from the Databricks workspace's egress/NAT IPs. **Do not** open `19120` to
   `0.0.0.0/0` — Nessie has no auth; restrict to Databricks egress IPs and close after the test.
   Verify from a host that can reach the VM: `curl -v http://<host>:19120/iceberg/main/v1/config`.
   A `Connection timed out` from Databricks = the security group / network path, not the notebook.

### Step 1 — Run the connectivity test  ▶ `connectivity_test_crosscloud.py`

1. Import the file into Databricks (it uses `# COMMAND ----------` cell markers).
2. Set the widgets: `nessie_uri` (e.g. `https://<host>:19120/iceberg/main/` — branch `main` in the path),
   `warehouse` (the Nessie warehouse **name**, e.g. `warehouse` — not the s3 path), `aws_region`,
   `aws_access_key_id`, `aws_secret_access_key`. These mirror `infra/trino/catalog/iceberg_rest.properties`
   (`uri=…/iceberg/` + `prefix=main` on Trino → branch-in-path on Spark; `warehouse=warehouse`).
3. Run all cells top to bottom.
4. **Pass criteria:**
   - Step 3 lists namespaces/tables → the Nessie REST endpoint is reachable.
   - Step 4/5 return a count and sample rows → S3 read + creds work.
   - `occ_rows = 0` still passes — it just means the table has no data yet.
5. If it fails, the last cell maps each failure to its likely cause (endpoint vs. S3 creds vs. library).

Bring back whatever it prints — pass or error — and we'll take the next step from there.
