# Support request — External Iceberg (REST) access to Unity Catalog for a Trino engine

**Summary:** Enable an **external Apache Iceberg REST Catalog** client (our Trino/dbt engine on an AWS EC2 VM)
to **read and write specific UC-managed Iceberg tables** in Unity Catalog. This was a time-boxed PoC to
validate UC ↔ external-engine interop for the CTV occurrence-flow productionization. Scope: one test schema
and a single service identity (no per-user access needed).

**External engine:** Trino 483 (open-source), Docker on an AWS EC2 VM · **Access protocol:** Unity Catalog
Iceberg REST Catalog API + OAuth2 (client credentials).

> **Status — GRANTED & TESTED (2026-08-18/19).** Access was provisioned and the interop was validated
> end-to-end. **Read (`SELECT`) and append (`INSERT`) work** on UC-managed Iceberg (v2 and v3). **Row-level
> `UPDATE`/`DELETE`/`MERGE` and update/delete CDC do not work from Trino** — an engine-side (Trino 483) limit,
> not an access problem. Full findings: `docs/catalog/uc_managed_iceberg_trino_write_capabilities.md`. This file is now
> the record of *what was provisioned and how the engine is configured*, kept for reuse on future media-table
> cutovers.

---

## Correct REST endpoint (important)

The endpoint is **`/api/2.1/unity-catalog/iceberg-rest`** — **not** the older `/api/2.1/unity-catalog/iceberg`,
which is **deprecated** and returns "Legacy Iceberg endpoints … are deprecated. Please migrate to …
/iceberg-rest/v1/". The workspace URL **must include the workspace id**, else requests 303-redirect to a login
page. Also avoid a trailing slash on the host (it produces a `net//api…` double slash in the OAuth
server-uri fallback).

Confirmed working endpoint for this workspace:
`https://adb-<workspace-id>.<n>.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest`.

## What was needed on the Databricks side (reuse for future cutovers)

1. **External data access enabled on the UC metastore** (off by default; metastore admin toggles it).
2. **A test schema with UC-managed *Iceberg* tables.** Only **UC-managed Iceberg** is externally writable —
   Delta/UniForm and foreign Iceberg are read-only to external engines, and there is **no `LOCATION` clause**
   for UC Iceberg tables (managed only; placement is steered by a catalog/schema *managed storage location*,
   not a per-table path — see the capability doc §2b).
3. **Grants to the service principal** on the test catalog/schema/table:
   - `USE CATALOG` on the catalog
   - `USE SCHEMA` on the schema
   - **`EXTERNAL USE SCHEMA`** on the schema  ← required for external Iceberg read/write
   - `SELECT` on the read/write test table(s), `MODIFY` on the write-test table, `CREATE TABLE` if the engine
     creates tables (note: Trino `CREATE TABLE` returned "Failed to create transaction" — tables were created
     on Databricks; see capability doc).
4. **A service principal + credential** (via the secure channel, not the ticket). A Databricks-managed service
   principal is sufficient — **no new Azure/Entra object required**. We authenticated with the **OAuth client
   id + secret** (machine-to-machine, client-credentials flow); a PAT also works for a smoke test.

## Storage / credential reality (the key finding — read this)

We **cannot** use UC credential vending for storage from Trino today: **Trino 483 implements IRC vended
credentials for S3 only, not Azure ADLS** (`trinodb/trino` #23238, open). With vending enabled, UC returns a
per-table ADLS SAS token but Trino ignores it and falls back to `DefaultAzureCredential`, which on our AWS VM
has no `AZURE_CLIENT_ID` / no Azure IMDS → the read fails ("Error processing metadata for table").

**Workaround that unblocked read + append:** disable vending and give Trino an **explicit Azure
storage-account key**. So, in addition to the Databricks grants above, we need from the storage/DBA side:

- The **ADLS storage account name + account key** for the account backing the managed tables — here
  **`vxxdbwcommonpesteu2`** (the same account/key the `uc_reference_sync` already uses, so no new secret was
  required). Confirmed via `SHOW CREATE TABLE`: the table's `data_location` is
  `abfss://dbwcontainer@vxxdbwcommonpesteu2.dfs.core.windows.net/deltas/mrdpp/tempwork/__unitystorage/…`.
- If a future managed table lands in a **different** storage account, we need that account's key **or** an
  Entra service principal with **`Storage Blob Data Contributor`** on its container.

(When Trino ships Azure vended credentials — #23238 — this account-key requirement goes away and pure vending
becomes viable.)

## Engine-side configuration (for reference / reproducibility)

**`infra/trino/catalog/unity_catalog.properties`** (scratch catalog `unity_catalog`, env-driven, no secrets in
the file — all `${ENV:…}` from `.env`):

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=${ENV:DATABRICKS_HOST}/api/2.1/unity-catalog/iceberg-rest
iceberg.rest-catalog.warehouse=${ENV:UC_CATALOG}                 # the UC catalog name (= mrdpp_prod)
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=${ENV:DATABRICKS_TOKEN}        # (or oauth2.credential=<client-id>:<secret>)
iceberg.rest-catalog.vended-credentials-enabled=false            # Azure vending unsupported on Trino 483 (#23238)
fs.azure.enabled=true                                            # renamed from fs.native-azure.enabled in 483
azure.auth-type=ACCESS_KEY
azure.access-key=${ENV:UC_AZURE_STORAGE_ACCOUNT_KEY}             # explicit key for the ADLS account above
```

**`docker-compose.yml`** — the `trino` service passes the UC vars through to the container:

```yaml
    environment:
      # … existing AWS/PG vars …
      DATABRICKS_HOST: ${DATABRICKS_HOST:-}
      UC_CATALOG: ${UC_CATALOG:-}
      DATABRICKS_TOKEN: ${DATABRICKS_TOKEN:-}
      UC_AZURE_STORAGE_ACCOUNT_NAME: ${UC_AZURE_STORAGE_ACCOUNT_NAME:-}
      UC_AZURE_STORAGE_ACCOUNT_KEY: ${UC_AZURE_STORAGE_ACCOUNT_KEY:-}
```

**`.env`** (gitignored, on the VM) provides `DATABRICKS_HOST` (no trailing slash, incl. workspace id),
`UC_CATALOG=mrdpp_prod`, `DATABRICKS_TOKEN` (PAT) or the OAuth client id:secret, and
`UC_AZURE_STORAGE_ACCOUNT_NAME` / `UC_AZURE_STORAGE_ACCOUNT_KEY`. After changing compose env, recreate the
container (`docker compose up -d trino`), not just `restart`.

## Values received from the DBA (fill/keep current)

- **Workspace URL (incl. id):** `https://adb-1373526707855139.19.azuredatabricks.net`
- **UC catalog (REST warehouse):** `mrdpp_prod`
- **Test schema / tables:** `tempwork` · `trino_interop_test` (v2), `trino_interop_v3` (v3)
- **Service principal credential:** OAuth client id + secret (via secure channel)
- **ADLS storage account / key:** `vxxdbwcommonpesteu2` (reference-sync key, secure channel)

## Acceptance results (what we verified from Trino)

1. List schema + tables through the REST catalog — **PASS**.
2. `SELECT` from a managed Iceberg table (data via the explicit Azure key) — **PASS** (v2 + v3).
3. `INSERT` / append into a managed Iceberg table (external write) — **PASS** (v2 + v3).
4. `UPDATE` / `DELETE` / `MERGE` from Trino — **FAIL** (`NoClassDefFoundError: org/apache/hadoop/fs/Path`;
   Trino-483 delete-writer limit, not access; unchanged on v3).
5. CDC (`system.table_changes`) — **PARTIAL**: reads append snapshots; refuses delete/overwrite/deletion-vector
   snapshots. Not an access issue.

## References (Databricks docs)

- Enable external data access to Unity Catalog: https://learn.microsoft.com/en-us/azure/databricks/external-access/admin
- Access Databricks tables from Apache Iceberg clients: https://learn.microsoft.com/en-us/azure/databricks/external-access/iceberg
- Unity Catalog credential vending for external system access: https://docs.databricks.com/aws/en/external-access/credential-vending
- Trino #23238 — Iceberg REST vended credentials for Azure (open): https://github.com/trinodb/trino/issues/23238
- Full engine-side findings & options: `docs/catalog/uc_managed_iceberg_trino_write_capabilities.md`
