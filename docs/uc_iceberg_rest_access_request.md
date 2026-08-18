# Support request — External Iceberg (REST) access to Unity Catalog for a Trino engine

**Summary:** Please enable an **external Apache Iceberg REST Catalog** client (our Trino/dbt engine running
on an AWS EC2 VM) to **read and write specific UC-managed Iceberg tables** in Unity Catalog, using **Unity
Catalog credential vending** for the underlying Azure (ADLS) storage. This is a time-boxed PoC to validate
UC ↔ external-engine interop for the CTV occurrence-flow productionization. Scope is one test schema and a
single service identity (no per-user access needed).

**Requested by:** <your name> · **Team:** CTV Data Engineering · **Priority:** <e.g. Medium>
**External engine:** Trino 483 (open-source), Docker on an AWS EC2 VM · **Access protocol:** Unity Catalog
Iceberg REST Catalog API (`/api/2.1/unity-catalog/iceberg`) with OAuth bearer token + credential vending.

---

## What we need you to do

1. **Enable external data access on the Unity Catalog metastore.** It is off by default. A **metastore
   admin** enables it in the account/metastore settings ("External data access" / "Enable external data
   access"). Ref: *Enable external data access to Unity Catalog* (Databricks docs).

2. **Choose or create a test schema and at least one UC-managed *Iceberg* table** we can read **and** write.
   - Writes only work on **UC-managed Iceberg** tables (Delta/UniForm tables are read-only to external
     engines), so please make the write-test table managed Iceberg.
   - If it's easier, grant us `CREATE TABLE` on the schema and we'll create the test table ourselves.
   - *(Low priority, optional)* one Delta/UniForm table too, so we can also confirm a read-only path.

3. **Grant the service identity** (the principal whose token we'll use — see #4) these privileges on the
   test catalog/schema/table:
   - `USE CATALOG` on the catalog
   - `USE SCHEMA` on the schema
   - **`EXTERNAL USE SCHEMA`** on the schema  ← required for external Iceberg read/write
   - `SELECT` on the read/write test table(s)
   - `MODIFY` on the write-test table (and `CREATE TABLE` on the schema if we create it ourselves)

4. **Create a service identity and provide its credential** (via our secure secret channel — **not** in the
   ticket body). This is a **service principal** — a non-human/automation identity for our Trino engine;
   **no new Azure account or user mailbox is required**:
   - A **Databricks-managed service principal** is sufficient (created in the Databricks account/workspace;
     **no Microsoft Entra ID object needed**). Use an **Entra ID app-registration service principal** instead
     only if your policy requires all identities to live in Entra.
   - Grant this service principal the privileges in #3 (it — not a human user — is the identity that needs
     `EXTERNAL USE SCHEMA` etc.).
   - Provide either a **PAT generated for the service principal**, or its **OAuth client id + secret**
     (machine-to-machine) — whichever your policy prefers. (A human user's PAT can work for a quick smoke
     test, but a dedicated service principal is the right choice for an automated engine and avoids
     SSO / conditional-access problems.)

5. **Confirm credential vending is available** for the storage backing these tables (Azure ADLS), so the
   external client can obtain **short-lived storage credentials per table**. If a storage credential /
   external location must be associated with the schema for vending to work, please ensure it's configured
   for the test schema. (This is what lets our AWS-hosted Trino reach the Azure data files.)

## Values to send back to us

- **Workspace URL including the workspace id** — e.g. `https://adb-<workspace-id>.<region>.azuredatabricks.net`
  (the REST endpoint we'll call is `<workspace-url>/api/2.1/unity-catalog/iceberg`; without the workspace id
  the request 303-redirects to a login page).
- **UC catalog name** (top-level) to target — used as the REST "warehouse" parameter.
- **Test schema name** and the **test table name(s)** (managed Iceberg for read+write; optional Delta/UniForm
  for read-only).
- **The token / SP secret** via the secure channel (not in the ticket).

## Acceptance criteria (what we'll verify from the external engine)

1. List the schema and its tables through the REST catalog.
2. `SELECT` from the managed Iceberg test table (data read via **vended Azure credentials**).
3. `INSERT` / `CREATE TABLE` into a managed Iceberg test table (external write).
4. *(Optional)* `SELECT` from a Delta/UniForm table (read-only).

## Notes / context for you

- We are **not** requesting per-user access or fine-grained row/column policies for this test — a single
  service identity with the grants above is sufficient.
- **External write to UC-managed Iceberg is in Public Preview** on Databricks — we're validating it
  deliberately; a note on the current GA status/timeline would be helpful if you have it.
- No changes to your production tables are requested — just a dedicated **test schema** + the grants above.

## References (Databricks docs)

- Enable external data access to Unity Catalog: https://learn.microsoft.com/en-us/azure/databricks/external-access/admin
- Access Databricks tables from Apache Iceberg clients: https://learn.microsoft.com/en-us/azure/databricks/external-access/iceberg
- Unity Catalog credential vending for external system access: https://docs.databricks.com/aws/en/external-access/credential-vending
