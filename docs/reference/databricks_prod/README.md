# Databricks production reference (read-only)

Snapshot of the two design docs that describe the **running Azure Databricks *production*
pipeline** (PySpark + Unity Catalog + Delta, medallion bronze→silver→gold). Copied here from
the Drive project folder so the Polaris/Iceberg design is versioned against a fixed reference.

| File | What it is |
| :-- | :-- |
| `claude.md` | Architecture map of the VXC / MRDPP platform — orchestration, medallion layout, the shared creative workflow, per-media occurrence flows, and the dbt+Iceberg migration coupling list. |
| `Digital_Flow_DeepDive.md` | Code-grounded, piece-by-piece deep dive of the Digital/CTV family (Pieces 1–5): ingestion, creative generation → Postgres, creative sync-back, raw→gold occurrence. Names `db_scripts/notebook_files/table_ddl/*.py` as **the schema source of truth**. |

## How to read these

These document **prod Databricks code**, *not* the dbt+Iceberg implementation. They are used to:

1. **Data-type parity** — pull the exact source column types (incl. **v3 / VARIANT**) that the
   Polaris Iceberg tables must mirror. The authoritative types live in the Databricks
   `table_ddl/*.py` (in the connected `@master_readonly_copy_venkat` repo), which these docs point to.
2. **Business semantics** — sanity-check the logic (occurrence gate, holding buffer, VX1 gold gate,
   creative state machine, watermark/concurrency behavior).

They are **NOT** the code to port. The dbt+Iceberg logic for all 5 pieces **already exists and is
validated** in the Nessie PoC (`dbt/` + `ingestion/`). The Polaris build reuses that logic; the
only substantive change is **v3 + VARIANT** (data-type parity). See
`docs/runbooks/polaris/polaris_pipeline_runbook.md`.

> ⚠️ **Stale guidance inside these docs.** `Digital_Flow_DeepDive.md` (and `claude.md` §7) predates the
> catalog decision and says *"VARIANT → map to string/JSON for the migration."* That was the **Nessie-era
> workaround** (Nessie doesn't serve v3). **Superseded:** Polaris was chosen specifically to keep VARIANT,
> so VARIANT is preserved end-to-end (pipeline **and** spend/reference sync). Ignore the map-to-string note.

_Source: Drive project folder `1UK1t-JYEPEZu41ZcVZD7gK9szvzZ2kye`. Read-only snapshot — the Drive copies remain the living originals._
