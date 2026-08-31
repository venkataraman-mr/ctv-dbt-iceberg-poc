# DDL layout

DDL is split by catalog so the Nessie pipeline and the Polaris PoC stay isolated:

| Path | Contents |
| :-- | :-- |
| `ddl/nessie/` | The current **Nessie** pipeline's Trino DDL — `00_schemas.sql` … `09_silver_watermark_control_piece5.sql` — plus the detailed run-order [`README.md`](nessie/README.md). |
| `ddl/polaris/` | The **Polaris** PoC's Trino DDL — **empty for now**, filled while building the parallel Polaris pipeline (v3 + VARIANT tables). |
| `ddl/postgres/nessie/` | The Nessie pipeline's Postgres scripts — `piece3_tempwork`, `piece4_seed`, `piece4_sync_procs`, `piece5_occ_id_seq`. |
| `ddl/postgres/polaris/` | The Polaris PoC's Postgres scripts — **empty for now** (separate `tempwork` clone tables + sequences so the two runs don't collide). |

Run order and per-piece detail for the Nessie pipeline: [`ddl/nessie/README.md`](nessie/README.md).
