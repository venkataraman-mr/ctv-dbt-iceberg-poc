# Piece 5 — digital occurrence gold flow (raw → gold): high-level flow & build plan

Status: **IN PROGRESS — plan locked, Half A stage 1 built (2026-08-10).** Port of the Databricks
`DigitalRawocctoGoldocc` job (`occurrences/digital/class_files/raw_occ_to_gold_occ.py`, 1327 lines) to our
Trino/dbt/Iceberg stack: read new raw occurrences from `bronze.digital_raw_occurrence`, gate them against the
Piece-4-populated `gold.creative`, enrich, and MERGE into `gold.digital_gold_occurrence` (or park in a hold
buffer). This is the last piece; it also **lights up occurrence-id + last-seen** (the Piece-4 tasks that read
`gold.digital_gold_occurrence`). Companion to the creative sync plan (`docs/ctv_creative_sync_plan.md`).

The flow is named "digital" (not CTV-only) deliberately — it is the same pipeline for all digital media /
providers (BIS, BISSocial, PlayOn, AVOD BISCTV). CTV is one provider code (`'AVOD BISCTV'`).

## 1. Shape of the job — two halves, two watermarks

```
HALF A  DIGITAL_RAW_OCC_TO_GOLD_OCC (version, bronze.digital_raw_occurrence — append-only):
  new raw occ (CDF) ∪ hold buffer (silver.digital_staging_occurrence)
    → deployment chain / roles / mediator (daisy chains)
    → enrich (media_property, market, origin_channel, is_house_ad) + gate against gold.creative
    → Not Hold: + prelim spend → MERGE gold.digital_gold_occurrence (partition capture_month)
    → MERGE buffer: park Hold / release Not Hold  → advance watermark

HALF B  DIGITAL_CRTV_CHANGES_TO_GOLD_OCC (Databricks: version on gold.creative; OURS: TIMESTAMP on
        gold.creative.updated_timestamp — gold.creative is MERGE-written, so Trino version-CDF is invalid):
  changed creatives → re-evaluate held staging occurrences whose creative now qualifies → gold + release buffer
    → update delete_flag / house_ad flags  → advance watermark
```

Sequencing (per §8.7 + confirmed): **Half A first, then Half B.**

## 2. Cross-cutting decisions (locked 2026-08-10)

- **Full BIS deployment-chain machinery + full enrichment** (Venkat): build the daisy-chain →
  `gold.digital_deployment_chain{,_role,_mediator}` persistence + the purchase-method md5 + the deployment-chain
  gate branches, and populate the complete `gold.digital_gold_occurrence` row (prelim spend, market_id,
  origin_channel_id, is_house_ad). **CTV nuance:** the gate's `deployment_chain` join is restricted to
  `provider_code IN ('BIS','BISSocial')`, so for `'AVOD BISCTV'` those branches never fire — CTV resolves via
  the **simple branch** (creative present as Advert with `primary_product_id` set → Not Hold). The machinery is
  built faithfully and is simply inert for CTV rows.
- **Version watermark on append-only bronze; timestamp watermark on MERGE-written gold.creative.** Trino
  `system.table_changes` is valid on `bronze.digital_raw_occurrence` (append-only), so Half A reuses the Piece-1
  / Job-A version-CDF pattern. `gold.creative` is MERGE-written (delete files) → Half B uses a timestamp
  watermark on `updated_timestamp` (same adaptation as first-seen-info).
- **Reuse the validated Piece-3 CTEs.** `crtv_staging_candidate` is already a faithful transliteration of the
  sibling `DigitalRawocctoCrtvStaging` and shares the version-CDF read, `media_property`/`market`/`source_channel`
  joins, `provider_dma_city_name`, and the market fallback (gmm → mpdpm.market_id → 302). Half A reuses these.
- **Split for Trino limits.** Half A is staged into small models (like Piece 4 / task 8) to stay under the
  150-stage limit and to stream, never hash, any large table.
- **US only, non-retransmit, insert-only** raw occurrences (`country_iso_2_code = 'US'`,
  `coalesce(retransmit,false)=false`, `_change_type='insert'`), deduped to the newest per
  (country, provider_code, provider_occurrence_id).

## 3. Staged dbt model plan (Half A) — models tagged `p5_digital_raw_to_gold_occ`

| Stage | Model | Port of | Notes |
|---|---|---|---|
| 1 | `digital_occ_raw_cdf` | STEP-1 get_new_raw_occurrence_data_cdf | **BUILT.** Version-CDF read of bronze (first run = full read); US/insert/non-retransmit; dedup by (country, provider, occurrence_id). |
| 2 | `digital_occ_deploychain` | STEP-2 persist_roles_mediator + persist_deployment_chain | Explode daisy_chain → MERGE `gold.digital_deployment_chain_role` / `_mediator` / `digital_deployment_chain` (purchase_method + col2 md5). |
| 3 | `digital_occ_combined` | STEP-3 combine (union + media/market) | UNION new raw + staging buffer, dedup (prefer staging), `provider_raw_json`, landing-page domain, daisy transforms (col1/col2/md5), media_property/market/source_channel joins (reused). |
| 4 | `digital_occ_classified` | STEP-3 gate | Join `gold.creative` (parent+child, Advert), `creative_dedupe_map` (Map), `origin`, `deployment_chain`, `product_flatten` → `occurrence_hold_flag`, `is_house_ad`, `deployment_chain_id`, `creative_id`, `media_property_id`, `market_id`. |
| 5 | `digital_occ_gold` (writer) | STEP-4/5/6 persist_to_gold_occ + persist_to_intermediate_staging + watermark | Not-Hold + prelim spend (property → media fallback) → MERGE `gold.digital_gold_occurrence` (on country+capture_date+month+occurrence_id, month-pruned); MERGE `silver.digital_staging_occurrence` (park Hold '1-RawOccurrence' / release Not-Hold '2-IntermediateStaging'); `watermark_version_finish`. |

Half B: `digital_occ_crtv_changes` — timestamp watermark on `gold.creative.updated_timestamp`; re-evaluate held
staging occurrences whose creative now qualifies → gold + release; `update_gold_occ_delete_flag` +
`update_house_ad_flag`; advance `DIGITAL_CRTV_CHANGES_TO_GOLD_OCC`.

## 4. Watermark inventory (ddl/09)

| Watermark | Kind | Source | Half |
|---|---|---|---|
| `DIGITAL_RAW_OCC_TO_GOLD_OCC` | VERSION | `bronze.digital_raw_occurrence` (append-only) | A |
| `DIGITAL_CRTV_CHANGES_TO_GOLD_OCC` | TIMESTAMP | `gold.creative.updated_timestamp` (MERGE-written) | B |

## 5. The gate (occurrence_hold_flag) — faithful

```
-- BIS/BISSocial (daisy-chain) branches (inert for CTV):
Not Hold  when daisy md5 present AND creative(parent)=Advert w/ primary_product_id AND child present AND deployment_chain_id present
Not Hold  when daisy md5 present AND creative(original)=Advert w/ primary_product_id AND deployment_chain_id present
-- simple branches (CTV path):
Not Hold  when creative(parent)=Advert w/ primary_product_id AND child present
Not Hold  when creative(original)=Advert w/ primary_product_id
else Hold
```
`creative` is joined twice: `crtv` on `coalesce(parent_creative_url_hash, creative_url_hash)` and `crtv_child`
on `creative_url_hash`, both filtered `classification_type = 'Advert'`. `deployment_chain` join is BIS/BISSocial
only. Held occurrences go to `silver.digital_staging_occurrence` and are released by Half B when the creative appears.

## 6. Sources / targets

Reads: `bronze.digital_raw_occurrence` (ref), `silver.digital_staging_occurrence`, `gold.creative`,
`silver.creative_dedupe_map`; reference: `km_preparation_gold_db.media_property_flatten`,
`km_preparation_db.media_property_data_provider_map` / `source_channel` / `data_provider` / `origin`,
`reference.provider_global_market_map` / `global_market`, `productcentral.product_flatten`,
`spend.digital_dmi_prelim_spend_average_by_property` / `_by_media`.
Writes: `gold.digital_gold_occurrence`, `gold.digital_deployment_chain{,_role,_mediator}`,
`silver.digital_staging_occurrence` (buffer).

## 7. Constants (occurrences/common/constants.py)

`country_code_US='US'`; `status_flag_active='ACTIVE'`; `bc_active_status_id=92`; `match_type_map='Map'`;
`source_bis_code='BIS'`, `source_bis_social_code='BISSocial'`, `source_playon_code='PlayOn'`,
`source_bis_ctv_code='AVOD BISCTV'`; `classification_type_Advert='Advert'` / `NonAd` / `BadAd`; market default `302`.

## 8. Open flags / to validate

- **First-run full load** — Half A stage 1 with the version watermark NULL does a full read of bronze (~820k rows
  from Piece 1). The classify + gold MERGE process the whole set once; then incremental.
- **Daisy-chain md5 fidelity** — the `purchase_method + '|'-joined sorted id.roleId` md5 must byte-match prod
  (Spark `md5` == Trino `to_hex(md5(to_utf8(...)))`), same class of risk as the creative reverse-translation.
- **Column-name reconciliation** — `media_property_flatten` / `product_flatten` / `origin` column names are the
  prod names lower-cased for Trino; confirm on the VM.
- **is_house_ad** — via `product_flatten` parent/subsidiary vs `media_property_flatten.vx0_parent_company_id`.

## 9. Learnings (updated as built)

- (stage 1) Reuses the Piece-1/Job-A version-CDF branch pattern (first-run full / no-change / incremental via
  `system.table_changes`); dedup ordered by `capture_timestamp desc` (Trino has no `_commit_timestamp` CDF column).
- Two-phase version watermark: `watermark_version_begin` (stage 1) pins the end snapshot + marks InProgress;
  `watermark_version_finish` (the writer, stage 5) promotes it AFTER the gold write, so a mid-run failure doesn't
  advance the watermark.

## 10. Run commands (filled as built)

```
# once: seed the two occurrence watermarks
docker exec -i trino trino --catalog iceberg -f /dev/stdin < ddl/09_silver_watermark_control_piece5.sql
# stage 1 (built): version-CDF read of bronze
docker compose run --rm dbt dbt run --select digital_occ_raw_cdf
# full Half A / full job commands: added as stages 2-5 + Half B land.
```
