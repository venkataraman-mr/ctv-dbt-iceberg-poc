# Piece 5 — digital occurrence gold flow (raw → gold): high-level flow & build plan

Status: **COMPLETE (built) & VALIDATED end-to-end (2026-08-11).** Port of the Databricks
`DigitalRawocctoGoldocc` job (`occurrences/digital/class_files/raw_occ_to_gold_occ.py`, 1327 lines) to our
Trino/dbt/Iceberg stack: read new raw occurrences from `bronze.digital_raw_occurrence`, gate them against the
Piece-4-populated `gold.creative`, enrich, and MERGE into `gold.digital_gold_occurrence` (or park in a hold
buffer). This is the LAST piece — with it the **whole PoC (Pieces 1–5) is complete**. It also **lit up
occurrence-id + last-seen** (the Piece-4 tasks that read `gold.digital_gold_occurrence`; validated after Half A).
The full job runs as `dbt run --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC`. Companion to the creative sync plan
(`docs/ctv_creative_sync_plan.md`).

First full run (2026-08-11): 811,764 raw → 746,245 gold occurrences (occurrence_id block [75,000,000,000 …
75,000,746,244]) + 65,519 held in the buffer; deployment chains/roles/mediators matched prod; Half B a clean
no-op (Half A had already resolved parents). Companion to the creative sync plan.

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

## 3. Staged dbt model plan — all models tagged `DIGITAL_RAW_OCC_TO_GOLD_OCC` (all BUILT + VALIDATED)

| Stage | Model | Port of | Notes / first-run result |
|---|---|---|---|
| A1 | `digital_occ_raw_cdf` | STEP-1 get_new_raw_occurrence_data_cdf | Version-CDF read of bronze (first run = full read = **811,764**); US/insert/non-retransmit; dedup by (country, provider, occurrence_id). |
| A2 | `digital_occ_deploychain` | STEP-2 persist_roles_mediator + persist_deployment_chain | Transform each DISTINCT daisy_chain array in place (sorted by index) → col1/col2 + md5 → MERGE `gold.digital_deployment_chain{,_role,_mediator}`. **2 chains / 1 role / 1 mediator; matches prod.** deployment_chain_id = `from_big_endian_64(xxhash64(md5))`. |
| A3 | `digital_occ_combined` | STEP-3 combine (union + media/market) | UNION new raw + staging buffer, dedup (prefer staging), `provider_raw_json`, landing-page domain, occ daisy md5, media_property/market/source_channel + dedupe-map parent joins (Piece-3 reuse). **811,764 rows / 534,640 with parent / 211 markets.** |
| A4 | `digital_occ_classified` | STEP-3 gate | Join `gold.creative` (parent+child, Advert), `creative_dedupe_map` (Map), `origin`, `deployment_chain`, `product_flatten` (ppf1/ppf2), `media_property_flatten_vx0_vw` → `occurrence_hold_flag`, `delete_flag`, `is_house_ad`, `deployment_chain_id`, `creative_id`. video-source/non-video-mime filter. **746,245 Not Hold / 65,519 Hold; 47,273 house ads.** |
| A5 | `digital_occ_gold` (writer) | STEP-4/5/6 persist_to_gold_occ + persist_to_intermediate_staging + watermark | Not-Hold + prelim spend (property → media fallback) + **occurrence_id from the 75B Postgres sequence** → MERGE `gold.digital_gold_occurrence` (on country+capture_date+month+occurrence_id); MERGE `silver.digital_staging_occurrence` (park Hold / release Not-Hold); `watermark_version_finish`. **746,245 gold occ (id 75,000,000,000…); 65,519 held.** |
| B  | `digital_occ_crtv_changes` | STEP-8/9/9.1 get_new_creative_data_cdf + re-parent + delete_flag | TIMESTAMP watermark on `gold.creative.updated_timestamp`; UPDATE existing gold occ: re-parent on dedup mapping change + set delete_flag on NonAd/BadAd; advance watermark. **33,338 changed / 0 gold updates (Half A already resolved parents).** update_house_ad_flag PARKED (needs digital_spend_availability). |

Hold RELEASE is handled by Half A (it unions the buffer each run and releases Not-Hold '2-IntermediateStaging');
Half B only UPDATES existing gold rows.

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

## 8. Flags — resolved 2026-08-11 (+ remaining)

- **First-run full load** — RESOLVED: 811,764 bronze rows processed once; watermark promoted; now incremental.
- **Daisy-chain md5 fidelity** — RESOLVED: deployment chain md5/roles/mediators matched prod delta tables.
- **Column-name reconciliation** — RESOLVED: all reference-table column names (media_property/product_flatten/
  origin/spend) ran clean on the VM.
- **is_house_ad** — RESOLVED: 47,273 house ads flagged via `product_flatten` parent/subsidiary vs
  `media_property_flatten_vx0_vw.vx0_parent_company_id`.
- **REMAINING (parked):** `update_house_ad_flag` (Half B STEP-10) — needs `gold.digital_spend_availability`
  (populated by `update_spend_availability`, which we skipped) + `source_ic_code`/`source_EDO_code`. Secondary
  house-ad refresh; add if/when spend-availability lands.

## 9. Learnings (2026-08-11)

- **occurrence_id: Postgres sequence, NOT xxhash64** (Venkat). `gold.digital_gold_occurrence.occurrence_id` is
  an IDENTITY column in prod (not in the gold INSERT). We reserve it from `tempwork.occurrence_id_seq_ctv_poc`
  (START 75,000,000,000) via `reserve_occurrence_ids(n)` — the exact block-reservation pattern used for
  `creative_id` (Piece 3): `system.execute` the reservation proc, read the block back via `system.query`, assign
  `block_start + row_number()-1`. First run produced ids [75,000,000,000 … 75,000,746,244]. (xxhash64 is used
  only for `deployment_chain_id`, which is acceptable per Venkat.)
- **Transform the daisy_chain array IN PLACE over DISTINCT chains** — exploding per-occurrence then `array_agg`
  grouped by chain duplicates each chain's elements once per occurrence → "concatenated string is too large".
  Dedup to distinct chains first and `array_sort(transform(json_parse(daisy_chain) …))` in place per row.
- **Reuses the Piece-1/Job-A version-CDF branch pattern** (first-run full / no-change / incremental via
  `system.table_changes`); dedup ordered by `capture_timestamp desc` (Trino has no `_commit_timestamp` CDF column).
- **Two-phase version watermark:** `watermark_version_begin` (A1) pins the end snapshot + marks InProgress;
  `watermark_version_finish` (the writer, A5) promotes it AFTER the gold write, so a mid-run failure doesn't advance.
- **Half B on gold.creative uses a TIMESTAMP watermark, not version-CDF** — gold.creative is MERGE-written
  (delete files) so Trino version-CDF is invalid there; timestamp watermark on `updated_timestamp` (like first-seen-info).
- **Reused the validated Piece-3 CTEs** (media_property/market/source_channel + market fallback) verbatim — big
  time saver and already prod-checked.
- **CTV nuance validated:** `deployment_chain_id = -1` on the CTV gold rows (the dc join is BIS/BISSocial only);
  prelim_spend/impressions null (CTV media properties aren't in the prelim-spend average tables — the join is
  clean, just no match). Both faithful to prod.
- **Cleanup + full-job:** all 6 models tagged `DIGITAL_RAW_OCC_TO_GOLD_OCC`; `on-run-end` drops the bronze scratch
  after a clean run (opt out `--vars 'keep_DIGITAL_RAW_OCC_TO_GOLD_OCC_tables: true'`). Full job:
  `dbt run --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC` (DAG order verified: A1 → {A2,A3} → A4 → A5 → B).
- **Lights up Piece 4:** after Half A populated gold occurrences, `crtv_occid_update` resolved 13,333
  `first_seen_occurrence_id`s and `crtv_lastseen_update` refreshed `last_seen_timestamp` for the 21,750
  creatives-with-occurrences (`CTV_LAST_SEEN_DIGITAL` advanced off 1900).

## 10. Run commands

```
# once: seed the two occurrence watermarks + the 75B occurrence_id sequence/proc (Postgres, via SQL client)
docker exec -i trino trino --catalog iceberg -f /dev/stdin < ddl/09_silver_watermark_control_piece5.sql
#   \i ddl/postgres/piece5_occ_id_seq_ctv_poc.sql   (on Postgres; needs tempwork_admin_role)

# FULL JOB (all 6 in DAG order + scratch cleanup):
docker compose run --rm dbt dbt run --select tag:DIGITAL_RAW_OCC_TO_GOLD_OCC

# single stage (chained -> run the whole tag, or add --vars 'keep_DIGITAL_RAW_OCC_TO_GOLD_OCC_tables: true' to keep scratch):
docker compose run --rm dbt dbt run --select digital_occ_raw_cdf
docker compose run --rm dbt dbt run --select digital_occ_raw_cdf
# full Half A / full job commands: added as stages 2-5 + Half B land.
```
