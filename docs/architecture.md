# Architecture

Source of truth: **Google Drive → `CTV_occurrence_flow_architecture_MASTER_v2`**.

One-paragraph summary: open-source lakehouse on one EC2 VM — PyIceberg ingestion (no Spark),
Trino + dbt transforms, Apache Iceberg on S3, Nessie catalog (RocksDB on EBS, auth off).
Reference data: scheduled sync Azure->Iceberg now, UC managed Iceberg direct-read later.
Creative flow: push -> Postgres creative_staging -> creatives.creative (cron-loaded) ->
dbt sync-back -> gold.creative; IDs from prod Postgres sequences (UCA dropped); key = creative_url_hash.
Occurrence flow: bronze -> raw->gold (Half A gate+holding, Half B timestamp watermark).
Concurrency: Iceberg snapshot isolation + commit-conflict retry + per-job watermarks.
