-- Bronze canonical CTV/Digital raw occurrence (append-only) on POLARIS. Produced by the dbt-trino
-- staging->raw incremental model (dbt_polaris/models/occurrences/digital_raw_occurrence.sql), which
-- appends into this table. Pre-created here so structure exists before the pipeline runs; the model's
-- config (partitioning/sorted_by/format_version) matches this DDL, so a dbt --full-refresh recreates
-- it identically.
--
-- 42 columns, matching the source Databricks bronze.digital_raw_occurrence table_ddl EXACTLY
-- (bronze.py). Spark->Trino/Iceberg type mapping: STRING->VARCHAR, INT->INTEGER, BIGINT->BIGINT,
-- SMALLINT->SMALLINT, DATE->DATE, TIMESTAMP->TIMESTAMP(6) WITH TIME ZONE, BOOLEAN->BOOLEAN.
--
-- *** v3 + VARIANT (the hard requirement). ***  The source has exactly TWO VARIANT columns —
-- daisy_chain and raw_json — kept as real `variant` here (Nessie stored them as VARCHAR; that
-- workaround is retired on Polaris). Table is format_version = 3 so it can hold variant. Trino/dbt
-- writes these via CAST(... AS variant).
--
-- *** NO sorted_by (Polaris/variant limitation). ***  The legacy CLUSTER BY was (capture_month,
-- provider_occurrence_id). We keep partitioning by capture_month, but we CANNOT add
-- sorted_by=provider_occurrence_id: Trino's sort-on-write serializes every column (incl. the variant)
-- through its legacy Hive-type mapping, which rejects `variant` ("Unsupported Hive type: variant") —
-- confirmed: same INSERT succeeds with partitioning only, fails the moment sorted_by is present. So
-- any table with a VARIANT column must omit sorted_by. Perf-only loss (clustering); correctness and
-- partition pruning are unaffected.
CREATE TABLE IF NOT EXISTS polaris.bronze.digital_raw_occurrence (
    country_iso_2_code                 VARCHAR,
    provider_code                      VARCHAR,
    source_channel                     VARCHAR,
    provider_occurrence_id             VARCHAR,
    provider_creative_id               BIGINT,
    provider_source_id                 BIGINT,
    capture_date                       DATE,
    capture_month                      INTEGER,
    capture_timestamp                  TIMESTAMP(6) WITH TIME ZONE,
    eventhub_enqueued_timestamp        TIMESTAMP(6) WITH TIME ZONE,
    created_timestamp                  TIMESTAMP(6) WITH TIME ZONE,
    region_dma_id                      INTEGER,
    region_dma_name                    VARCHAR,
    region_country_code                VARCHAR,
    region_city_id                     INTEGER,
    region_city_name                   VARCHAR,
    region_state_id                    INTEGER,
    region_state_name                  VARCHAR,
    creative_type                      VARCHAR,
    creative_mime_type                 VARCHAR,
    publisher_id                       BIGINT,
    publisher_domain                   VARCHAR,
    creative_width                     INTEGER,
    creative_height                    INTEGER,
    creative_duration                  INTEGER,
    creative_url                       VARCHAR,
    creative_url_hash                  BIGINT,
    retransmit                         BOOLEAN,
    provider_campaign_id               BIGINT,
    provider_campaign_product_id       BIGINT,
    provider_campaign_advertiser_id    BIGINT,
    provider_campaign_name             VARCHAR,
    provider_campaign_product_name     VARCHAR,
    provider_campaign_advertiser_name  VARCHAR,
    provider_campaign_description      VARCHAR,
    provider_campaign_landing_page     VARCHAR,
    occurrence_description             VARCHAR,
    occurrence_link_url                VARCHAR,
    daisy_chain                        VARIANT,
    purchase_method_id                 INTEGER,   -- source is SMALLINT; Iceberg has no 16-bit int and
                                                  -- the Polaris/Iceberg REST connector rejects smallint
                                                  -- ("Type not supported for Iceberg: smallint") -> INTEGER
    ad_insertion_point                 VARCHAR,
    raw_json                           VARIANT
)
WITH (
    format = 'PARQUET',
    format_version = 3,
    partitioning = ARRAY['capture_month'],
    -- NO sorted_by: incompatible with the VARIANT columns on write (see note above).
    -- SHARED TABLE across the Digital family (Digital / Social / AVOD-CTV all append here,
    -- disambiguated by provider_code/source_channel). When multiple media ingestion jobs run in
    -- parallel they append concurrently; max_commit_retry lets colliding Iceberg commits retry
    -- instead of failing (append-only + NOT EXISTS guard => safe). Conflicts are capture_month-scoped.
    max_commit_retry = 20
);
