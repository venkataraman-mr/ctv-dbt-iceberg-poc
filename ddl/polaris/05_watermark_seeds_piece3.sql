-- Piece 3 watermark seeds on POLARIS. Run ONCE before the first Job A / Job B run (skip a row if it
-- already exists). All three are VERSION-based (read bronze.digital_raw_occurrence via
-- system.table_changes); NULL last_commit_version => first run does the one-time full read, then advances.
-- Requires polaris.silver.watermark_control to exist (ddl/polaris/03).
--   Job A            = DIGITAL_RAW_OCC_TO_CRTV_STAGING             (creative staging + first-seen seed)
--   Job B first-seen = DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE   (first-seen earliest-occurrence update)
--   Job B summary    = DIGITAL_RAW_OCC_SUMMARY_PSQL               (occurrence summary + park/release buffer)

INSERT INTO polaris.silver.watermark_control
    (watermark_name, start_timestamp, end_timestamp, last_commit_version,
     current_commit_version, transaction_status, created_timestamp, updated_timestamp)
VALUES ('DIGITAL_RAW_OCC_TO_CRTV_STAGING', NULL, NULL, NULL, NULL,
        'SUCCEEDED', current_timestamp, current_timestamp);

INSERT INTO polaris.silver.watermark_control
    (watermark_name, start_timestamp, end_timestamp, last_commit_version,
     current_commit_version, transaction_status, created_timestamp, updated_timestamp)
VALUES ('DIGITAL_RAW_OCC_TO_CRTV_FIRST_SEEN_UPDATE', NULL, NULL, NULL, NULL,
        'SUCCEEDED', current_timestamp, current_timestamp);

INSERT INTO polaris.silver.watermark_control
    (watermark_name, start_timestamp, end_timestamp, last_commit_version,
     current_commit_version, transaction_status, created_timestamp, updated_timestamp)
VALUES ('DIGITAL_RAW_OCC_SUMMARY_PSQL', NULL, NULL, NULL, NULL,
        'SUCCEEDED', current_timestamp, current_timestamp);
