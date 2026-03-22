-- Add quality tracking columns to import_files for ingestion hardening.

BEGIN;

ALTER TABLE import_files
    ADD COLUMN IF NOT EXISTS skipped_canonical_count INT;

ALTER TABLE import_files
    ADD COLUMN IF NOT EXISTS duplicate_rate NUMERIC(5,2);

ALTER TABLE import_files
    ADD COLUMN IF NOT EXISTS numeric_sanitization_count INT;

COMMIT;