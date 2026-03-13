-- Add workbook sheet inventory for workbook-level imports.

BEGIN;

CREATE TABLE import_file_sheets (
    import_file_sheet_id BIGSERIAL PRIMARY KEY,
    import_file_id INT NOT NULL REFERENCES import_files(import_file_id),
    sheet_name VARCHAR(255) NOT NULL,
    sheet_index INT NOT NULL,
    sheet_role VARCHAR(100),
    is_relevant BOOLEAN NOT NULL DEFAULT FALSE,
    detected_row_count INT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (import_file_id, sheet_index),
    UNIQUE (import_file_id, sheet_name)
);

CREATE INDEX idx_import_file_sheets_import_file_id
    ON import_file_sheets(import_file_id);

CREATE INDEX idx_import_file_sheets_role
    ON import_file_sheets(sheet_role);

COMMIT;
