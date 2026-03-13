-- Add PDD SKU-daily layered import structures.

BEGIN;

INSERT INTO source_file_types (
    platform_id,
    file_type_code,
    file_type_name,
    grain_code,
    version,
    description,
    is_active
)
SELECT
    platform_id,
    'pdd_sku_daily',
    'PDD SKU Daily Sales',
    'sku_day',
    1,
    'Pinduoduo workbook SKU source export loaded into raw rows, PDD SKU staging, and canonical SKU-day facts.',
    TRUE
FROM platforms
WHERE platform_code = 'pdd'
ON CONFLICT (platform_id, file_type_code, version) DO NOTHING;

CREATE TABLE stg_pdd_sku_day_sales (
    stg_pdd_sku_day_sales_id BIGSERIAL PRIMARY KEY,
    import_file_id INT NOT NULL REFERENCES import_files(import_file_id),
    raw_import_row_id BIGINT NOT NULL REFERENCES raw_import_rows(raw_import_row_id),
    shop_id INT NOT NULL REFERENCES shops(shop_id),
    source_row_number INT NOT NULL,
    sales_date DATE NOT NULL,
    product_name TEXT,
    product_id TEXT,
    merchant_sku_code TEXT,
    product_specification TEXT,
    gross_quantity NUMERIC(14,2),
    gross_product_amount NUMERIC(14,2),
    merchant_net_amount NUMERIC(14,2),
    sku_id TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (raw_import_row_id)
);

CREATE INDEX idx_stg_pdd_sku_day_sales_import_file_id
    ON stg_pdd_sku_day_sales(import_file_id);

CREATE INDEX idx_stg_pdd_sku_day_sales_shop_date
    ON stg_pdd_sku_day_sales(shop_id, sales_date);

CREATE INDEX idx_stg_pdd_sku_day_sales_sku_id
    ON stg_pdd_sku_day_sales(sku_id);

CREATE TABLE fact_sku_day_sales (
    fact_sku_day_sales_id BIGSERIAL PRIMARY KEY,
    shop_id INT NOT NULL REFERENCES shops(shop_id),
    sales_date DATE NOT NULL,
    product_id TEXT NOT NULL DEFAULT '',
    sku_id TEXT NOT NULL DEFAULT '',
    merchant_sku_code TEXT NOT NULL DEFAULT '',
    product_specification TEXT NOT NULL DEFAULT '',
    gross_quantity NUMERIC(14,2),
    gross_product_amount NUMERIC(14,2),
    merchant_net_amount NUMERIC(14,2),
    import_file_id INT NOT NULL REFERENCES import_files(import_file_id),
    source_row_count INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (shop_id, sales_date, product_id, sku_id, merchant_sku_code, product_specification)
);

CREATE INDEX idx_fact_sku_day_sales_shop_id
    ON fact_sku_day_sales(shop_id);

CREATE INDEX idx_fact_sku_day_sales_sales_date
    ON fact_sku_day_sales(sales_date);

CREATE INDEX idx_fact_sku_day_sales_import_file_id
    ON fact_sku_day_sales(import_file_id);

CREATE INDEX idx_fact_sku_day_sales_sku_id
    ON fact_sku_day_sales(sku_id);

COMMIT;
