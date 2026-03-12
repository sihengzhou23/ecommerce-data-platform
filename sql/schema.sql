-- schema.sql
-- Ecommerce Data Platform v1
-- Focus: PDD shop-daily sales + import lineage

CREATE TABLE platforms (
    platform_id SERIAL PRIMARY KEY,
    platform_code VARCHAR(20) UNIQUE NOT NULL,
    platform_name VARCHAR(100) NOT NULL
);

CREATE TABLE shops (
    shop_id SERIAL PRIMARY KEY,
    platform_id INT NOT NULL REFERENCES platforms(platform_id),
    shop_code VARCHAR(50) UNIQUE NOT NULL,
    shop_name VARCHAR(255),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE import_files (
    import_file_id SERIAL PRIMARY KEY,
    platform_id INT NOT NULL REFERENCES platforms(platform_id),
    shop_id INT REFERENCES shops(shop_id),
    report_type VARCHAR(50) NOT NULL,
    file_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    sheet_name VARCHAR(255),
    file_hash VARCHAR(128),
    row_count_raw INT,
    imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

CREATE TABLE fact_shop_day_sales (
    fact_shop_day_sales_id SERIAL PRIMARY KEY,
    shop_id INT NOT NULL REFERENCES shops(shop_id),
    sales_date DATE NOT NULL,
    shop_visitor_count INT,
    shop_pageview_count INT,
    product_visitor_count INT,
    product_pageview_count INT,
    buyer_count INT,
    order_count INT,
    gross_sales_amount NUMERIC(14,2),
    conversion_rate NUMERIC(12,6),
    avg_order_value NUMERIC(14,2),
    uv_value NUMERIC(14,2),
    product_favorite_user_count INT,
    refund_amount NUMERIC(14,2),
    import_file_id INT REFERENCES import_files(import_file_id),
    source_row_number INT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (shop_id, sales_date)
);

CREATE INDEX idx_shops_platform_id
    ON shops(platform_id);

CREATE INDEX idx_import_files_platform_id
    ON import_files(platform_id);

CREATE INDEX idx_import_files_shop_id
    ON import_files(shop_id);

CREATE INDEX idx_fact_shop_day_sales_shop_id
    ON fact_shop_day_sales(shop_id);

CREATE INDEX idx_fact_shop_day_sales_sales_date
    ON fact_shop_day_sales(sales_date);

CREATE INDEX idx_fact_shop_day_sales_import_file_id
    ON fact_shop_day_sales(import_file_id);

-- Seed platform row for Pinduoduo
INSERT INTO platforms (platform_code, platform_name)
VALUES ('pdd', 'Pinduoduo')
ON CONFLICT (platform_code) DO NOTHING;
