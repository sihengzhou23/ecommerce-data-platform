-- Migrate the initial PDD shop-daily slice to the layered import model.
-- This migration:
-- 1. adds source_file_types
-- 2. adds raw_import_rows
-- 3. adds stg_pdd_shop_day_sales
-- 4. narrows fact_shop_day_sales to canonical shop-day metrics
-- 5. backfills raw/staging lineage for existing fact rows

BEGIN;

DO $$
DECLARE
    missing_fact_columns TEXT;
BEGIN
    SELECT string_agg(required_columns.column_name, ', ' ORDER BY required_columns.column_name)
    INTO missing_fact_columns
    FROM (
        VALUES
            ('shop_id'),
            ('sales_date'),
            ('shop_visitor_count'),
            ('shop_pageview_count'),
            ('product_visitor_count'),
            ('product_pageview_count'),
            ('buyer_count'),
            ('order_count'),
            ('gross_sales_amount'),
            ('conversion_rate'),
            ('avg_order_value'),
            ('uv_value'),
            ('product_favorite_user_count'),
            ('refund_amount'),
            ('import_file_id'),
            ('source_row_number'),
            ('created_at')
    ) AS required_columns(column_name)
    WHERE NOT EXISTS (
        SELECT 1
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND c.table_name = 'fact_shop_day_sales'
          AND c.column_name = required_columns.column_name
    );

    IF missing_fact_columns IS NOT NULL THEN
        RAISE EXCEPTION 'Migration expects old fact_shop_day_sales columns, missing: %', missing_fact_columns;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND c.table_name = 'import_files'
          AND c.column_name = 'report_type'
    ) THEN
        RAISE EXCEPTION 'Migration expects import_files.report_type from the old schema.';
    END IF;
END
$$;

ALTER TABLE shops
    DROP CONSTRAINT IF EXISTS shops_shop_code_key;

ALTER TABLE shops
    ADD CONSTRAINT shops_platform_id_shop_code_key
    UNIQUE (platform_id, shop_code);

CREATE TABLE source_file_types (
    source_file_type_id SERIAL PRIMARY KEY,
    platform_id INT NOT NULL REFERENCES platforms(platform_id),
    file_type_code VARCHAR(100) NOT NULL,
    file_type_name VARCHAR(255) NOT NULL,
    grain_code VARCHAR(50) NOT NULL,
    version INT NOT NULL DEFAULT 1,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (platform_id, file_type_code, version)
);

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
    'pdd_shop_daily',
    'PDD Shop Daily Sales',
    'shop_day',
    1,
    'Pinduoduo shop daily Excel export loaded into raw rows, PDD staging, and canonical shop-day facts.',
    TRUE
FROM platforms
WHERE platform_code = 'pdd';

ALTER TABLE import_files
    ADD COLUMN source_file_type_id INT;

UPDATE import_files i
SET source_file_type_id = sft.source_file_type_id
FROM source_file_types sft
WHERE i.platform_id = sft.platform_id
  AND i.report_type = 'shop_daily'
  AND sft.file_type_code = 'pdd_shop_daily'
  AND sft.version = 1;

ALTER TABLE import_files
    ALTER COLUMN source_file_type_id SET NOT NULL;

ALTER TABLE import_files
    ADD CONSTRAINT import_files_source_file_type_id_fkey
    FOREIGN KEY (source_file_type_id)
    REFERENCES source_file_types(source_file_type_id);

CREATE INDEX idx_source_file_types_platform_id
    ON source_file_types(platform_id);

CREATE INDEX idx_import_files_source_file_type_id
    ON import_files(source_file_type_id);

CREATE TABLE raw_import_rows (
    raw_import_row_id BIGSERIAL PRIMARY KEY,
    import_file_id INT NOT NULL REFERENCES import_files(import_file_id),
    row_number INT NOT NULL,
    sheet_name VARCHAR(255),
    raw_payload JSONB NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_raw_import_rows_import_file_id
    ON raw_import_rows(import_file_id);

CREATE UNIQUE INDEX idx_raw_import_rows_file_sheet_row
    ON raw_import_rows(import_file_id, COALESCE(sheet_name, ''), row_number);

CREATE TABLE stg_pdd_shop_day_sales (
    stg_pdd_shop_day_sales_id BIGSERIAL PRIMARY KEY,
    import_file_id INT NOT NULL REFERENCES import_files(import_file_id),
    raw_import_row_id BIGINT NOT NULL REFERENCES raw_import_rows(raw_import_row_id),
    shop_id INT NOT NULL REFERENCES shops(shop_id),
    source_row_number INT NOT NULL,
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
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (raw_import_row_id)
);

CREATE INDEX idx_stg_pdd_shop_day_sales_import_file_id
    ON stg_pdd_shop_day_sales(import_file_id);

CREATE INDEX idx_stg_pdd_shop_day_sales_shop_date
    ON stg_pdd_shop_day_sales(shop_id, sales_date);

INSERT INTO raw_import_rows (
    import_file_id,
    row_number,
    sheet_name,
    raw_payload,
    ingested_at
)
SELECT
    f.import_file_id,
    f.source_row_number,
    i.sheet_name,
    jsonb_strip_nulls(
        jsonb_build_object(
            '日期', to_char(f.sales_date, 'YYYY-MM-DD'),
            '店铺访客数', f.shop_visitor_count,
            '店铺浏览量', f.shop_pageview_count,
            '商品访客数', f.product_visitor_count,
            '商品浏览数', f.product_pageview_count,
            '成交买家数', f.buyer_count,
            '成交订单数', f.order_count,
            '成交金额', f.gross_sales_amount,
            '成交转化率', f.conversion_rate,
            '客单价', f.avg_order_value,
            'UV价值', f.uv_value,
            '商品收藏用户数', f.product_favorite_user_count,
            '退款金额', f.refund_amount
        )
    ),
    COALESCE(f.created_at, CURRENT_TIMESTAMP)
FROM fact_shop_day_sales f
JOIN import_files i
  ON i.import_file_id = f.import_file_id;

INSERT INTO stg_pdd_shop_day_sales (
    import_file_id,
    raw_import_row_id,
    shop_id,
    source_row_number,
    sales_date,
    shop_visitor_count,
    shop_pageview_count,
    product_visitor_count,
    product_pageview_count,
    buyer_count,
    order_count,
    gross_sales_amount,
    conversion_rate,
    avg_order_value,
    uv_value,
    product_favorite_user_count,
    refund_amount,
    created_at
)
SELECT
    f.import_file_id,
    r.raw_import_row_id,
    f.shop_id,
    f.source_row_number,
    f.sales_date,
    f.shop_visitor_count,
    f.shop_pageview_count,
    f.product_visitor_count,
    f.product_pageview_count,
    f.buyer_count,
    f.order_count,
    f.gross_sales_amount,
    f.conversion_rate,
    f.avg_order_value,
    f.uv_value,
    f.product_favorite_user_count,
    f.refund_amount,
    COALESCE(f.created_at, CURRENT_TIMESTAMP)
FROM fact_shop_day_sales f
JOIN raw_import_rows r
  ON r.import_file_id = f.import_file_id
 AND r.row_number = f.source_row_number;

CREATE TABLE fact_shop_day_sales_backup AS
SELECT *
FROM fact_shop_day_sales;

DROP TABLE fact_shop_day_sales;

CREATE TABLE fact_shop_day_sales (
    fact_shop_day_sales_id SERIAL PRIMARY KEY,
    shop_id INT NOT NULL REFERENCES shops(shop_id),
    sales_date DATE NOT NULL,
    buyer_count INT,
    order_count INT,
    gross_sales_amount NUMERIC(14,2),
    refund_amount NUMERIC(14,2),
    import_file_id INT NOT NULL REFERENCES import_files(import_file_id),
    source_row_number INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (shop_id, sales_date)
);

CREATE INDEX idx_fact_shop_day_sales_shop_id
    ON fact_shop_day_sales(shop_id);

CREATE INDEX idx_fact_shop_day_sales_sales_date
    ON fact_shop_day_sales(sales_date);

CREATE INDEX idx_fact_shop_day_sales_import_file_id
    ON fact_shop_day_sales(import_file_id);

INSERT INTO fact_shop_day_sales (
    shop_id,
    sales_date,
    buyer_count,
    order_count,
    gross_sales_amount,
    refund_amount,
    import_file_id,
    source_row_number,
    created_at
)
SELECT
    shop_id,
    sales_date,
    buyer_count,
    order_count,
    gross_sales_amount,
    refund_amount,
    import_file_id,
    source_row_number,
    created_at
FROM fact_shop_day_sales_backup;

DROP TABLE fact_shop_day_sales_backup;

ALTER TABLE import_files
    DROP COLUMN report_type;

COMMIT;
