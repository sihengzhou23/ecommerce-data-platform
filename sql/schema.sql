-- schema.sql
-- Ecommerce Data Platform v1
-- Layered import model: metadata -> raw -> staging -> canonical facts

CREATE TABLE platforms (
    platform_id SERIAL PRIMARY KEY,
    platform_code VARCHAR(20) UNIQUE NOT NULL,
    platform_name VARCHAR(100) NOT NULL
);

CREATE TABLE shops (
    shop_id SERIAL PRIMARY KEY,
    platform_id INT NOT NULL REFERENCES platforms(platform_id),
    shop_code VARCHAR(50) NOT NULL,
    shop_name VARCHAR(255),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (platform_id, shop_code)
);

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

CREATE TABLE import_files (
    import_file_id SERIAL PRIMARY KEY,
    source_file_type_id INT NOT NULL REFERENCES source_file_types(source_file_type_id),
    platform_id INT NOT NULL REFERENCES platforms(platform_id),
    shop_id INT REFERENCES shops(shop_id),
    file_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    sheet_name VARCHAR(255),
    file_hash VARCHAR(128),
    row_count_raw INT,
    imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    skipped_canonical_count INT,
    duplicate_rate NUMERIC(5,2),
    numeric_sanitization_count INT
);

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

CREATE TABLE raw_import_rows (
    raw_import_row_id BIGSERIAL PRIMARY KEY,
    import_file_id INT NOT NULL REFERENCES import_files(import_file_id),
    row_number INT NOT NULL,
    sheet_name VARCHAR(255),
    raw_payload JSONB NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

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
    -- Canonical business rule for the currently supported grain:
    -- one canonical shop-day row per shop per sales_date.
    UNIQUE (shop_id, sales_date)
);

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

CREATE INDEX idx_shops_platform_id
    ON shops(platform_id);

CREATE INDEX idx_source_file_types_platform_id
    ON source_file_types(platform_id);

CREATE INDEX idx_import_files_source_file_type_id
    ON import_files(source_file_type_id);

CREATE INDEX idx_import_files_platform_id
    ON import_files(platform_id);

CREATE INDEX idx_import_files_shop_id
    ON import_files(shop_id);

CREATE INDEX idx_import_file_sheets_import_file_id
    ON import_file_sheets(import_file_id);

CREATE INDEX idx_import_file_sheets_role
    ON import_file_sheets(sheet_role);

CREATE INDEX idx_raw_import_rows_import_file_id
    ON raw_import_rows(import_file_id);

CREATE UNIQUE INDEX idx_raw_import_rows_file_sheet_row
    ON raw_import_rows(import_file_id, COALESCE(sheet_name, ''), row_number);

CREATE INDEX idx_stg_pdd_shop_day_sales_import_file_id
    ON stg_pdd_shop_day_sales(import_file_id);

CREATE INDEX idx_stg_pdd_shop_day_sales_shop_date
    ON stg_pdd_shop_day_sales(shop_id, sales_date);

CREATE INDEX idx_stg_pdd_sku_day_sales_import_file_id
    ON stg_pdd_sku_day_sales(import_file_id);

CREATE INDEX idx_stg_pdd_sku_day_sales_shop_date
    ON stg_pdd_sku_day_sales(shop_id, sales_date);

CREATE INDEX idx_stg_pdd_sku_day_sales_sku_id
    ON stg_pdd_sku_day_sales(sku_id);

CREATE INDEX idx_fact_shop_day_sales_shop_id
    ON fact_shop_day_sales(shop_id);

CREATE INDEX idx_fact_shop_day_sales_sales_date
    ON fact_shop_day_sales(sales_date);

CREATE INDEX idx_fact_shop_day_sales_import_file_id
    ON fact_shop_day_sales(import_file_id);

CREATE INDEX idx_fact_sku_day_sales_shop_id
    ON fact_sku_day_sales(shop_id);

CREATE INDEX idx_fact_sku_day_sales_sales_date
    ON fact_sku_day_sales(sales_date);

CREATE INDEX idx_fact_sku_day_sales_import_file_id
    ON fact_sku_day_sales(import_file_id);

CREATE INDEX idx_fact_sku_day_sales_sku_id
    ON fact_sku_day_sales(sku_id);

CREATE SCHEMA IF NOT EXISTS reporting;

CREATE TABLE reporting.pdd_spu_monitoring_preview_snapshot (
    snapshot_id BIGSERIAL PRIMARY KEY,
    shop_code TEXT NOT NULL,
    sales_date DATE NOT NULL,
    spu_name TEXT NOT NULL,
    campaign_name TEXT NOT NULL DEFAULT '',
    listing_name TEXT NOT NULL DEFAULT '',
    product_visitor_count NUMERIC(14,2),
    product_pageview_count NUMERIC(14,2),
    unit_count NUMERIC(14,2),
    buyer_count NUMERIC(14,2),
    order_count NUMERIC(14,2),
    gross_sales_amount NUMERIC(14,2),
    product_favorite_user_count NUMERIC(14,2),
    derived_conversion_rate NUMERIC(12,6),
    derived_avg_order_value NUMERIC(14,2),
    source_row_count INT NOT NULL,
    refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (shop_code, sales_date, spu_name, campaign_name, listing_name)
);

CREATE INDEX idx_pdd_spu_monitoring_preview_snapshot_shop_date
    ON reporting.pdd_spu_monitoring_preview_snapshot(shop_code, sales_date);

CREATE OR REPLACE VIEW reporting.vw_pdd_dashboard_shop_daily AS
WITH current_shop_day AS (
    SELECT
        s.shop_code,
        COALESCE(s.shop_name, s.shop_code) AS shop_name,
        f.sales_date,
        f.buyer_count,
        f.order_count,
        f.gross_sales_amount,
        f.refund_amount,
        stg.shop_visitor_count,
        stg.shop_pageview_count,
        stg.product_visitor_count,
        stg.product_pageview_count,
        stg.product_favorite_user_count
    FROM fact_shop_day_sales f
    JOIN shops s
      ON s.shop_id = f.shop_id
    LEFT JOIN stg_pdd_shop_day_sales stg
      ON stg.import_file_id = f.import_file_id
     AND stg.shop_id = f.shop_id
     AND stg.source_row_number = f.source_row_number
     AND stg.sales_date = f.sales_date
)
SELECT
    'PDD' AS "平台",
    shop_code AS "店铺编码",
    shop_name AS "店铺",
    sales_date AS "日期",
    shop_visitor_count AS "店铺访客数",
    shop_pageview_count AS "店铺浏览量",
    product_visitor_count AS "商品访客数",
    product_pageview_count AS "商品浏览数",
    buyer_count AS "买家数",
    order_count AS "订单数",
    gross_sales_amount AS "成交金额",
    refund_amount AS "退款金额",
    product_favorite_user_count AS "商品收藏用户数",
    ROUND(buyer_count::numeric / NULLIF(shop_visitor_count, 0), 4) AS "转化率",
    ROUND(gross_sales_amount::numeric / NULLIF(buyer_count, 0), 2) AS "客单价",
    ROUND(gross_sales_amount::numeric / NULLIF(shop_visitor_count, 0), 2) AS "UV价值"
FROM current_shop_day;

CREATE OR REPLACE VIEW reporting.vw_pdd_dashboard_shop_monthly AS
WITH current_shop_day AS (
    SELECT
        s.shop_code,
        COALESCE(s.shop_name, s.shop_code) AS shop_name,
        f.sales_date,
        f.buyer_count,
        f.order_count,
        f.gross_sales_amount,
        f.refund_amount,
        stg.shop_visitor_count,
        stg.shop_pageview_count,
        stg.product_visitor_count,
        stg.product_pageview_count,
        stg.product_favorite_user_count
    FROM fact_shop_day_sales f
    JOIN shops s
      ON s.shop_id = f.shop_id
    LEFT JOIN stg_pdd_shop_day_sales stg
      ON stg.import_file_id = f.import_file_id
     AND stg.shop_id = f.shop_id
     AND stg.source_row_number = f.source_row_number
     AND stg.sales_date = f.sales_date
)
SELECT
    'PDD' AS "平台",
    shop_code AS "店铺编码",
    shop_name AS "店铺",
    TO_CHAR(DATE_TRUNC('month', sales_date), 'YYYY-MM') AS "月份",
    COUNT(*) AS "覆盖天数",
    SUM(shop_visitor_count) AS "店铺访客数",
    SUM(shop_pageview_count) AS "店铺浏览量",
    SUM(product_visitor_count) AS "商品访客数",
    SUM(product_pageview_count) AS "商品浏览数",
    SUM(buyer_count) AS "买家数",
    SUM(order_count) AS "订单数",
    SUM(gross_sales_amount) AS "成交金额",
    SUM(refund_amount) AS "退款金额",
    SUM(product_favorite_user_count) AS "商品收藏用户数",
    ROUND(SUM(buyer_count)::numeric / NULLIF(SUM(shop_visitor_count), 0), 4) AS "转化率",
    ROUND(SUM(gross_sales_amount)::numeric / NULLIF(SUM(buyer_count), 0), 2) AS "客单价",
    ROUND(SUM(gross_sales_amount)::numeric / NULLIF(SUM(shop_visitor_count), 0), 2) AS "UV价值"
FROM current_shop_day
GROUP BY shop_code, shop_name, DATE_TRUNC('month', sales_date);

CREATE OR REPLACE VIEW reporting.vw_pdd_dashboard_sku_daily AS
WITH sku_display_name AS (
    SELECT
        stg.import_file_id,
        stg.shop_id,
        stg.sales_date,
        COALESCE(stg.product_id, '') AS product_id,
        COALESCE(stg.sku_id, '') AS sku_id,
        COALESCE(stg.merchant_sku_code, '') AS merchant_sku_code,
        COALESCE(stg.product_specification, '') AS product_specification,
        MAX(stg.product_name) AS product_name
    FROM stg_pdd_sku_day_sales stg
    GROUP BY
        stg.import_file_id,
        stg.shop_id,
        stg.sales_date,
        COALESCE(stg.product_id, ''),
        COALESCE(stg.sku_id, ''),
        COALESCE(stg.merchant_sku_code, ''),
        COALESCE(stg.product_specification, '')
)
SELECT
    'PDD' AS "平台",
    s.shop_code AS "店铺编码",
    COALESCE(s.shop_name, s.shop_code) AS "店铺",
    f.sales_date AS "日期",
    d.product_name AS "商品名称",
    NULLIF(f.product_id, '') AS "商品ID",
    NULLIF(f.sku_id, '') AS "SKU-ID",
    NULLIF(f.merchant_sku_code, '') AS "商家SKU编码",
    NULLIF(f.product_specification, '') AS "商品规格",
    f.gross_quantity AS "销量",
    f.gross_product_amount AS "商品总价",
    f.merchant_net_amount AS "商家实收金额",
    f.source_row_count AS "来源行数"
FROM fact_sku_day_sales f
JOIN shops s
  ON s.shop_id = f.shop_id
LEFT JOIN sku_display_name d
  ON d.import_file_id = f.import_file_id
 AND d.shop_id = f.shop_id
 AND d.sales_date = f.sales_date
 AND d.product_id = f.product_id
 AND d.sku_id = f.sku_id
 AND d.merchant_sku_code = f.merchant_sku_code
 AND d.product_specification = f.product_specification;

CREATE OR REPLACE VIEW reporting.vw_pdd_dashboard_spu_monitoring_trial AS
SELECT
    'PDD' AS "平台",
    '试运行' AS "状态",
    shop_code AS "店铺编码",
    shop_code AS "店铺",
    sales_date AS "日期",
    spu_name AS "SPU",
    NULLIF(campaign_name, '') AS "计划名",
    NULLIF(listing_name, '') AS "链接名称",
    product_visitor_count AS "商品访客量",
    product_pageview_count AS "商品浏览量",
    unit_count AS "销量",
    buyer_count AS "买家数",
    order_count AS "订单数",
    gross_sales_amount AS "销售额",
    product_favorite_user_count AS "商品收藏用户数",
    derived_conversion_rate AS "转化率",
    derived_avg_order_value AS "客单价",
    source_row_count AS "来源行数",
    refreshed_at AS "刷新时间",
    '试运行预览：基于标准化 SPU源数据 生成，供 Metabase 预览使用，暂不视为正式事实层。' AS "说明"
FROM reporting.pdd_spu_monitoring_preview_snapshot;

COMMENT ON VIEW reporting.vw_pdd_dashboard_shop_daily IS
    'Metabase 店铺总览页面：基于当前可信店铺日事实层和补充分层流量指标生成。';

COMMENT ON VIEW reporting.vw_pdd_dashboard_shop_monthly IS
    'Metabase 月度汇总页面：基于店铺日事实层月度聚合生成。';

COMMENT ON VIEW reporting.vw_pdd_dashboard_sku_daily IS
    'Metabase SKU表现页面：基于当前可信 SKU 日事实层生成。';

COMMENT ON VIEW reporting.vw_pdd_dashboard_spu_monitoring_trial IS
    'Metabase SPU监控（试运行）页面：基于标准化 SPU源数据 刷新的试运行预览。';

CREATE OR REPLACE VIEW reporting.vw_pdd_cockpit_top_summary_daily AS
SELECT
    "平台",
    "日期",
    COUNT(DISTINCT "店铺编码") AS "覆盖店铺数",
    SUM("店铺访客数") AS "店铺访客数",
    SUM("店铺浏览量") AS "店铺浏览量",
    SUM("商品访客数") AS "商品访客数",
    SUM("商品浏览数") AS "商品浏览数",
    SUM("买家数") AS "买家数",
    SUM("订单数") AS "订单数",
    SUM("成交金额") AS "成交金额",
    SUM("退款金额") AS "退款金额",
    SUM("商品收藏用户数") AS "商品收藏用户数",
    ROUND(SUM("买家数")::numeric / NULLIF(SUM("店铺访客数"), 0), 4) AS "转化率",
    ROUND(SUM("成交金额")::numeric / NULLIF(SUM("买家数"), 0), 2) AS "客单价",
    ROUND(SUM("成交金额")::numeric / NULLIF(SUM("店铺访客数"), 0), 2) AS "UV价值"
FROM reporting.vw_pdd_dashboard_shop_daily
GROUP BY "平台", "日期";

CREATE OR REPLACE VIEW reporting.vw_pdd_cockpit_focus_shop_rank_30d AS
WITH latest_date AS (
    SELECT MAX("日期") AS latest_sales_date
    FROM reporting.vw_pdd_dashboard_shop_daily
),
windowed AS (
    SELECT v.*
    FROM reporting.vw_pdd_dashboard_shop_daily v
    CROSS JOIN latest_date d
    WHERE v."日期" > d.latest_sales_date - INTERVAL '30 day'
)
SELECT
    '近30天' AS "统计窗口",
    "平台",
    "店铺编码",
    "店铺",
    COUNT(*) AS "覆盖天数",
    SUM("店铺访客数") AS "店铺访客数",
    SUM("买家数") AS "买家数",
    SUM("订单数") AS "订单数",
    SUM("成交金额") AS "成交金额",
    SUM("退款金额") AS "退款金额",
    ROUND(SUM("买家数")::numeric / NULLIF(SUM("店铺访客数"), 0), 4) AS "转化率",
    ROUND(SUM("成交金额")::numeric / NULLIF(SUM("买家数"), 0), 2) AS "客单价",
    ROUND(SUM("成交金额")::numeric / NULLIF(SUM("店铺访客数"), 0), 2) AS "UV价值",
    DENSE_RANK() OVER (ORDER BY SUM("成交金额") DESC) AS "成交金额排名",
    DENSE_RANK() OVER (ORDER BY SUM("买家数") DESC) AS "买家数排名"
FROM windowed
GROUP BY "平台", "店铺编码", "店铺";

CREATE OR REPLACE VIEW reporting.vw_pdd_cockpit_focus_sku_rank_30d AS
WITH latest_date AS (
    SELECT MAX("日期") AS latest_sales_date
    FROM reporting.vw_pdd_dashboard_sku_daily
),
windowed AS (
    SELECT v.*
    FROM reporting.vw_pdd_dashboard_sku_daily v
    CROSS JOIN latest_date d
    WHERE v."日期" > d.latest_sales_date - INTERVAL '30 day'
)
SELECT
    '近30天' AS "统计窗口",
    "平台",
    "店铺编码",
    "店铺",
    "商品名称",
    "商品ID",
    "SKU-ID",
    "商家SKU编码",
    "商品规格",
    SUM("销量") AS "销量",
    SUM("商品总价") AS "商品总价",
    SUM("商家实收金额") AS "商家实收金额",
    SUM("来源行数") AS "来源行数",
    DENSE_RANK() OVER (ORDER BY SUM("商家实收金额") DESC) AS "商家实收金额排名",
    DENSE_RANK() OVER (ORDER BY SUM("销量") DESC) AS "销量排名"
FROM windowed
GROUP BY
    "平台",
    "店铺编码",
    "店铺",
    "商品名称",
    "商品ID",
    "SKU-ID",
    "商家SKU编码",
    "商品规格";

CREATE OR REPLACE VIEW reporting.vw_pdd_cockpit_product_cards_30d AS
WITH latest_date AS (
    SELECT MAX("日期") AS latest_sales_date
    FROM reporting.vw_pdd_dashboard_sku_daily
),
windowed AS (
    SELECT v.*
    FROM reporting.vw_pdd_dashboard_sku_daily v
    CROSS JOIN latest_date d
    WHERE v."日期" > d.latest_sales_date - INTERVAL '30 day'
),
ranked_product AS (
    SELECT
        "店铺编码",
        "店铺",
        "商品名称",
        COALESCE("商品规格", '') AS product_specification_key,
        DENSE_RANK() OVER (
            PARTITION BY "店铺编码"
            ORDER BY SUM("商家实收金额") DESC, "商品名称", COALESCE("商品规格", '')
        ) AS product_card_rank
    FROM windowed
    GROUP BY "店铺编码", "店铺", "商品名称", COALESCE("商品规格", '')
)
SELECT
    '近30天' AS "统计窗口",
    w."平台",
    w."店铺编码",
    w."店铺",
    r.product_card_rank AS "卡片排序",
    w."商品名称",
    w."商品规格",
    w."日期",
    SUM(w."销量") AS "销量",
    SUM(w."商品总价") AS "商品总价",
    SUM(w."商家实收金额") AS "商家实收金额"
FROM windowed w
JOIN ranked_product r
  ON r."店铺编码" = w."店铺编码"
 AND r."商品名称" = w."商品名称"
 AND r.product_specification_key = COALESCE(w."商品规格", '')
WHERE r.product_card_rank <= 6
GROUP BY
    w."平台",
    w."店铺编码",
    w."店铺",
    r.product_card_rank,
    w."商品名称",
    w."商品规格",
    w."日期";

CREATE OR REPLACE VIEW reporting.vw_pdd_cockpit_spu_trial_cards AS
WITH ranked AS (
    SELECT
        v.*,
        DENSE_RANK() OVER (
            PARTITION BY v."店铺编码"
            ORDER BY v."销售额" DESC, v."SPU"
        ) AS sales_rank
    FROM reporting.vw_pdd_dashboard_spu_monitoring_trial v
)
SELECT
    "平台",
    "状态",
    "店铺编码",
    "店铺",
    "日期",
    sales_rank AS "销售额排名",
    "SPU",
    "计划名",
    "链接名称",
    "商品访客量",
    "商品浏览量",
    "销量",
    "买家数",
    "订单数",
    "销售额",
    "商品收藏用户数",
    "转化率",
    "客单价",
    "来源行数",
    "刷新时间",
    "说明"
FROM ranked;

COMMENT ON VIEW reporting.vw_pdd_cockpit_top_summary_daily IS
    'PDD 单页经营驾驶舱顶部汇总趋势：按天汇总所有店铺的可信经营指标。';

COMMENT ON VIEW reporting.vw_pdd_cockpit_focus_shop_rank_30d IS
    'PDD 单页经营驾驶舱重点店铺榜：基于近30天店铺经营指标排序。';

COMMENT ON VIEW reporting.vw_pdd_cockpit_focus_sku_rank_30d IS
    'PDD 单页经营驾驶舱重点SKU榜：基于近30天可信 SKU 表现排序。';

COMMENT ON VIEW reporting.vw_pdd_cockpit_product_cards_30d IS
    'PDD 单页经营驾驶舱商品监控卡片：基于近30天可信 SKU 表现生成的分店铺商品小卡片序列。';

COMMENT ON VIEW reporting.vw_pdd_cockpit_spu_trial_cards IS
    'PDD 单页经营驾驶舱 SPU监控卡片（试运行）：基于试运行 SPU 预览视图排序。';

-- Seed base platform row.
INSERT INTO platforms (platform_code, platform_name)
VALUES ('pdd', 'Pinduoduo')
ON CONFLICT (platform_code) DO NOTHING;

-- Seed the current report contract.
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
WHERE platform_code = 'pdd'
ON CONFLICT (platform_id, file_type_code, version) DO NOTHING;

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

-- Seed recurring PDD workbook raw-sheet contracts for parquet normalization.
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
    p.platform_id,
    seed.file_type_code,
    seed.file_type_name,
    seed.grain_code,
    1,
    seed.description,
    TRUE
FROM platforms p
CROSS JOIN (
    VALUES
        (
            'pdd_workbook_spu_source',
            'PDD Workbook SPU Source',
            'spu_day',
            'Recurring PDD workbook raw SPU source sheet normalized into parquet.'
        ),
        (
            'pdd_workbook_sku_source',
            'PDD Workbook SKU Source',
            'sku_day',
            'Recurring PDD workbook raw SKU source sheet normalized into parquet.'
        ),
        (
            'pdd_workbook_shop_daily_source',
            'PDD Workbook Shop Daily Source',
            'shop_day',
            'Recurring PDD workbook raw shop daily source sheet normalized into parquet.'
        ),
        (
            'pdd_workbook_promo_total_source',
            'PDD Workbook Promotion Total Source',
            'shop_day',
            'Recurring PDD workbook raw promotion total sheet normalized into parquet.'
        ),
        (
            'pdd_workbook_promo_campaign_source',
            'PDD Workbook Promotion Campaign Source',
            'spu_day',
            'Recurring PDD workbook raw promotion campaign sheet normalized into parquet.'
        ),
        (
            'pdd_workbook_shop_product_source',
            'PDD Workbook Shop Product Source',
            'product_sku_snapshot',
            'Recurring PDD workbook raw shop product info sheet normalized into parquet.'
        ),
        (
            'pdd_workbook_product_rating_source',
            'PDD Workbook Product Rating Source',
            'product_day',
            'Recurring PDD workbook raw product rating sheet normalized into parquet.'
        )
) AS seed(file_type_code, file_type_name, grain_code, description)
WHERE p.platform_code = 'pdd'
ON CONFLICT (platform_id, file_type_code, version) DO NOTHING;

-- ============================================================
-- 7-Day Trend Signal Views (Decision Layer Foundation)
-- ============================================================

CREATE OR REPLACE VIEW reporting.vw_pdd_spu_day_derived AS
SELECT 
    s.shop_code,
    f.sales_date,
    f.product_id AS spu_proxy,
    COUNT(DISTINCT f.sku_id) AS sku_variant_count,
    SUM(f.gross_quantity) AS total_quantity,
    SUM(f.gross_product_amount) AS total_product_amount,
    SUM(f.merchant_net_amount) AS total_net_revenue,
    SUM(f.source_row_count) AS source_row_count
FROM fact_sku_day_sales f
JOIN shops s ON s.shop_id = f.shop_id
WHERE f.product_id != ''
GROUP BY s.shop_code, f.sales_date, f.product_id;

CREATE OR REPLACE VIEW reporting.vw_pdd_shop_7d_trend AS
WITH this_week AS (
    SELECT 
        shop_id,
        SUM(gross_sales_amount) AS revenue_this_week,
        SUM(buyer_count) AS buyers_this_week,
        SUM(order_count) AS orders_this_week,
        COUNT(DISTINCT sales_date) AS days_this_week
    FROM fact_shop_day_sales
    WHERE sales_date >= CURRENT_DATE - 7
    GROUP BY shop_id
),
last_week AS (
    SELECT 
        shop_id,
        SUM(gross_sales_amount) AS revenue_last_week,
        SUM(buyer_count) AS buyers_last_week,
        SUM(order_count) AS orders_last_week,
        COUNT(DISTINCT sales_date) AS days_last_week
    FROM fact_shop_day_sales
    WHERE sales_date >= CURRENT_DATE - 14 
      AND sales_date < CURRENT_DATE - 7
    GROUP BY shop_id
)
SELECT 
    s.shop_code,
    COALESCE(t.revenue_this_week, 0) AS revenue_this_week,
    COALESCE(t.buyers_this_week, 0) AS buyers_this_week,
    COALESCE(t.orders_this_week, 0) AS orders_this_week,
    t.days_this_week,
    COALESCE(l.revenue_last_week, 0) AS revenue_last_week,
    COALESCE(l.buyers_last_week, 0) AS buyers_last_week,
    COALESCE(l.orders_last_week, 0) AS orders_last_week,
    l.days_last_week,
    CASE 
        WHEN t.revenue_this_week IS NULL THEN 'no_data'
        WHEN l.revenue_last_week IS NULL OR l.days_last_week < 4 THEN 'new'
        WHEN t.revenue_this_week > l.revenue_last_week * 1.1 THEN 'rising'
        WHEN t.revenue_this_week < l.revenue_last_week * 0.9 THEN 'falling'
        ELSE 'stable'
    END AS revenue_trend,
    CASE
        WHEN t.days_this_week < 4 THEN 'insufficient_data'
        ELSE 'sufficient'
    END AS data_quality
FROM shops s
LEFT JOIN this_week t ON t.shop_id = s.shop_id
LEFT JOIN last_week l ON l.shop_id = s.shop_id
WHERE s.is_active = TRUE;

CREATE OR REPLACE VIEW reporting.vw_pdd_spu_7d_trend AS
WITH this_week AS (
    SELECT 
        shop_id,
        product_id AS spu_proxy,
        SUM(merchant_net_amount) AS revenue_this_week,
        SUM(gross_quantity) AS quantity_this_week,
        COUNT(DISTINCT sales_date) AS days_this_week
    FROM fact_sku_day_sales
    WHERE sales_date >= CURRENT_DATE - 7
      AND product_id != ''
    GROUP BY shop_id, product_id
),
last_week AS (
    SELECT 
        shop_id,
        product_id AS spu_proxy,
        SUM(merchant_net_amount) AS revenue_last_week,
        SUM(gross_quantity) AS quantity_last_week,
        COUNT(DISTINCT sales_date) AS days_last_week
    FROM fact_sku_day_sales
    WHERE sales_date >= CURRENT_DATE - 14 
      AND sales_date < CURRENT_DATE - 7
      AND product_id != ''
    GROUP BY shop_id, product_id
),
all_spus AS (
    SELECT shop_id, spu_proxy FROM this_week
    UNION
    SELECT shop_id, spu_proxy FROM last_week
)
SELECT 
    s.shop_code,
    a.spu_proxy,
    COALESCE(t.revenue_this_week, 0) AS revenue_this_week,
    COALESCE(t.quantity_this_week, 0) AS quantity_this_week,
    t.days_this_week,
    COALESCE(l.revenue_last_week, 0) AS revenue_last_week,
    COALESCE(l.quantity_last_week, 0) AS quantity_last_week,
    l.days_last_week,
    CASE 
        WHEN t.revenue_this_week IS NULL THEN 'churned'
        WHEN l.revenue_last_week IS NULL OR l.days_last_week < 4 THEN 'new'
        WHEN t.revenue_this_week > l.revenue_last_week * 1.1 THEN 'rising'
        WHEN t.revenue_this_week < l.revenue_last_week * 0.9 THEN 'falling'
        ELSE 'stable'
    END AS revenue_trend,
    CASE
        WHEN t.days_this_week < 4 THEN 'insufficient_data'
        ELSE 'sufficient'
    END AS data_quality
FROM all_spus a
JOIN shops s ON s.shop_id = a.shop_id AND s.is_active = TRUE
LEFT JOIN this_week t ON t.shop_id = a.shop_id AND t.spu_proxy = a.spu_proxy
LEFT JOIN last_week l ON l.shop_id = a.shop_id AND l.spu_proxy = a.spu_proxy;

CREATE OR REPLACE VIEW reporting.vw_pdd_sku_execution_lever AS
SELECT 
    s.shop_code,
    f.sales_date,
    f.product_id AS spu_proxy,
    f.sku_id,
    f.merchant_sku_code,
    f.product_specification,
    f.gross_quantity,
    f.gross_product_amount,
    f.merchant_net_amount,
    f.source_row_count,
    RANK() OVER (
        PARTITION BY s.shop_code, f.sales_date, f.product_id 
        ORDER BY f.merchant_net_amount DESC
    ) AS sku_revenue_rank_within_spu
FROM fact_sku_day_sales f
JOIN shops s ON s.shop_id = f.shop_id
WHERE f.sku_id != ''
  AND f.product_id != '';
