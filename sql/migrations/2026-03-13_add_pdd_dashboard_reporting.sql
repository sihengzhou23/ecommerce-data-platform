BEGIN;

CREATE SCHEMA IF NOT EXISTS reporting;

CREATE TABLE IF NOT EXISTS reporting.pdd_spu_monitoring_preview_snapshot (
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

CREATE INDEX IF NOT EXISTS idx_pdd_spu_monitoring_preview_snapshot_shop_date
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

COMMIT;
