# 7-Day Trend Signal Foundation v1

## Purpose

This doc defines the minimum trend signal infrastructure needed to support operational decisions: **prioritize 7-day trends over single-day fluctuations**.

---

## Candidate Grains for Trend Evaluation

| Grain | Use Case | Source Fact |
|-------|----------|-------------|
| Shop | Which shops need attention? | fact_shop_day_sales |
| SPU/Product | Which products are rising/falling? | fact_sku_day_sales aggregated by product_id |
| SKU | Which variants convert? | fact_sku_day_sales |

**Why 7-day?**
- Single-day fluctuations are noisy (weekend effects, promotion spikes)
- 7 days smooths variance while remaining actionable
- The operating system needs "this week" vs "last week" comparison

---

## Signal Definitions

### Week-Over-Week Change Classification

| Signal | Logic | Decision Implication |
|--------|-------|---------------------|
| `rising` | this_week > last_week × 1.1 | Keep/expand |
| `falling` | this_week < last_week × 0.9 | Investigate/fix |
| `stable` | within ±10% | Maintain |
| `new` | no last_week data | Evaluate potential |
| `insufficient_data` | < 4 days in current week | Cannot judge |

---

## Implementation: SQL Views

### 1. Shop 7-Day Trend View

```sql
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
        WHEN t.days_this_week IS NULL THEN 'no_data'
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
```

### 2. SPU (product_id) 7-Day Trend View

```sql
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
)
SELECT 
    s.shop_code,
    COALESCE(t.spu_proxy, l.spu_proxy) AS spu_proxy,
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
FROM shops s
LEFT JOIN this_week t ON t.shop_id = s.shop_id
LEFT JOIN last_week l ON l.shop_id = s.shop_id AND l.spu_proxy = t.spu_proxy
WHERE s.is_active = TRUE;
```

### 3. SKU 7-Day Trend View (Top Products Only)

```sql
CREATE OR REPLACE VIEW reporting.vw_pdd_sku_7d_trend AS
WITH recent_spus AS (
    -- Focus on SPUs with meaningful revenue in last 30 days
    SELECT DISTINCT shop_id, product_id
    FROM fact_sku_day_sales
    WHERE sales_date >= CURRENT_DATE - 30
      AND merchant_net_amount > 0
),
this_week AS (
    SELECT 
        f.shop_id,
        f.product_id,
        f.sku_id,
        f.merchant_sku_code,
        f.product_specification,
        SUM(f.merchant_net_amount) AS revenue_this_week,
        SUM(f.gross_quantity) AS quantity_this_week,
        COUNT(DISTINCT f.sales_date) AS days_this_week
    FROM fact_sku_day_sales f
    JOIN recent_spus rs ON rs.shop_id = f.shop_id AND rs.product_id = f.product_id
    WHERE f.sales_date >= CURRENT_DATE - 7
    GROUP BY f.shop_id, f.product_id, f.sku_id, f.merchant_sku_code, f.product_specification
),
last_week AS (
    SELECT 
        f.shop_id,
        f.product_id,
        f.sku_id,
        f.merchant_sku_code,
        f.product_specification,
        SUM(f.merchant_net_amount) AS revenue_last_week,
        SUM(f.gross_quantity) AS quantity_last_week,
        COUNT(DISTINCT f.sales_date) AS days_last_week
    FROM fact_sku_day_sales f
    JOIN recent_spus rs ON rs.shop_id = f.shop_id AND rs.product_id = f.product_id
    WHERE f.sales_date >= CURRENT_DATE - 14 
      AND f.sales_date < CURRENT_DATE - 7
    GROUP BY f.shop_id, f.product_id, f.sku_id, f.merchant_sku_code, f.product_specification
)
SELECT 
    s.shop_code,
    COALESCE(t.product_id, l.product_id) AS spu_proxy,
    COALESCE(t.sku_id, l.sku_id) AS sku_id,
    COALESCE(t.merchant_sku_code, l.merchant_sku_code) AS merchant_sku_code,
    COALESCE(t.product_specification, l.product_specification) AS product_specification,
    COALESCE(t.revenue_this_week, 0) AS revenue_this_week,
    COALESCE(t.quantity_this_week, 0) AS quantity_this_week,
    COALESCE(l.revenue_last_week, 0) AS revenue_last_week,
    CASE 
        WHEN t.revenue_this_week IS NULL THEN 'churned'
        WHEN l.revenue_last_week IS NULL THEN 'new'
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
    AND l.product_id = t.product_id 
    AND l.sku_id = t.sku_id
WHERE s.is_active = TRUE;
```

---

## Insufficient Data Handling

| Condition | Signal | Decision Implication |
|-----------|--------|---------------------|
| < 4 days in this week | `insufficient_data` | Cannot recommend action |
| No last week data | `new` | Evaluate potential |
| Churned (had data, now zero) | `churned` | Investigate why |

**Rule**: `prohibited` actions take precedence when `insufficient_data`.

---

## Data Quality Dependency

7-day trends are only meaningful if:
1. Daily data is complete (no large gaps) - **validated via ingestion hardening**
2. Identifier mapping is stable (same SKU across days) - **product_id is stable**
3. Outliers are understood (promotion days vs organic) - **deferred for v1**

---

## Deferred (Not Required for v1-core)

- Complex moving averages (MA5, MA7, MA30)
- Seasonality detection
- Anomaly scoring
- Predictive forecasts
- Materialized trend tables (use views for now)

---

## Integration with Decision Layer

The 7-day trend signals feed the decision layer:

| Decision Type | Trend Input | Data Quality Required |
|---------------|-------------|----------------------|
| today_must_do | falling trend | sufficient |
| today_prohibited | insufficient_data | N/A (blocked) |
| this_week_priority | rising from falling→stable | sufficient |

---

## References

- `sql/schema.sql` - existing reporting views
- `docs/data_model_v1.md` - canonical fact definitions
- `docs/spu_sku_operating_bridge.md` - SPU proxy definition
