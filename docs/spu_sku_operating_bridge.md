# SPU/SKU Operating Model Bridge v1

## Purpose

This doc defines the minimum SPU-level representation needed to support the operating control system's decision model: **SPU as the decision unit, SKU as the conversion lever**.

---

## Current Canonical Facts

### SKU-Day Facts (Ready)
```
fact_sku_day_sales: shop + sales_date + product_id + sku_id + merchant_sku_code + product_specification
  - gross_quantity, gross_product_amount, merchant_net_amount
  - source_row_count (for quality tracking)
```

### Shop-Day Facts (Ready)
```
fact_shop_day_sales: shop + sales_date
  - buyer_count, order_count, gross_sales_amount, refund_amount
```

---

## SPU Proxy Definition for v1-core

### Primary SPU Proxy: `product_id`

**Definition**: For v1-core, use `product_id` from `fact_sku_day_sales` as the SPU proxy.

**Why this is acceptable**:
- `product_id` is stable across the identity bundle grain
- Each `product_id` typically maps to multiple SKUs (expected for multi-spec products)
- Directly available in canonical facts (no additional ingest needed)

**Limitations**:
- `product_id` is a PDD internal identifier, not a business-meaningful SPU name
- One `product_id` = one SPU (reasonable for v1-core)
- Does NOT map to external SPU源数据 sheet (that remains trial)

### SKU Execution Lever: `sku_id` + `product_specification`

**Definition**: SKU variants are identified by `sku_id` or the combination of `merchant_sku_code` + `product_specification`.

**Safe use**:
- SKU-level conversion breakdown: compare `gross_quantity` / `gross_product_amount` across SKUs within same product_id
- SKU ranking within product: order by `merchant_net_amount` DESC

---

## What Is Safe for v1-core Decision Layer

### ✅ Safe to Use Now

| Use Case | Source | Confidence |
|----------|--------|------------|
| SPU-level 7-day revenue | `SUM(merchant_net_amount) GROUP BY shop_id, product_id` | High |
| SKU-level 7-day breakdown | `fact_sku_day_sales` filtered by product_id | High |
| SKU ranking within SPU | ORDER BY merchant_net_amount within product_id | High |
| Shop-level aggregation | `fact_shop_day_sales` | High |

### ⚠️ Acceptable Proxy (v1-core only)

| Use Case | Proxy Used | Caveat |
|----------|------------|--------|
| SPU decision unit | `product_id` | Not business-meaningful name; internal ID only |
| SPU trend analysis | 7-day aggregate by product_id | Does not use SPU源数据 sheet |

### ❌ Not Ready / Deferred

| Item | Reason |
|------|--------|
| SPU源数据 canonicalization | Trial data, not validated for production use |
| SPU name mapping | Requires external mapping table (not available) |
| Campaign/listing attribution | Not in current canonical facts |
| Cross-shop product normalization | Out of scope for v1 |

---

## Implementation: SPU-Level Aggregated View

Create a view that aggregates SKU-day facts to product (SPU proxy) level:

```sql
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
```

**Usage for decision layer**:
```sql
-- Top SPUs by 7-day revenue for a shop
SELECT spu_proxy, SUM(total_net_revenue) AS revenue_7d
FROM reporting.vw_pdd_spu_day_derived
WHERE sales_date >= CURRENT_DATE - 7
  AND shop_code = 'pdd_1'
GROUP BY spu_proxy
ORDER BY revenue_7d DESC
LIMIT 10;
```

---

## SKU Execution Lever View

```sql
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
```

---

## Decision Layer Input Examples

### "Which SPUs need attention?" (7-day trend)
```sql
SELECT shop_code, spu_proxy, SUM(total_net_revenue) AS revenue_7d
FROM reporting.vw_pdd_spu_day_derived
WHERE sales_date >= CURRENT_DATE - 7
GROUP BY shop_code, spu_proxy;
```

### "Which SKU variant is driving this SPU?" (Conversion lever)
```sql
SELECT sku_id, product_specification, merchant_net_amount
FROM reporting.vw_pdd_sku_execution_lever
WHERE shop_code = 'pdd_1' 
  AND spu_proxy = '123456789'
  AND sales_date >= CURRENT_DATE - 7
ORDER BY merchant_net_amount DESC;
```

---

## What Remains Deferred

- **Canonical SPU dimension table**: Requires SPU源数据 validation and external mapping
- **SKU→SPU foreign key relationship**: `product_id` as SPU proxy is sufficient for v1-core
- **SPU-level trend storage**: Use views, not materialized tables
- **SPU源数据 integration**: Keep as trial preview only, not canonical

---

## References

- `docs/data_model_v1.md` - current canonical model
- `docs/contracts/pdd_sku_daily_workbook_v1.md` - SKU identity findings
- `sql/schema.sql` - fact_sku_day_sales definition
