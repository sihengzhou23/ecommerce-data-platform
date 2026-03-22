# Decision-Layer Readiness Gate

## Question: Can we start first decision-layer design tomorrow?

**Answer**: **YES** (with caveats)

---

## Prerequisites: What Must Be True

| # | Prerequisite | Status | Evidence |
|---|--------------|--------|----------|
| 1 | Canonical shop-day facts exist and are trustworthy | ✅ Ready | `fact_shop_day_sales` - idempotent at grain |
| 2 | Canonical SKU-day facts exist and are trustworthy | ✅ Ready | `fact_sku_day_sales` - aggregates duplicates, filters missing product_id |
| 3 | SPU proxy defined | ✅ Ready | product_id as SPU proxy (see `docs/spu_sku_operating_bridge.md`) |
| 4 | 7-day trend signal infrastructure defined | ✅ Ready | SQL views defined (see `docs/seven_day_trend_signal_foundation.md`) |
| 5 | Ingestion hardening requirements documented | ✅ Implemented v1 | product_id filtering, quality metrics logged |

---

## What's Ready Now

- **Shop-level daily facts**: `fact_shop_day_sales` (buyer_count, order_count, gross_sales_amount, refund_amount)
- **SKU-level daily facts**: `fact_sku_day_sales` (gross_quantity, merchant_net_amount, etc.)
- **SPU proxy**: `product_id` from SKU facts → aggregates to SPU level
- **Trend signals**: Week-over-week classification (rising/falling/stable/new/insufficient)
- **Decision unit**: SPU (via product_id) for decision, SKU for execution lever

---

## Acceptable Proxies for v1-core

| Item | Proxy | Caveat |
|------|-------|---------|
| SPU decision unit | product_id | Internal ID, not business name |
| SPU trend | 7-day aggregate by product_id | Does not use SPU源数据 |
| SKU conversion | sku_id + product_specification | Raw PDD identifiers |

---

## What Remains Open After Today

| Item | Status | Action |
|------|--------|--------|
| Ingestion hardening implementation | ✅ Implemented v1 | SKUs filtered by product_id, quality metrics logged |
| SPU源数据 canonicalization | Deferred | Requires validation |
| Trend view SQL creation | ✅ Created | `reporting.vw_pdd_shop_7d_trend`, `vw_pdd_spu_7d_trend` exist |
| Action-list engine v0 | Not yet created | After trend views |
| Shop operating classification | Not yet created | After trend views |

---

## Immediate Next Steps (Tomorrow)

The trend views already exist. Next steps:

1. **Validate 7-day data coverage**
   - Run queries to confirm each shop has ≥7 days in recent window

2. **Test trend queries**
   - Verify week-over-week classification produces sensible results

3. **Create SPU-level derived view**
   - `reporting.vw_pdd_spu_day_derived` (aggregate SKU→product_id) - already exists

4. **Decision layer design**
   - Begin action-list engine based on trend signals

---

## If Answer Were NO

If any of these were true, we would NOT be ready:
- ❌ No canonical facts at appropriate grains
- ❌ No SPU proxy defined
- ❌ No trend infrastructure
- ❌ Critical data quality issues unresolved

---

## References

- `docs/data_model_v1.md` - canonical model
- `docs/spu_sku_operating_bridge.md` - SPU proxy definition
- `docs/seven_day_trend_signal_foundation.md` - trend signal spec
- `docs/ingestion_hardening_requirements.md` - data quality policy
- `docs/pdd_control_logic_v1_1_core_build_plan.md` - full build plan
