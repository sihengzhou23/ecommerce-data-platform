# Decision-Layer Readiness Gate

## Question: Can we start first decision-layer design tomorrow?

**Answer**: **YES** (with caveats)

---

## Prerequisites: What Must Be True

| # | Prerequisite | Status | Evidence |
|---|--------------|--------|----------|
| 1 | Canonical shop-day facts exist and are trustworthy | ✅ Ready | `fact_shop_day_sales` - idempotent at grain |
| 2 | Canonical SKU-day facts exist and are trustworthy | ✅ Ready | `fact_sku_day_sales` - aggregates duplicates |
| 3 | SPU proxy defined | ✅ Ready | product_id as SPU proxy (see `docs/spu_sku_operating_bridge.md`) |
| 4 | 7-day trend signal infrastructure defined | ✅ Ready | SQL views defined (see `docs/seven_day_trend_signal_foundation.md`) |
| 5 | Ingestion hardening requirements documented | ✅ Ready | Explicit policies defined (see `docs/ingestion_hardening_requirements.md`) |

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
| Ingestion hardening implementation | Documented only | Implement when needed |
| SPU源数据 canonicalization | Deferred | Requires validation |
| Trend view SQL creation | Not yet created | Tomorrow's first task |
| Action-list engine v0 | Not yet created | After trend views |
| Shop operating classification | Not yet created | After trend views |

---

## Immediate Next Steps (Tomorrow)

If answer is **YES**, implement in this order:

1. **Create 7-day trend SQL views** (highest priority)
   - `reporting.vw_pdd_shop_7d_trend`
   - `reporting.vw_pdd_spu_7d_trend`
   - `reporting.vw_pdd_sku_7d_trend`

2. **Validate 7-day data coverage**
   - Run queries to confirm each shop has ≥7 days in recent window

3. **Create SPU-level derived view**
   - `reporting.vw_pdd_spu_day_derived` (aggregate SKU→product_id)

4. **Test trend queries**
   - Verify week-over-week classification produces sensible results

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
