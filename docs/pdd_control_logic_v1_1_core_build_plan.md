# PDD OpenCloud Control Logic V1.1-Core Build Plan

## Purpose

This blueprint defines the minimum buildable control-system release that honors the boss's direction: **system judges, operators execute** — not a dashboard, not ordinary reporting.

## Current Repo Reality

| Layer | What Exists |
|-------|-------------|
| Canonical facts | `fact_shop_day_sales`, `fact_sku_day_sales` |
| SPU-level | Trial preview only (`reporting.pdd_spu_monitoring_preview_snapshot`) |
| Trend signals | 30-day rolling views, no dedicated 7-day trend classification |
| Action engine | None |
| Category/role logic | None |

## What Stays in V1.1-Core (Minimum Scope)

### 1. Shop Operating Identification

- **Input**: `fact_shop_day_sales` + `stg_pdd_shop_day_sales` (traffic metrics)
- **Output**: Per-shop operating status classification
- **Logic**: 
  - Classify shops as `active` / `dormant` / `declining` based on 7-day presence and revenue trend
  - No budget/capability modeling — just identification of which shops need operator attention
- **Confidence**: High (direct from canonical facts)

### 2. Category/Role Identification (Lightweight)

- **Input**: Aggregated from `fact_sku_day_sales` by product category (via product_name prefix or manual mapping)
- **Output**: Category role per shop — `main_revenue` / `growth` / `test`
- **Logic**: 
  - Top 3 products by 7-day revenue = main_revenue
  - Products with rising 7-day trend but low base = growth
  - Rest = test/neutral
- **Confidence**: Medium (depends on product_name quality)

### 3. SPU Stage Identification v1

- **Input**: Aggregate `fact_sku_day_sales` by `product_id` (as SPU proxy)
- **Output**: SPU stage — `launch` / `grow` / `mature` / `decline`
- **Logic**:
  - First 7 days with sales = launch
  - 7-30 days with rising trend = grow
  - 30+ days with stable trend = mature
  - 7-day trend < 70% of 30-day avg = decline
- **Note**: Not using SPU源数据 yet — derive from SKU facts to avoid trial-data dependency
- **Confidence**: Medium (product_id as SPU proxy is approximate)

### 4. SPU × SKU Basic Linkage

- **Input**: `fact_sku_day_sales` already contains `product_id` + `sku_id` + `product_specification`
- **Output**: SKU-level conversion breakdown per SPU
- **Logic**:
  - For each SPU (product_id), list its SKUs with 7-day revenue and quantity
  - Rank SKUs by conversion contribution within the SPU
- **Confidence**: High (direct from canonical SKU facts)

### 5. 7-Day Trend Signal Layer

- **Input**: Canonical facts
- **Output**: Trend classification view
- **Signals**:
  - `rising`: this_week > last_week × 1.1
  - `falling`: this_week < last_week × 0.9
  - `stable`: within ±10%
  - `new`: no prior week data
  - `insufficient_data`: < 4 days in window
- **Implementation**: SQL views, not materialized tables (recompute on query)
- **Confidence**: High (direct from canonical facts)

### 6. Action-List Engine v0

- **Input**: Trend signals + SPU stage + SKU linkage
- **Output**: Action lists for operator

| Action Type | Trigger Condition |
|-------------|-------------------|
| today_must_do | SPU in decline stage OR SKU with falling trend in top-3 SPU |
| today_prohibited | New SPU with < 4 days data (insufficient judgment) |
| this_week_priority | SPU shifted from falling → stable/rising |

- **Confidence**:
  - `must_do`: High for decline detection, Medium for SKU breakdown
  - `prohibited`: High (system cannot judge insufficient data)
  - `weekly_priority`: Medium (depends on trend stability)

## What Is Explicitly Deferred

| Deferred Item | Reason |
|---------------|--------|
| Full budget engine | No source data for cost/budget in current workbooks |
| Tool-switch automation | Requires attribution data not in current slice |
| Page diagnosis engine | Requires SPU源数据 to be canonical first |
| Attribution-dependent logic | No order-level data in current workbooks |
| Resource-cap tuning | Requires promotion spend data not available |
| Advanced category mapping | Manual for now; auto-mapping deferred |
| SPU源数据 canonicalization | Keep as trial until mapping hypothesis validated |

## First-Release Output Model

The V1.1-core control system outputs one decision table per shop:

```
action_output_v1
├── shop_code
├── action_date
├── action_type        -- 'today_must_do' | 'today_prohibited' | 'this_week_priority'
├── priority_score     -- 1-10 (10 = urgent)
├── confidence_grade   -- 'high' | 'medium' | 'low'
├── spu_product_id
├── sku_id
├── reason_summary     -- 1-sentence judgment
├── trend_signal       -- rising/falling/stable/new/insufficient
└── suggested_action   -- concrete operator instruction
```

### Sample Outputs

| Action Type | Reason Summary |
|-------------|----------------|
| today_must_do | "SPU X has been in decline for 7 consecutive days — review pricing or promotional support" |
| today_prohibited | "SPU Y has only 2 days of sales data — insufficient trend signal to recommend action" |
| this_week_priority | "SPU Z moved from falling to stable — maintain current investment level" |

## Rule Principles (Honor Boss Direction)

1. **Shop-differentiated**: Each shop gets its own action list — no cross-shop recommendation blending
2. **SPU as decision unit**: Actions are framed at SPU level first
3. **SKU as conversion lever**: SKU details provided as drill-down within each SPU action
4. **7-day trend first**: Single-day noise ignored; all judgments based on week-over-week comparison
5. **Conflict resolution**: If must_do and prohibited conflict → prohibited wins (system won't recommend on insufficient signal)
6. **Confidence labeling**: Every action includes explicit confidence grade — high (direct signal), medium (derived), low (estimated)

## Minimum Data/Support Requirements

### Must Be True Before V1.1-Core

| Requirement | Current Status | Gap |
|-------------|----------------|-----|
| 7-day continuous data per shop | Partial (pdd_1 has 14+ months) | Need to validate all shops have 7-day coverage |
| product_id stability | Documented in SKU contract | Some product_id map to multiple SKUs — expected |
| Trend view infrastructure | Conceptual only | Need to create SQL views |
| Shop operating classification | None | Need to create logic/views |
| SPU-stage identification | None | Need to derive from SKU facts |

### High-Confidence Signals

- `fact_shop_day_sales` buyer_count, order_count, gross_sales_amount
- `fact_sku_day_sales` gross_quantity, merchant_net_amount
- 7-day week-over-week comparisons on above metrics

### Medium-Confidence (Estimated)

- SPU stage derived from product_id (proxy for true SPU)
- Category role from product_name prefix
- SKU conversion contribution within SPU

### Not Yet Available (Do Not Claim)

- Promotion spend / ROI
- Attribution / traffic source
- Order-level conversion paths
- Budget allocation data

## Recommended Implementation Order

### Phase 1: Foundation (Week 1)

1. Create 7-day trend signal views
   - `reporting.vw_pdd_shop_7d_trend`
   - `reporting.vw_pdd_sku_7d_trend`
   - `reporting.vw_pdd_spu_7d_derived` (aggregate SKU→product_id)

2. Validate data completeness
   - Check each shop has ≥7 days of recent data
   - Flag shops with gaps

### Phase 2: Core Logic (Week 2)

3. Shop operating identification
   - Classify shops: active/dormant/declining
   - Create view: `reporting.vw_pdd_shop_operating_status`

4. SPU stage identification v1
   - Derive stages from 7-day vs 30-day trend
   - Create view: `reporting.vw_pdd_spu_stage_v1`

### Phase 3: Action Engine (Week 3)

5. Build action-list engine v0
   - Create view: `reporting.vw_pdd_action_output_v1`
   - Implement must_do / prohibited / weekly_priority logic

6. Add confidence grading and reason summaries
   - Populate confidence_grade based on signal source
   - Add human-readable reason_summary

### Phase 4: Validation (Week 4)

7. End-to-end test with existing 5 shops
   - Verify outputs make sense
   - Adjust thresholds (1.1/0.9) based on observed patterns

8. Document operator-facing output format
   - Confirm with boss that output structure is usable

## Non-Goals for V1.1-Core

- No Metabase dashboard for actions (export to CSV/JSON instead)
- No automated action execution
- No SPU源数据 canonicalization
- No cross-shop recommendations
- No budget/cost modeling

## Success Criteria

- [ ] Each shop gets a daily action list with ≤20 rows
- [ ] All actions have confidence_grade populated
- [ ] No action recommended on insufficient data (prohibited wins)
- [ ] 7-day trend is the primary signal (no single-day spikes driving decisions)
- [ ] Output is exportable (CSV/JSON) for operator consumption

---

**Doc status**: V1.1-core blueprint. Not aspirational — implementation-aligned.
