# Architecture Status Note

## Current Center of Gravity

**PDD workbook ingestion** is the platform core. The implemented pipeline:

1. Excel workbook → sheet detection → raw import → staging → canonical facts
2. Two hardened contracts: `pdd_shop_daily` and `pdd_sku_daily`
3. Idempotent canonical facts at defined grains

This is the durable foundation. All other capabilities are downstream.

---

## Reporting Is Downstream

Metabase dashboards and cockpit views (`reporting.vw_*`) are **inspection artifacts**, not architecture core.

They combine:
- Canonical facts (shop-day, sku-day)
- Staging supplements (traffic metrics not yet canonicalized)
- Derived calculations (monthly rollups, rankings)

Breaking changes to canonical facts affect reporting. Breaking changes to staging do not.

---

## What Was Deferred

The following were NOT added because no source evidence supports them:

- Sales order / order line entities
- Customer entity
- Inventory snapshots
- Payment transactions
- Refund/return as separate objects
- Supplier/procurement
- Cross-platform product master
- Semantic metrics layer

Adding any of these requires:
1. Evidence of source export availability
2. Grain definition and uniqueness rule
3. Canonical contract documentation

---

## Implementation Priority

**Next: Decision layer foundation (three focused additions)**

The minimum foundation before decision logic:

1. **Ingestion hardening** - validate data quality before trend/decision investment
2. **SPU/SKU bridge** - derive SPU-level from existing SKU facts, validate SPU源数据 mapping
3. **7-day trend signals** - rolling 7-day views with week-over-week classification

See individual docs for detailed scope.

**Not next:**
- More dashboard expansion
- Cross-platform entity modeling
- Generic abstraction layers
- Full product master tables

---

## Doc Alignment

The following docs reflect implemented reality:
- `docs/data_model_v1.md` - Canonical model structure
- `docs/workbook_import_contracts.md` - Import pattern overview
- `docs/contracts/pdd_shop_daily_workbook_v1.md` - Shop-daily contract
- `docs/contracts/pdd_sku_daily_workbook_v1.md` - SKU-daily contract

## Foundation for Decision Layer

Three focused docs define the minimum foundation before decision layer work:

### 1. Ingestion Hardening
- `docs/ingestion_hardening_requirements.md` - Minimum validation tasks before decision logic

### 2. SPU/SKU Operating Bridge
- `docs/spu_sku_operating_bridge.md` - Minimum SPU-level representation for decision unit

### 3. 7-Day Trend Signal Foundation
- `docs/seven_day_trend_signal_foundation.md` - Trend signals for operational decisions

The following docs are now superseded by implemented reality:
- `docs/architecture.md` - Generic aspirational architecture
- `docs/data_mode.md` - Generic data model (not aligned with schema)

These superseded docs can be archived or updated in a future iteration.
