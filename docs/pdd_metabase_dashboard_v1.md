# PDD Dashboard v1 for Metabase

## Goal

Prepare a thin Metabase-ready reporting layer on top of PostgreSQL `edp` without building a custom frontend.

This layer now supports two presentation modes:
- page-style reporting views
- a screenshot-aligned single-page PDD operating cockpit

The dashboard target remains:
- 3 stable boss-facing surfaces backed by trusted data
- 1 provisional SPU preview surface

## Reporting objects

Metabase-facing objects live in the PostgreSQL schema `reporting`.

### Stable pages

1. `reporting.vw_pdd_dashboard_shop_daily`
   - page intent: `店铺总览`
   - source: `fact_shop_day_sales` + `stg_pdd_shop_day_sales`
   - trusted metrics only: traffic, buyers, orders, sales, refunds, favorites, derived conversion/AOV/UV based on trusted base measures

2. `reporting.vw_pdd_dashboard_shop_monthly`
   - page intent: `月度汇总`
   - source: monthly aggregation over current shop-day layer
   - trusted metrics only: monthly traffic, buyers, orders, sales, refunds, favorites, derived conversion/AOV/UV

3. `reporting.vw_pdd_dashboard_sku_daily`
   - page intent: `SKU表现`
   - source: `fact_sku_day_sales` with display-name enrichment from `stg_pdd_sku_day_sales`
   - trusted metrics only: quantity, gross amount, merchant net amount, source row count

### Provisional page

4. `reporting.vw_pdd_dashboard_spu_monitoring_trial`
   - page intent: `SPU监控（试运行）`
   - source: refreshed snapshot table `reporting.pdd_spu_monitoring_preview_snapshot`
   - upstream logic: normalized `SPU源数据` parquet processed output
   - status: preview / trial only, not a formal fact layer

## Chinese-first presentation rule

- object names stay English-first for maintainability
- dashboard-facing column aliases are Chinese-first
- view comments are Chinese-first for business users
- internal warehouse tables remain English-first

## Refresh workflow

Apply reporting objects:

```bash
psql -d edp -f /Users/ai-lab/Projects/ecommerce-data-platform/sql/migrations/2026-03-13_add_pdd_dashboard_reporting.sql
```

Refresh provisional SPU preview snapshot:

```bash
.venv/bin/python etl/refresh_pdd_spu_monitoring_preview.py
```

## Trust boundary

Trusted / stable enough for dashboard business review:
- `fact_shop_day_sales`
- `stg_pdd_shop_day_sales` traffic fields used only to derive report metrics from trusted row-level business context
- `fact_sku_day_sales`

Provisional / preview only:
- SPU monitoring snapshot derived from normalized `SPU源数据`

Not exposed in v1 dashboard layer:
- raw source rows
- source-provided ambiguous shop-day metrics where source definitions are not fully stable across files
- dashboard-style workbook formulas or WPS presentation fields

## Suggested Metabase page mapping

- `店铺总览` -> `reporting.vw_pdd_dashboard_shop_daily`
- `月度汇总` -> `reporting.vw_pdd_dashboard_shop_monthly`
- `SKU表现` -> `reporting.vw_pdd_dashboard_sku_daily`
- `SPU监控（试运行）` -> `reporting.vw_pdd_dashboard_spu_monitoring_trial`

## Screenshot-aligned cockpit mapping

For the boss-facing single-page cockpit aligned to the downloaded screenshot, use:

- `reporting.vw_pdd_cockpit_top_summary_daily`
- `reporting.vw_pdd_cockpit_focus_shop_rank_30d`
- `reporting.vw_pdd_cockpit_focus_sku_rank_30d`
- `reporting.vw_pdd_cockpit_product_cards_30d`
- `reporting.vw_pdd_cockpit_spu_trial_cards`

Detailed layout guidance lives in `docs/pdd_metabase_cockpit_v1.md`.
