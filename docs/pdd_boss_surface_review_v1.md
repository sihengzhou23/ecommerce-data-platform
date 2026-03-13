# PDD Boss Surface Review v1

## Goal

This note inspects the three boss-facing PDD workbook surfaces and defines the first lightweight review deliverable before a future dashboard or decision layer exists.

Target surfaces:
- `日报`
- `月报`
- `SPU监控`

## Sheet-family classification across the 5 workbooks

### `日报`

- **Business purpose**: daily and weekly operating review for store performance
- **Observed layout**: mixed weekly and single-day blocks, merged presentation headers, formulas, and manual formatting
- **Layout stability**: medium-low as a direct modeling source, but stable enough as a business surface family
- **Recurring metrics seen**:
  - total visitors
  - total sales
  - promotion spend / promotion sales
  - ROI
  - pageviews
  - UV value /成交UV价值
  - store rating
  - daily and weekly windows
- **Classification**: report surface / derived management summary
- **Modeled backing today**:
  - core traffic and sales metrics can be reconstructed from the current shop-daily slice
  - promotion and some reputation/display metrics still depend on unmodeled or partially modeled slices

### `月报`

- **Business purpose**: monthly boss-facing executive summary
- **Observed layout**: compact monthly table, relatively stable header row, low row count, presentation-oriented but tabular
- **Layout stability**: medium
- **Recurring metrics seen**:
  - monthly payment amount
  - buyers
  - paying AOV
  - payment conversion rate
  - followers /关注用户数
  - order count
  - promotion spend and promotion share
- **Classification**: derived management summary
- **Modeled backing today**:
  - monthly sales, buyers, orders, refunds can be derived from current shop-day facts
  - monthly conversion and UV-style metrics can be derived from shop-day staging aggregates
  - followers and promotion breakout remain outside the current shop-daily fact slice

### `SPU监控`

- **Business purpose**: item-level inspection, intervention, and campaign monitoring surface
- **Observed layout**: image cells, link labels, matrix sections, manual inspection blocks, selectors, and ranking-style presentation
- **Layout stability**: low as a direct source table; high as a business intent signal
- **Recurring metrics seen**:
  - daily sales
  - daily visitors
  - promotion efficiency style metrics
  - SPU-level inspection lists
- **Classification**: mixed inspection surface backed by a missing modeled slice
- **Modeled backing today**:
  - not reconstructable directly from current shop-day facts
  - best bridge is the normalized `SPU源数据` dataset and a future SPU-day modeled slice

## First boss-facing deliverable

Exported review set written to:

- `/Volumes/DataHub/ecommerce/processed/pdd/boss_review_v1/日报_review.csv`
- `/Volumes/DataHub/ecommerce/processed/pdd/boss_review_v1/月报_review.csv`
- `/Volumes/DataHub/ecommerce/processed/pdd/boss_review_v1/SPU监控_review.csv`
- `/Volumes/DataHub/ecommerce/processed/pdd/boss_review_v1/surface_classification.md`
- `/Volumes/DataHub/ecommerce/processed/pdd/boss_review_v1/run_manifest.json`

### `日报_review`

- Reconstructed from `fact_shop_day_sales` plus current `stg_pdd_shop_day_sales`
- Includes current daily traffic, buyers, orders, sales, refunds, favorite users, and both source-vs-derived value metrics where available
- Intended as a review export, not a pixel-perfect workbook clone

### `月报_review`

- Derived by aggregating current shop-day rows to month
- Includes monthly traffic, buyers, orders, sales, refunds, favorite users, and derived conversion / UV / AOV style metrics
- Explicitly does not claim to reproduce workbook-only fields like followers or promotion breakout yet

### `SPU监控_review`

- Derived from normalized `SPU源数据` parquet, not from the presentation sheet itself
- Uses the latest available date per shop to create a provisional SPU monitoring snapshot
- Aggregates duplicate raw source rows at `shop + date + SPU + campaign + listing` for review purposes
- Serves mainly as a bridge artifact to learn what the future SPU decision layer should contain

## Implication for the next dashboard / decision layer

- `日报` should become a daily shop operating surface backed by modeled shop-day facts plus promotion metrics.
- `月报` should become a monthly executive rollup backed by monthly aggregations over canonical daily facts.
- `SPU监控` is the clearest next modeling target: it indicates demand for a dedicated PDD SPU-day slice derived from `SPU源数据`.

## Non-goals in this pass

- no dashboard UI
- no new canonical fact family beyond what already exists
- no attempt to recreate workbook formatting, images, or WPS-specific layout behavior
