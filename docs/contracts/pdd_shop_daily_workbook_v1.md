# PDD Shop-Daily Workbook Import Contract v1

## Purpose

This contract defines the first hardened import rule for the PDD shop-daily vertical slice.

Scope is intentionally narrow:
- source family: recurring PDD reporting workbooks
- supported logical extract: shop-daily source sheet only
- canonical load target: `fact_shop_day_sales`
- out of scope: SKU-daily, SPU-daily, promotion facts, dashboard tabs, order-level modeling

## Source assumptions

- Physical source unit is one Excel workbook, for example `pdd_5_workbook_2026.xlsx`.
- The workbook belongs to one shop only.
- The authoritative shop-daily source is the single sheet whose name contains `日报数据源`.
- The formatted `*日报` sheet is presentation-only and must not feed ETL.
- Header row is expected on row `1` of the source sheet.
- Source rows after the header may contain blanks; blank rows are skipped.
- Source date values may be Excel serial dates.
- Source numeric text may occasionally contain malformed repeated decimal points such as `776..03`.

## Workbook and shop identity

- Platform is fixed as `pdd` / `Pinduoduo`.
- Shop identity is assigned by ETL runtime arguments:
  - `--shop-code`
  - `--shop-name`
- Current uniqueness rule is `shops(platform_id, shop_code)`.
- The loader does not infer shop identity from workbook contents; it relies on the caller to pass the correct shop code for the workbook.

## Source sheet selection

- Default behavior: auto-detect the only sheet whose name contains `日报数据源`.
- Optional override: `--sheet <sheet_name>`.
- Validation rules:
  - fail if no sheet name contains `日报数据源`
  - fail if more than one sheet name contains `日报数据源`
  - fail if the selected sheet is empty
  - fail if the header row is not on row `1`

## Header mapping

The loader accepts the following source header variants.

| staging field | accepted source headers |
| --- | --- |
| `sales_date` | `日期` |
| `shop_visitor_count` | `店铺访客数` |
| `shop_pageview_count` | `店铺浏览量` |
| `product_visitor_count` | `商品访客数` |
| `product_pageview_count` | `商品浏览数` |
| `buyer_count` | `成交买家数`, `支付买家数` |
| `order_count` | `成交订单数`, `支付订单数` |
| `gross_sales_amount` | `成交金额`, `支付金额` |
| `conversion_rate` | `成交转化率`, `支付转化率` |
| `avg_order_value` | `客单价` |
| `uv_value` | `UV价值` |
| `product_favorite_user_count` | `商品收藏用户数` |
| `refund_amount` | `退款金额` |

Known extra source columns currently ignored for canonical loading:
- `商品收藏率`
- `店铺评分`
- `店铺评分（1/23店铺评价分排名）`

## Field definitions

### Metadata / lineage

- `import_files`: one row per physical workbook import
- `import_file_sheets`: one row per workbook sheet inventory entry
- `raw_import_rows`: one row per retained source row from the selected `日报数据源` sheet
- `stg_pdd_shop_day_sales`: typed PDD-specific staging rows
- `fact_shop_day_sales`: canonical `shop + day` rows

### Staging fields

- `sales_date`: business date from `日期`
- `shop_visitor_count`: shop-level UV metric from the source sheet
- `shop_pageview_count`: shop-level PV metric from the source sheet
- `product_visitor_count`: product-level UV metric from the source sheet
- `product_pageview_count`: product-level PV metric from the source sheet
- `buyer_count`: paid / transacting buyer count from PDD source terminology
- `order_count`: paid / transacting order count from PDD source terminology
- `gross_sales_amount`: paid / transacted gross amount from PDD source terminology
- `conversion_rate`: source-provided conversion ratio
- `avg_order_value`: source-provided customer order value metric
- `uv_value`: source-provided value per shop visitor metric
- `product_favorite_user_count`: product favorite users count
- `refund_amount`: source-provided refund amount

### Canonical fact fields

Only these metrics are currently loaded to `fact_shop_day_sales`:
- `buyer_count`
- `order_count`
- `gross_sales_amount`
- `refund_amount`

Reason: these are the clearest shared `shop + day` measures in the current slice. Traffic and value metrics remain in staging until cross-platform semantics are better established.

## Type parsing rules

- Blank strings become `NULL`.
- `日期`:
  - if numeric, parse as Excel serial date using base `1899-12-30`
  - otherwise cast as a date string
- Integer-like metrics:
  - parse as numeric first, then `FLOOR(... )::INT`
  - this tolerates accidental `.0` style formatting
- Decimal metrics:
  - cast to numeric after sanitizing repeated decimal points with `REGEXP_REPLACE(value, '\.{2,}', '.', 'g')`
- Raw source text is preserved unchanged in `raw_import_rows.raw_payload`, even when staging sanitizes malformed numerics.

## Validation rules

- required mapped headers must be present after synonym resolution
- workbook sheet inventory is always recorded for the import
- only the selected `日报数据源` rows are loaded to `raw_import_rows` for this pipeline
- every staging row must reference:
  - `import_file_id`
  - `raw_import_row_id`
  - `shop_id`
- every fact row must reference:
  - `import_file_id`
  - `source_row_number`
- canonical uniqueness rule is intentional: `UNIQUE (shop_id, sales_date)`

## ETL flow

1. upsert platform and shop metadata
2. upsert `source_file_types` contract metadata for `pdd_shop_daily`
3. register one workbook in `import_files`
4. inventory all workbook tabs in `import_file_sheets`
5. load selected `日报数据源` rows into `raw_import_rows`
6. transform to `stg_pdd_shop_day_sales`
7. upsert canonical metrics into `fact_shop_day_sales`

## Idempotent re-run behavior

Current behavior is intentionally split by layer.

- `import_files`: not idempotent by physical file; each run creates a new import record
- `import_file_sheets`: new rows per import record
- `raw_import_rows`: new rows per import record
- `stg_pdd_shop_day_sales`: new rows per import record
- `fact_shop_day_sales`: idempotent at canonical grain because upsert key is `shop_id + sales_date`

Operational implication:
- rerunning the same workbook preserves historical import lineage
- rerunning does not duplicate canonical fact rows for the same shop-day grain
- rerunning does not delete older fact dates that are absent from a newer workbook refresh

## Metric semantics validation

The following observations were validated empirically across the five current workbooks.

### `成交转化率` / `支付转化率`

Most rows behave as:

`buyer_count / shop_visitor_count`

Evidence:
- sample `pdd_1` row 2: `481 / 8804 = 0.0546`, matches source `0.0546`
- sample `pdd_5` row 2: `6 / 38 = 0.1579`, matches source `0.1579`

Observed ambiguity / risk:
- not all rows match exactly in source data
- some rows appear obviously wrong, for example `pdd_4` row 172 has source conversion `4.95`, which is inconsistent with `152 / 3070 ≈ 0.0495`
- interpretation for this slice: treat the source column as a ratio intended to represent buyer conversion off shop visitors, but keep it in staging only because source quality and naming consistency are not fully trustworthy

### `客单价`

Most rows behave as:

`gross_sales_amount / buyer_count`

Evidence:
- sample `pdd_1` row 2: `14703.36 / 481 = 30.57`, matches source `30.57`
- sample `pdd_5` row 3: `259 / 9 = 28.78`, matches source `28.78`

Observed ambiguity / risk:
- this is usually buyer-based, not order-based
- a small number of rows do not reconcile cleanly to buyers or orders, indicating source anomalies or manual edits
- interpretation for this slice: treat `客单价` as source-provided average sales per buyer, but do not canonicalize it yet

### `UV价值`

Most rows behave as:

`gross_sales_amount / shop_visitor_count`

Evidence:
- sample `pdd_1` row 2: `14703.36 / 8804 = 1.67`, matches source `1.67`
- sample `pdd_5` row 2: `155.4 / 38 = 4.09`, matches source `4.09`

Observed ambiguity / risk:
- several rows do not reconcile, including cases such as `0.091` where `0.91` would be more plausible
- interpretation for this slice: treat `UV价值` as source-provided value per shop visitor, but keep it in staging only

## Known data-quality issues seen in current workbooks

- malformed numeric text exists, for example `776..03`
- some daily rows contain internally inconsistent derived metrics
- some sheets use `支付*` language while others use `成交*`
- some rows have blank shop rating text
- newer workbook snapshots may cover a shorter date range than older imports

## Additional shop validation using the same contract

The same contract and loader pattern were applied to:

- workbook: `pdd_1_workbook_2026.xlsx`
- shop: `pdd_1`
- selected source sheet: `①日报数据源`

Load result:
- `425` raw rows
- `425` staging rows
- `425` canonical fact rows for `pdd_1`
- date range: `2025-01-01` to `2026-03-01`

What needed to be generalized:
- source sheet names vary by shop prefix (`①日报数据源`, `⑤日报数据源`, etc.), so sheet detection must be token-based, not hardcoded
- buyer, order, amount, and conversion headers vary between `支付*` and `成交*`
- shop identity must come from runtime parameters, not a hardcoded default

What remained stable:
- workbook-level import model
- one relevant `日报数据源` sheet per workbook
- header row on row `1`
- main traffic and sales columns outside the `支付*` / `成交*` synonym set
- staging model and canonical fact grain

## Non-goals for this contract version

- no SKU/SPU fact modeling
- no dashboard or presentation sheet ingestion
- no order or order-item modeling
- no automatic pruning of stale canonical dates on refresh
