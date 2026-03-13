# PDD SKU-Daily Workbook Import Contract v1

## Purpose

This contract defines the second modeled PDD slice for the recurring `SKU源数据` workbook sheet.

Scope is intentionally narrow:
- source family: recurring PDD reporting workbooks
- supported logical extract: SKU-daily source sheet only
- staging target: `stg_pdd_sku_day_sales`
- canonical target: `fact_sku_day_sales`
- out of scope: dashboards, order modeling, product master modeling, SKU master dimensions

## Source assumptions

- Physical source unit is one workbook such as `pdd_3_workbook_2026.xlsx`.
- The workbook belongs to one shop.
- The authoritative SKU source is the single sheet whose name contains `SKU源数据`.
- Header row is expected on row `1`.
- Blank rows after the header are skipped.
- Source date values may be Excel serial dates.
- Source metric values are loaded as source-provided daily SKU rows and are not assumed to be already unique at the final SKU-day grain.

## Workbook and shop identity

- Platform is fixed as `pdd` / `Pinduoduo`.
- Shop identity is assigned by runtime arguments:
  - `--shop-code`
  - `--shop-name`
- The loader does not infer shop identity from sheet content.

## Source sheet selection

- Default behavior: auto-detect the only sheet whose name contains `SKU源数据`.
- Optional override: `--sheet <sheet_name>`.
- Validation rules:
  - fail if no sheet name contains `SKU源数据`
  - fail if more than one sheet name contains `SKU源数据`
  - fail if the selected sheet is empty
  - fail if the header row is not on row `1`

## Header mapping

| staging field | accepted source headers |
| --- | --- |
| `sales_date` | `日期` |
| `product_name` | `商品` |
| `product_id` | `商品id` |
| `merchant_sku_code` | `商家编码-SKU维度` |
| `product_specification` | `商品规格` |
| `gross_quantity` | `商品数量(件)` |
| `gross_product_amount` | `商品总价(元)` |
| `merchant_net_amount` | `商家实收金额（元）`, `商家实收金额(元)` |
| `sku_id` | `SKU-ID` |

Observed recurring header variance:
- only `merchant_net_amount` varies by full-width vs half-width parentheses

## Field meanings

### Metadata / lineage

- `import_files`: one row per workbook import for the `pdd_sku_daily` contract
- `import_file_sheets`: sheet inventory for that import
- `raw_import_rows`: one row per retained source row from the selected `SKU源数据` sheet
- `stg_pdd_sku_day_sales`: typed PDD-specific SKU source rows
- `fact_sku_day_sales`: canonical aggregated SKU-day fact rows

### Staging fields

- `sales_date`: business date from `日期`
- `product_name`: source product title text
- `product_id`: source product identifier from PDD workbook
- `merchant_sku_code`: merchant-maintained SKU code from the source workbook
- `product_specification`: source SKU/specification text
- `gross_quantity`: source quantity count
- `gross_product_amount`: source listed gross amount
- `merchant_net_amount`: source merchant received amount
- `sku_id`: source SKU identifier from PDD workbook

### Canonical fact grain

The first canonical grain is intentionally conservative:

- `shop_id + sales_date + product_id + sku_id + merchant_sku_code + product_specification`

Reason:
- source rows are frequently duplicated within a day at the raw row level
- no single identifier is fully trustworthy across all workbooks
- product master modeling is out of scope
- aggregating by the full source identity bundle is the safest first canonical cut

### Canonical fact fields

`fact_sku_day_sales` currently keeps only:
- source identity bundle fields used for the grain
- `gross_quantity`
- `gross_product_amount`
- `merchant_net_amount`
- `import_file_id`
- `source_row_count`

`product_name` stays in staging only because it behaves more like a display attribute than a durable canonical key.

## Type parsing rules

- Blank strings become `NULL` in staging.
- In the canonical fact, identity fields are normalized to empty string for uniqueness and upsert behavior.
- `日期`:
  - if numeric, parse as Excel serial date using base `1899-12-30`
  - otherwise cast as a date string
- numeric metrics are cast to `NUMERIC(14,2)`.
- raw source text remains preserved unchanged in `raw_import_rows.raw_payload`.

## Validation rules

- all required mapped headers must be present after synonym resolution
- workbook sheet inventory is always recorded
- only the selected `SKU源数据` rows are loaded for this pipeline
- every staging row must reference:
  - `import_file_id`
  - `raw_import_row_id`
  - `shop_id`
- every fact row must reference:
  - `import_file_id`
  - aggregated `source_row_count`

## ETL flow

1. upsert platform and shop metadata
2. upsert `source_file_types` for `pdd_sku_daily`
3. register one workbook in `import_files`
4. inventory all workbook tabs in `import_file_sheets`
5. load selected `SKU源数据` rows into `raw_import_rows`
6. transform to `stg_pdd_sku_day_sales`
7. aggregate to conservative canonical rows in `fact_sku_day_sales`

## Idempotent re-run behavior

- `import_files`, `import_file_sheets`, `raw_import_rows`, and `stg_pdd_sku_day_sales` preserve each rerun as a new traced import
- `fact_sku_day_sales` is idempotent at the canonical identity bundle grain because it upserts on:
  - `shop_id`
  - `sales_date`
  - `product_id`
  - `sku_id`
  - `merchant_sku_code`
  - `product_specification`

## Schema validation across the current 5 workbooks

Stable recurring headers:
- `日期`
- `商品`
- `商品id`
- `商家编码-SKU维度`
- `商品规格`
- `商品数量(件)`
- `商品总价(元)`
- `SKU-ID`

Recurring variant:
- `商家实收金额（元）` in `pdd_1`
- `商家实收金额(元)` in `pdd_2`..`pdd_5`

No additional recurring header variants were required.

## Identity findings and risks

The current workbook family shows that source identity is imperfect.

### Missing identifier fields

- `pdd_1`: `商品规格` missing in `180` rows, `商家编码-SKU维度` missing in `2` rows
- `pdd_3`: `SKU-ID` missing in `1` row, `商家编码-SKU维度` missing in `8` rows, `商品规格` missing in `6` rows
- `pdd_4`: `商家编码-SKU维度` missing in `1` row, `商品规格` missing in `1` row
- `pdd_2` and `pdd_5`: no missing values in those four identity fields

### Duplicate source-row behavior

The raw `SKU源数据` sheets are not unique at the final SKU-day identity bundle. Exact same-day duplicate rows are common, so canonical facts must aggregate source rows rather than assume one raw row equals one SKU-day fact row.

### Identifier consistency risks

Observed across the five workbooks:
- some `SKU-ID` values map to more than one `商品id`
- a small number of `SKU-ID` values map to more than one `商家编码-SKU维度`
- some `商家编码-SKU维度` values map to more than one `SKU-ID`
- many `商品id` values map to multiple `商品规格`, which is expected for multi-spec products and is one reason specification must stay in the grain for now

Interpretation:
- `SKU-ID` is useful but not sufficient alone
- `merchant_sku_code` is useful but not sufficient alone
- `product_id` is useful but not sufficient alone
- `product_specification` is required in the conservative grain to avoid over-collapsing variants

## Validation results for the current workbook family

Raw row counts after blank-row filtering:
- `pdd_1`: `141227`
- `pdd_2`: `41828`
- `pdd_3`: `112265`
- `pdd_4`: `34340`
- `pdd_5`: `3101`

Source date ranges:
- `pdd_1`: `2025-01-01` to `2026-03-01`
- `pdd_2`: `2025-01-01` to `2026-03-01`
- `pdd_3`: `2025-01-01` to `2026-03-01`
- `pdd_4`: `2025-04-03` to `2026-02-01`
- `pdd_5`: `2025-10-28` to `2026-03-04`

## Non-goals for this contract version

- no SKU master dimension
- no product master dimension
- no order or order-item modeling
- no dashboard or presentation sheet ingestion
- no assumption that source display names are canonical business keys
