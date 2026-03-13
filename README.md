# Ecommerce Data Platform

A PostgreSQL-first ecommerce data center for consolidating messy platform exports into a clean canonical model.

## Current state

The project now has its first working vertical slice:
- source platform: **Pinduoduo (PDD)**
- report type: **shop-daily sales**
- first shop loaded: **`pdd_5`**
- database: **PostgreSQL `edp`**
- source file root: **`/Volumes/DataHub/ecommerce`**

Current modeled slices:
- `pdd_shop_daily` -> `stg_pdd_shop_day_sales` -> `fact_shop_day_sales`
- `pdd_sku_daily` -> `stg_pdd_sku_day_sales` -> `fact_sku_day_sales`

The current default workbook for the shop-daily loader is:
- `/Volumes/DataHub/ecommerce/raw/pdd/workbooks/pdd_5_workbook_2026.xlsx`

The recurring workbook family currently available is:
- `/Volumes/DataHub/ecommerce/raw/pdd/workbooks/pdd_1_workbook_2026.xlsx`
- `/Volumes/DataHub/ecommerce/raw/pdd/workbooks/pdd_2_workbook_2026.xlsx`
- `/Volumes/DataHub/ecommerce/raw/pdd/workbooks/pdd_3_workbook_2026.xlsx`
- `/Volumes/DataHub/ecommerce/raw/pdd/workbooks/pdd_4_workbook_2026.xlsx`
- `/Volumes/DataHub/ecommerce/raw/pdd/workbooks/pdd_5_workbook_2026.xlsx`

## Objective

Build a centralized ecommerce data platform that:
- preserves raw source files
- maps messy platform exports into stable import contracts
- loads canonical business facts into PostgreSQL
- becomes the foundation for reporting, automation, and later AI-assisted workflows

## Current architecture

Current implementation follows this layered shape:

1. **Metadata / control**
   - `platforms`
   - `shops`
   - `source_file_types`
   - `import_files`
   - `import_file_sheets`

2. **Raw landing**
   - `raw_import_rows`
   - stores the original imported row payload as JSONB for replay and audit

3. **Report-specific staging**
    - `stg_pdd_shop_day_sales`
    - typed and cleaned fields for the current PDD shop-daily export contract
   - `stg_pdd_sku_day_sales`
   - typed and cleaned fields for the current PDD SKU-daily export contract

4. **Canonical warehouse**
    - `fact_shop_day_sales`
    - only shared `shop + day` metrics that are stable enough to compare across sources
   - `fact_sku_day_sales`
   - conservative `shop + day + source SKU identity bundle` metrics for the recurring SKU source sheet

5. **Inspection / validation**
   - `psql` for fast checks
   - DBeaver for browsing and ad hoc queries

## Storage boundaries

Operational storage is standardized under `/Volumes/DataHub/ecommerce`:

- `raw/` — original downloaded source files, for example `/Volumes/DataHub/ecommerce/raw/pdd/workbooks/`
- `processed/` — machine-generated normalized outputs, for example `/Volumes/DataHub/ecommerce/processed/pdd/workbook_family_v1/`
- warehouse layer — PostgreSQL `edp` for metadata, staging, and canonical facts

Repository responsibilities stay narrow:

- code in `etl/`
- schema and migrations in `sql/`
- contracts and notes in `docs/`
- repo-local `data/` is dev-only and non-primary

## Current scope

Phase 1 is intentionally narrow:
- CSV / Excel ingestion first
- PDD shop-daily sales first
- clean canonical fact design first
- no premature dashboards, automation frameworks, or cross-platform abstractions

## Repository structure

- `sql/` database schema
- `etl/` ingestion scripts
- `docs/` documentation and contracts
- `dashboard/` reporting assets (later)
- `data/` dev-only local scratch area if needed; not the operational storage home

## Key files

- `sql/schema.sql` — current layered PostgreSQL schema
- `sql/migrations/2026-03-12_layered_imports.sql` — migration from the original four-table slice
- `sql/migrations/2026-03-12_shop_key_tightening.sql` — adjusts `shops` uniqueness to `(platform_id, shop_code)`
- `sql/migrations/2026-03-12_workbook_sheet_inventory.sql` — adds workbook sheet inventory metadata
- `sql/migrations/2026-03-13_seed_pdd_workbook_source_types.sql` — seeds recurring workbook raw-sheet contracts
- `sql/migrations/2026-03-13_add_pdd_sku_daily.sql` — adds PDD SKU-daily staging and canonical fact tables
- `etl/load_pdd_shop_daily.py` — PDD loader with metadata -> raw -> staging -> canonical flow
- `etl/load_pdd_sku_daily.py` — PDD SKU-daily loader with metadata -> raw -> staging -> canonical flow
- `etl/build_pdd_workbook_family_parquet.py` — recurring PDD workbook extractor and parquet normalizer
- `etl/build_pdd_boss_review_exports.py` — boss-facing review exports for 日报, 月报, and SPU监控 surfaces
- `etl/refresh_pdd_spu_monitoring_preview.py` — refreshes the provisional SPU monitoring snapshot for Metabase
- `docs/contracts/pdd_shop_daily_workbook_v1.md` — formal PDD shop-daily import contract and metric semantics notes
- `docs/contracts/pdd_sku_daily_workbook_v1.md` — formal PDD SKU-daily import contract and identity notes
- `docs/pdd_boss_surface_review_v1.md` — classification and review strategy for boss-facing PDD workbook surfaces
- `docs/pdd_metabase_dashboard_v1.md` — Metabase-ready reporting layer for the first PDD dashboard
- `docs/pdd_metabase_cockpit_v1.md` — screenshot-aligned single-page PDD operating cockpit plan for Metabase

## How to apply the schema

```bash
psql -d edp -f /Users/ai-lab/Projects/ecommerce-data-platform/sql/schema.sql
```

## How to migrate an existing database

```bash
psql -d edp -f /Users/ai-lab/Projects/ecommerce-data-platform/sql/migrations/2026-03-12_layered_imports.sql
psql -d edp -f /Users/ai-lab/Projects/ecommerce-data-platform/sql/migrations/2026-03-12_shop_key_tightening.sql
psql -d edp -f /Users/ai-lab/Projects/ecommerce-data-platform/sql/migrations/2026-03-12_workbook_sheet_inventory.sql
psql -d edp -f /Users/ai-lab/Projects/ecommerce-data-platform/sql/migrations/2026-03-13_seed_pdd_workbook_source_types.sql
psql -d edp -f /Users/ai-lab/Projects/ecommerce-data-platform/sql/migrations/2026-03-13_add_pdd_sku_daily.sql
psql -d edp -f /Users/ai-lab/Projects/ecommerce-data-platform/sql/migrations/2026-03-13_add_pdd_dashboard_reporting.sql
```

## How to run the first ETL

From the repo root:

```bash
DATABASE_URL='postgresql://ai-lab@localhost:5432/edp' python3 etl/load_pdd_shop_daily.py
python3 etl/load_pdd_shop_daily.py --list-sheets --dry-run
DATABASE_URL='postgresql://ai-lab@localhost:5432/edp' python3 etl/load_pdd_sku_daily.py
python3 etl/load_pdd_sku_daily.py --list-sheets --dry-run
```

## How to build recurring workbook parquet datasets

The parquet builder uses the repo venv because `pyarrow` is installed there:

```bash
.venv/bin/python etl/build_pdd_workbook_family_parquet.py --dry-run
.venv/bin/python etl/build_pdd_workbook_family_parquet.py
```

Outputs are written deterministically to:

- `/Volumes/DataHub/ecommerce/processed/pdd/workbook_family_v1/`
- `/Volumes/DataHub/ecommerce/processed/pdd/workbook_family_v1/sheet_inventory.parquet`
- `/Volumes/DataHub/ecommerce/processed/pdd/workbook_family_v1/sheet_classification.json`
- `/Volumes/DataHub/ecommerce/processed/pdd/workbook_family_v1/run_manifest.json`
- `/Volumes/DataHub/ecommerce/processed/pdd/workbook_family_v1/datasets/<dataset_code>/data.parquet`
- `/Volumes/DataHub/ecommerce/processed/pdd/workbook_family_v1/datasets/<dataset_code>/schema_manifest.json`

The parquet builder now defaults to DataHub `processed/` instead of writing primary outputs inside the repo.

## How to build boss review exports

The boss review export uses the warehouse plus normalized `SPU源数据` parquet and writes to DataHub processed storage:

```bash
.venv/bin/python etl/build_pdd_boss_review_exports.py
```

Outputs are written to:

- `/Volumes/DataHub/ecommerce/processed/pdd/boss_review_v1/日报_review.csv`
- `/Volumes/DataHub/ecommerce/processed/pdd/boss_review_v1/月报_review.csv`
- `/Volumes/DataHub/ecommerce/processed/pdd/boss_review_v1/SPU监控_review.csv`
- `/Volumes/DataHub/ecommerce/processed/pdd/boss_review_v1/surface_classification.md`
- `/Volumes/DataHub/ecommerce/processed/pdd/boss_review_v1/run_manifest.json`

## How to prepare PDD Dashboard v1 for Metabase

Dashboard-facing reporting objects are exposed from PostgreSQL schema `reporting`.

Apply the reporting layer and refresh the provisional SPU snapshot:

```bash
psql -d edp -f /Users/ai-lab/Projects/ecommerce-data-platform/sql/migrations/2026-03-13_add_pdd_dashboard_reporting.sql
.venv/bin/python etl/refresh_pdd_spu_monitoring_preview.py
```

Metabase-ready objects:

- `reporting.vw_pdd_dashboard_shop_daily` -> `店铺总览`
- `reporting.vw_pdd_dashboard_shop_monthly` -> `月度汇总`
- `reporting.vw_pdd_dashboard_sku_daily` -> `SKU表现`
- `reporting.vw_pdd_dashboard_spu_monitoring_trial` -> `SPU监控（试运行）`

Screenshot-aligned cockpit objects:

- `reporting.vw_pdd_cockpit_top_summary_daily`
- `reporting.vw_pdd_cockpit_focus_shop_rank_30d`
- `reporting.vw_pdd_cockpit_focus_sku_rank_30d`
- `reporting.vw_pdd_cockpit_product_cards_30d`
- `reporting.vw_pdd_cockpit_spu_trial_cards`

The technical implementation stays English-first, while dashboard-facing column aliases and descriptions are Chinese-first.

## Layer responsibilities

- `source_file_types` defines a specific export contract, such as `pdd_shop_daily` version `1`, not just a platform name.
- `import_files` registers the physical workbook as the import unit.
- `import_file_sheets` inventories workbook tabs as logical sub-units and marks the relevant source tab for a pipeline.
- `raw_import_rows` keeps original imported rows so the same file can be reprocessed without reparsing business logic from the fact table.
- `stg_pdd_shop_day_sales` holds PDD-specific typed fields, including metrics that may not be safe to standardize yet.
- `stg_pdd_sku_day_sales` holds typed PDD SKU source rows and preserves source-level SKU identity attributes before canonical aggregation.
- `fact_shop_day_sales` keeps only canonical `shop + day` metrics: `buyer_count`, `order_count`, `gross_sales_amount`, and `refund_amount`.
- `fact_shop_day_sales` intentionally keeps `UNIQUE (shop_id, sales_date)` as the current canonical business rule: one canonical row per shop per business day for this supported grain.
- `fact_sku_day_sales` keeps conservative SKU-day metrics at `shop + day + product_id + sku_id + merchant_sku_code + product_specification`.

## Adding a future file type

1. Add a new `source_file_types` row for the exact report contract and version.
2. Register the workbook in `import_files` and inventory all tabs in `import_file_sheets`.
3. Land rows from the relevant source sheet into `raw_import_rows` with `import_file_id` lineage.
4. Create a report-specific staging table like `stg_<platform>_<report_type>` and transform raw JSON into typed fields there.
5. Load only clearly shared metrics into canonical facts; leave source-specific metrics in staging until definitions are proven comparable.

## Workbook-level PDD shop-daily notes

- The workbook is the physical import unit; the source tab is a logical sub-unit.
- For PDD shop-daily loads, the loader auto-detects the sheet whose name contains `日报数据源` and uses that instead of the formatted `*日报` tab.
- The loader inventories every sheet in `import_file_sheets` and flags the detected `日报数据源` tab with `sheet_role = 'shop_daily_source'`.
- Shop-daily source headers support both `支付*` and `成交*` variants for buyer, order, amount, and conversion metrics.
- The formal contract for this slice lives in `docs/contracts/pdd_shop_daily_workbook_v1.md`.

## Workbook-level PDD SKU-daily notes

- For PDD SKU-daily loads, the loader auto-detects the sheet whose name contains `SKU源数据`.
- The formal contract for this slice lives in `docs/contracts/pdd_sku_daily_workbook_v1.md`.
- The only recurring header variant currently handled is `商家实收金额（元）` vs `商家实收金额(元)`.
- The canonical SKU-day fact aggregates duplicate raw source rows at the conservative identity bundle grain instead of assuming raw-row uniqueness.

## PDD workbook family slice

This first workbook slice treats the five existing PDD Excel workbooks as a recurring workbook family, not as order-export files.

- **Raw-source sheets**: `SPU源数据`, `SKU源数据`, `日报数据源`, `推广数据源（总）`, `推广数据源（单）`, `店铺商品信息`, `商品评分数据源`
- **Mixed/presentation sheets**: monthly sheets, formatted daily sheets, monitoring sheets, matrix/helper tabs, trend tabs, and similar business-facing sheets
- **Ignore sheets**: generic `Sheet*` tabs and `WpsReserved_CellImgList`

The parquet builder inventories every workbook sheet, classifies it, and only normalizes the recurring raw-source sheets into stable datasets:

- `spu_source`
- `sku_source`
- `shop_daily_source`
- `promotion_total_source`
- `promotion_campaign_source`
- `shop_product_source`
- `product_rating_source`

Normalization rules for this slice:

- keep one stable parquet dataset per recurring raw-source sheet family
- preserve workbook path, workbook identifier, sheet name, sheet index, source row number, workbook hash, and deterministic batch id on every row
- normalize explicit header variants conservatively, such as `支付*` vs `成交*`, and full-width vs half-width punctuation in `商家实收金额`
- surface workbook-specific schema differences in `schema_manifest.json` instead of silently guessing unsupported fields
- preserve unmatched non-empty source columns in `source_extra_payload_json`
- rebuild outputs into the same target directory on every run so reruns stay deterministic and idempotent

## Boss-facing review surfaces

- `日报` is currently treated as a report surface / derived management summary that can be partially reconstructed from the shop-daily modeled slice.
- `月报` is treated as a derived management summary that can be rebuilt by monthly aggregation over shop-day facts.
- `SPU监控` is treated as a mixed inspection surface and the clearest signal for a future SPU-day modeled slice backed by `SPU源数据`.
- Classification and rationale are documented in `docs/pdd_boss_surface_review_v1.md`.

## Metabase dashboard layer

- The first PDD dashboard layer is implemented as a thin PostgreSQL reporting schema for Metabase, not a custom frontend.
- Stable pages are backed only by already trusted modeled slices: `fact_shop_day_sales`, `stg_pdd_shop_day_sales`, and `fact_sku_day_sales`.
- `SPU监控（试运行）` is clearly marked as provisional and refreshed from normalized `SPU源数据` logic via `reporting.pdd_spu_monitoring_preview_snapshot`.
- Implementation and usage notes are documented in `docs/pdd_metabase_dashboard_v1.md`.

## Screenshot-aligned cockpit

- The first boss-facing PDD dashboard is now treated as a single-page Chinese operating cockpit aligned to the reference PDF screenshot, not as a generic BI page set.
- Top summary charts, ranked focus lists, and grouped product/SPU monitoring cards are mapped into Metabase-ready reporting objects in the `reporting` schema.
- Stable cockpit sections use trusted shop-day and SKU-day reporting views.
- Provisional SPU sections are clearly separated and marked `试运行`.
- Cockpit mapping guidance lives in `docs/pdd_metabase_cockpit_v1.md`.

## Useful validation queries

```sql
SELECT COUNT(*) FROM fact_shop_day_sales;
SELECT MIN(sales_date), MAX(sales_date) FROM fact_shop_day_sales;
SELECT COUNT(*) FROM raw_import_rows;
SELECT COUNT(*) FROM import_file_sheets;
SELECT COUNT(*) FROM stg_pdd_shop_day_sales;
SELECT COUNT(*) FROM stg_pdd_sku_day_sales;
SELECT COUNT(*) FROM fact_sku_day_sales;
SELECT * FROM reporting.vw_pdd_dashboard_shop_daily LIMIT 5;
SELECT * FROM reporting.vw_pdd_dashboard_shop_monthly LIMIT 5;
SELECT * FROM reporting.vw_pdd_dashboard_sku_daily LIMIT 5;
SELECT * FROM reporting.vw_pdd_dashboard_spu_monitoring_trial LIMIT 5;
SELECT * FROM reporting.vw_pdd_cockpit_top_summary_daily LIMIT 5;
SELECT * FROM reporting.vw_pdd_cockpit_focus_shop_rank_30d LIMIT 5;
SELECT * FROM reporting.vw_pdd_cockpit_focus_sku_rank_30d LIMIT 5;
SELECT * FROM reporting.vw_pdd_cockpit_product_cards_30d LIMIT 5;
SELECT * FROM reporting.vw_pdd_cockpit_spu_trial_cards LIMIT 5;
SELECT * FROM source_file_types;
SELECT * FROM import_files ORDER BY imported_at DESC;
```

## Latest validation

- Workbook sheet auto-detection works against the current PDD pattern and selects the `*日报数据源` tab.
- `pdd_1_workbook_2026.xlsx` dry-run succeeds with the `支付*` header variant.
- `pdd_5_workbook_2026.xlsx` end-to-end load succeeds with the `成交*` header variant.
- `pdd_1_workbook_2026.xlsx` also loads end to end as an additional shop using the same contract and ETL pattern.
- `pdd_2_workbook_2026.xlsx`, `pdd_3_workbook_2026.xlsx`, and `pdd_4_workbook_2026.xlsx` also load through the same hardened contract and ETL path.
- Latest workbook import metadata records `20` inventoried sheets and `128` raw/staging rows for import `8`.
- Current live DB totals after validation: `import_file_sheets = 190`, `raw_import_rows = 334874`, `stg_pdd_shop_day_sales = 2113`, `fact_shop_day_sales = 1715`, `stg_pdd_sku_day_sales = 332761`, `fact_sku_day_sales = 100184`.
- Current fact coverage by shop: `pdd_1 = 425`, `pdd_2 = 425`, `pdd_3 = 425`, `pdd_4 = 305`, `pdd_5 = 135`.
- Current fact date coverage by shop: `pdd_1 2025-01-01..2026-03-01`, `pdd_2 2025-01-01..2026-03-01`, `pdd_3 2025-01-01..2026-03-01`, `pdd_4 2025-04-03..2026-02-01`, `pdd_5 2025-10-28..2026-03-11`.
- SKU-daily loads now exist for all 5 shops with `stg_pdd_sku_day_sales = 332761` and `fact_sku_day_sales = 100184`.
- SKU fact coverage by shop: `pdd_1 = 43825`, `pdd_2 = 13672`, `pdd_3 = 30005`, `pdd_4 = 11075`, `pdd_5 = 1607`.
- SKU fact date coverage by shop: `pdd_1 2025-01-01..2026-03-01`, `pdd_2 2025-01-01..2026-03-01`, `pdd_3 2025-01-01..2026-03-01`, `pdd_4 2025-04-03..2026-02-01`, `pdd_5 2025-10-28..2026-03-04`.
- Recurring workbook parquet normalization writes a stable batch `3eb10c49e76f0944` across all 5 workbooks.
- Parquet row counts: `spu_source = 34287`, `sku_source = 332761`, `shop_daily_source = 1708`, `promotion_total_source = 1705`, `promotion_campaign_source = 21547`, `shop_product_source = 4258`, `product_rating_source = 53806`.
- Re-running the parquet builder with the same inputs produces the same `run_manifest.json` hash, confirming deterministic/idempotent output for unchanged source files.
- Boss review exports are now available under `/Volumes/DataHub/ecommerce/processed/pdd/boss_review_v1/` with three review files: `日报_review.csv`, `月报_review.csv`, and `SPU监控_review.csv`.

## Boundary rules

- Keep original files only under `/Volumes/DataHub/ecommerce/raw/`.
- Keep generated normalized datasets only under `/Volumes/DataHub/ecommerce/processed/`.
- Keep warehouse tables only in PostgreSQL `edp`.
- Keep the repo for implementation assets only: Python, SQL, docs, contracts, and optional dev-only scratch data.

## Next priorities

1. continue building `PDD销售驾驶舱 v1` in Metabase using the new cockpit reporting views
2. add the next stable cockpit sections:
   - top summary trend 2
   - focus shop ranking
   - focus SKU ranking
   - first product monitoring cards
3. keep `SPU监控` clearly marked as `试运行` until a formal SPU fact slice exists
4. decide how the boss/company should access the dashboard next:
   - proper hosted Metabase deployment
   - or another temporary sharing path
5. after the cockpit is usable, move into richer SPU modeling and the first semantic decision layer for a future ecommerce decision agent
