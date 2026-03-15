# Canonical Data Model v1

## Scope

This document describes the implemented canonical model as of v1. It reflects actual repo reality, not aspirational design.

The platform center of gravity is **PDD workbook ingestion** with durable daily sales facts. Cross-platform generic entity modeling is deferred unless source evidence justifies it.

---

## Layer 0: Import / Lineage

Metadata tables that track the ingestion pipeline.

| Table | Purpose |
| --- | --- |
| `import_files` | One row per physical workbook import |
| `import_file_sheets` | One row per workbook sheet inventory entry |
| `raw_import_rows` | One row per retained source row from the selected sheet |

Each import creates a new import record. Re-running the same workbook preserves historical lineage but does not duplicate canonical facts.

---

## Layer 1: Source-Specific Staging

Staging tables typed to the source platform and sheet.

| Table | Source |
| --- | --- |
| `stg_pdd_shop_day_sales` | PDD 日报数据源 sheet |
| `stg_pdd_sku_day_sales` | PDD SKU源数据 sheet |

These tables preserve source-specific field names and include traffic/value metrics that are not yet trusted for canonical use.

---

## Layer 2: Canonical Business Layer

The durable business facts. Idempotent at their defined grain.

### Dimensions

| Table | Grain | Description |
| --- | --- | --- |
| `platforms` | platform | Sales platform (e.g., Pinduoduo) |
| `shops` | platform + shop_code | Shop/account under a platform |

### Facts

| Table | Grain | Canonical Fields |
| --- | --- | --- |
| `fact_shop_day_sales` | shop + sales_date | buyer_count, order_count, gross_sales_amount, refund_amount |
| `fact_sku_day_sales` | shop + sales_date + product_id + sku_id + merchant_sku_code + product_specification | gross_quantity, gross_product_amount, merchant_net_amount |

### Grain Rationale

**shop_day_sales**: The clearest shared `shop + day` measures. Traffic and value metrics (visitor counts, conversion rate, AOV, UV value) remain in staging because cross-platform semantics are not yet established.

**sku_day_sales**: Uses a conservative identity bundle grain because source rows are frequently duplicated within a day and no single identifier is fully trustworthy. Aggregates source rows rather than assuming one raw row equals one canonical fact row.

---

## Layer 3: Reporting / Downstream

Views in the `reporting` schema are downstream inspection artifacts, not the platform core.

Example views:
- `reporting.vw_pdd_dashboard_shop_daily`
- `reporting.vw_pdd_dashboard_sku_daily`
- `reporting.vw_pdd_cockpit_top_summary_daily`
- `reporting.vw_pdd_cockpit_focus_shop_rank_30d`
- `reporting.vw_pdd_cockpit_focus_sku_rank_30d`

These views combine canonical facts with staging supplements for display. They are explicitly downstream and non-canonical.

---

## Currently Justified Canonical Objects

Based on implemented PDD workbook ingestion:

1. **platform** - Fixed to PDD (Pinduoduo) for v1
2. **shop** - Shop/account identity from runtime parameters
3. **fact_shop_day_sales** - Shop-level daily sales facts
4. **fact_sku_day_sales** - SKU-level daily sales facts with product identity bundle

Product identity (product_id, sku_id, merchant_sku_code, product_specification) is captured as part of the SKU fact grain but is **not** modeled as a separate canonical product dimension.

---

## Deferred Entities

The following are NOT modeled in v1 and require source evidence before adding:

| Entity | Reason for Deferral |
| --- | --- |
| `sales_order` / `sales_order_line` | No order-level export in current PDD workbooks |
| `customer` | No customer export; buyer is an aggregate count |
| `inventory` | No inventory snapshot export; only sales |
| `shipment` | No shipment export in current workbooks |
| `payment transactions` | No transaction-level export |
| `refund/return` | Only refund_amount field available (not modeled as separate entity) |
| `supplier/procurement` | Out of scope for sales-focused ingestion |
| `semantic metrics layer` | Cross-platform metric definitions not yet stable |
| `cross-platform product master` | Product identity varies by source; no canonical mapping exists yet |
| `SPU/listing` | SPU源数据 exists in workbooks but is trial/preview only |

---

## Idempotency Expectations

| Layer | Behavior |
| --- | --- |
| Layer 0 (import_files, raw_import_rows) | New rows per import run |
| Layer 1 (staging) | New rows per import run |
| Layer 2 (canonical) | Upsert at defined grain; idempotent for same shop-day or shop-day-sku grain |
| Layer 3 (reporting) | Derived on each query |

---

## Contract Extension Pattern

The workbook import pattern is designed to be extended for other sheets within the PDD workbook family:

- `pdd_shop_daily` → `stg_pdd_shop_day_sales` → `fact_shop_day_sales`
- `pdd_sku_daily` → `stg_pdd_sku_day_sales` → `fact_sku_day_sales`
- Future: other sheets (SPU源数据, 推广数据, etc.) would follow the same layered pattern

Each contract specifies:
- Source file type code
- Target sheet token for auto-detection
- Header mapping variants
- Staging-to-canonical field flow
- Canonical grain definition

See `docs/contracts/pdd_shop_daily_workbook_v1.md` and `docs/contracts/pdd_sku_daily_workbook_v1.md` for detailed contract examples.
