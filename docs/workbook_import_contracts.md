# Workbook Import Contracts

## Purpose

This document describes the common import pattern for workbook-based ingestion, which is the foundation of v1. Source-specific contracts extend this pattern for each workbook family.

The platform currently uses **workbook ingestion** as its primary data acquisition method. Each workbook contains multiple sheets, and the ETL pipeline selects specific sheets based on contract rules.

---

## Workbook Lineage Model

The import pipeline tracks data from physical file to canonical fact:

```
import_files
    → import_file_sheets
        → raw_import_rows
            → stg_<platform>_<grain>
                → fact_<grain>
```

| Layer | Table | Cardinality | Purpose |
| --- | --- | --- | --- |
| L0 | `import_files` | 1 per workbook | Register the physical file |
| L0 | `import_file_sheets` | N per workbook | Inventory all workbook tabs |
| L0 | `raw_import_rows` | N per selected sheet | Preserve source row verbatim |
| L1 | `stg_<platform>_<grain>` | N per selected sheet | Typed staging with field mapping |
| L2 | `fact_<grain>` | 1 per canonical grain | Canonical business fact |

---

## Sheet Detection Pattern

Workbooks often contain multiple sheets. The ETL uses token-based detection:

1. **Token matching**: The loader searches for a sheet name containing a target token (e.g., `日报数据源`, `SKU源数据`)
2. **Single-match validation**: Fails if zero or multiple sheets match the token
3. **Override option**: Allows `--sheet <name>` to explicitly select a sheet

```python
TARGET_SHEET_TOKEN = "日报数据源"  # or "SKU源数据"
```

### Why token-based?

Workbook sheet names vary by shop prefix (e.g., `①日报数据源`, `⑤日报数据源`). Token matching tolerates this variation without hardcoding every variant.

---

## Header Mapping Pattern

Source headers vary between workbooks. Each contract defines variant mappings:

| Canonical Field | Accepted Source Headers |
| --- | --- |
| `sales_date` | `日期` |
| `buyer_count` | `成交买家数`, `支付买家数` |
| `gross_sales_amount` | `成交金额`, `支付金额` |

The loader resolves variants at runtime by checking which header exists in the source.

### Field Flow

1. **Raw preservation**: Original header names stored in `raw_import_rows.raw_payload` as JSON
2. **Variant resolution**: Canonical field mapped from first matching variant
3. **Type casting**: Numeric sanitization (e.g., `776..03` → `776.03`), date parsing from Excel serial numbers
4. **Staging retention**: Full source-specific field set preserved in staging table

---

## Validation Stages

Each contract enforces validation at multiple stages:

### Stage 1: Sheet Selection
- Fail if no sheet matches target token
- Fail if multiple sheets match target token
- Fail if selected sheet is empty

### Stage 2: Header Resolution
- Fail if required mapped headers are missing after variant resolution
- Headers are resolved per-row from row 1 of the selected sheet

### Stage 3: Row Loading
- Blank rows are skipped
- Required fields are checked (not null in staging)

### Stage 4: Canonical Upsert
- Unique constraint on canonical grain enforces idempotent upsert
- Re-running same workbook updates canonical facts at the defined grain

---

## Idempotency by Layer

| Layer | Idempotent? | Behavior |
| --- | --- | --- |
| `import_files` | No | New row per import run (tracks history) |
| `import_file_sheets` | No | New rows per import record |
| `raw_import_rows` | No | New rows per import record |
| `stg_<platform>_<grain>` | No | New rows per import record |
| `fact_<grain>` | **Yes** | Upsert at canonical grain |

Re-running a workbook:
- Preserves historical import lineage (L0-L1)
- Updates canonical facts at the defined grain
- Does NOT delete older canonical dates absent from newer imports

---

## Source-Specific Contract Extension

Each workbook family extends the common pattern with:

1. **File type code**: Identifier for the source (e.g., `pdd_shop_daily`, `pdd_sku_daily`)
2. **Target sheet token**: Sheet name pattern to detect
3. **Header variants**: Source-specific column synonyms
4. **Staging fields**: Full field set from source
5. **Canonical fields**: Subset trusted for canonical use
6. **Grain definition**: Canonical uniqueness rule

### Existing Contracts

| Contract | Sheet Token | Canonical Grain |
| --- | --- | --- |
| `pdd_shop_daily` | `日报数据源` | shop + sales_date |
| `pdd_sku_daily` | `SKU源数据` | shop + sales_date + product_id + sku_id + merchant_sku_code + product_specification |

### Future Extension

New sheets from the PDD workbook family (e.g., `SPU源数据`, promotion data) would follow the same pattern:
1. Define source file type in `source_file_types`
2. Create staging table `stg_pdd_<sheet_name>`
3. Create canonical fact table if grain is justified
4. Document in a contract markdown file

---

## Type Parsing Rules

### Date Handling
- Excel serial dates parsed using base `1899-12-30`
- Text dates cast directly to DATE type

### Numeric Handling
- Blank strings become NULL
- Repeated decimal points sanitized: `REGEXP_REPLACE(value, '\.{2,}', '.', 'g')`
- Integer metrics cast with `FLOOR(... )::INT`

### Raw Preservation
- Original source text preserved unchanged in `raw_import_rows.raw_payload`
- Staging may sanitize malformed values; raw remains for audit

---

## Non-Goals

- No automatic sheet pruning or data expiration in canonical layer
- No cross-workbook deduplication at the import level (handled in canonical aggregation)
- No generic workbook parsing; each contract is explicit about its sheet and headers

---

## References

- PDD Shop-Daily Contract: `docs/contracts/pdd_shop_daily_workbook_v1.md`
- PDD SKU-Daily Contract: `docs/contracts/pdd_sku_daily_workbook_v1.md`
- Canonical Model: `docs/data_model_v1.md`
