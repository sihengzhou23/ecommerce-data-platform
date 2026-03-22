# Ingestion Hardening Requirements v1

## Purpose

This doc defines the minimum next hardening tasks for the current PDD workbook ingestion pipeline. These tasks should be validated before decision logic depends on the canonical facts.

## What Exists Now

- Two hardened contracts: `pdd_shop_daily` and `pdd_sku_daily`
- Header variant mapping with synonym resolution
- Numeric sanitization (repeated decimals `776..03` → `776.03`, blank strings → NULL)
- Canonical idempotent upserts at defined grains
- Raw row preservation in `raw_import_rows`

---

## Hardening Checklist (Required Before Decision Logic)

### 1. Header Variant Coverage ✅

**Status**: Implemented via contract variant mapping.

| Check | Shop-Daily | SKU-Daily |
|-------|------------|-----------|
| Required headers resolve | ✅ via `SOURCE_COLUMN_VARIANTS` | ✅ via `SOURCE_COLUMN_VARIANTS` |
| Fail on missing columns | ✅ raises `ValueError` | ✅ raises `ValueError` |

**Verification**: Run dry-run on each workbook (pdd_1-pdd_5). All must pass without missing column errors.

### 2. Missing Identifier Handling ⚠️ REQUIRED POLICY

**Current behavior** (per contract docs):
- `product_id`, `sku_id`, `merchant_sku_code`, `product_specification` are coalesced to empty string `''` for upsert behavior
- Missing identity rows aggregate with other empty-identity rows

**Observed from contracts**:
| Shop | Missing product_id | Missing sku_id | Missing merchant_sku_code | Missing product_specification |
|------|-------------------|----------------|--------------------------|------------------------------|
| pdd_1 | 0 | 0 | 2 | 180 |
| pdd_2 | 0 | 0 | 0 | 0 |
| pdd_3 | 0 | 1 | 8 | 6 |
| pdd_4 | 0 | 0 | 1 | 1 |
| pdd_5 | 0 | 0 | 0 | 0 |

**Required policy for v1-core**:
- **Critical identifier**: `product_id` (used for SPU proxy)
- **Critical identifier**: `sku_id` (used for SKU-level decisions)
- Rows missing `product_id` → **SKIP from canonical** (log to staging-only)
- Rows missing `sku_id` but having `product_id` → **ALLOW** (aggregate to product_id level)
- Rows missing `product_specification` → **ALLOW** (aggregates to SKU-level via other identifiers)

**Implementation** (✅ Implemented):
- Filter added in canonical SQL: `WHERE product_id IS NOT NULL AND product_id != ''`
- Rows with missing product_id preserved in staging for audit
- Quality metrics logged to `import_files.skipped_canonical_count`

### 3. Duplicate Rate Check ⚠️ REQUIRED

**Current behavior**: SKU facts aggregate via `SUM(metric)` with `source_row_count`. Shop facts use grain collapse.

**Required logging per import**:
| Metric | Calculation | Alert Threshold |
|--------|-------------|-----------------|
| SKU source vs canonical ratio | `SUM(source_row_count) / COUNT(DISTINCT canonical_key)` | > 3x → investigate |
| Shop duplicate rows | `COUNT(raw_rows) - COUNT(DISTINCT sales_date)` | Any > 0 |

**Implementation** (✅ Implemented):
- Added `duplicate_rate` column to `import_files` table
- Populated via SQL after canonical load for both SKU and shop loads
- Printed in post-load quality summary

### 4. Numeric Sanitization Logging ⚠️ REQUIRED

**Current behavior**: ETL sanitizes malformed numerics via regex, but no tracking of what was sanitized.

**Required logging**:
- Log to import_files.notes: "Numeric sanitization: N rows affected"
- Capture at staging load time via SQL

**Implementation** (✅ Implemented):
- Added `numeric_sanitization_count` column to `import_files` table
- Populated via SQL after staging load (checks for `\.{2,}` pattern in raw payloads)
- Printed in post-load quality summary

**Decision impact**: If > 10% of rows have sanitized numerics, flag for source data quality review.

### 5. Date Completeness Check ⚠️ REQUIRED

**Required check**: Each shop must have at least 7 consecutive days of data for 7-day trends to be meaningful.

```sql
-- Check for gaps > 1 day in recent 14 days
SELECT shop_id, sales_date 
FROM fact_shop_day_sales 
WHERE sales_date >= CURRENT_DATE - 14
ORDER BY shop_id, sales_date
-- manual review for gaps
```

---

## Validation Checklist Summary

| Check | Required? | Shop-Daily | SKU-Daily | Action If Fail |
|-------|-----------|------------|-----------|----------------|
| Header resolution | ✅ | ✅ Pass | ✅ Pass | Block import |
| product_id present | ✅ | N/A | ✅ Filter out empty | Remove from canonical |
| Duplicate rate | ✅ | ✅ Logged | ✅ Logged | Log, investigate |
| Numeric sanitization | ✅ Log | N/A | ✅ Logged | Flag for review |
| 7-day data coverage | ✅ | ≥7 days | ≥7 days | Defer trend logic |

---

## Deferred (Not Required for v1-core)

- Cross-workbook deduplication rules
- Data quality scoring per shop
- Automatic alerting on import anomalies
- Header variant automated test harness (manual dry-run sufficient for v1)

---

## References

- `docs/contracts/pdd_shop_daily_workbook_v1.md`
- `docs/contracts/pdd_sku_daily_workbook_v1.md`
- `etl/load_pdd_shop_daily.py`
- `etl/load_pdd_sku_daily.py`
