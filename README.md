# Ecommerce Data Platform

A PostgreSQL-first ecommerce data center for consolidating messy platform exports into a clean canonical model.

## Current state

The project now has its first working vertical slice:
- source platform: **Pinduoduo (PDD)**
- report type: **shop-daily sales**
- first shop loaded: **`pdd_5`**
- database: **PostgreSQL `edp`**
- source file root: **`/Volumes/DataHub/ecommerce`**

The first sample workbook loaded is:
- `/Volumes/DataHub/ecommerce/raw/pdd/shop-daily/pdd_5_daily_2026.xlsx`

## Objective

Build a centralized ecommerce data platform that:
- preserves raw source files
- maps messy platform exports into stable import contracts
- loads canonical business facts into PostgreSQL
- becomes the foundation for reporting, automation, and later AI-assisted workflows

## Current architecture

Current implementation follows this shape:

1. **Raw source storage**
   - DataHub drive at `/Volumes/DataHub/ecommerce`
   - raw files remain untouched

2. **ETL / normalization**
   - Python scripts parse source exports
   - source columns are mapped into canonical fields

3. **Canonical warehouse**
   - PostgreSQL database: `edp`
   - first warehouse tables:
     - `platforms`
     - `shops`
     - `import_files`
     - `fact_shop_day_sales`

4. **Inspection / validation**
   - `psql` for fast checks
   - DBeaver for browsing and ad hoc queries

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
- `data/` local development data if needed

## Key files

- `sql/schema.sql` — current PostgreSQL schema for the first PDD shop-daily slice
- `etl/load_pdd_shop_daily.py` — first ETL loader for the sample PDD workbook

## How to apply the schema

```bash
psql -d edp -f /Users/ai-lab/Projects/ecommerce-data-platform/sql/schema.sql
```

## How to run the first ETL

From the repo root:

```bash
DATABASE_URL='postgresql://ai-lab@localhost:5432/edp' python3 etl/load_pdd_shop_daily.py
```

## Useful validation queries

```sql
SELECT COUNT(*) FROM fact_shop_day_sales;
SELECT MIN(sales_date), MAX(sales_date) FROM fact_shop_day_sales;
SELECT * FROM shops;
SELECT * FROM import_files ORDER BY imported_at DESC;
```

## Next priorities

1. clean up and parameterize the first ETL slightly
2. write the PDD shop-daily import contract
3. confirm metric definitions for key PDD fields
4. ingest another PDD shop using the same pattern
5. expand to SKU-daily or another source only after the pattern is stable
