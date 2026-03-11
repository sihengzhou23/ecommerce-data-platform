# Data Model

## Overview

The first version of the database is designed around management reporting needs.

The boss primarily needs:
- total sales
- platform contribution
- product contribution
- trend over time

Therefore, the first version of the data model focuses on unified sales results rather than trying to standardize every platform-specific metric.

---

## Core Tables

### 1. platforms

Stores the list of sales platforms.

Fields:
- platform_id
- platform_name

Examples:
- Pinduoduo
- Taobao
- Douyin
- JD

---

### 2. stores

Stores shop/account information under each platform.

Fields:
- store_id
- store_name
- platform_id

A platform may have multiple stores.

---

### 3. products

Stores the company's standardized product list.

Fields:
- product_id
- product_name
- category

This is the internal product dimension used for unified reporting.

---

### 4. daily_sales

The core fact table for standardized reporting.

Each row represents:

one date  
+ one platform  
+ one store  
+ one product  
+ one set of sales results

Fields:
- sales_id
- sales_date
- platform_id
- store_id
- product_id
- sales_amount
- units_sold
- orders_count

This table supports:
- daily sales reporting
- platform share
- product share
- cross-platform product analysis

---

## Future Supporting Tables

### product_mapping

Purpose:
map platform-specific product names to the internal standardized product list.

Why needed:
the same product may appear under different names across platforms.

Possible fields:
- mapping_id
- platform_id
- platform_product_name
- product_id

---

### raw_imports

Purpose:
track imported files and ingestion history.

Possible fields:
- import_id
- platform_id
- file_name
- import_time
- source_type
- status

---

## Why the First Version Is Minimal

Different platforms define traffic, advertising, and algorithmic metrics differently.

Examples:
- visitor counts
- ROI
- traffic source metrics
- livestream metrics
- recommendation metrics

These should not be mixed into the first unified reporting table unless definitions are stable and comparable.

The first version therefore standardizes only core commercial outcomes:
- sales amount
- units sold
- orders count

---

## Reporting Logic

### For channel teams
The same centralized database can serve filtered views by platform.

Example:
- Pinduoduo team sees only Pinduoduo data
- Taobao team sees only Taobao data

### For management
Management views aggregate across all platforms.

Example:
- total company sales
- platform share of sales
- product share of sales across channels

---

## Grain of the Core Table

The first target grain is:

date + platform + store + product

This may later be refined to include SKU if the business consistently manages performance at SKU level.

---

## Future Expansion

Later versions may include:

- platform-specific detail tables
- advertising metrics tables
- traffic metrics tables
- SKU-level mapping tables
- automated ingestion metadata
- anomaly detection outputs

The current priority is to establish a stable and auditable reporting foundation.