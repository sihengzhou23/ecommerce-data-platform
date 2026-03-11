# Architecture

## Project Overview

This project builds a centralized data platform for a multi-platform e-commerce business.

The business currently relies on manually downloaded platform reports, WPS spreadsheets, and WPS dashboards for reporting. The goal of this project is to replace fragmented spreadsheet-based reporting with a structured data pipeline built on PostgreSQL, with future support for automation and dashboards.

---

## Current Workflow

Platform backends  
→ Staff download reports manually  
→ Staff整理/录入 WPS tables  
→ WPS dashboard  
→ Management review

Problems with the current workflow:

- data is fragmented across platforms
- definitions are inconsistent across teams
- manual updates are repetitive and error-prone
- cross-platform aggregation is difficult
- hard to automate reporting and analysis

---

## Target Workflow

Platform data  
→ raw reports / exported files  
→ transformation and mapping  
→ PostgreSQL centralized database  
→ dashboards / management reporting  
→ future AI and automation layer

---

## System Layers

### 1. Data Source Layer

Data originates from multiple e-commerce platforms.

Examples:
- Pinduoduo
- Taobao
- Douyin
- JD
- other channels

Each platform may have:
- different report structures
- different metric definitions
- different product naming conventions
- different access methods (API / manual download / future browser automation)

---

### 2. Raw Data Layer

This layer stores or references the original exported reports from each platform.

Purpose:
- preserve source data
- keep an audit trail
- allow future reprocessing
- avoid losing platform-specific details

At the current stage, raw files may be stored as Excel / CSV files in a project data folder, while import metadata is tracked separately.

---

### 3. Standardized Data Layer

This layer transforms heterogeneous platform reports into a unified schema.

Only comparable business metrics should be standardized here, such as:
- sales date
- platform
- store
- product
- sales amount
- units sold
- orders count

This layer supports:
- company-wide aggregation
- boss dashboard
- cross-platform product analysis
- future BI dashboards

---

### 4. Platform-Specific Detail Layer

Some metrics differ substantially across platforms and should not be forced into a unified cross-platform table.

Examples:
- platform-specific traffic metrics
- platform-specific advertising metrics
- ROI definitions
- recommendation or livestream metrics

These will be stored separately in future platform-specific tables or raw processing pipelines.

Purpose:
- preserve analytical detail
- support team-specific analysis
- support future AI and operational diagnosis

---

### 5. Reporting / Dashboard Layer

Different stakeholders require different views of the same data:

- channel teams view platform-specific data
- management views aggregated cross-platform data

The database remains centralized, but dashboards and filtered views differ by audience.

---

### 6. Automation Layer (Future)

For platforms without stable API access, browser automation tools such as OpenClaw may later be used to:

- log into platform backends
- download reports automatically
- place files into a standard ingestion folder
- trigger data update workflows

This layer is not the database itself. It is an execution layer that helps acquire and process data.

---

## Core Design Principles

1. Centralized database, separated views  
   One database stores unified data, while teams and management see different dashboards.

2. Preserve raw data, standardize only what matters  
   Not all metrics should be standardized. Only metrics that are comparable and relevant to management should enter the unified reporting layer.

3. Start with current operational reality  
   Initial ingestion may rely on manually maintained WPS tables if raw platform exports are not yet consistently available.

4. Build incrementally  
   Phase 1 focuses on data structure and reporting.
   Later phases can add automation, dashboarding, and AI analysis.

---

## High-Level Data Flow

Raw platform reports / existing WPS source tables  
→ field mapping and product mapping  
→ PostgreSQL standardized tables  
→ dashboard / reporting layer  
→ future automation and AI analysis

---

## Planned Phases

### Phase 1
- identify platforms, stores, and stakeholders
- identify current source tables
- define boss-facing metrics
- build initial PostgreSQL schema

### Phase 2
- ingest first real data source
- standardize key sales metrics
- validate numbers against current WPS dashboard

### Phase 3
- build dashboard layer
- support platform-specific filtered views and management summary views

### Phase 4
- automate report collection where possible
- integrate OpenClaw or similar browser automation for platforms without APIs
- expand into diagnostics and AI-assisted analysis