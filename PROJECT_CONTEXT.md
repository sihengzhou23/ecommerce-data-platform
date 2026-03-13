# Project Context – Ecommerce Data Platform

## 1. Project Overview

This project builds a lightweight internal data platform for an ecommerce company.

The company sells cosmetic and nail products through multiple ecommerce platforms in China, including:

- Pinduoduo
- Douyin
- Tianmao
- Taobao
- Weipinhui
- Xiaohongshu
- Wechat
- Tiktok
- jingdong

Currently, employees manually download reports from each platform and upload them to shared spreadsheets (WPS). Management views daily performance reports through Feishu.

The goal of this project is to gradually automate this workflow.

The system will eventually:

1. Collect raw data from ecommerce platforms
2. Store data in a centralized database
3. Process and clean the data
4. Generate daily reports automatically
5. Provide AI-assisted analysis for operations decisions

This project is being developed on a Mac mini environment.

---

## 2. Current Development Stage

Current stage: First vertical slice implemented

Completed:

- Git repository created
- Basic project structure initialized
- Local development environment configured
- PostgreSQL database `edp` created
- v1 schema implemented for the first PDD shop-daily slice
- First ETL script implemented for a real PDD Excel workbook
- First sample shop-daily workbook loaded into PostgreSQL
- Operational storage boundary standardized around `/Volumes/DataHub/ecommerce`

Not yet implemented:

- generalized multi-platform ingestion
- formalized import contracts for multiple report types
- reporting pipeline
- AI agent integration

---

## 3. Technology Stack

Primary technologies used in this project:

Database  
PostgreSQL

Programming language  
Python

Version control  
Git + GitHub

Development environment  
Mac mini (local server)

Future integrations:

- AI agents (OpenClaw)
- Feishu API
- Platform APIs (Pinduoduo, Douyin etc.)

---

## 4. High-Level System Architecture

Future system architecture:

Platforms  
(Pinduoduo / Douyin / Taobao / Xiaohongshu)

↓

Data ingestion  
(manual download → automated API later)

↓

ETL scripts (Python)

↓

Database (PostgreSQL)

↓

Analytics layer

↓

Daily reports

↓

AI agent assistance (OpenClaw)

---

## 5. Data Workflow (Current)

Current real-world workflow inside the company:

1. Employees download platform reports
2. Files are uploaded into WPS shared spreadsheets
3. Each day new rows are appended
4. Management reads daily metrics in Feishu

Future automated workflow:

Platform Data

↓

Raw Files

↓

Python ETL Scripts

↓

PostgreSQL Database

↓

Data Processing

↓

Automated Reports

↓

Feishu Dashboard

---

## 6. Current Database Tables

Current implemented core tables:

platforms  
Platform dimension table. Currently seeded with `pdd`.

shops  
Shop dimension table for platform-owned stores.

import_files  
Import lineage table for source workbook loads.

fact_shop_day_sales  
Canonical daily fact table at grain: `shop + date`.

Note: the first implementation is intentionally narrow and centered on PDD shop-daily sales.

---

## 7. Directory Structure

Implementation repo:

project-root/

etl/  
Python ETL scripts

sql/  
SQL schema and queries

docs/  
Documentation and journal notes

dashboard/  
Reporting assets (later)

data/  
Local development scratch data only if needed; not the operational storage home

Physical source-data storage lives outside the repo on DataHub:
- `/Volumes/DataHub/ecommerce/raw`
- `/Volumes/DataHub/ecommerce/processed`
- `/Volumes/DataHub/ecommerce/warehouse`
- `/Volumes/DataHub/ecommerce/contracts`
- `/Volumes/DataHub/ecommerce/docs`

Operational boundary rule:
- raw source files stay under `raw/`
- machine-generated normalized outputs stay under `processed/`
- PostgreSQL `edp` is the warehouse layer for metadata, staging, and canonical facts
- the repo stays focused on code, SQL, docs, contracts, and optional dev-only scratch data

---

## 8. Coding Guidelines

When generating code for this project:

- Use Python 3
- Prefer clear, readable code over clever solutions
- Include comments explaining business logic
- Keep scripts modular and reusable
- Avoid hardcoded paths when possible
- Use environment variables for credentials

---

## 9. AI Agent Usage (Future)

This project will eventually integrate AI agents using OpenClaw.

Agents may assist with:

- Generating data reports
- Querying the database
- Providing operational suggestions
- Automating routine tasks
- Helping developers write or modify scripts

Agents must follow project documentation and existing code structure.

---

## 10. Important Constraints

Data originates from Chinese ecommerce platforms.

Initially:

Data ingestion is manual (downloaded reports).

Later:

API integrations may replace manual downloads.

Security considerations:

- Do not expose API keys
- Do not modify database schemas without confirmation
- Avoid destructive database operations

---

## 11. Development Philosophy

This system is designed to evolve gradually.

Phase 1  
Manual Excel/CSV ingestion + structured PostgreSQL warehouse

Phase 2  
Repeatable import contracts and normalized ETL across more shops / report types

Phase 3  
Automated reporting

Phase 4  
AI-assisted operations

The system prioritizes reliability and simplicity over complexity.

---

## 12. Instructions for AI Assistants

When assisting with this project:

1. Read this document first
2. Understand the current development stage
3. Follow existing directory structure
4. Do not introduce unnecessary frameworks
5. Prefer incremental improvements

The system is under active development.
