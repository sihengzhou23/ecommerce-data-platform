# Project Context – Ecommerce Data Platform

## 1. Project Overview

This project builds a lightweight internal data platform for an ecommerce company.

The company sells cosmetic and nail products through multiple ecommerce platforms in China, including:

- Pinduoduo
- Douyin
- Taobao
- Xiaohongshu

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

Current stage: Data Infrastructure Setup

Completed:

- Git repository created
- Basic project structure initialized
- Local development environment configured
- Planning database schema
- Planning ETL pipeline

Not yet implemented:

- Automated data ingestion
- Database schema implementation
- Reporting pipeline
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

## 6. Planned Database Tables

Initial core tables may include:

orders  
Stores order-level data from ecommerce platforms.

products  
Stores product catalog and SKU information.

ads  
Stores advertising performance metrics.

inventory  
Stores stock levels.

daily_metrics  
Aggregated daily performance metrics.

Note: table schemas are still under design.

---

## 7. Directory Structure

Example project structure:

project-root/

data_raw/  
Raw downloaded files

scripts/  
Python ETL scripts

sql/  
SQL schema and queries

reports/  
Generated reports

docs/  
Documentation

config/  
Configuration files

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
Manual data ingestion + structured database

Phase 2  
Automated ETL

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