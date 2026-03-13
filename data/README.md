# Local Dev Data

`data/` is repo-local and dev-only.

It is not the primary operational storage location.

Operational boundaries for this project are:
- raw source files: `/Volumes/DataHub/ecommerce/raw/`
- machine-generated processed outputs: `/Volumes/DataHub/ecommerce/processed/`
- warehouse database and related assets: PostgreSQL `edp`

Use `data/` only for temporary local experiments that should not become the main storage path.
