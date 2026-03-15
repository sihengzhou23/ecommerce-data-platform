## 2026-03-11

Goal:
Start building ecommerce data platform.

Work done:
- Installed PostgreSQL
- Created ecommerce database
- Designed initial schema
- Created project repository structure

Next step:
- Collect data source from channel managers
- Refine database schema

## 2026-03-13

Goal:
Turn the PDD data foundation into the first boss-facing browser surface.

Work done:
- committed strategically to going deep on PDD before expanding to other platforms
- completed PDD shop-day and SKU-day modeled slices across all 5 shops
- standardized storage boundaries under `/Volumes/DataHub/ecommerce`
- produced Chinese-first boss review exports for `日报`, `月报`, and `SPU监控`
- installed and launched Metabase locally
- inspected the WPS/PDF reference surface and corrected the dashboard target from generic BI pages to a screenshot-aligned single-page operating cockpit
- added cockpit-oriented reporting views in PostgreSQL `reporting`
- created the first Metabase dashboard shell: `PDD销售驾驶舱 v1`
- created the first boss-facing sales trend chart in Metabase

Next step:
- continue building the stable cockpit sections in Metabase
- add ranking and product monitoring cards
- keep SPU sections visibly provisional
- prepare for either proper hosted Metabase access or another short-term sharing method

## 2026-03-14

Goal:
Convert the project from a reporting-heavy posture into a cleaner control-system foundation that can support the first narrow decision layer.

Work done:
- configured ACPX/OpenCode remote control so Commander can launch headless repo work from Telegram
- aligned repo docs to current reality with `docs/data_model_v1.md`, `docs/workbook_import_contracts.md`, and `docs/architecture_status.md`
- clarified the real product destination: not a dashboard, but a shop-differentiated PDD operating control system
- captured minimum pre-decision-layer design docs:
  - `docs/ingestion_hardening_requirements.md`
  - `docs/spu_sku_operating_bridge.md`
  - `docs/seven_day_trend_signal_foundation.md`
  - `docs/decision_layer_readiness_gate.md`
  - `docs/pdd_control_logic_v1_1_core_build_plan.md`
- added low-risk reporting views to `sql/schema.sql` for SPU proxying, 7-day trends, and SKU execution leverage
- tightened the SPU 7-day trend logic to safely handle churn / last-week-only rows

Next step:
- finalize and commit the new docs + schema changes
- optionally apply the updated schema to the live `edp` database
- start the first narrow decision-layer design from the new readiness gate
- translate the Feishu/OpenCloud business blueprint into a filtered v1-core field map (build now / proxy now / defer)
