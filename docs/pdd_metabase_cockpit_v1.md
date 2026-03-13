# PDD Metabase Cockpit v1

## Reference surface

The target reference is the downloaded PDF screenshot:

- `/Volumes/DataHub/ecommerce/raw/pdd/dashboard_screenshot_1773440593823.pdf`

Dashboard v1 is no longer treated as a generic BI page set. It is treated as the first browser-based replacement for the boss-facing WPS operating cockpit shown in that screenshot.

## Screenshot-to-dashboard mapping

### A. Top-level summary charts

Purpose:
- recreate the first visual scan area the boss sees at the top of the page
- emphasize platform-wide trend movement before drilling into lists or cards

Metabase source:
- `reporting.vw_pdd_cockpit_top_summary_daily`

Suggested cards:
1. `PDD 总成交金额趋势`
2. `PDD 总买家数 / 转化率趋势`

Notes:
- stable section
- backed only by trusted shop-day reporting data

### B. Ranked focus lists

Purpose:
- reproduce the screenshot’s ranked attention blocks under the top charts
- answer “which shops / SKUs need attention now?”

Metabase sources:
- `reporting.vw_pdd_cockpit_focus_shop_rank_30d`
- `reporting.vw_pdd_cockpit_focus_sku_rank_30d`

Suggested cards:
1. `重点店铺榜（近30天）`
2. `重点SKU榜（近30天）`

Notes:
- stable section
- shop rankings come from trusted shop-day facts
- SKU rankings come from trusted SKU-day facts

### C. Grouped product monitoring cards

Purpose:
- approximate the screenshot’s dense grid of small product/SPU monitoring tiles
- give the boss a scanning surface rather than a single detail table

Metabase source:
- `reporting.vw_pdd_cockpit_product_cards_30d`

Suggested usage:
- build small trend cards by `卡片排序`
- group cards by `店铺`
- each card uses one `商品名称 + 商品规格` over the recent 30-day window

Notes:
- stable section
- still SKU-level, not SPU-level
- this is the nearest trusted replacement for part of the screenshot’s product card area

### D. SPU monitoring cards (trial)

Purpose:
- provide the first browser-based replacement for the screenshot’s SPU-focused monitoring zone
- explicitly mark this zone as a trial preview

Metabase sources:
- `reporting.vw_pdd_dashboard_spu_monitoring_trial`
- `reporting.vw_pdd_cockpit_spu_trial_cards`

Suggested cards:
1. `SPU监控（试运行）总表`
2. `SPU监控（试运行）重点卡片`

Notes:
- provisional section
- powered by normalized `SPU源数据` logic, not a formal SPU fact family
- should be visually separated in Metabase with `试运行` labeling

## Stable vs provisional separation

Stable sections:
- top-level summary charts
- ranked focus lists
- grouped product monitoring cards based on trusted SKU data

Provisional section:
- `SPU监控（试运行）`

This should be obvious in the dashboard title, section headers, and descriptions.

## Recommended single-page Metabase layout

1. top row: 2 wide summary charts
2. second row: 2 ranked focus lists
3. middle rows: grouped SKU monitoring cards
4. lower rows: `SPU监控（试运行）` cards and preview table

This preserves the screenshot’s operating-cockpit feel better than splitting everything into generic page tabs.

## Reporting objects used

- `reporting.vw_pdd_cockpit_top_summary_daily`
- `reporting.vw_pdd_cockpit_focus_shop_rank_30d`
- `reporting.vw_pdd_cockpit_focus_sku_rank_30d`
- `reporting.vw_pdd_cockpit_product_cards_30d`
- `reporting.vw_pdd_dashboard_shop_daily`
- `reporting.vw_pdd_dashboard_sku_daily`
- `reporting.vw_pdd_dashboard_spu_monitoring_trial`
- `reporting.vw_pdd_cockpit_spu_trial_cards`

## Why this is still v1

- it uses only already trusted shop-day and SKU-day reporting data for stable areas
- it does not claim that SPU monitoring is fully modeled yet
- it aims to feel familiar to the boss before a richer decision layer or company-wide dashboard exists
