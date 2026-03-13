#!/usr/bin/env python3

import argparse
import csv
import json
import os
import shutil
import subprocess
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

try:
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "pyarrow is required for boss review exports. Run this script with the repo venv: .venv/bin/python"
    ) from exc


DEFAULT_DB_URL = os.environ.get("DATABASE_URL", "postgresql://ai-lab@localhost:5432/edp")
PARQUET_ROOT = Path("/Volumes/DataHub/ecommerce/processed/pdd/workbook_family_v1")
OUTPUT_ROOT = Path("/Volumes/DataHub/ecommerce/processed/pdd/boss_review_v1")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build boss-facing PDD review exports for 日报, 月报, and SPU监控 surfaces."
    )
    parser.add_argument("--db-url", default=DEFAULT_DB_URL)
    parser.add_argument("--parquet-root", default=str(PARQUET_ROOT))
    parser.add_argument("--output-root", default=str(OUTPUT_ROOT))
    return parser.parse_args()


def run_psql_csv(db_url, sql):
    result = subprocess.run(
        ["psql", db_url, "--csv", "-c", sql],
        check=True,
        capture_output=True,
        text=True,
    )
    return list(csv.DictReader(result.stdout.splitlines()))


def write_csv(output_path, rows, fieldnames):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_text(output_path, content):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")


def to_decimal(value):
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return Decimal("0")


def format_decimal(value, places=2):
    quant = Decimal("1") if places == 0 else Decimal(f"1.{'0' * places}")
    return str(value.quantize(quant))


def build_daily_review(db_url):
    sql = """
    WITH current_shop_day AS (
        SELECT
            s.shop_code,
            f.sales_date,
            f.buyer_count,
            f.order_count,
            f.gross_sales_amount,
            f.refund_amount,
            stg.shop_visitor_count,
            stg.shop_pageview_count,
            stg.product_visitor_count,
            stg.product_pageview_count,
            stg.conversion_rate AS source_conversion_rate,
            stg.avg_order_value AS source_avg_order_value,
            stg.uv_value AS source_uv_value,
            stg.product_favorite_user_count,
            f.import_file_id,
            f.source_row_number
        FROM fact_shop_day_sales f
        JOIN shops s
          ON s.shop_id = f.shop_id
        LEFT JOIN stg_pdd_shop_day_sales stg
          ON stg.import_file_id = f.import_file_id
         AND stg.shop_id = f.shop_id
         AND stg.source_row_number = f.source_row_number
         AND stg.sales_date = f.sales_date
    )
    SELECT
        shop_code,
        sales_date,
        shop_visitor_count,
        shop_pageview_count,
        product_visitor_count,
        product_pageview_count,
        buyer_count,
        order_count,
        gross_sales_amount,
        refund_amount,
        product_favorite_user_count,
        ROUND(buyer_count::numeric / NULLIF(shop_visitor_count, 0), 4) AS derived_conversion_rate,
        source_conversion_rate,
        ROUND(gross_sales_amount::numeric / NULLIF(buyer_count, 0), 2) AS derived_avg_order_value,
        source_avg_order_value,
        ROUND(gross_sales_amount::numeric / NULLIF(shop_visitor_count, 0), 2) AS derived_uv_value,
        source_uv_value,
        import_file_id,
        source_row_number
    FROM current_shop_day
    ORDER BY shop_code, sales_date;
    """
    rows = run_psql_csv(db_url, sql)
    for row in rows:
        row["报表类型"] = "日报"
        row["报表定位"] = "经营复盘表"
        row["说明"] = "基于当前店铺日事实层和店铺日分层指标重建，暂未补齐推广明细和部分口碑展示字段。"
    return rows


def build_monthly_review(db_url):
    sql = """
    WITH current_shop_day AS (
        SELECT
            s.shop_code,
            f.sales_date,
            f.buyer_count,
            f.order_count,
            f.gross_sales_amount,
            f.refund_amount,
            stg.shop_visitor_count,
            stg.shop_pageview_count,
            stg.product_visitor_count,
            stg.product_pageview_count,
            stg.product_favorite_user_count
        FROM fact_shop_day_sales f
        JOIN shops s
          ON s.shop_id = f.shop_id
        LEFT JOIN stg_pdd_shop_day_sales stg
          ON stg.import_file_id = f.import_file_id
         AND stg.shop_id = f.shop_id
         AND stg.source_row_number = f.source_row_number
         AND stg.sales_date = f.sales_date
    )
    SELECT
        shop_code,
        TO_CHAR(DATE_TRUNC('month', sales_date), 'YYYY-MM') AS sales_month,
        COUNT(*) AS day_count,
        SUM(shop_visitor_count) AS shop_visitor_count,
        SUM(shop_pageview_count) AS shop_pageview_count,
        SUM(product_visitor_count) AS product_visitor_count,
        SUM(product_pageview_count) AS product_pageview_count,
        SUM(buyer_count) AS buyer_count,
        SUM(order_count) AS order_count,
        SUM(gross_sales_amount) AS gross_sales_amount,
        SUM(refund_amount) AS refund_amount,
        SUM(product_favorite_user_count) AS product_favorite_user_count,
        ROUND(SUM(gross_sales_amount)::numeric / NULLIF(SUM(buyer_count), 0), 2) AS derived_avg_order_value,
        ROUND(SUM(buyer_count)::numeric / NULLIF(SUM(shop_visitor_count), 0), 4) AS derived_conversion_rate,
        ROUND(SUM(gross_sales_amount)::numeric / NULLIF(SUM(shop_visitor_count), 0), 2) AS derived_uv_value
    FROM current_shop_day
    GROUP BY shop_code, DATE_TRUNC('month', sales_date)
    ORDER BY shop_code, sales_month;
    """
    rows = run_psql_csv(db_url, sql)
    for row in rows:
        row["报表类型"] = "月报"
        row["报表定位"] = "月度管理汇总"
        row["说明"] = "按当前店铺日事实层月度汇总生成，店铺关注用户数和推广拆分字段暂未完整建模。"
    return rows


def build_spu_review(parquet_root):
    data_path = parquet_root / "datasets" / "spu_source" / "data.parquet"
    if not data_path.exists():
        raise FileNotFoundError(f"SPU source parquet not found: {data_path}")

    rows = pq.read_table(data_path).to_pylist()
    latest_date_by_shop = {}
    for row in rows:
        shop_code = row["shop_code"]
        sales_date = row["sales_date"]
        if not sales_date:
            continue
        if shop_code not in latest_date_by_shop or sales_date > latest_date_by_shop[shop_code]:
            latest_date_by_shop[shop_code] = sales_date

    grouped = defaultdict(lambda: {
        "product_visitor_count": Decimal("0"),
        "product_pageview_count": Decimal("0"),
        "unit_count": Decimal("0"),
        "buyer_count": Decimal("0"),
        "order_count": Decimal("0"),
        "gross_sales_amount": Decimal("0"),
        "product_favorite_user_count": Decimal("0"),
        "source_row_count": 0,
    })

    for row in rows:
        shop_code = row["shop_code"]
        sales_date = row["sales_date"]
        if latest_date_by_shop.get(shop_code) != sales_date:
            continue
        key = (
            shop_code,
            sales_date,
            row.get("spu_name") or "",
            row.get("campaign_name") or "",
            row.get("listing_name") or "",
        )
        agg = grouped[key]
        agg["product_visitor_count"] += to_decimal(row.get("product_visitor_count"))
        agg["product_pageview_count"] += to_decimal(row.get("product_pageview_count"))
        agg["unit_count"] += to_decimal(row.get("unit_count"))
        agg["buyer_count"] += to_decimal(row.get("buyer_count"))
        agg["order_count"] += to_decimal(row.get("order_count"))
        agg["gross_sales_amount"] += to_decimal(row.get("gross_sales_amount"))
        agg["product_favorite_user_count"] += to_decimal(row.get("product_favorite_user_count"))
        agg["source_row_count"] += 1

    review_rows = []
    for key, agg in grouped.items():
        shop_code, sales_date, spu_name, campaign_name, listing_name = key
        visitors = agg["product_visitor_count"]
        buyers = agg["buyer_count"]
        gross_sales = agg["gross_sales_amount"]
        review_rows.append(
            {
                "报表类型": "SPU监控",
                "报表定位": "经营巡检面",
                "店铺": shop_code,
                "日期": sales_date,
                "SPU": spu_name,
                "计划名": campaign_name,
                "链接名称": listing_name,
                "商品访客量": format_decimal(agg["product_visitor_count"], 0),
                "商品浏览量": format_decimal(agg["product_pageview_count"], 0),
                "销量": format_decimal(agg["unit_count"], 0),
                "买家数": format_decimal(agg["buyer_count"], 0),
                "订单数": format_decimal(agg["order_count"], 0),
                "销售额": format_decimal(gross_sales, 2),
                "商品收藏用户数": format_decimal(agg["product_favorite_user_count"], 0),
                "转化率": format_decimal((buyers / visitors) if visitors else Decimal("0"), 4),
                "客单价": format_decimal((gross_sales / buyers) if buyers else Decimal("0"), 2),
                "来源行数": str(agg["source_row_count"]),
                "说明": "基于标准化 SPU源数据 生成的临时监控视图，未复刻原始图片和矩阵排版，可作为下一步 SPU 决策层的过渡输出。",
            }
        )

    review_rows.sort(
        key=lambda row: (
            row["店铺"],
            row["日期"],
            Decimal(row["销售额"]),
        ),
        reverse=True,
    )
    return review_rows


def build_surface_classification_markdown():
    return """# PDD Boss Surface Classification v1

## Summary

- `日报`: report surface and derived management summary
- `月报`: derived management summary
- `SPU监控`: mixed inspection surface backed by a missing modeled SPU slice

## 日报 family

- **Business purpose**: short-horizon operating review for daily and weekly store performance
- **Layout stability**: medium-low; sheet structure mixes day and week blocks, merged headers, formula cells, and presentation formatting
- **Metric set**: traffic, sales, promotion totals, ROI, UV value, rating, weekly rollups
- **Classification**: report surface
- **Modeled backing today**: mostly reconstructable from `fact_shop_day_sales` plus `stg_pdd_shop_day_sales`; promotion and rating fields are only partial or unstaged

## 月报 family

- **Business purpose**: boss-facing monthly rollup for store performance
- **Layout stability**: medium; tabular monthly summary exists, but promotion breakout columns vary by shop (`搜索推广`, `场景推广`, `标准推广`)
- **Metric set**: monthly buyers, orders, sales, paying AOV, conversion, followers, promotion spend share
- **Classification**: derived management summary
- **Modeled backing today**: monthly aggregation over current shop-daily fact is sufficient for core sales metrics; followers and promotion breakout remain outside the current modeled shop-daily slice

## SPU监控 family

- **Business purpose**: item-level inspection and intervention surface for important SPUs
- **Layout stability**: low as a modeling surface; workbook tabs rely on images, links, matrix blocks, picker cells, and manual inspection layout
- **Metric set**: daily sales, daily visitors, promotion efficiency, and SPU ranking/selection context
- **Classification**: mixed inspection surface
- **Modeled backing today**: only partially represented; normalized `SPU源数据` can supply the monitoring metrics, but the boss-facing sheet itself is a presentation layer over a still-missing SPU modeled slice

## Implication for the future decision layer

- `日报_review` should become the daily shop operating surface backed by modeled shop-day facts and promotion metrics.
- `月报_review` should become a monthly executive rollup backed by monthly aggregates over canonical facts.
- `SPU监控_review` is the strongest signal for the next modeled slice: a PDD SPU-day layer derived from `SPU源数据`.
"""


def build_manifest(daily_rows, monthly_rows, spu_rows):
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "exports": {
            "日报_review": len(daily_rows),
            "月报_review": len(monthly_rows),
            "SPU监控_review": len(spu_rows),
        },
        "source_layers": {
            "日报_review": "fact_shop_day_sales + stg_pdd_shop_day_sales",
            "月报_review": "monthly aggregation over current shop-day layer",
            "SPU监控_review": "normalized processed parquet from SPU源数据",
        },
    }


def materialize(output_root, daily_rows, monthly_rows, spu_rows):
    output_root.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output_root.parent) as temp_dir:
        temp_root = Path(temp_dir) / output_root.name
        temp_root.mkdir(parents=True, exist_ok=True)

        write_csv(
            temp_root / "日报_review.csv",
            [
                {
                    "报表类型": row["报表类型"],
                    "报表定位": row["报表定位"],
                    "店铺": row["shop_code"],
                    "日期": row["sales_date"],
                    "店铺访客数": row["shop_visitor_count"],
                    "店铺浏览量": row["shop_pageview_count"],
                    "商品访客数": row["product_visitor_count"],
                    "商品浏览数": row["product_pageview_count"],
                    "买家数": row["buyer_count"],
                    "订单数": row["order_count"],
                    "成交金额": row["gross_sales_amount"],
                    "退款金额": row["refund_amount"],
                    "商品收藏用户数": row["product_favorite_user_count"],
                    "推导转化率": row["derived_conversion_rate"],
                    "源表转化率": row["source_conversion_rate"],
                    "推导客单价": row["derived_avg_order_value"],
                    "源表客单价": row["source_avg_order_value"],
                    "推导UV价值": row["derived_uv_value"],
                    "源表UV价值": row["source_uv_value"],
                    "导入批次": row["import_file_id"],
                    "来源行号": row["source_row_number"],
                    "说明": row["说明"],
                }
                for row in daily_rows
            ],
            [
                "报表类型",
                "报表定位",
                "店铺",
                "日期",
                "店铺访客数",
                "店铺浏览量",
                "商品访客数",
                "商品浏览数",
                "买家数",
                "订单数",
                "成交金额",
                "退款金额",
                "商品收藏用户数",
                "推导转化率",
                "源表转化率",
                "推导客单价",
                "源表客单价",
                "推导UV价值",
                "源表UV价值",
                "导入批次",
                "来源行号",
                "说明",
            ],
        )
        write_csv(
            temp_root / "月报_review.csv",
            [
                {
                    "报表类型": row["报表类型"],
                    "报表定位": row["报表定位"],
                    "店铺": row["shop_code"],
                    "月份": row["sales_month"],
                    "覆盖天数": row["day_count"],
                    "店铺访客数": row["shop_visitor_count"],
                    "店铺浏览量": row["shop_pageview_count"],
                    "商品访客数": row["product_visitor_count"],
                    "商品浏览数": row["product_pageview_count"],
                    "买家数": row["buyer_count"],
                    "订单数": row["order_count"],
                    "成交金额": row["gross_sales_amount"],
                    "退款金额": row["refund_amount"],
                    "商品收藏用户数": row["product_favorite_user_count"],
                    "推导客单价": row["derived_avg_order_value"],
                    "推导转化率": row["derived_conversion_rate"],
                    "推导UV价值": row["derived_uv_value"],
                    "说明": row["说明"],
                }
                for row in monthly_rows
            ],
            [
                "报表类型",
                "报表定位",
                "店铺",
                "月份",
                "覆盖天数",
                "店铺访客数",
                "店铺浏览量",
                "商品访客数",
                "商品浏览数",
                "买家数",
                "订单数",
                "成交金额",
                "退款金额",
                "商品收藏用户数",
                "推导客单价",
                "推导转化率",
                "推导UV价值",
                "说明",
            ],
        )
        write_csv(
            temp_root / "SPU监控_review.csv",
            spu_rows,
            [
                "报表类型",
                "报表定位",
                "店铺",
                "日期",
                "SPU",
                "计划名",
                "链接名称",
                "商品访客量",
                "商品浏览量",
                "销量",
                "买家数",
                "订单数",
                "销售额",
                "商品收藏用户数",
                "转化率",
                "客单价",
                "来源行数",
                "说明",
            ],
        )
        write_text(temp_root / "surface_classification.md", build_surface_classification_markdown())
        write_text(temp_root / "run_manifest.json", json.dumps(build_manifest(daily_rows, monthly_rows, spu_rows), ensure_ascii=False, indent=2) + "\n")

        if output_root.exists():
            shutil.rmtree(output_root)
        shutil.move(str(temp_root), str(output_root))


def main():
    args = parse_args()
    parquet_root = Path(args.parquet_root)
    output_root = Path(args.output_root)

    daily_rows = build_daily_review(args.db_url)
    monthly_rows = build_monthly_review(args.db_url)
    spu_rows = build_spu_review(parquet_root)
    materialize(output_root, daily_rows, monthly_rows, spu_rows)

    print(f"Wrote boss review exports to {output_root}")
    print(f"- 日报_review: {len(daily_rows)} rows")
    print(f"- 月报_review: {len(monthly_rows)} rows")
    print(f"- SPU监控_review: {len(spu_rows)} rows")


if __name__ == "__main__":
    main()
