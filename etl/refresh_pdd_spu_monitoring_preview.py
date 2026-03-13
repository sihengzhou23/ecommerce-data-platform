#!/usr/bin/env python3

import argparse
import csv
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
        "pyarrow is required for SPU monitoring preview refresh. Run with .venv/bin/python"
    ) from exc


DEFAULT_DB_URL = "postgresql://ai-lab@localhost:5432/edp"
DEFAULT_PARQUET_ROOT = Path("/Volumes/DataHub/ecommerce/processed/pdd/workbook_family_v1")
CSV_COLUMNS = [
    "shop_code",
    "sales_date",
    "spu_name",
    "campaign_name",
    "listing_name",
    "product_visitor_count",
    "product_pageview_count",
    "unit_count",
    "buyer_count",
    "order_count",
    "gross_sales_amount",
    "product_favorite_user_count",
    "derived_conversion_rate",
    "derived_avg_order_value",
    "source_row_count",
    "refreshed_at",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Refresh reporting.pdd_spu_monitoring_preview_snapshot from normalized SPU parquet."
    )
    parser.add_argument("--db-url", default=DEFAULT_DB_URL)
    parser.add_argument("--parquet-root", default=str(DEFAULT_PARQUET_ROOT))
    return parser.parse_args()


def to_decimal(value):
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return Decimal("0")


def build_rows(parquet_root, refreshed_at):
    data_path = Path(parquet_root) / "datasets" / "spu_source" / "data.parquet"
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

    output_rows = []
    for key, agg in grouped.items():
        shop_code, sales_date, spu_name, campaign_name, listing_name = key
        visitors = agg["product_visitor_count"]
        buyers = agg["buyer_count"]
        gross_sales = agg["gross_sales_amount"]
        output_rows.append(
            {
                "shop_code": shop_code,
                "sales_date": sales_date,
                "spu_name": spu_name,
                "campaign_name": campaign_name,
                "listing_name": listing_name,
                "product_visitor_count": str(agg["product_visitor_count"]),
                "product_pageview_count": str(agg["product_pageview_count"]),
                "unit_count": str(agg["unit_count"]),
                "buyer_count": str(agg["buyer_count"]),
                "order_count": str(agg["order_count"]),
                "gross_sales_amount": str(gross_sales),
                "product_favorite_user_count": str(agg["product_favorite_user_count"]),
                "derived_conversion_rate": str((buyers / visitors) if visitors else Decimal("0")),
                "derived_avg_order_value": str((gross_sales / buyers) if buyers else Decimal("0")),
                "source_row_count": str(agg["source_row_count"]),
                "refreshed_at": refreshed_at,
            }
        )

    output_rows.sort(key=lambda row: (row["shop_code"], row["sales_date"], row["spu_name"], row["campaign_name"], row["listing_name"]))
    return output_rows


def write_csv_file(csv_path, rows):
    with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def build_sql(csv_path):
    escaped_csv = str(csv_path).replace("'", "''")
    return f"""
\\set ON_ERROR_STOP on

BEGIN;

CREATE TEMP TABLE temp_spu_preview (
    shop_code TEXT,
    sales_date DATE,
    spu_name TEXT,
    campaign_name TEXT,
    listing_name TEXT,
    product_visitor_count NUMERIC(14,2),
    product_pageview_count NUMERIC(14,2),
    unit_count NUMERIC(14,2),
    buyer_count NUMERIC(14,2),
    order_count NUMERIC(14,2),
    gross_sales_amount NUMERIC(14,2),
    product_favorite_user_count NUMERIC(14,2),
    derived_conversion_rate NUMERIC(12,6),
    derived_avg_order_value NUMERIC(14,2),
    source_row_count INT,
    refreshed_at TIMESTAMP
);

\\copy temp_spu_preview ({', '.join(CSV_COLUMNS)}) FROM '{escaped_csv}' WITH (FORMAT csv, HEADER true)

TRUNCATE TABLE reporting.pdd_spu_monitoring_preview_snapshot;

INSERT INTO reporting.pdd_spu_monitoring_preview_snapshot (
    shop_code,
    sales_date,
    spu_name,
    campaign_name,
    listing_name,
    product_visitor_count,
    product_pageview_count,
    unit_count,
    buyer_count,
    order_count,
    gross_sales_amount,
    product_favorite_user_count,
    derived_conversion_rate,
    derived_avg_order_value,
    source_row_count,
    refreshed_at
)
SELECT
    shop_code,
    sales_date,
    spu_name,
    campaign_name,
    listing_name,
    product_visitor_count,
    product_pageview_count,
    unit_count,
    buyer_count,
    order_count,
    gross_sales_amount,
    product_favorite_user_count,
    derived_conversion_rate,
    derived_avg_order_value,
    source_row_count,
    refreshed_at
FROM temp_spu_preview;

COMMIT;
"""


def run_psql(db_url, sql_path):
    subprocess.run(["psql", db_url, "-f", str(sql_path)], check=True)


def main():
    args = parse_args()
    refreshed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = build_rows(args.parquet_root, refreshed_at)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        csv_path = temp_dir_path / "spu_preview.csv"
        sql_path = temp_dir_path / "refresh_spu_preview.sql"
        write_csv_file(csv_path, rows)
        sql_path.write_text(build_sql(csv_path), encoding="utf-8")
        run_psql(args.db_url, sql_path)

    print(f"Refreshed reporting.pdd_spu_monitoring_preview_snapshot with {len(rows)} rows")


if __name__ == "__main__":
    main()
