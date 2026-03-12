#!/usr/bin/env python3

import argparse
import csv
import os
import subprocess
import tempfile
import zipfile
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
import xml.etree.ElementTree as ET


WORKBOOK_PATH = "/Volumes/DataHub/ecommerce/raw/pdd/shop-daily/pdd_5_daily_2026.xlsx"
SHEET_NAME = "表格视图"
REPORT_TYPE = "shop_daily"

PLATFORM_CODE = "pdd"
PLATFORM_NAME = "Pinduoduo"
SHOP_CODE = "pdd_5"
SHOP_NAME = "pdd_5"

FIELD_MAPPING = {
    "日期": "sales_date",
    "店铺访客数": "shop_visitor_count",
    "店铺浏览量": "shop_pageview_count",
    "商品访客数": "product_visitor_count",
    "商品浏览数": "product_pageview_count",
    "成交买家数": "buyer_count",
    "成交订单数": "order_count",
    "成交金额": "gross_sales_amount",
    "成交转化率": "conversion_rate",
    "客单价": "avg_order_value",
    "UV价值": "uv_value",
    "商品收藏用户数": "product_favorite_user_count",
    "退款金额": "refund_amount",
}

CANONICAL_COLUMNS = [
    "sales_date",
    "shop_visitor_count",
    "shop_pageview_count",
    "product_visitor_count",
    "product_pageview_count",
    "buyer_count",
    "order_count",
    "gross_sales_amount",
    "conversion_rate",
    "avg_order_value",
    "uv_value",
    "product_favorite_user_count",
    "refund_amount",
    "source_row_number",
]

INTEGER_FIELDS = {
    "shop_visitor_count",
    "shop_pageview_count",
    "product_visitor_count",
    "product_pageview_count",
    "buyer_count",
    "order_count",
    "product_favorite_user_count",
    "source_row_number",
}

DECIMAL_FIELDS = {
    "gross_sales_amount",
    "conversion_rate",
    "avg_order_value",
    "uv_value",
    "refund_amount",
}

NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "doc_rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pkg_rel": "http://schemas.openxmlformats.org/package/2006/relationships",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load the first PDD shop daily workbook into PostgreSQL."
    )
    parser.add_argument("--workbook", default=WORKBOOK_PATH)
    parser.add_argument("--sheet", default=SHEET_NAME)
    parser.add_argument("--shop-code", default=SHOP_CODE)
    parser.add_argument("--shop-name", default=SHOP_NAME)
    parser.add_argument("--db-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse the workbook and print a preview without loading PostgreSQL.",
    )
    return parser.parse_args()


def excel_serial_to_date(serial_value):
    serial_number = int(float(serial_value))
    return date(1899, 12, 30) + timedelta(days=serial_number)


def column_letters_to_index(cell_reference):
    letters = ""
    for char in cell_reference:
        if char.isalpha():
            letters += char
        else:
            break
    index = 0
    for char in letters:
        index = index * 26 + (ord(char.upper()) - ord("A") + 1)
    return index - 1


def load_shared_strings(workbook_zip):
    if "xl/sharedStrings.xml" not in workbook_zip.namelist():
        return []

    root = ET.fromstring(workbook_zip.read("xl/sharedStrings.xml"))
    shared_strings = []
    for item in root.findall("main:si", NS):
        parts = [node.text or "" for node in item.iterfind(".//main:t", NS)]
        shared_strings.append("".join(parts))
    return shared_strings


def get_sheet_path(workbook_zip, sheet_name):
    workbook_root = ET.fromstring(workbook_zip.read("xl/workbook.xml"))
    rels_root = ET.fromstring(workbook_zip.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels_root.findall("pkg_rel:Relationship", NS)
    }

    sheets_node = workbook_root.find("main:sheets", NS)
    if sheets_node is None:
        raise ValueError("Workbook does not contain a sheets collection.")

    for sheet in sheets_node:
        if sheet.attrib["name"] == sheet_name:
            rel_id = sheet.attrib[
                "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
            ]
            return f"xl/{rel_map[rel_id]}"

    raise ValueError(f"Sheet not found: {sheet_name}")


def get_cell_value(cell, shared_strings):
    value_node = cell.find("main:v", NS)
    if value_node is None:
        inline_node = cell.find("main:is", NS)
        if inline_node is None:
            return ""
        parts = [node.text or "" for node in inline_node.iterfind(".//main:t", NS)]
        return "".join(parts)

    value = value_node.text or ""
    if cell.attrib.get("t") == "s":
        return shared_strings[int(value)]
    return value


def read_sheet_rows(workbook_path, sheet_name):
    with zipfile.ZipFile(workbook_path) as workbook_zip:
        shared_strings = load_shared_strings(workbook_zip)
        sheet_path = get_sheet_path(workbook_zip, sheet_name)
        sheet_root = ET.fromstring(workbook_zip.read(sheet_path))

    rows = []
    for row in sheet_root.findall(".//main:sheetData/main:row", NS):
        row_values = []
        for cell in row.findall("main:c", NS):
            index = column_letters_to_index(cell.attrib["r"])
            while len(row_values) <= index:
                row_values.append("")
            row_values[index] = get_cell_value(cell, shared_strings)
        rows.append((int(row.attrib["r"]), row_values))
    return rows


def normalize_cell(field_name, raw_value):
    if raw_value in (None, ""):
        return None

    value = str(raw_value).strip()
    if value == "":
        return None

    if field_name == "sales_date":
        return excel_serial_to_date(value).isoformat()
    if field_name in INTEGER_FIELDS:
        return str(int(float(value)))
    if field_name in DECIMAL_FIELDS:
        return format(Decimal(value), "f")
    return value


def map_rows(sheet_rows):
    if not sheet_rows:
        raise ValueError("The worksheet is empty.")

    header_row_number, header_cells = sheet_rows[0]
    if header_row_number != 1:
        raise ValueError("Expected the header row to be on row 1.")

    header_positions = {}
    for index, chinese_name in enumerate(header_cells):
        canonical_name = FIELD_MAPPING.get(str(chinese_name).strip())
        if canonical_name:
            header_positions[canonical_name] = index

    missing_columns = [
        chinese_name
        for chinese_name, canonical_name in FIELD_MAPPING.items()
        if canonical_name not in header_positions
    ]
    if missing_columns:
        raise ValueError(f"Missing expected columns: {', '.join(missing_columns)}")

    mapped_rows = []
    for source_row_number, row_values in sheet_rows[1:]:
        record = {"source_row_number": str(source_row_number)}
        is_empty = True
        for canonical_name, column_index in header_positions.items():
            raw_value = row_values[column_index] if column_index < len(row_values) else ""
            normalized_value = normalize_cell(canonical_name, raw_value)
            record[canonical_name] = normalized_value or ""
            if normalized_value not in (None, ""):
                is_empty = False

        if is_empty:
            continue
        mapped_rows.append(record)

    return mapped_rows


def write_staging_csv(rows, output_path):
    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CANONICAL_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in CANONICAL_COLUMNS})


def build_sql_script(import_csv_path, workbook_path, sheet_name, row_count, shop_code, shop_name):
    escaped_csv_path = str(import_csv_path).replace("'", "''")
    escaped_workbook_path = str(workbook_path).replace("'", "''")
    escaped_file_name = Path(workbook_path).name.replace("'", "''")
    escaped_sheet_name = sheet_name.replace("'", "''")
    escaped_shop_code = shop_code.replace("'", "''")
    escaped_shop_name = shop_name.replace("'", "''")

    return f"""
\\set ON_ERROR_STOP on

BEGIN;

INSERT INTO platforms (platform_code, platform_name)
VALUES ('{PLATFORM_CODE}', '{PLATFORM_NAME}')
ON CONFLICT (platform_code) DO UPDATE
SET platform_name = EXCLUDED.platform_name;

WITH upsert_shop AS (
    INSERT INTO shops (platform_id, shop_code, shop_name)
    SELECT platform_id, '{escaped_shop_code}', '{escaped_shop_name}'
    FROM platforms
    WHERE platform_code = '{PLATFORM_CODE}'
    ON CONFLICT (shop_code) DO UPDATE
    SET platform_id = EXCLUDED.platform_id,
        shop_name = EXCLUDED.shop_name,
        is_active = TRUE
    RETURNING shop_id
)
SELECT shop_id FROM upsert_shop
UNION
SELECT shop_id FROM shops WHERE shop_code = '{escaped_shop_code}'
LIMIT 1
\\gset

INSERT INTO import_files (
    platform_id,
    shop_id,
    report_type,
    file_path,
    file_name,
    sheet_name,
    row_count_raw,
    notes
)
SELECT
    p.platform_id,
    s.shop_id,
    '{REPORT_TYPE}',
    '{escaped_workbook_path}',
    '{escaped_file_name}',
    '{escaped_sheet_name}',
    {row_count},
    'Initial PDD shop daily ETL load'
FROM platforms p
JOIN shops s ON s.platform_id = p.platform_id
WHERE p.platform_code = '{PLATFORM_CODE}'
  AND s.shop_code = '{escaped_shop_code}'
RETURNING import_file_id
\\gset

CREATE TEMP TABLE staging_fact_shop_day_sales (
    sales_date DATE,
    shop_visitor_count INT,
    shop_pageview_count INT,
    product_visitor_count INT,
    product_pageview_count INT,
    buyer_count INT,
    order_count INT,
    gross_sales_amount NUMERIC(14,2),
    conversion_rate NUMERIC(12,6),
    avg_order_value NUMERIC(14,2),
    uv_value NUMERIC(14,2),
    product_favorite_user_count INT,
    refund_amount NUMERIC(14,2),
    source_row_number INT
);

\\copy staging_fact_shop_day_sales (sales_date, shop_visitor_count, shop_pageview_count, product_visitor_count, product_pageview_count, buyer_count, order_count, gross_sales_amount, conversion_rate, avg_order_value, uv_value, product_favorite_user_count, refund_amount, source_row_number) FROM '{escaped_csv_path}' WITH (FORMAT csv, HEADER true)

INSERT INTO fact_shop_day_sales (
    shop_id,
    sales_date,
    shop_visitor_count,
    shop_pageview_count,
    product_visitor_count,
    product_pageview_count,
    buyer_count,
    order_count,
    gross_sales_amount,
    conversion_rate,
    avg_order_value,
    uv_value,
    product_favorite_user_count,
    refund_amount,
    import_file_id,
    source_row_number
)
SELECT
    :shop_id,
    sales_date,
    shop_visitor_count,
    shop_pageview_count,
    product_visitor_count,
    product_pageview_count,
    buyer_count,
    order_count,
    gross_sales_amount,
    conversion_rate,
    avg_order_value,
    uv_value,
    product_favorite_user_count,
    refund_amount,
    :import_file_id,
    source_row_number
FROM staging_fact_shop_day_sales
ON CONFLICT (shop_id, sales_date) DO UPDATE
SET shop_visitor_count = EXCLUDED.shop_visitor_count,
    shop_pageview_count = EXCLUDED.shop_pageview_count,
    product_visitor_count = EXCLUDED.product_visitor_count,
    product_pageview_count = EXCLUDED.product_pageview_count,
    buyer_count = EXCLUDED.buyer_count,
    order_count = EXCLUDED.order_count,
    gross_sales_amount = EXCLUDED.gross_sales_amount,
    conversion_rate = EXCLUDED.conversion_rate,
    avg_order_value = EXCLUDED.avg_order_value,
    uv_value = EXCLUDED.uv_value,
    product_favorite_user_count = EXCLUDED.product_favorite_user_count,
    refund_amount = EXCLUDED.refund_amount,
    import_file_id = EXCLUDED.import_file_id,
    source_row_number = EXCLUDED.source_row_number;

COMMIT;
"""


def run_psql(sql_path, db_url):
    command = ["psql"]
    if db_url:
        command.append(db_url)
    command.extend(["-f", str(sql_path)])
    subprocess.run(command, check=True)


def main():
    args = parse_args()
    workbook_path = Path(args.workbook)

    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")

    sheet_rows = read_sheet_rows(workbook_path, args.sheet)
    mapped_rows = map_rows(sheet_rows)

    if args.dry_run:
        print(f"Parsed {len(mapped_rows)} data rows from {workbook_path}")
        for preview_row in mapped_rows[:5]:
            print(preview_row)
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        csv_path = temp_dir_path / "fact_shop_day_sales.csv"
        sql_path = temp_dir_path / "load_pdd_shop_daily.sql"

        write_staging_csv(mapped_rows, csv_path)
        sql_path.write_text(
            build_sql_script(
                csv_path,
                workbook_path,
                args.sheet,
                len(mapped_rows),
                args.shop_code,
                args.shop_name,
            ),
            encoding="utf-8",
        )
        run_psql(sql_path, args.db_url)

    print(
        f"Loaded {len(mapped_rows)} rows from {workbook_path.name} into fact_shop_day_sales."
    )


if __name__ == "__main__":
    main()
