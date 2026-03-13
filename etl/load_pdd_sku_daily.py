#!/usr/bin/env python3

import argparse
import csv
import hashlib
import json
import os
import subprocess
import tempfile
import zipfile
from datetime import date, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET


WORKBOOK_PATH = "/Volumes/DataHub/ecommerce/raw/pdd/workbooks/pdd_5_workbook_2026.xlsx"

PLATFORM_CODE = "pdd"
PLATFORM_NAME = "Pinduoduo"
SHOP_CODE = "pdd_5"
SHOP_NAME = "pdd_5"

FILE_TYPE_CODE = "pdd_sku_daily"
FILE_TYPE_NAME = "PDD SKU Daily Sales"
GRAIN_CODE = "sku_day"
FILE_TYPE_VERSION = 1
FILE_TYPE_DESCRIPTION = (
    "Pinduoduo workbook import with SKU-daily data sourced from the SKU源数据 "
    "sheet into raw rows, PDD SKU staging, and canonical SKU-day facts."
)
TARGET_SHEET_TOKEN = "SKU源数据"
TARGET_SHEET_ROLE = "sku_daily_source"

SOURCE_COLUMN_VARIANTS = {
    "sales_date": ["日期"],
    "product_name": ["商品"],
    "product_id": ["商品id"],
    "merchant_sku_code": ["商家编码-SKU维度"],
    "product_specification": ["商品规格"],
    "gross_quantity": ["商品数量(件)"],
    "gross_product_amount": ["商品总价(元)"],
    "merchant_net_amount": ["商家实收金额（元）", "商家实收金额(元)"],
    "sku_id": ["SKU-ID"],
}

PREVIEW_MAPPING = {
    "sales_date": "sales_date",
    "product_name": "product_name",
    "product_id": "product_id",
    "merchant_sku_code": "merchant_sku_code",
    "product_specification": "product_specification",
    "gross_quantity": "gross_quantity",
    "gross_product_amount": "gross_product_amount",
    "merchant_net_amount": "merchant_net_amount",
    "sku_id": "sku_id",
}

RAW_ROW_COLUMNS = ["row_number", "sheet_name", "raw_payload"]
SHEET_INVENTORY_COLUMNS = [
    "sheet_name",
    "sheet_index",
    "sheet_role",
    "is_relevant",
    "detected_row_count",
]

NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "doc_rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pkg_rel": "http://schemas.openxmlformats.org/package/2006/relationships",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load PDD SKU-daily data from a multi-sheet workbook into PostgreSQL."
    )
    parser.add_argument("--workbook", default=WORKBOOK_PATH)
    parser.add_argument("--shop-code", default=SHOP_CODE)
    parser.add_argument("--shop-name", default=SHOP_NAME)
    parser.add_argument("--db-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument(
        "--sheet",
        help="Override the auto-detected source sheet name. Defaults to the sheet containing SKU源数据.",
    )
    parser.add_argument(
        "--list-sheets",
        action="store_true",
        help="List workbook sheets, detected row counts, and the inferred target sheet.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse the workbook and print a preview without loading PostgreSQL.",
    )
    return parser.parse_args()


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


def get_workbook_sheet_defs(workbook_zip):
    workbook_root = ET.fromstring(workbook_zip.read("xl/workbook.xml"))
    rels_root = ET.fromstring(workbook_zip.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels_root.findall("pkg_rel:Relationship", NS)
    }

    sheets_node = workbook_root.find("main:sheets", NS)
    if sheets_node is None:
        raise ValueError("Workbook does not contain a sheets collection.")

    sheet_defs = []
    for sheet_index, sheet in enumerate(sheets_node, start=1):
        rel_id = sheet.attrib[
            "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
        ]
        sheet_defs.append(
            {
                "sheet_index": sheet_index,
                "sheet_name": sheet.attrib["name"],
                "sheet_path": f"xl/{rel_map[rel_id]}",
            }
        )
    return sheet_defs


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


def read_sheet_rows_from_zip(workbook_zip, shared_strings, sheet_path):
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


def inspect_workbook(workbook_path):
    with zipfile.ZipFile(workbook_path) as workbook_zip:
        shared_strings = load_shared_strings(workbook_zip)
        sheet_defs = get_workbook_sheet_defs(workbook_zip)

        inspected = []
        for sheet_def in sheet_defs:
            sheet_rows = read_sheet_rows_from_zip(
                workbook_zip,
                shared_strings,
                sheet_def["sheet_path"],
            )
            inspected.append(
                {
                    **sheet_def,
                    "sheet_rows": sheet_rows,
                    "detected_row_count": len(sheet_rows),
                }
            )
    return inspected


def detect_target_sheet_name(sheet_infos):
    candidates = [
        sheet_info["sheet_name"]
        for sheet_info in sheet_infos
        if TARGET_SHEET_TOKEN in sheet_info["sheet_name"]
    ]
    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        raise ValueError(f"No sheet name contains {TARGET_SHEET_TOKEN}.")
    raise ValueError(
        f"Multiple sheets match {TARGET_SHEET_TOKEN}: {', '.join(candidates)}"
    )


def choose_target_sheet(sheet_infos, override_sheet_name=None):
    if override_sheet_name:
        for sheet_info in sheet_infos:
            if sheet_info["sheet_name"] == override_sheet_name:
                return sheet_info
        raise ValueError(f"Sheet not found: {override_sheet_name}")

    target_sheet_name = detect_target_sheet_name(sheet_infos)
    for sheet_info in sheet_infos:
        if sheet_info["sheet_name"] == target_sheet_name:
            return sheet_info
    raise ValueError(f"Sheet not found after detection: {target_sheet_name}")


def normalize_preview_value(field_name, raw_value):
    if raw_value in (None, ""):
        return None

    value = str(raw_value).strip()
    if value == "":
        return None

    if field_name == "sales_date" and value.replace(".", "", 1).isdigit():
        serial_number = int(float(value))
        return (date(1899, 12, 30) + timedelta(days=serial_number)).isoformat()
    return value


def resolve_header_positions(header_cells):
    raw_header_positions = {}
    for index, header_value in enumerate(header_cells):
        header_name = str(header_value).strip()
        if header_name:
            raw_header_positions[header_name] = index

    canonical_positions = {}
    missing_columns = []
    for canonical_name, variants in SOURCE_COLUMN_VARIANTS.items():
        matched_header = None
        matched_index = None
        for variant in variants:
            if variant in raw_header_positions:
                matched_header = variant
                matched_index = raw_header_positions[variant]
                break
        if matched_index is None:
            missing_columns.append(" / ".join(variants))
        else:
            canonical_positions[canonical_name] = {
                "header_name": matched_header,
                "column_index": matched_index,
            }

    if missing_columns:
        raise ValueError(f"Missing expected columns: {', '.join(missing_columns)}")

    return canonical_positions, raw_header_positions


def extract_raw_rows(sheet_rows, sheet_name):
    if not sheet_rows:
        raise ValueError("The worksheet is empty.")

    header_row_number, header_cells = sheet_rows[0]
    if header_row_number != 1:
        raise ValueError("Expected the header row to be on row 1.")

    canonical_positions, raw_header_positions = resolve_header_positions(header_cells)

    raw_rows = []
    for row_number, row_values in sheet_rows[1:]:
        payload = {}
        is_empty = True
        for header_name, column_index in raw_header_positions.items():
            cell_value = row_values[column_index] if column_index < len(row_values) else ""
            normalized = str(cell_value).strip()
            payload[header_name] = normalized
            if normalized != "":
                is_empty = False

        if is_empty:
            continue

        raw_rows.append(
            {
                "row_number": row_number,
                "sheet_name": sheet_name,
                "raw_payload": payload,
                "canonical_positions": canonical_positions,
            }
        )

    return raw_rows, canonical_positions


def build_preview_rows(raw_rows, canonical_positions):
    preview_rows = []
    for raw_row in raw_rows[:5]:
        preview = {"source_row_number": str(raw_row["row_number"])}
        for canonical_name, preview_name in PREVIEW_MAPPING.items():
            source_header = canonical_positions[canonical_name]["header_name"]
            preview_value = normalize_preview_value(
                canonical_name,
                raw_row["raw_payload"].get(source_header),
            )
            preview[preview_name] = "" if preview_value is None else preview_value
        preview_rows.append(preview)
    return preview_rows


def build_sheet_inventory(sheet_infos, target_sheet_name):
    inventory_rows = []
    for sheet_info in sheet_infos:
        is_relevant = sheet_info["sheet_name"] == target_sheet_name
        inventory_rows.append(
            {
                "sheet_name": sheet_info["sheet_name"],
                "sheet_index": sheet_info["sheet_index"],
                "sheet_role": TARGET_SHEET_ROLE if is_relevant else "",
                "is_relevant": "true" if is_relevant else "false",
                "detected_row_count": sheet_info["detected_row_count"],
            }
        )
    return inventory_rows


def compute_file_hash(workbook_path):
    digest = hashlib.sha256()
    with open(workbook_path, "rb") as workbook_file:
        for chunk in iter(lambda: workbook_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(rows, fieldnames, output_path):
    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def prepare_raw_row_csv_rows(raw_rows):
    csv_rows = []
    for raw_row in raw_rows:
        csv_rows.append(
            {
                "row_number": raw_row["row_number"],
                "sheet_name": raw_row["sheet_name"],
                "raw_payload": json.dumps(raw_row["raw_payload"], ensure_ascii=False),
            }
        )
    return csv_rows


def build_sql_script(
    workbook_path,
    target_sheet_name,
    raw_row_count,
    file_hash,
    shop_code,
    shop_name,
    raw_csv_path,
    sheet_inventory_csv_path,
):
    escaped_workbook_path = str(workbook_path).replace("'", "''")
    escaped_file_name = Path(workbook_path).name.replace("'", "''")
    escaped_target_sheet_name = target_sheet_name.replace("'", "''")
    escaped_shop_code = shop_code.replace("'", "''")
    escaped_shop_name = shop_name.replace("'", "''")
    escaped_file_type_name = FILE_TYPE_NAME.replace("'", "''")
    escaped_file_type_description = FILE_TYPE_DESCRIPTION.replace("'", "''")
    escaped_file_hash = file_hash.replace("'", "''")
    escaped_raw_csv_path = str(raw_csv_path).replace("'", "''")
    escaped_sheet_inventory_csv_path = str(sheet_inventory_csv_path).replace("'", "''")

    def payload_text(*keys):
        parts = [f"NULLIF(BTRIM(r.raw_payload ->> '{key}'), '')" for key in keys]
        return f"COALESCE({', '.join(parts)})"

    def payload_numeric_text(*keys):
        return (
            "REGEXP_REPLACE("
            f"{payload_text(*keys)}, "
            "'\\.{2,}', '.', 'g'"
            ")"
        )

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
    ON CONFLICT (platform_id, shop_code) DO UPDATE
    SET shop_name = EXCLUDED.shop_name,
        is_active = TRUE
    RETURNING shop_id
)
SELECT shop_id FROM upsert_shop
UNION
SELECT shop_id
FROM shops
WHERE platform_id = (SELECT platform_id FROM platforms WHERE platform_code = '{PLATFORM_CODE}')
  AND shop_code = '{escaped_shop_code}'
LIMIT 1
\\gset

INSERT INTO source_file_types (
    platform_id,
    file_type_code,
    file_type_name,
    grain_code,
    version,
    description,
    is_active
)
SELECT
    platform_id,
    '{FILE_TYPE_CODE}',
    '{escaped_file_type_name}',
    '{GRAIN_CODE}',
    {FILE_TYPE_VERSION},
    '{escaped_file_type_description}',
    TRUE
FROM platforms
WHERE platform_code = '{PLATFORM_CODE}'
ON CONFLICT (platform_id, file_type_code, version) DO UPDATE
SET file_type_name = EXCLUDED.file_type_name,
    grain_code = EXCLUDED.grain_code,
    description = EXCLUDED.description,
    is_active = TRUE;

SELECT source_file_type_id
FROM source_file_types
WHERE platform_id = (SELECT platform_id FROM platforms WHERE platform_code = '{PLATFORM_CODE}')
  AND file_type_code = '{FILE_TYPE_CODE}'
  AND version = {FILE_TYPE_VERSION}
\\gset

INSERT INTO import_files (
    source_file_type_id,
    platform_id,
    shop_id,
    file_path,
    file_name,
    sheet_name,
    file_hash,
    row_count_raw,
    notes
)
SELECT
    :source_file_type_id,
    p.platform_id,
    s.shop_id,
    '{escaped_workbook_path}',
    '{escaped_file_name}',
    '{escaped_target_sheet_name}',
    '{escaped_file_hash}',
    {raw_row_count},
    'PDD workbook-level SKU daily load from SKU源数据'
FROM platforms p
JOIN shops s ON s.platform_id = p.platform_id
WHERE p.platform_code = '{PLATFORM_CODE}'
  AND s.shop_code = '{escaped_shop_code}'
RETURNING import_file_id
\\gset

CREATE TEMP TABLE temp_import_file_sheets (
    sheet_name VARCHAR(255),
    sheet_index INT,
    sheet_role VARCHAR(100),
    is_relevant BOOLEAN,
    detected_row_count INT
);

\\copy temp_import_file_sheets (sheet_name, sheet_index, sheet_role, is_relevant, detected_row_count) FROM '{escaped_sheet_inventory_csv_path}' WITH (FORMAT csv, HEADER true)

INSERT INTO import_file_sheets (
    import_file_id,
    sheet_name,
    sheet_index,
    sheet_role,
    is_relevant,
    detected_row_count
)
SELECT
    :import_file_id,
    sheet_name,
    sheet_index,
    NULLIF(sheet_role, ''),
    is_relevant,
    detected_row_count
FROM temp_import_file_sheets;

CREATE TEMP TABLE temp_raw_import_rows (
    row_number INT,
    sheet_name VARCHAR(255),
    raw_payload JSONB
);

\\copy temp_raw_import_rows (row_number, sheet_name, raw_payload) FROM '{escaped_raw_csv_path}' WITH (FORMAT csv, HEADER true)

INSERT INTO raw_import_rows (
    import_file_id,
    row_number,
    sheet_name,
    raw_payload
)
SELECT
    :import_file_id,
    row_number,
    NULLIF(sheet_name, ''),
    raw_payload
FROM temp_raw_import_rows;

INSERT INTO stg_pdd_sku_day_sales (
    import_file_id,
    raw_import_row_id,
    shop_id,
    source_row_number,
    sales_date,
    product_name,
    product_id,
    merchant_sku_code,
    product_specification,
    gross_quantity,
    gross_product_amount,
    merchant_net_amount,
    sku_id
)
SELECT
    r.import_file_id,
    r.raw_import_row_id,
    :shop_id,
    r.row_number,
    CASE
        WHEN {payload_text('日期')} ~ '^[0-9]+(\\.[0-9]+)?$'
            THEN DATE '1899-12-30' + FLOOR(({payload_text('日期')})::NUMERIC)::INT
        ELSE ({payload_text('日期')})::DATE
    END AS sales_date,
    {payload_text('商品')},
    {payload_text('商品id')},
    {payload_text('商家编码-SKU维度')},
    {payload_text('商品规格')},
    ({payload_numeric_text('商品数量(件)')})::NUMERIC(14,2),
    ({payload_numeric_text('商品总价(元)')})::NUMERIC(14,2),
    ({payload_numeric_text('商家实收金额（元）', '商家实收金额(元)')})::NUMERIC(14,2),
    {payload_text('SKU-ID')}
FROM raw_import_rows r
WHERE r.import_file_id = :import_file_id
  AND r.sheet_name = '{escaped_target_sheet_name}';

INSERT INTO fact_sku_day_sales (
    shop_id,
    sales_date,
    product_id,
    sku_id,
    merchant_sku_code,
    product_specification,
    gross_quantity,
    gross_product_amount,
    merchant_net_amount,
    import_file_id,
    source_row_count
)
SELECT
    shop_id,
    sales_date,
    COALESCE(product_id, ''),
    COALESCE(sku_id, ''),
    COALESCE(merchant_sku_code, ''),
    COALESCE(product_specification, ''),
    SUM(gross_quantity),
    SUM(gross_product_amount),
    SUM(merchant_net_amount),
    :import_file_id,
    COUNT(*)
FROM stg_pdd_sku_day_sales
WHERE import_file_id = :import_file_id
GROUP BY
    shop_id,
    sales_date,
    COALESCE(product_id, ''),
    COALESCE(sku_id, ''),
    COALESCE(merchant_sku_code, ''),
    COALESCE(product_specification, '')
ON CONFLICT (shop_id, sales_date, product_id, sku_id, merchant_sku_code, product_specification) DO UPDATE
SET gross_quantity = EXCLUDED.gross_quantity,
    gross_product_amount = EXCLUDED.gross_product_amount,
    merchant_net_amount = EXCLUDED.merchant_net_amount,
    import_file_id = EXCLUDED.import_file_id,
    source_row_count = EXCLUDED.source_row_count;

COMMIT;
"""


def run_psql(sql_path, db_url):
    command = ["psql"]
    if db_url:
        command.append(db_url)
    command.extend(["-f", str(sql_path)])
    subprocess.run(command, check=True)


def print_sheet_inventory(sheet_infos, inferred_target_sheet_name):
    for sheet_info in sheet_infos:
        marker = "*" if sheet_info["sheet_name"] == inferred_target_sheet_name else " "
        print(
            f"{marker} {sheet_info['sheet_index']:>2} | {sheet_info['sheet_name']} | "
            f"rows={sheet_info['detected_row_count']}"
        )


def main():
    args = parse_args()
    workbook_path = Path(args.workbook)

    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")

    sheet_infos = inspect_workbook(workbook_path)
    inferred_target_sheet_name = detect_target_sheet_name(sheet_infos)

    if args.list_sheets:
        print(f"Workbook: {workbook_path}")
        print_sheet_inventory(sheet_infos, inferred_target_sheet_name)
        if not args.dry_run:
            return

    target_sheet = choose_target_sheet(sheet_infos, args.sheet)
    raw_rows, canonical_positions = extract_raw_rows(
        target_sheet["sheet_rows"],
        target_sheet["sheet_name"],
    )

    if args.dry_run:
        print(
            f"Parsed {len(raw_rows)} data rows from {workbook_path.name} "
            f"using source sheet {target_sheet['sheet_name']}"
        )
        for preview_row in build_preview_rows(raw_rows, canonical_positions):
            print(preview_row)
        return

    file_hash = compute_file_hash(workbook_path)
    sheet_inventory_rows = build_sheet_inventory(sheet_infos, target_sheet["sheet_name"])
    raw_csv_rows = prepare_raw_row_csv_rows(raw_rows)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        raw_csv_path = temp_dir_path / "raw_import_rows.csv"
        sheet_inventory_csv_path = temp_dir_path / "import_file_sheets.csv"
        sql_path = temp_dir_path / "load_pdd_sku_daily.sql"

        write_csv(raw_csv_rows, RAW_ROW_COLUMNS, raw_csv_path)
        write_csv(sheet_inventory_rows, SHEET_INVENTORY_COLUMNS, sheet_inventory_csv_path)
        sql_path.write_text(
            build_sql_script(
                workbook_path,
                target_sheet["sheet_name"],
                len(raw_rows),
                file_hash,
                args.shop_code,
                args.shop_name,
                raw_csv_path,
                sheet_inventory_csv_path,
            ),
            encoding="utf-8",
        )
        run_psql(sql_path, args.db_url)

    print(
        f"Loaded {len(raw_rows)} rows from {workbook_path.name} using sheet "
        f"{target_sheet['sheet_name']} through import_file_sheets, raw_import_rows, "
        "stg_pdd_sku_day_sales, and fact_sku_day_sales."
    )


if __name__ == "__main__":
    main()
