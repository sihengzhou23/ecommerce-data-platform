#!/usr/bin/env python3

import argparse
import hashlib
import json
import re
import shutil
import tempfile
import zipfile
from datetime import date, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.parquet as pq


WORKBOOK_ROOT = Path("/Volumes/DataHub/ecommerce/raw/pdd/workbooks")
OUTPUT_ROOT = Path("/Volumes/DataHub/ecommerce/processed/pdd/workbook_family_v1")
WORKBOOK_GLOB = "pdd_*_workbook_2026.xlsx"
DATASET_VERSION = 1

NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "pkg_rel": "http://schemas.openxmlformats.org/package/2006/relationships",
}

RAW_SOURCE_GROUP = "raw-source"
MIXED_GROUP = "mixed/presentation"
IGNORE_GROUP = "ignore"

DATASET_CONFIGS = {
    "spu_source": {
        "sheet_token": "SPU源数据",
        "grain_code": "spu_day",
        "canonical_fields": [
            ("sales_date", ["日期"]),
            ("spu_name", ["SPU"]),
            ("campaign_name", ["计划名"]),
            ("listing_name", ["链接名称"]),
            ("product_visitor_count", ["商品访客量"]),
            ("product_pageview_count", ["商品浏览量"]),
            ("unit_count", ["支付件数", "成交件数"]),
            ("buyer_count", ["支付买家数", "成交买家数"]),
            ("order_count", ["支付订单数", "成交订单数"]),
            ("gross_sales_amount", ["支付金额", "成交金额"]),
            ("conversion_rate", ["支付转化率", "成交转化率"]),
            ("order_rate", ["下单率"]),
            ("payment_rate", ["支付率", "成交率"]),
            ("product_favorite_user_count", ["商品收藏用户数"]),
            ("week_number", ["周数"]),
            ("week_sales_amount", ["周销售额"]),
            ("month_number", ["月数"]),
        ],
    },
    "sku_source": {
        "sheet_token": "SKU源数据",
        "grain_code": "sku_day",
        "canonical_fields": [
            ("sales_date", ["日期"]),
            ("product_name", ["商品"]),
            ("product_id", ["商品id"]),
            ("merchant_sku_code", ["商家编码-SKU维度"]),
            ("product_specification", ["商品规格"]),
            ("quantity_units", ["商品数量(件)"]),
            ("gross_product_amount", ["商品总价(元)"]),
            ("merchant_net_amount", ["商家实收金额（元）", "商家实收金额(元)"]),
            ("sku_id", ["SKU-ID"]),
        ],
    },
    "shop_daily_source": {
        "sheet_token": "日报数据源",
        "grain_code": "shop_day",
        "canonical_fields": [
            ("sales_date", ["日期"]),
            ("shop_visitor_count", ["店铺访客数"]),
            ("shop_pageview_count", ["店铺浏览量"]),
            ("product_visitor_count", ["商品访客数"]),
            ("product_pageview_count", ["商品浏览数"]),
            ("buyer_count", ["支付买家数", "成交买家数"]),
            ("order_count", ["支付订单数", "成交订单数"]),
            ("gross_sales_amount", ["支付金额", "成交金额"]),
            ("conversion_rate", ["支付转化率", "成交转化率"]),
            ("avg_order_value", ["客单价"]),
            ("uv_value", ["UV价值"]),
            ("product_favorite_user_count", ["商品收藏用户数"]),
            ("product_favorite_rate", ["商品收藏率"]),
            ("shop_rating_text", ["店铺评分（1/23店铺评价分排名）", "店铺评分"]),
            ("refund_amount", ["退款金额"]),
        ],
    },
    "promotion_total_source": {
        "sheet_token": "推广数据源（总）",
        "grain_code": "shop_day",
        "canonical_fields": [
            ("sales_date", ["日期"]),
            ("ad_spend_amount", ["花费(元)"]),
            ("ad_transaction_amount", ["交易额(元)"]),
            ("return_on_ad_spend", ["投入产出比"]),
            ("order_count", ["成交笔数"]),
            ("cost_per_order", ["每笔成交花费(元)"]),
            ("amount_per_order", ["每笔成交金额(元)"]),
            ("impression_count", ["曝光量"]),
            ("click_count", ["点击量"]),
            ("total_spend_label", ["总花费"]),
            ("total_transactions_label", ["总成交"]),
        ],
    },
    "promotion_campaign_source": {
        "sheet_token": "推广数据源（单）",
        "grain_code": "spu_day",
        "canonical_fields": [
            ("spu_name", ["spu"]),
            ("campaign_name", ["计划名"]),
            ("sales_date", ["日期"]),
            ("ad_spend_amount", ["花费(元)"]),
            ("ad_transaction_amount", ["交易额(元)"]),
            ("return_on_ad_spend", ["投入产出比"]),
            ("order_count", ["成交笔数"]),
            ("cost_per_order", ["每笔成交花费(元)"]),
            ("amount_per_order", ["每笔成交金额(元)"]),
            ("impression_count", ["曝光量"]),
            ("click_count", ["点击量"]),
            ("click_through_rate", ["点击率"]),
            ("click_conversion_rate", ["点击转化率"]),
            ("direct_transaction_amount", ["直接交易额（元）"]),
            ("indirect_transaction_amount", ["间接交易额（元）"]),
            ("direct_order_count", ["直接成交笔数"]),
            ("indirect_order_count", ["间接成交笔数"]),
        ],
    },
    "shop_product_source": {
        "sheet_token": "店铺商品信息",
        "grain_code": "product_sku_snapshot",
        "canonical_fields": [
            ("product_name", ["商品名称"]),
            ("product_spu_id", ["商品SPU -ID"]),
            ("product_specification", ["商品规格"]),
            ("merchant_sku_code", ["商家编码-SKU维度"]),
            ("product_sku_id", ["商品SKU-ID"]),
        ],
    },
    "product_rating_source": {
        "sheet_token": "商品评分数据源",
        "grain_code": "product_day",
        "canonical_fields": [
            ("sales_date", ["日期"]),
            ("product_id", ["ID"]),
            ("review_count", ["评价总数"]),
            ("product_dsr", ["商品DSR"]),
            ("description_score", ["描述相符评分"]),
            ("logistics_score", ["物流服务评分"]),
            ("service_score", ["服务态度评分"]),
        ],
    },
}

SCHEMA = pa.schema(
    [
        pa.field("batch_id", pa.string()),
        pa.field("dataset_version", pa.int64()),
        pa.field("dataset_code", pa.string()),
        pa.field("grain_code", pa.string()),
        pa.field("workbook_identifier", pa.string()),
        pa.field("workbook_path", pa.string()),
        pa.field("workbook_file_name", pa.string()),
        pa.field("workbook_file_hash", pa.string()),
        pa.field("shop_code", pa.string()),
        pa.field("sheet_name", pa.string()),
        pa.field("sheet_index", pa.int64()),
        pa.field("source_row_number", pa.int64()),
        pa.field("source_header_variant", pa.string()),
        pa.field("source_extra_payload_json", pa.string()),
        pa.field("sales_date", pa.string()),
        pa.field("spu_name", pa.string()),
        pa.field("campaign_name", pa.string()),
        pa.field("listing_name", pa.string()),
        pa.field("product_visitor_count", pa.string()),
        pa.field("product_pageview_count", pa.string()),
        pa.field("unit_count", pa.string()),
        pa.field("buyer_count", pa.string()),
        pa.field("order_count", pa.string()),
        pa.field("gross_sales_amount", pa.string()),
        pa.field("conversion_rate", pa.string()),
        pa.field("order_rate", pa.string()),
        pa.field("payment_rate", pa.string()),
        pa.field("product_favorite_user_count", pa.string()),
        pa.field("week_number", pa.string()),
        pa.field("week_sales_amount", pa.string()),
        pa.field("month_number", pa.string()),
        pa.field("product_name", pa.string()),
        pa.field("product_id", pa.string()),
        pa.field("merchant_sku_code", pa.string()),
        pa.field("product_specification", pa.string()),
        pa.field("quantity_units", pa.string()),
        pa.field("gross_product_amount", pa.string()),
        pa.field("merchant_net_amount", pa.string()),
        pa.field("sku_id", pa.string()),
        pa.field("shop_visitor_count", pa.string()),
        pa.field("shop_pageview_count", pa.string()),
        pa.field("avg_order_value", pa.string()),
        pa.field("uv_value", pa.string()),
        pa.field("product_favorite_rate", pa.string()),
        pa.field("shop_rating_text", pa.string()),
        pa.field("refund_amount", pa.string()),
        pa.field("ad_spend_amount", pa.string()),
        pa.field("ad_transaction_amount", pa.string()),
        pa.field("return_on_ad_spend", pa.string()),
        pa.field("cost_per_order", pa.string()),
        pa.field("amount_per_order", pa.string()),
        pa.field("impression_count", pa.string()),
        pa.field("click_count", pa.string()),
        pa.field("total_spend_label", pa.string()),
        pa.field("total_transactions_label", pa.string()),
        pa.field("click_through_rate", pa.string()),
        pa.field("click_conversion_rate", pa.string()),
        pa.field("direct_transaction_amount", pa.string()),
        pa.field("indirect_transaction_amount", pa.string()),
        pa.field("direct_order_count", pa.string()),
        pa.field("indirect_order_count", pa.string()),
        pa.field("product_spu_id", pa.string()),
        pa.field("product_sku_id", pa.string()),
        pa.field("review_count", pa.string()),
        pa.field("product_dsr", pa.string()),
        pa.field("description_score", pa.string()),
        pa.field("logistics_score", pa.string()),
        pa.field("service_score", pa.string()),
    ]
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Normalize recurring PDD workbook raw-source sheets into Parquet datasets."
    )
    parser.add_argument("--workbook-root", default=str(WORKBOOK_ROOT))
    parser.add_argument("--workbook-glob", default=WORKBOOK_GLOB)
    parser.add_argument("--output-root", default=str(OUTPUT_ROOT))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect and validate schemas without writing parquet outputs.",
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


def get_cell_value(cell, shared_strings):
    value_node = cell.find("main:v", NS)
    if value_node is None:
        inline_node = cell.find("main:is", NS)
        if inline_node is None:
            return ""
        return "".join(node.text or "" for node in inline_node.iterfind(".//main:t", NS))

    value = value_node.text or ""
    if cell.attrib.get("t") == "s":
        return shared_strings[int(value)]
    return value


def read_sheet_rows(workbook_zip, shared_strings, sheet_path):
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


def get_sheet_defs(workbook_zip):
    workbook_root = ET.fromstring(workbook_zip.read("xl/workbook.xml"))
    rels_root = ET.fromstring(workbook_zip.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels_root.findall("pkg_rel:Relationship", NS)
    }

    sheet_defs = []
    sheets_node = workbook_root.find("main:sheets", NS)
    if sheets_node is None:
        raise ValueError("Workbook does not contain a sheets collection.")
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


def compute_file_hash(file_path):
    digest = hashlib.sha256()
    with open(file_path, "rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_sheet_name(sheet_name):
    return sheet_name.strip()


def classify_sheet(sheet_name):
    normalized = normalize_sheet_name(sheet_name)
    if any(config["sheet_token"] in normalized for config in DATASET_CONFIGS.values()):
        return RAW_SOURCE_GROUP
    if normalized == "WpsReserved_CellImgList" or re.fullmatch(r"Sheet\d+", normalized):
        return IGNORE_GROUP
    return MIXED_GROUP


def parse_workbook_identifier(workbook_path):
    match = re.fullmatch(r"(pdd_\d+)_workbook_(\d{4})\.xlsx", workbook_path.name)
    if not match:
        raise ValueError(f"Unexpected workbook name: {workbook_path.name}")
    return match.group(1), f"{match.group(1)}_workbook_{match.group(2)}"


def build_header_positions(header_cells):
    positions = {}
    for index, header_value in enumerate(header_cells):
        header_name = str(header_value).strip()
        if header_name:
            positions[header_name] = index
    return positions


def resolve_dataset_mapping(dataset_code, header_cells):
    header_positions = build_header_positions(header_cells)
    config = DATASET_CONFIGS[dataset_code]
    resolved = {}
    missing = []
    matched_headers = []
    for canonical_name, variants in config["canonical_fields"]:
        matched = None
        for variant in variants:
            if variant in header_positions:
                matched = variant
                break
        if matched is None:
            resolved[canonical_name] = None
            if len(variants) == 1:
                missing.append(variants[0])
        else:
            resolved[canonical_name] = matched
            matched_headers.append(matched)
    return resolved, header_positions, missing, matched_headers


def normalize_date_text(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    if re.fullmatch(r"\d+(\.\d+)?", text):
        return (date(1899, 12, 30) + timedelta(days=int(float(text)))).isoformat()
    return text


def get_row_value(row_values, header_positions, header_name):
    if header_name is None:
        return None
    index = header_positions[header_name]
    if index >= len(row_values):
        return None
    value = str(row_values[index]).strip()
    return value or None


def build_source_header_variant(mapping):
    variant = {key: value for key, value in mapping.items() if value is not None}
    return json.dumps(variant, ensure_ascii=False, sort_keys=True)


def inspect_workbooks(workbook_paths):
    workbook_infos = []
    for workbook_path in workbook_paths:
        shop_code, workbook_identifier = parse_workbook_identifier(workbook_path)
        with zipfile.ZipFile(workbook_path) as workbook_zip:
            shared_strings = load_shared_strings(workbook_zip)
            sheet_defs = get_sheet_defs(workbook_zip)
            sheets = []
            for sheet_def in sheet_defs:
                sheet_rows = read_sheet_rows(workbook_zip, shared_strings, sheet_def["sheet_path"])
                sheets.append(
                    {
                        **sheet_def,
                        "sheet_group": classify_sheet(sheet_def["sheet_name"]),
                        "sheet_rows": sheet_rows,
                        "detected_row_count": len(sheet_rows),
                    }
                )
        workbook_infos.append(
            {
                "workbook_path": str(workbook_path),
                "workbook_file_name": workbook_path.name,
                "workbook_file_hash": compute_file_hash(workbook_path),
                "shop_code": shop_code,
                "workbook_identifier": workbook_identifier,
                "sheets": sheets,
            }
        )
    return workbook_infos


def compute_batch_id(workbook_infos):
    parts = []
    for workbook_info in sorted(workbook_infos, key=lambda item: item["workbook_identifier"]):
        parts.append(
            "|".join(
                [
                    workbook_info["workbook_identifier"],
                    workbook_info["workbook_file_hash"],
                ]
            )
        )
    parts.append(f"dataset_version={DATASET_VERSION}")
    digest = hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()
    return digest[:16]


def collect_dataset_rows(workbook_infos, batch_id):
    dataset_rows = {dataset_code: [] for dataset_code in DATASET_CONFIGS}
    schema_summary = {dataset_code: {"workbooks": {}, "unexpected_headers": {}} for dataset_code in DATASET_CONFIGS}
    sheet_inventory = []

    for workbook_info in workbook_infos:
        for sheet in workbook_info["sheets"]:
            sheet_inventory.append(
                {
                    "batch_id": batch_id,
                    "workbook_identifier": workbook_info["workbook_identifier"],
                    "workbook_path": workbook_info["workbook_path"],
                    "workbook_file_name": workbook_info["workbook_file_name"],
                    "shop_code": workbook_info["shop_code"],
                    "sheet_name": sheet["sheet_name"],
                    "sheet_index": sheet["sheet_index"],
                    "sheet_group": sheet["sheet_group"],
                    "detected_row_count": sheet["detected_row_count"],
                }
            )

        for dataset_code, config in DATASET_CONFIGS.items():
            matches = [sheet for sheet in workbook_info["sheets"] if config["sheet_token"] in sheet["sheet_name"]]
            if len(matches) != 1:
                raise ValueError(
                    f"Workbook {workbook_info['workbook_file_name']} expected exactly one sheet for {dataset_code}, found {len(matches)}"
                )
            sheet = matches[0]
            if not sheet["sheet_rows"]:
                raise ValueError(f"Sheet {sheet['sheet_name']} is empty in {workbook_info['workbook_file_name']}")

            header_row_number, header_cells = sheet["sheet_rows"][0]
            if header_row_number != 1:
                raise ValueError(
                    f"Sheet {sheet['sheet_name']} in {workbook_info['workbook_file_name']} does not start headers on row 1"
                )

            resolved_mapping, header_positions, missing_headers, matched_headers = resolve_dataset_mapping(dataset_code, header_cells)
            unexpected_headers = [header for header in header_positions if header not in matched_headers]
            schema_summary[dataset_code]["workbooks"][workbook_info["workbook_identifier"]] = {
                "sheet_name": sheet["sheet_name"],
                "headers": list(header_positions.keys()),
                "resolved_mapping": resolved_mapping,
                "missing_headers": missing_headers,
                "unexpected_headers": unexpected_headers,
            }
            schema_summary[dataset_code]["unexpected_headers"][workbook_info["workbook_identifier"]] = unexpected_headers

            source_header_variant = build_source_header_variant(resolved_mapping)
            for row_number, row_values in sheet["sheet_rows"][1:]:
                row_payload = {}
                is_empty = True
                for header_name, header_index in header_positions.items():
                    cell_text = str(row_values[header_index]).strip() if header_index < len(row_values) else ""
                    row_payload[header_name] = cell_text
                    if cell_text:
                        is_empty = False
                if is_empty:
                    continue

                row: dict[str, object | None] = {name: None for name in SCHEMA.names}
                row["batch_id"] = batch_id
                row["dataset_version"] = DATASET_VERSION
                row["dataset_code"] = dataset_code
                row["grain_code"] = config["grain_code"]
                row["workbook_identifier"] = workbook_info["workbook_identifier"]
                row["workbook_path"] = workbook_info["workbook_path"]
                row["workbook_file_name"] = workbook_info["workbook_file_name"]
                row["workbook_file_hash"] = workbook_info["workbook_file_hash"]
                row["shop_code"] = workbook_info["shop_code"]
                row["sheet_name"] = sheet["sheet_name"]
                row["sheet_index"] = sheet["sheet_index"]
                row["source_row_number"] = row_number
                row["source_header_variant"] = source_header_variant

                extra_payload = {}
                matched_header_set = {header for header in resolved_mapping.values() if header is not None}
                for header_name, cell_text in row_payload.items():
                    if header_name not in matched_header_set and cell_text != "":
                        extra_payload[header_name] = cell_text

                for canonical_name in [field[0] for field in config["canonical_fields"]]:
                    header_name = resolved_mapping[canonical_name]
                    value = get_row_value(row_values, header_positions, header_name)
                    if canonical_name == "sales_date":
                        value = normalize_date_text(value)
                    row[canonical_name] = value

                row["source_extra_payload_json"] = (
                    json.dumps(extra_payload, ensure_ascii=False, sort_keys=True) if extra_payload else None
                )
                dataset_rows[dataset_code].append(row)

    for dataset_code in dataset_rows:
        dataset_rows[dataset_code].sort(
            key=lambda row: (
                row["workbook_identifier"] or "",
                row["sheet_index"] or 0,
                row["source_row_number"] or 0,
            )
        )

    sheet_inventory.sort(
        key=lambda row: (
            row["workbook_identifier"],
            row["sheet_index"],
        )
    )
    return dataset_rows, sheet_inventory, schema_summary


def write_json(output_path, payload):
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_parquet(output_path, rows):
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    pq.write_table(table, output_path, compression="zstd")


def write_sheet_inventory(output_path, rows):
    schema = pa.schema(
        [
            pa.field("batch_id", pa.string()),
            pa.field("workbook_identifier", pa.string()),
            pa.field("workbook_path", pa.string()),
            pa.field("workbook_file_name", pa.string()),
            pa.field("shop_code", pa.string()),
            pa.field("sheet_name", pa.string()),
            pa.field("sheet_index", pa.int64()),
            pa.field("sheet_group", pa.string()),
            pa.field("detected_row_count", pa.int64()),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, output_path, compression="zstd")


def materialize_outputs(output_root, workbook_infos, batch_id, dataset_rows, sheet_inventory, schema_summary):
    output_root.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output_root.parent) as temp_dir:
        temp_root = Path(temp_dir) / output_root.name
        datasets_root = temp_root / "datasets"
        datasets_root.mkdir(parents=True, exist_ok=True)

        for dataset_code, rows in dataset_rows.items():
            dataset_dir = datasets_root / dataset_code
            dataset_dir.mkdir(parents=True, exist_ok=True)
            write_parquet(dataset_dir / "data.parquet", rows)
            write_json(
                dataset_dir / "schema_manifest.json",
                {
                    "dataset_code": dataset_code,
                    "batch_id": batch_id,
                    "dataset_version": DATASET_VERSION,
                    "grain_code": DATASET_CONFIGS[dataset_code]["grain_code"],
                    "row_count": len(rows),
                    "schema_differences": schema_summary[dataset_code],
                },
            )

        write_sheet_inventory(temp_root / "sheet_inventory.parquet", sheet_inventory)
        write_json(
            temp_root / "sheet_classification.json",
            {
                "batch_id": batch_id,
                "workbooks": [
                    {
                        "workbook_identifier": workbook_info["workbook_identifier"],
                        "workbook_path": workbook_info["workbook_path"],
                        "sheets": [
                            {
                                "sheet_name": sheet["sheet_name"],
                                "sheet_index": sheet["sheet_index"],
                                "sheet_group": sheet["sheet_group"],
                            }
                            for sheet in workbook_info["sheets"]
                        ],
                    }
                    for workbook_info in workbook_infos
                ],
            },
        )
        write_json(
            temp_root / "run_manifest.json",
            {
                "batch_id": batch_id,
                "dataset_version": DATASET_VERSION,
                "workbook_count": len(workbook_infos),
                "datasets": {dataset_code: len(rows) for dataset_code, rows in dataset_rows.items()},
                "workbooks": [
                    {
                        "workbook_identifier": workbook_info["workbook_identifier"],
                        "workbook_file_name": workbook_info["workbook_file_name"],
                        "workbook_file_hash": workbook_info["workbook_file_hash"],
                    }
                    for workbook_info in workbook_infos
                ],
            },
        )

        if output_root.exists():
            shutil.rmtree(output_root)
        shutil.move(str(temp_root), str(output_root))


def main():
    args = parse_args()
    workbook_root = Path(args.workbook_root)
    output_root = Path(args.output_root)
    workbook_paths = sorted(workbook_root.glob(args.workbook_glob))
    if not workbook_paths:
        raise FileNotFoundError(f"No workbooks matched {args.workbook_glob} in {workbook_root}")

    workbook_infos = inspect_workbooks(workbook_paths)
    batch_id = compute_batch_id(workbook_infos)
    dataset_rows, sheet_inventory, schema_summary = collect_dataset_rows(workbook_infos, batch_id)

    if args.dry_run:
        print(f"Batch {batch_id}")
        for dataset_code, rows in dataset_rows.items():
            print(f"{dataset_code}: {len(rows)} rows")
        return

    materialize_outputs(output_root, workbook_infos, batch_id, dataset_rows, sheet_inventory, schema_summary)
    print(f"Wrote normalized parquet datasets to {output_root}")
    for dataset_code, rows in dataset_rows.items():
        print(f"- {dataset_code}: {len(rows)} rows")


if __name__ == "__main__":
    main()
