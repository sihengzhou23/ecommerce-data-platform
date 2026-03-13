-- Seed recurring PDD workbook raw-sheet source contracts.

BEGIN;

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
    p.platform_id,
    seed.file_type_code,
    seed.file_type_name,
    seed.grain_code,
    1,
    seed.description,
    TRUE
FROM platforms p
CROSS JOIN (
    VALUES
        (
            'pdd_workbook_spu_source',
            'PDD Workbook SPU Source',
            'spu_day',
            'Recurring PDD workbook raw SPU source sheet normalized into parquet.'
        ),
        (
            'pdd_workbook_sku_source',
            'PDD Workbook SKU Source',
            'sku_day',
            'Recurring PDD workbook raw SKU source sheet normalized into parquet.'
        ),
        (
            'pdd_workbook_shop_daily_source',
            'PDD Workbook Shop Daily Source',
            'shop_day',
            'Recurring PDD workbook raw shop daily source sheet normalized into parquet.'
        ),
        (
            'pdd_workbook_promo_total_source',
            'PDD Workbook Promotion Total Source',
            'shop_day',
            'Recurring PDD workbook raw promotion total sheet normalized into parquet.'
        ),
        (
            'pdd_workbook_promo_campaign_source',
            'PDD Workbook Promotion Campaign Source',
            'spu_day',
            'Recurring PDD workbook raw promotion campaign sheet normalized into parquet.'
        ),
        (
            'pdd_workbook_shop_product_source',
            'PDD Workbook Shop Product Source',
            'product_sku_snapshot',
            'Recurring PDD workbook raw shop product info sheet normalized into parquet.'
        ),
        (
            'pdd_workbook_product_rating_source',
            'PDD Workbook Product Rating Source',
            'product_day',
            'Recurring PDD workbook raw product rating sheet normalized into parquet.'
        )
) AS seed(file_type_code, file_type_name, grain_code, description)
WHERE p.platform_code = 'pdd'
ON CONFLICT (platform_id, file_type_code, version) DO NOTHING;

COMMIT;
