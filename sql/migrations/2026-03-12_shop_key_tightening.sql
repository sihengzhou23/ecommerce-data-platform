-- Tighten shop uniqueness after the layered-import migration.
-- Shop codes are unique within a platform, not globally.

BEGIN;

ALTER TABLE shops
    DROP CONSTRAINT IF EXISTS shops_shop_code_key;

ALTER TABLE shops
    ADD CONSTRAINT shops_platform_id_shop_code_key
    UNIQUE (platform_id, shop_code);

COMMIT;
