CREATE TABLE platforms (
    platform_id SERIAL PRIMARY KEY,
    platform_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE stores (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(100),
    platform_id INT REFERENCES platforms(platform_id)
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100)
);

CREATE TABLE daily_sales (
    sales_id SERIAL PRIMARY KEY,
    sales_date DATE NOT NULL,
    platform_id INT REFERENCES platforms(platform_id),
    store_id INT REFERENCES stores(store_id),
    product_id INT REFERENCES products(product_id),
    sales_amount NUMERIC(12,2),
    units_sold INT,
    orders_count INT
);