create table if not exists raw.orders (id SERIAL PRIMARY KEY, order_id VARCHAR, order_date DATETIME, status VARCHAR)
create table if not exists raw.order_items (id SERIAL PRIMARY KEY, order_id VARCHAR, product_sku VARCHAR, quantity INT, unit_price NUMERIC, discount NUMERIC)
create table if not exists raw.products (id SERIAL PRIMARY KEY, product_sku VARCHAR, product_name VARCHAR, taxonomy VARCHAR, base_price NUMERIC)
