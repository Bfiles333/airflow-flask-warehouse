create table if not exists raw.orders (id SERIAL PRIMARY KEY, order_id integer, order_date timestamp, status VARCHAR);
create table if not exists raw.order_items (id SERIAL PRIMARY KEY, order_id foreign key, product_id foreign key, quantity integer, unit_price NUMERIC(4, 2), discount NUMERIC(4, 2));
create table if not exists raw.products (id SERIAL PRIMARY KEY, product_sku VARCHAR, product_name VARCHAR, base_price NUMERIC(4, 2));
