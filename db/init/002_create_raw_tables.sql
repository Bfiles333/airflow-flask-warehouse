create table if not exists raw.orders (id SERIAL PRIMARY KEY UNIQUE, order_date TIMESTAMP WITH TIME ZONE, status VARCHAR);
create table if not exists raw.products (id SERIAL PRIMARY KEY UNIQUE, product_sku VARCHAR, product_name VARCHAR, base_price NUMERIC(9, 2));
CREATE TABLE IF NOT EXISTS raw.order_items (id SERIAL PRIMARY KEY UNIQUE,order_id INT,product_sku VARCHAR,quantity INTEGER,unit_price NUMERIC(9, 2),discount NUMERIC(9, 2),FOREIGN KEY (order_id) REFERENCES raw.orders(id));
