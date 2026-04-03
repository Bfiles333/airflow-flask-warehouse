from flask import Flask, jsonify
import os
from sqlalchemy import create_engine
import pandas as pd
import logging
from io import StringIO
import csv
import time
from datetime import datetime

app = Flask(__name__)
logger = logging.getLogger("airflow.task")

DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL)


def dump_dataframe_via_copy_expert(table, raw_conn, keys, df):
    """Dump a dataframe into a postgres table via COPY command."""
    with raw_conn.cursor() as cur:
        s_buf = StringIO()
        df.to_csv(
            s_buf, index=False, header=False, quoting=csv.QUOTE_MINIMAL, na_rep="\\N"
        )
        s_buf.seek(0)

        columns = ", ".join(f'"{k}"' for k in keys)
        sql = f"""
            COPY {table} ({columns})
            FROM STDIN WITH (
                FORMAT CSV,
                NULL '\\N'
            )
        """

        start = time.time()
        logger.info(f"Starting COPY into {table}...")
        cur.copy_expert(sql=sql, file=s_buf)
        logger.info(f"Finished COPY into {table} in {time.time() - start:.2f}s")


@app.route("/run/seed_raw_tables", methods=["POST"])
def seed_raw_tables():
    """Take the seed data from the provided csv files and dump them into corresponding postgres tables for further processing."""
    raw_conn = None
    try:
        raw_conn = engine.raw_connection()

        logger.info("Loading seed data...")
        orders = pd.read_csv("/app/data/orders.csv")
        order_items = pd.read_csv("/app/data/order_items.csv")
        products = pd.read_csv("/app/data/products.csv")

        logger.info("Seed data loaded. Truncating tables...")
        with raw_conn.cursor() as cur:
            cur.execute(
                """
                TRUNCATE raw.order_items, raw.orders, raw.products
                RESTART IDENTITY
            """
            )
        raw_conn.commit()

        logger.info("Tables truncated. Loading orders...")
        dump_dataframe_via_copy_expert(
            table='"raw"."orders"', raw_conn=raw_conn, keys=orders.columns, df=orders
        )
        raw_conn.commit()

        logger.info("Orders loaded. Loading products...")
        dump_dataframe_via_copy_expert(
            table='"raw"."products"',
            raw_conn=raw_conn,
            keys=products.columns,
            df=products,
        )
        raw_conn.commit()

        logger.info("Products loaded. Loading order_items...")
        dump_dataframe_via_copy_expert(
            table='"raw"."order_items"',
            raw_conn=raw_conn,
            keys=order_items.columns,
            df=order_items,
        )
        raw_conn.commit()

        logger.info("All seed data loaded successfully.")
        return {"status": "success"}

    except Exception as e:
        if raw_conn is not None:
            raw_conn.rollback()
        logger.exception("Seeding failed")
        return {"status": "error", "message": str(e)}, 500

    finally:
        if raw_conn is not None:
            raw_conn.close()


@app.route("/run/product_discount_sales", methods=["POST"])
def product_discount_sales():
    with engine.connect() as conn:
        product_df = pd.read_sql("select * from raw.products", con=conn)
        order_item_df = pd.read_sql("select * from raw.order_items", con=conn)
        final_data = []

        logger.info(f"Processing products sold on date: {datetime.today().date()}")
        for _, group in order_item_df.groupby("product_sku"):
            discounted_subset = group[group["discount"] > 0]
            final_data.append(
                {
                    "product_sku": group["product_sku"].iloc[0],
                    "total_units_sold": int(group["quantity"].sum()),
                    "units_sold_on_sale": (
                        int(discounted_subset["quantity"].sum())
                        if not discounted_subset.empty
                        else 0
                    ),
                    "max_discount": (
                        float(group["discount"].max())
                        if not discounted_subset.empty
                        else 0
                    ),
                    "avg_discount": (
                        float(group["discount"].mean().round(2))
                        if not discounted_subset.empty
                        else 0
                    ),
                    "unit_price": group["unit_price"].iloc[0],
                    "info_date": datetime.today(),
                }
            )

        logger.info(f"processing products not sold on date: {datetime.today().date()}")
        for _, product in product_df[
            ~product_df["sku"].isin(order_item_df["product_sku"].unique().tolist())
        ]:
            final_data.append(
                {
                    "product_sku": product["product_sku"].iloc[0],
                    "total_units_sold": 0,
                    "units_sold_on_sale": 0,
                    "max_discount": 0,
                    "avg_discount": 0,
                    "unit_price": product["unit_price"].iloc[0],
                    "info_date": datetime.today(),
                }
            )

        logger.info("All data processed, consolidating into a pandas dataframe...")
        final_df = pd.DataFrame.from_records(final_data)

        logger.info(
            f"{len(final_df)} records processed, dumping into product_discount_sales_data table..."
        )
        with engine.raw_connection() as conn:
            dump_dataframe_via_copy_expert(
                table="dwh.product_discount_sales_data",
                raw_conn=conn,
                keys=final_df.keys(),
                df=final_df,
            )
            conn.commit()

        logger.info("Dataframe successfully dumped")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
