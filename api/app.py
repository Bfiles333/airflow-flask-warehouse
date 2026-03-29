from flask import Flask, jsonify
import os
from sqlalchemy import create_engine
import pandas as pd
import logging
from io import StringIO
import csv
import time

app = Flask(__name__)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL)


def dump_dataframe_via_copy_expert(table, raw_conn, keys, df):
    with raw_conn.cursor() as cur:
        s_buf = StringIO()
        df.to_csv(
            s_buf,
            index=False,
            header=False,
            quoting=csv.QUOTE_MINIMAL,
            na_rep="\\N"
        )
        s_buf.seek(0)

        columns = ', '.join(f'"{k}"' for k in keys)
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
    raw_conn = None
    try:
        raw_conn = engine.raw_connection()

        logger.info("Loading seed data...")
        orders = pd.read_csv("/app/data/orders.csv")
        order_items = pd.read_csv("/app/data/order_items.csv")
        products = pd.read_csv("/app/data/products.csv")

        logger.info("Seed data loaded. Truncating tables...")
        with raw_conn.cursor() as cur:
            cur.execute("""
                TRUNCATE raw.order_items, raw.orders, raw.products
                RESTART IDENTITY
            """)
        raw_conn.commit()

        logger.info("Tables truncated. Loading orders...")
        dump_dataframe_via_copy_expert(
            table='"raw"."orders"',
            raw_conn=raw_conn,
            keys=orders.columns,
            df=orders
        )
        raw_conn.commit()

        logger.info("Orders loaded. Loading products...")
        dump_dataframe_via_copy_expert(
            table='"raw"."products"',
            raw_conn=raw_conn,
            keys=products.columns,
            df=products
        )
        raw_conn.commit()

        logger.info("Products loaded. Loading order_items...")
        dump_dataframe_via_copy_expert(
            table='"raw"."order_items"',
            raw_conn=raw_conn,
            keys=order_items.columns,
            df=order_items
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
            
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)