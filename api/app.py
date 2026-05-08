from flask import Flask
import os
from sqlalchemy import create_engine
import pandas as pd
import logging
from io import StringIO
import csv
import time
import boto3

app = Flask(__name__)
logger = logging.getLogger("mini_flask")
logger.setLevel(logging.INFO)

fh = logging.FileHandler("mini_flask.log")
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL)

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
)

S3_BUCKET = os.environ.get("S3_BUCKET", "warehouse-data")


def read_csv_from_s3(key):
    logger.info(f"Reading s3://{S3_BUCKET}/{key}")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(obj["Body"])


def dump_dataframe_via_copy_expert(table, raw_conn, keys, df):
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


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}


@app.route("/run/seed_raw_tables", methods=["POST"])
def seed_raw_tables():
    raw_conn = None
    try:
        raw_conn = engine.raw_connection()

        orders = read_csv_from_s3("orders.csv")
        order_items = read_csv_from_s3("order_items.csv")
        products = read_csv_from_s3("products.csv")

        logger.info("Truncating raw tables...")
        with raw_conn.cursor() as cur:
            cur.execute(
                "TRUNCATE raw.order_items, raw.orders, raw.products RESTART IDENTITY"
            )
        raw_conn.commit()

        dump_dataframe_via_copy_expert(
            table='"raw"."orders"', raw_conn=raw_conn, keys=orders.columns, df=orders
        )
        raw_conn.commit()

        dump_dataframe_via_copy_expert(
            table='"raw"."products"',
            raw_conn=raw_conn,
            keys=products.columns,
            df=products,
        )
        raw_conn.commit()

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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
