from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests


def seed_raw_tables():
    url = "http://flask-api:5000/run/seed_raw_tables"

    response = requests.post(url)

    if response.status_code != 200:
        raise Exception(f"ETL failed: {response.text}")

    print("ETL succeeded:", response.json())


with DAG(
    dag_id="daily_sales_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger for now
    catchup=False,
    tags=["etl"],
) as dag:

    trigger_etl = PythonOperator(
        task_id="seed_raw_tables",
        python_callable=seed_raw_tables,
    )

    trigger_etl