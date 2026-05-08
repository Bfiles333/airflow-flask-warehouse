from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests


def seed_raw_tables():
    url = "http://flask-api:5000/run/seed_raw_tables"
    response = requests.post(url)
    if response.status_code != 200:
        raise Exception(f"Seeding failed: {response.text}")
    print("Raw data seeding successful:", response.json())


with DAG(
    dag_id="daily_discount_sales_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl"],
) as dag:

    trigger_data_ingestion = PythonOperator(
        task_id="seed_raw_tables",
        python_callable=seed_raw_tables,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
    )

    trigger_data_ingestion >> run_dbt
