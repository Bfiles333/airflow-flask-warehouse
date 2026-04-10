# Airflow Flask Warehouse

## Overview

This project is a containerized ETL pipeline that demonstrates a simple data warehouse architecture using Airflow, Flask, and Postgres.

Airflow orchestrates the pipeline by triggering Flask API endpoints, which ingest and transform CSV-based retail data into a Postgres data warehouse with separate raw and reporting layers.

---

## Architecture

The system consists of three main components:

* **Postgres**

  * Stores both raw and transformed (DWH) data
  * Organized into two schemas:

    * `raw` → ingested source data
    * `dwh` → processed reporting data

* **Flask API**

  * Acts as the execution layer
  * Exposes endpoints to:

    * load seed data into raw tables
    * process and transform data into warehouse tables

* **Airflow**

  * Orchestrates the pipeline
  * Calls Flask endpoints via HTTP
  * Provides scheduling, monitoring, and retry capabilities

---

## Pipeline Flow

The pipeline is composed of two main stages:

### 1. Seed Raw Tables

* Reads CSV files:

  * `orders.csv`
  * `order_items.csv`
  * `products.csv`
* Loads data into:

  * `raw.orders`
  * `raw.order_items`
  * `raw.products`
* Uses Postgres `COPY` for efficient bulk loading

### 2. Process Discount Sales Data

* Reads data from raw tables
* Aggregates product-level metrics:

  * total units sold
  * units sold on discount
  * average discount
  * maximum discount
* Writes results into:

  * `dwh.product_discount_sales_data`

---

## Project Structure

```
airflow-flask-warehouse/
│
├── airflow/
│   └── dags/
│       └── daily_sales_etl.py
│
├── api/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── db/
│   └── init/
│       ├── 001_create_schemas.sql
│       ├── 002_create_raw_tables.sql
│       └── 003_create_dwh_tables.sql
│
├── data/
│   ├── orders.csv
│   ├── order_items.csv
│   └── products.csv
│
├── docker-compose.yaml
├── .env.example
└── README.md
```

---

## How to Run

### 1. Clone the repository

```
git clone https://github.com/Bfiles333/airflow-flask-warehouse.git
cd airflow-flask-warehouse
```

### 2. Create environment file

```
cp .env.example .env
```

### 3. Start services

```
docker compose up --build
```

### 4. Access Airflow UI

```
http://localhost:8080
```

Login:

```
username: admin
password: admin
```

### 5. Run the pipeline

* Enable the DAG
* Trigger it manually from the UI

---

## DAG Design

The pipeline is implemented as a single Airflow DAG with two tasks:

1. `seed_raw_tables`
2. `process_daily_discount_sales`

Task dependency:

```
seed_raw_tables >> process_daily_discount_sales
```

This ensures that raw data is always loaded before transformations are executed.

---

## Design Decisions

### Raw vs DWH Layering

Instead of processing CSVs directly into a final table, the pipeline uses a layered approach:

```
CSV → raw → dwh
```

This allows:

* easier debugging
* reprocessing without re-ingestion
* clearer separation of concerns

---

### Flask as Execution Layer

Flask is used to encapsulate business logic and data processing.

Benefits:

* reusable endpoints
* easy integration with Airflow
* separation between orchestration and execution

---

### Airflow for Orchestration

Airflow handles:

* scheduling
* retries
* monitoring

It interacts with Flask via HTTP calls, mimicking real-world service-based architectures.

---

### Bulk Loading with COPY

Instead of inserting rows one-by-one, the pipeline uses:

```
COPY FROM STDIN
```

This significantly improves performance when loading large datasets into Postgres.

---

## Data Model

### Raw Tables

* `raw.orders`
* `raw.order_items`
* `raw.products`

### DWH Table

* `dwh.product_discount_sales_data`

Contains aggregated product-level metrics for reporting.

---

## Limitations / Future Improvements

* Add idempotent loads (avoid duplicate daily records)
* Add data quality validation checks
* Add audit/logging table for pipeline runs
* Parameterize processing date (instead of using current timestamp)
* Replace pandas transformations with SQL-based transformations
* Add automated scheduling instead of manual DAG trigger

---

## Example Output

Example metrics generated per product:

* total units sold
* units sold on discount
* average discount
* maximum discount

---

## Tech Stack

* Python
* Flask
* Apache Airflow
* PostgreSQL
* Docker / Docker Compose
* Pandas

---

## Summary

This project demonstrates:

* building a multi-service data pipeline
* orchestrating ETL workflows with Airflow
* designing raw vs warehouse data layers
* performing efficient bulk data loading
* exposing processing logic via APIs

It is designed as a practical, end-to-end example of a small-scale data engineering system.
