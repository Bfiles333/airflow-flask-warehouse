# Airflow Flask Warehouse

A containerized, end-to-end data pipeline that ingests retail CSV data from S3, stages it in a raw layer, and transforms it into a reporting-ready data warehouse — orchestrated by Apache Airflow, executed via a Flask microservice, and transformed by dbt.

---

## Tech Stack

| Tool | Version | Role |
|---|---|---|
| Apache Airflow | 2.8.1 | Orchestration & scheduling |
| dbt-postgres | — | SQL-based transformation layer |
| Flask | — | Ingestion execution layer (REST API) |
| PostgreSQL | 15 | Raw + DWH storage |
| LocalStack | latest | Local S3 simulation (AWS-compatible) |
| boto3 | — | S3 client |
| Pandas | — | In-memory CSV parsing |
| Docker Compose | — | Multi-service containerization |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        Docker Network                         │
│                                                              │
│  ┌────────────────┐   HTTP POST   ┌──────────────────────┐   │
│  │    Airflow     │ ────────────► │     Flask API        │   │
│  │  (DAG task 1)  │               │  reads S3 → raw PG   │   │
│  └────────┬───────┘               └──────────────────────┘   │
│           │                                                   │
│           │ BashOperator          ┌──────────────────────┐   │
│           └─────────────────────► │       dbt run        │   │
│             (DAG task 2)          │  raw → dwh SQL model  │   │
│                                   └──────────────────────┘   │
│                                                              │
│  ┌──────────────────┐    ┌────────────────────────────────┐  │
│  │   LocalStack S3  │    │         PostgreSQL 15           │  │
│  │  warehouse-data/ │    │   schema: raw  |  schema: dwh  │  │
│  └──────────────────┘    └────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

---

## Pipeline

```
S3 (LocalStack)
  orders.csv        ──►  raw.orders        ┐
  order_items.csv   ──►  raw.order_items   ├──► (dbt) dwh.product_discount_sales_data
  products.csv      ──►  raw.products      ┘
```

**Task 1 — Seed raw tables** (`seed_raw_tables`): Airflow calls a Flask endpoint that reads three CSV files from S3 using boto3 and bulk-loads them into the `raw` schema using PostgreSQL's `COPY FROM STDIN`.

**Task 2 — dbt transform** (`run_dbt_models`): Airflow runs `dbt run` via BashOperator. The dbt model joins `raw.order_items` and `raw.products`, computes per-product discount metrics in SQL, and materializes the result as `dwh.product_discount_sales_data`.

### DAG

```
seed_raw_tables >> run_dbt_models
```

Scheduled `@daily`. `catchup=False` so missed runs do not backfill.

---

## dbt Model: `product_discount_sales_data`

The transform is a single SQL model with two CTEs:

```sql
sold_products   -- aggregates order_items by product_sku
all_products    -- LEFT JOINs products to sold_products (preserves unsold products with 0s)
```

| Column | Description |
|---|---|
| `product_sku` | Product identifier |
| `product_name` | Product display name |
| `unit_price` | Listed unit price |
| `total_units_sold` | All units sold |
| `units_sold_on_sale` | Units sold with a non-zero discount |
| `avg_discount` | Mean discount across discounted line items |
| `max_discount` | Highest discount applied |
| `info_date` | Processing timestamp |

---

## Project Structure

```
airflow-flask-warehouse/
│
├── airflow/
│   └── dags/
│       └── daily_sales_etl.py        # Airflow DAG — seed via Flask, transform via dbt
│
├── api/
│   ├── app.py                        # Flask — reads S3, bulk-loads raw tables
│   ├── Dockerfile
│   └── requirements.txt
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                  # Connects to Postgres via env vars
│   ├── macros/
│   │   └── generate_schema_name.sql  # Writes to dwh schema directly
│   └── models/
│       ├── sources.yml               # Declares raw schema as dbt source
│       └── product_discount_sales_data.sql
│
├── db/
│   └── init/
│       ├── 001_create_schemas.sql
│       └── 002_create_raw_tables.sql
│
├── data/
│   ├── orders.csv
│   ├── order_items.csv
│   └── products.csv                  # Uploaded to LocalStack S3 on startup
│
├── docker-compose.yaml
├── .env.example
└── README.md
```

---

## Key Engineering Decisions

### S3 as the Data Source (via LocalStack)
CSVs are served from a local S3-compatible store rather than read from disk. This mirrors real-world ingestion patterns where source files land in object storage before being loaded into a warehouse. LocalStack makes this fully self-contained — no AWS account required.

### dbt for the Transform Layer
Business logic lives in SQL, not Python. dbt manages the DWH table schema, handles `DROP/CREATE` on each run, and makes the transformation independently testable with `dbt test`. This replaces the previous Pandas-based Flask endpoint.

### Flask as the Ingestion Layer
Flask encapsulates the raw loading logic behind a REST endpoint. Airflow stays a pure orchestrator making HTTP calls — it does not touch data directly.

### Bulk Loading with `COPY FROM STDIN`
Data is streamed from S3 into Postgres via the native `COPY` protocol, bypassing row-by-row inserts for efficient bulk loading.

### Transaction Safety
The seed endpoint commits per table and rolls back the entire connection on any failure, preventing partial loads from silently corrupting the raw layer.

### Two-Layer Architecture (raw → dwh)
Source data always lands in `raw` first. This makes the pipeline reprocessable without re-ingesting from S3 and keeps ingestion concerns separate from transformation concerns.

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/Bfiles333/airflow-flask-warehouse.git
cd airflow-flask-warehouse

# 2. Configure environment
cp .env.example .env

# 3. Start all services
docker compose up --build
```

On startup, `localstack-init` automatically creates the `warehouse-data` S3 bucket and uploads the three CSV files.

**Airflow UI:** [http://localhost:8080](http://localhost:8080) — `admin` / `admin`

Enable the `daily_discount_sales_etl` DAG. It runs on a daily schedule or can be triggered manually.

**Flask health check:** [http://localhost:5000/health](http://localhost:5000/health)

---

## Roadmap

- Swap LocalStack for real S3/GCS to demonstrate cloud-native ingestion
- Add `dbt test` task to the DAG for data quality assertions before writing to `dwh`
- Parameterize `info_date` — pass processing date from Airflow to dbt via `--vars`
- Add incremental dbt model to avoid full table replacement on each run
- Add audit log table tracking pipeline run metadata (rows written, duration, status)
