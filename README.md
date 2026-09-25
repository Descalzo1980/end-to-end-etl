# End-to-End ETL Pipeline

Учебный end-to-end проект для практики Data Engineering.

## Architecture

```text
Python Generator
       ↓
   PostgreSQL
       ↓
     Airflow
       ↓
   ClickHouse
       ↓
   Data Marts
```

## Current Stack

* Python
* PostgreSQL
* Apache Airflow
* ClickHouse
* Docker / Docker Compose
* SQL

## Current Status

The project currently includes:

* PostgreSQL database with users, products, orders and order items
* Python data generator
* Dockerized environment
* Apache Airflow
* ClickHouse as a data warehouse
* Incremental loading from PostgreSQL to ClickHouse
* SQL transformations
* `daily_sales` data mart

Current Airflow pipeline:

```text
generate_data
      ↓
check_postgres
      ↓
load_incremental
      ↓
build_daily_sales
```

The pipeline generates test data in PostgreSQL, incrementally loads changed users into ClickHouse and builds a daily sales data mart.

The `daily_sales` data mart contains:

* `sale_date` — sales date
* `orders_count` — number of completed orders
* `items_count` — number of sold items
* `revenue` — total sales revenue

## Project Structure

```text
end-to-end-etl/
├── airflow/
│   ├── dags/
│   │   └── etl.py
│   └── Dockerfile
├── generator/
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── postgres/
│   └── init.sql
├── docker-compose.yml
└── README.md
```

## Running the Project

Start the environment:

```bash
docker compose up -d --build
```

Airflow UI:

```text
http://localhost:8081
```

The main DAG is:

```text
etl_pipeline
```

## Data Mart

The project currently contains the following data mart in ClickHouse:

```text
shop_dwh.daily_sales
```

Example:

```text
sale_date   | orders_count | items_count | revenue
------------+--------------+-------------+---------
2026-09-23  | 28           | 213         | 53360.22
2026-09-24  | 858          | 6269        | 1564636.51
2026-09-25  | 6            | 60          | 12685.93
```

The data mart is built from PostgreSQL `orders` and `order_items` tables using SQL aggregation.

## Roadmap

* [x] PostgreSQL source database
* [x] Python data generator
* [x] Docker environment
* [x] Apache Airflow
* [x] Basic Airflow ETL DAG
* [x] Extract data from PostgreSQL
* [x] Load data into ClickHouse
* [x] Incremental loading
* [x] Transform data with SQL
* [x] Build `daily_sales` data mart
* [ ] Add dbt
* [ ] Add Apache Superset
* [ ] Improve data quality checks
* [ ] Make the data mart incremental
* [ ] Make the pipeline fully idempotent