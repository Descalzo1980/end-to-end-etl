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
* Airflow DAG for data generation, PostgreSQL validation and loading data into ClickHouse

Current Airflow pipeline:

```text
generate_data
      ↓
check_postgres
      ↓
load_users_to_clickhouse
```

The pipeline currently generates test data in PostgreSQL, validates the loaded data and loads the `users` table into ClickHouse.

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

## Roadmap

* [x] PostgreSQL source database
* [x] Python data generator
* [x] Docker environment
* [x] Apache Airflow
* [x] Basic Airflow ETL DAG
* [x] Extract data from PostgreSQL
* [x] Load data into ClickHouse
* [ ] Transform data with SQL
* [ ] Build data marts
* [ ] Add dbt
* [ ] Add Apache Superset
* [ ] Improve data quality checks
* [ ] Make the pipeline incremental
* [ ] Make the pipeline idempotent