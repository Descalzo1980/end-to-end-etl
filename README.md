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
* Airflow DAG for data generation and PostgreSQL validation

Current Airflow pipeline:

```text
generate_data
      ↓
check_postgres
```

The pipeline generates test data in PostgreSQL and then validates the loaded data.

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
* [ ] Extract data from PostgreSQL
* [ ] Load data into ClickHouse
* [ ] Transform data with SQL
* [ ] Build data marts
* [ ] Add dbt
* [ ] Add Apache Superset
* [ ] Improve data quality checks
* [ ] Make the pipeline idempotent