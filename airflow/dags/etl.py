
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

import psycopg


def check_postgres():
    connection = psycopg.connect(
        host="postgres",
        port=5432,
        dbname="shop",
        user="etl_user",
        password="etl_password",
    )

    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]

    connection.close()

    print(f"Users in PostgreSQL: {count}")


with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2026, 9, 24),
    schedule=None,
    catchup=False,
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/generator/main.py",
    )

    check_postgres_task = PythonOperator(
        task_id="check_postgres",
        python_callable=check_postgres,
    )

    generate_data >> check_postgres_task