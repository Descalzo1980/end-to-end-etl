import base64
from datetime import datetime
from urllib.request import Request, urlopen

import psycopg
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


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





def load_users_to_clickhouse():
    truncate_query = "TRUNCATE TABLE shop_dwh.users"
    credentials = base64.b64encode(b"etl:etl_password").decode()
    insert_query = """
        INSERT INTO shop_dwh.users
        SELECT *
        FROM postgresql(
            'postgres:5432',
            'shop',
            'users',
            'etl_user',
            'etl_password'
        )
    """

    for query in (truncate_query, insert_query):
        request = Request(
            "http://clickhouse:8123/",
            data=query.encode(),
            method="POST",
            headers={
                "Content-Type": "text/plain",
                "Authorization": f"Basic {credentials}",
            },
        )

        with urlopen(request) as response:
            response.read()

    print("Users loaded from PostgreSQL to ClickHouse")


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

    load_users_task = PythonOperator(
        task_id="load_users_to_clickhouse",
        python_callable=load_users_to_clickhouse,
    )

    generate_data >> check_postgres_task >> load_users_task
