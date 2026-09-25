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

def use_watermark(**context):
    ti = context["ti"]

    watermark = ti.xcom_pull(task_ids="get_watermark")

    print(f"Received watermark: {watermark}")

# def load_users_to_clickhouse():
#     truncate_query = "TRUNCATE TABLE shop_dwh.users"
#     credentials = base64.b64encode(b"etl:etl_password").decode()
#     insert_query = """
#         INSERT INTO shop_dwh.users
#         SELECT *
#         FROM postgresql(
#             'postgres:5432',
#             'shop',
#             'users',
#             'etl_user',
#             'etl_password'
#         )
#     """

#     for query in (truncate_query, insert_query):
#         request = Request(
#             "http://clickhouse:8123/",
#             data=query.encode(),
#             method="POST",
#             headers={
#                 "Content-Type": "text/plain",
#                 "Authorization": f"Basic {credentials}",
#             },
#         )

#         with urlopen(request) as response:
#             response.read()

#     print("Users loaded from PostgreSQL to ClickHouse")

def load_incremental():
    connection = psycopg.connect(
        host="postgres",
        port=5432,
        dbname="shop",
        user="etl_user",
        password="etl_password",
    )

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT id, name, country, created_at, updated_at
            FROM users
            WHERE updated_at > (
                SELECT last_updated_at
                FROM etl_watermarks
                WHERE pipeline = 'users_to_clickhouse'
            )
            ORDER BY id
        """)

        users = cursor.fetchall()

    connection.close()

    print(f"Users to load: {len(users)}")

    if not users:
        return

    query = """
        INSERT INTO shop_dwh.users
        (id, name, country, created_at, updated_at)
        FORMAT JSONEachRow
    """

    data = "\n".join(
        [
            f'{{"id":{user[0]},"name":"{user[1]}","country":"{user[2]}",'
            f'"created_at":"{user[3]}","updated_at":"{user[4]}"}}'
            for user in users
        ]
    )

    request = Request(
        "http://clickhouse:8123/",
        data=(query + data).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic "
            + base64.b64encode(b"etl:etl_password").decode(),
        },
        method="POST",
    )

    with urlopen(request) as response:
        print(response.read().decode())


    connection = psycopg.connect(
        host="postgres",
        port=5432,
        dbname="shop",
        user="etl_user",
        password="etl_password",
    )

    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE etl_watermarks
            SET last_updated_at = (
                SELECT MAX(updated_at)
                FROM users
            )
            WHERE pipeline = 'users_to_clickhouse'
        """)

        connection.commit()

    connection.close()

    print("Watermark updated")

def get_watermark():
    connection = psycopg.connect(
        host="postgres",
        port=5432,
        dbname="shop",
        user="etl_user",
        password="etl_password",
    )

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT last_updated_at
            FROM etl_watermarks
            WHERE pipeline = 'users_to_clickhouse'
        """)

        watermark = cursor.fetchone()[0]

    connection.close()

    print(f"Current watermark: {watermark}")
    return watermark

def build_daily_sales():
    # Получаем агрегированные данные из PostgreSQL
    connection = psycopg.connect(
        host="postgres",
        port=5432,
        dbname="shop",
        user="etl_user",
        password="etl_password",
    )

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                o.created_at::date AS sale_date,
                COUNT(DISTINCT o.id) AS orders_count,
                SUM(oi.quantity) AS items_count,
                SUM(oi.quantity * oi.price) AS revenue
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.id
            WHERE o.status = 'completed'
            GROUP BY o.created_at::date
            ORDER BY sale_date
        """)

        rows = cursor.fetchall()

    connection.close()

    print(f"Daily sales rows: {len(rows)}")

    # Пересоздаём витрину в ClickHouse
    truncate_query = "TRUNCATE TABLE shop_dwh.daily_sales"

    request = Request(
        "http://clickhouse:8123/",
        data=truncate_query.encode(),
        headers={
            "Authorization": "Basic "
            + base64.b64encode(b"etl:etl_password").decode(),
        },
        method="POST",
    )

    with urlopen(request) as response:
        response.read()

    # Загружаем новую версию витрины
    query = """
        INSERT INTO shop_dwh.daily_sales
        (sale_date, orders_count, items_count, revenue)
        FORMAT JSONEachRow
    """

    data = "\n".join(
        [
            (
                f'{{"sale_date":"{row[0]}",'
                f'"orders_count":{row[1]},'
                f'"items_count":{row[2]},'
                f'"revenue":{row[3]}}}'
            )
            for row in rows
        ]
    )

    request = Request(
        "http://clickhouse:8123/",
        data=(query + data).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic "
            + base64.b64encode(b"etl:etl_password").decode(),
        },
        method="POST",
    )

    with urlopen(request) as response:
        response.read()

    print("Daily sales loaded to ClickHouse")

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

    # load_users_task = PythonOperator(
    #     task_id="load_users_to_clickhouse",
    #     python_callable=load_users_to_clickhouse,
    # )
    get_watermark_task = PythonOperator(
        task_id="get_watermark",
        python_callable=get_watermark,
    )
    use_watermark_task = PythonOperator(
    task_id="use_watermark",
    python_callable=use_watermark,
    )
    load_incremental_task = PythonOperator(
    task_id="load_incremental",
    python_callable=load_incremental,
    )
    build_daily_sales_task = PythonOperator(
    task_id="build_daily_sales",
    python_callable=build_daily_sales,
    )

    generate_data >> check_postgres_task >> load_incremental_task >> build_daily_sales_task