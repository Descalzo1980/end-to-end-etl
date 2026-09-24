from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


def hello():
    print("Hello, Stas!")


with DAG(
    dag_id="hello_stas",
    start_date=datetime(2026, 9, 24),
    schedule=None,
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id="hello",
        python_callable=hello,
    )