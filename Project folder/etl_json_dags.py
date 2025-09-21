from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
import json
from datetime import datetime, timedelta

# Step 1: Extract
def extract_data():
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()  # Airflow XCom will handle passing to next task
    else:
        raise Exception(f"API call failed: {response.status_code}")

# Step 2: Transform
def transform_data(ti):  # ti = task instance, lets us pull data from XCom
    raw_data = ti.xcom_pull(task_ids="extract")
    transformed = []
    for record in raw_data:
        transformed.append((
            record["id"],
            record["title"],
            record["body"]
        ))
    return transformed

# Step 3: Load
def load_data(ti):
    transformed_data = ti.xcom_pull(task_ids="transform")
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    insert_query = """
        INSERT INTO posts (id, title, body)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    cur.executemany(insert_query, transformed_data)
    conn.commit()
    cur.close()
    conn.close()

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="api_to_postgres_pipeline",
    default_args=default_args,
    description="ETL pipeline from API to Postgres",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task