"""
dags/weather_pipeline_health_dag.py
DAG kiểm tra kết nối hệ thống và luồng dữ liệu cơ bản cho:
- MinIO Data Lake (Bronze, Silver, Gold buckets)
- PostgreSQL Data Warehouse (weather_dwh - Star Schema)
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def check_minio_connection():
    from minio import Minio

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_pass = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    print(f"Connecting to MinIO at {minio_endpoint}...")
    client = Minio(minio_endpoint, access_key=minio_user, secret_key=minio_pass, secure=False)

    buckets = [b.name for b in client.list_buckets()]
    print(f"MinIO Buckets found: {buckets}")

    required_buckets = ["bronze", "silver", "gold"]
    for req in required_buckets:
        if req not in buckets:
            raise ValueError(f"Required bucket '{req}' is missing on MinIO!")
    print("All 3 required buckets (bronze, silver, gold) are verified successfully!")


def check_postgres_dwh_connection():
    import psycopg2

    host = os.getenv("POSTGRES_HOST", "postgres")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    dbname = os.getenv("DWH_DB", "weather_dwh")

    print(f"Connecting to PostgreSQL DWH ({dbname}) at {host}:{port}...")
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public';
    """)
    tables = [row[0] for row in cursor.fetchall()]
    print(f"PostgreSQL DWH tables found: {tables}")

    cursor.execute("SELECT count(*) FROM dim_location;")
    loc_count = cursor.fetchone()[0]
    print(f"Number of locations initialized in dim_location: {loc_count}")

    cursor.close()
    conn.close()
    print("PostgreSQL DWH connection & schema verified successfully!")


with DAG(
    dag_id="weather_system_healthcheck_dag",
    default_args=default_args,
    description="Kiểm tra kết nối MinIO Data Lake và PostgreSQL Data Warehouse",
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "monitoring", "minio", "postgres"],
) as dag:
    t1 = PythonOperator(
        task_id="verify_minio_data_lake",
        python_callable=check_minio_connection,
    )

    t2 = PythonOperator(
        task_id="verify_postgres_data_warehouse",
        python_callable=check_postgres_dwh_connection,
    )

    t1 >> t2
