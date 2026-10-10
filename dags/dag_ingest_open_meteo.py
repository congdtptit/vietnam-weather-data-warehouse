"""
dags/dag_ingest_open_meteo.py
DAG tự động hóa thu thập dữ liệu Open-Meteo và tải vào MinIO Bronze:
- 2016-2023 : Training
- 2024      : Validation
- 2025      : Testing
Cho 8 địa phương miền Bắc Việt Nam.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def run_ingestion_task(split: str = None, **kwargs):
    """Gọi script thu thập theo từng split tập dữ liệu"""
    import subprocess
    import sys

    script_path = "/opt/airflow/scripts/ingest_open_meteo.py"
    cmd = [sys.executable, script_path]
    if split:
        cmd.extend(["--split", split])

    print(f"Executing: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    print("STDOUT:\n", result.stdout)
    if result.returncode != 0:
        print("STDERR:\n", result.stderr)
        raise RuntimeError(f"Script ingest_open_meteo.py failed with code {result.returncode}")


with DAG(
    dag_id="open_meteo_ingest_to_bronze_dag",
    default_args=default_args,
    description="Thu thập dữ liệu khí tượng Open-Meteo (2016-2025) nạp tầng Bronze MinIO",
    schedule_interval=None,  # Manual trigger hoặc lập lịch theo nhu cầu
    catchup=False,
    tags=["weather", "open-meteo", "bronze", "ingestion"],
) as dag:
    t_train = PythonOperator(
        task_id="ingest_training_data_2016_2023",
        python_callable=run_ingestion_task,
        op_kwargs={"split": "training"},
    )

    t_val = PythonOperator(
        task_id="ingest_validation_data_2024",
        python_callable=run_ingestion_task,
        op_kwargs={"split": "validation"},
    )

    t_test = PythonOperator(
        task_id="ingest_testing_data_2025",
        python_callable=run_ingestion_task,
        op_kwargs={"split": "testing"},
    )

    t_train >> t_val >> t_test
