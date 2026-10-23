"""dags/dag_weather_pipeline.py.

Bronze -> Silver -> Gold -> DW for an explicit window of UTC years (workbook ID 17;
specification in docs/task-specs-weeks-2-5.md section 5). No schedule of its own: it runs when
triggered by hand or by weather_incremental_dag (ID 25) for a watermark window. Hourly
observations (Open-Meteo, Meteostat, and since 2026-10-23 ERA5 point time series) go to
fact_weather_observation; GHCNh daily station rain goes to fact_station_daily_rain.

Each task runs an existing script; a non-zero exit fails the task. Ingestion skips objects
whose manifest and checksum are current, transforms rewrite Silver for the window,
`quality_gate` (ID 24) stops the run before the DW when Silver breaks a contract rule, the Gold
loaders replace the window in the DW, and `reconcile` fails when DW row counts differ from
Silver per source x location x year (hourly) or per station x year (daily rain). Only
parameters and report keys pass between tasks.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SCRIPTS = "/opt/airflow/scripts"
LOCATIONS = ["", "HAN", "PTO", "TNG", "TUQ", "LCA", "HBI", "HPH", "NBI"]

YEARS = "--start-year {{ params.start_year }} --end-year {{ params.end_year }}"
LOCATION = "{% if params.location %} --location {{ params.location }}{% endif %}"


def validate_window(params: dict, **_) -> None:
    """Reject a window outside the project scope before any task runs."""
    start, end = int(params["start_year"]), int(params["end_year"])
    if not 2016 <= start <= end <= 2025:
        raise ValueError(f"window {start}-{end} is outside 2016-2025 or reversed")
    print(f"Window: UTC years {start}-{end}; location: {params.get('location') or 'all 8'}")


def script(name: str, args: str) -> str:
    return f"set -euo pipefail; python {SCRIPTS}/{name} {args}"


with DAG(
    dag_id="weather_bronze_to_dw_dag",
    description="Open-Meteo, Meteostat, ERA5, GHCNh: Bronze -> Silver -> Gold -> DW, then reconcile",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # one window at a time: runs must not overlap on the same partitions
    default_args={"owner": "data_engineering_team", "retries": 0},
    params={
        "start_year": Param(2024, type="integer", minimum=2016, maximum=2025),
        "end_year": Param(2024, type="integer", minimum=2016, maximum=2025),
        "location": Param("", type="string", enum=LOCATIONS, description="Empty = all 8"),
    },
    tags=["weather", "pipeline", "bronze", "silver", "gold", "dwh"],
) as dag:
    check = PythonOperator(task_id="validate_window", python_callable=validate_window)

    # Network sources: one retry for transient outages (the scripts already retry requests)
    network = {"retries": 1, "retry_delay": timedelta(minutes=2)}
    ingest_om = BashOperator(
        task_id="ingest_open_meteo",
        bash_command=script("ingest_open_meteo.py", YEARS + LOCATION),
        **network,
    )
    ingest_ms = BashOperator(
        task_id="ingest_meteostat",
        bash_command=script("ingest_meteostat.py", YEARS),
        **network,
    )
    silver_om = BashOperator(
        task_id="transform_open_meteo_to_silver",
        bash_command=script("transform_open_meteo_to_silver.py", YEARS + LOCATION + " --overwrite"),
    )
    silver_ms = BashOperator(
        task_id="transform_meteostat_to_silver",
        bash_command=script("transform_meteostat_to_silver.py", YEARS + " --overwrite"),
    )
    ingest_gh = BashOperator(
        task_id="ingest_ghcnh",
        bash_command=script("ingest_ghcnh.py", YEARS + LOCATION),
        **network,
    )
    silver_gh = BashOperator(
        task_id="transform_ghcnh_to_silver",
        bash_command=script("transform_ghcnh_to_silver.py", YEARS + LOCATION + " --overwrite"),
    )
    ingest_era5 = BashOperator(
        task_id="ingest_era5_timeseries",
        bash_command=script("ingest_era5_timeseries.py", YEARS + LOCATION),
        **network,
    )
    silver_era5 = BashOperator(
        task_id="transform_era5_to_silver",
        bash_command=script("transform_era5_to_silver.py", YEARS + LOCATION + " --overwrite"),
    )
    gate = BashOperator(
        task_id="quality_gate",
        bash_command=script("quality_gate.py", YEARS + LOCATION),
    )
    seed = BashOperator(task_id="seed_dimensions", bash_command=script("seed_dimensions.py", ""))
    load = BashOperator(
        task_id="load_gold_observations",
        bash_command=script("load_gold_observations.py", YEARS + LOCATION),
    )
    load_rain = BashOperator(
        task_id="load_station_daily_rain",
        bash_command=script("load_station_daily_rain.py", YEARS + LOCATION),
    )
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=script("reconcile_dw.py", YEARS + LOCATION),
    )

    check >> [ingest_om, ingest_ms, ingest_era5, ingest_gh]
    ingest_om >> silver_om
    ingest_ms >> silver_ms
    ingest_era5 >> silver_era5
    ingest_gh >> silver_gh
    [silver_om, silver_ms, silver_era5, silver_gh] >> gate >> seed >> [load, load_rain] >> reconcile
