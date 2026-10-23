"""dags/dag_rain_scoring.py.

Score the frozen CART RainNext24h model for an explicit window of UTC forecast origins and
publish the predictions (workbook ID 32; rules in docs/rain-label.md). Manual trigger only.

validate_window -> score_cart (Silver Open-Meteo features -> Gold predictions, one object per
origin day) -> publish_predictions (DW, one transaction, unchanged rows untouched) ->
reconcile_predictions (fails unless the DW scope equals Gold).

Each task runs a script; a non-zero exit fails the task and stops the tasks after it. Only
the test split is published (origins 2025-01-01 to 2025-12-30 UTC, so every target window
ends inside 2016-2025); the scripts check this again against the model's feature manifest.
"""

from datetime import date, datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SCRIPTS = "/opt/airflow/scripts"
FIRST_DAY, LAST_DAY = date(2025, 1, 1), date(2025, 12, 30)  # test split, targets inside 2025

WINDOW = (
    "--start-date {{ params.start_date }} --end-date {{ params.end_date }}"
    " --version {{ params.model_version }}"
)


def validate_window(params: dict, **_) -> None:
    """Reject a window outside the published test origins before any task runs."""
    start, end = date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"])
    if not FIRST_DAY <= start <= end <= LAST_DAY:
        raise ValueError(f"window {start}..{end} is outside {FIRST_DAY}..{LAST_DAY} or reversed")
    print(f"Window: UTC origin days {start}..{end}; model {params['model_version']}")


def script(name: str, args: str) -> str:
    return f"set -euo pipefail; python {SCRIPTS}/{name} {args}"


with DAG(
    dag_id="weather_rain_scoring_dag",
    description="CART RainNext24h: Silver features -> Gold predictions -> DW, then reconcile",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # one window at a time: runs must not overlap on the same predictions
    default_args={"owner": "data_engineering_team", "retries": 0},
    params={
        "start_date": Param("2025-01-01", type="string", format="date"),
        "end_date": Param("2025-01-31", type="string", format="date"),
        "model_version": Param("v1", type="string", enum=["v1"]),
    },
    tags=["weather", "ml", "cart", "scoring", "dwh"],
) as dag:
    check = PythonOperator(task_id="validate_window", python_callable=validate_window)
    score = BashOperator(task_id="score_cart", bash_command=script("score_cart.py", WINDOW))
    publish = BashOperator(
        task_id="publish_predictions",
        bash_command=script("publish_predictions.py", WINDOW),
    )
    reconcile = BashOperator(
        task_id="reconcile_predictions",
        bash_command=script("publish_predictions.py", WINDOW + " --check"),
    )

    check >> score >> publish >> reconcile
