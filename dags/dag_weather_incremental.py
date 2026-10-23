"""dags/dag_weather_incremental.py.

Incremental Bronze -> DW updates driven by a watermark (workbook ID 25; the window rule is in
scripts/pipeline_watermark.py).

- plan_window reads the watermark and plans the next window of UTC years: the new years plus
  a lookback of committed years for late corrections. When the DW is committed through 2025
  (the end of the project scope) it skips the rest of the run.
- run_window triggers weather_bronze_to_dw_dag for that window and waits. That DAG ingests,
  gates, loads, and reconciles; any failed task fails it, and then run_window fails.
- advance_watermark runs only after run_window succeeded. It moves the watermark from the
  planned base to the window end, so the watermark advances only after the DW commit and
  its reconciliation.

The daily schedule only sets how often the DAG looks for new data. The window comes from the
watermark, never from the wall clock. catchup is off and the DAG is paused at creation, so it
never replays past intervals on its own. Only the window and the watermark pass through XCom.
"""

import sys
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

SCRIPTS = "/opt/airflow/scripts"
PIPELINE_DAG = "weather_bronze_to_dw_dag"


def _watermark():
    """Import the watermark helper at run time, not at DAG parse."""
    if SCRIPTS not in sys.path:
        sys.path.insert(0, SCRIPTS)
    import pipeline_watermark

    return pipeline_watermark


def plan(params: dict, ti, **_):
    wm = _watermark()
    current = wm.read_watermark(wm.get_minio_client())
    window = wm.plan_window(current, int(params["lookback_years"]), int(params["max_years"]))
    if window is None:
        print(f"Up to date: committed through {wm.committed_year(current)}; nothing to load")
        return False
    print(f"Watermark {window['base_committed_through_year']}; window {window}")
    ti.xcom_push(key="plan", value=window)
    return {"start_year": window["start_year"], "end_year": window["end_year"], "location": ""}


def advance(ti, run_id: str, **_) -> None:
    wm = _watermark()
    window = ti.xcom_pull(task_ids="plan_window", key="plan")
    pipeline_run = ti.xcom_pull(task_ids="run_window", key="trigger_run_id")
    body = wm.advance(
        wm.get_minio_client(),
        window["base_committed_through_year"],
        [window["start_year"], window["end_year"]],
        pipeline_run,
        run_id,
    )
    print(f"Watermark advanced to {body['committed_through_year']} after {pipeline_run}")


with DAG(
    dag_id="weather_incremental_dag",
    description="Watermark-driven incremental Bronze -> DW updates within 2016-2025",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"owner": "data_engineering_team", "retries": 0},
    params={
        "lookback_years": Param(1, type="integer", minimum=0, maximum=2),
        "max_years": Param(1, type="integer", minimum=1, maximum=10),
    },
    tags=["weather", "pipeline", "incremental", "dwh"],
) as dag:
    plan_window = ShortCircuitOperator(task_id="plan_window", python_callable=plan)
    run_window = TriggerDagRunOperator(
        task_id="run_window",
        trigger_dag_id=PIPELINE_DAG,
        trigger_run_id="incremental__{{ run_id }}",
        conf=plan_window.output,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    advance_watermark = PythonOperator(task_id="advance_watermark", python_callable=advance)

    plan_window >> run_window >> advance_watermark
