from types import SimpleNamespace
from unittest.mock import Mock

import pytest


def test_dags_import_and_order(dag_bag):
    assert set(dag_bag.dags) == {
        "open_meteo_ingest_to_bronze_dag",
        "weather_bronze_to_dw_dag",
        "weather_system_healthcheck_dag",
    }
    dag = dag_bag.dags["open_meteo_ingest_to_bronze_dag"]
    assert dag.catchup is False
    assert dag.schedule_interval is None
    assert dag.get_task("ingest_training_data_2016_2023").downstream_task_ids == {
        "ingest_validation_data_2024"
    }
    assert dag.get_task("ingest_validation_data_2024").downstream_task_ids == {
        "ingest_testing_data_2025"
    }
    assert dag.get_task("ingest_testing_data_2025").downstream_task_ids == set()


@pytest.mark.parametrize("returncode", [0, 1])
def test_dag_subprocess_exit_status(dag_bag, monkeypatch, returncode):
    import subprocess

    run = Mock(
        return_value=SimpleNamespace(
            returncode=returncode, stdout="fixture", stderr="fixture error"
        )
    )
    monkeypatch.setattr(subprocess, "run", run)
    task = dag_bag.dags["open_meteo_ingest_to_bronze_dag"].get_task("ingest_validation_data_2024")
    if returncode:
        with pytest.raises(RuntimeError, match="code 1"):
            task.python_callable(split="validation")
    else:
        task.python_callable(split="validation")
    assert run.call_args.args[0][-2:] == ["--split", "validation"]


def test_pipeline_dag_order_and_settings(dag_bag):
    dag = dag_bag.dags["weather_bronze_to_dw_dag"]
    assert dag.catchup is False and dag.schedule_interval is None and dag.max_active_runs == 1
    down = {t.task_id: t.downstream_task_ids for t in dag.tasks}
    assert down["validate_window"] == {"ingest_open_meteo", "ingest_meteostat", "ingest_ghcnh"}
    assert down["ingest_ghcnh"] == {"transform_ghcnh_to_silver"}
    assert down["transform_ghcnh_to_silver"] == {"quality_gate"}
    assert down["quality_gate"] == {"seed_dimensions"}
    assert down["load_station_daily_rain"] == {"reconcile"}
    assert down["ingest_open_meteo"] == {"transform_open_meteo_to_silver"}
    assert down["ingest_meteostat"] == {"transform_meteostat_to_silver"}
    assert down["transform_open_meteo_to_silver"] == {"quality_gate"}
    assert down["transform_meteostat_to_silver"] == {"quality_gate"}
    assert down["seed_dimensions"] == {"load_gold_observations", "load_station_daily_rain"}
    assert down["load_gold_observations"] == {"reconcile"}
    assert down["reconcile"] == set()
    assert dag.get_task("ingest_open_meteo").retries == 1
    assert dag.get_task("load_gold_observations").retries == 0


@pytest.mark.parametrize(
    "params, expected",
    [
        ({"start_year": 2024, "end_year": 2024, "location": ""}, "--end-year 2024"),
        ({"start_year": 2023, "end_year": 2024, "location": "HAN"}, "--location HAN"),
    ],
)
def test_pipeline_commands_render_the_window(dag_bag, params, expected):
    dag = dag_bag.dags["weather_bronze_to_dw_dag"]
    for task_id in (
        "ingest_open_meteo",
        "ingest_ghcnh",
        "quality_gate",
        "load_gold_observations",
        "load_station_daily_rain",
        "reconcile",
    ):
        task = dag.get_task(task_id)
        cmd = task.render_template(task.bash_command, {"params": params})
        assert f"--start-year {params['start_year']}" in cmd and expected in cmd
        assert cmd.startswith("set -euo pipefail; python /opt/airflow/scripts/")
        if not params["location"]:
            assert "--location" not in cmd
    silver = dag.get_task("transform_open_meteo_to_silver")
    assert silver.render_template(silver.bash_command, {"params": params}).endswith("--overwrite")


@pytest.mark.parametrize("start, end", [(2015, 2016), (2025, 2026), (2024, 2023)])
def test_pipeline_rejects_windows_outside_scope(dag_bag, start, end):
    task = dag_bag.dags["weather_bronze_to_dw_dag"].get_task("validate_window")
    with pytest.raises(ValueError, match="outside"):
        task.python_callable(params={"start_year": start, "end_year": end, "location": ""})


def test_every_dw_writing_task_waits_for_the_quality_gate(dag_bag):
    dag = dag_bag.dags["weather_bronze_to_dw_dag"]
    for task_id in ("seed_dimensions", "load_gold_observations", "load_station_daily_rain"):
        task = dag.get_task(task_id)
        assert "quality_gate" in task.get_flat_relative_ids(upstream=True)
        assert task.trigger_rule == "all_success"
