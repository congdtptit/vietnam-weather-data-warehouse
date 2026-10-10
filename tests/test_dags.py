from types import SimpleNamespace
from unittest.mock import Mock

import pytest


def test_dags_import_and_order(dag_bag):
    assert set(dag_bag.dags) == {
        "open_meteo_ingest_to_bronze_dag",
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
