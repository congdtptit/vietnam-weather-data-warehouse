"""Unit tests for scripts/pipeline_watermark.py and dags/dag_weather_incremental.py (ID 25)."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pipeline_watermark as wm
import pytest
from minio.error import S3Error


def _client(store):
    def get(bucket, key):
        if key not in store:
            raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)
        body = store[key]
        return SimpleNamespace(read=lambda: body, close=lambda: None, release_conn=lambda: None)

    def put(bucket, key, data, length, content_type=None):
        store[key] = data.read()

    client = Mock()
    client.get_object.side_effect = get
    client.put_object.side_effect = put
    return client


def _at(year):
    return {"committed_through_year": year}


@pytest.mark.parametrize(
    "watermark, lookback, max_years, expected",
    [
        (None, 1, 1, (2016, 2016, [2016, 2016])),  # nothing committed: start of the scope
        (None, 1, 3, (2016, 2018, [2016, 2018])),
        (_at(2020), 1, 1, (2020, 2021, [2021, 2021])),  # one committed year re-processed
        (_at(2020), 0, 1, (2021, 2021, [2021, 2021])),
        (_at(2016), 2, 1, (2016, 2017, [2017, 2017])),  # lookback stops at 2016
        (_at(2023), 1, 5, (2023, 2025, [2024, 2025])),  # never after 2025
    ],
)
def test_plan_window(watermark, lookback, max_years, expected):
    window = wm.plan_window(watermark, lookback, max_years)
    assert (window["start_year"], window["end_year"], window["new_years"]) == expected
    assert window["base_committed_through_year"] == wm.committed_year(watermark)


def test_nothing_to_plan_once_2025_is_committed():
    assert wm.plan_window(_at(2025), lookback_years=2, max_years=10) is None


@pytest.mark.parametrize("lookback, max_years", [(-1, 1), (1, 0)])
def test_plan_rejects_bad_parameters(lookback, max_years):
    with pytest.raises(ValueError):
        wm.plan_window(None, lookback, max_years)


def test_advance_moves_forward_from_the_planned_base_only():
    store = {}
    client = _client(store)
    body = wm.advance(client, None, [2016, 2016], "incremental__a", "scheduled__a")
    assert (
        body["committed_through_year"] == 2016 and body["previous_committed_through_year"] is None
    )
    assert json.loads(store[wm.WATERMARK_KEY])["pipeline_run_id"] == "incremental__a"

    # A second run planned on the same base must not advance again
    with pytest.raises(RuntimeError, match="planned on None"):
        wm.advance(client, None, [2016, 2017], "incremental__b", "scheduled__b")
    assert wm.committed_year(wm.read_watermark(client)) == 2016

    wm.advance(client, 2016, [2016, 2017], "incremental__c", "scheduled__c")
    assert wm.read_watermark(client)["window_utc_years"] == [2016, 2017]
    with pytest.raises(ValueError, match="does not move"):
        wm.advance(client, 2017, [2016, 2017], "x", "y")
    with pytest.raises(ValueError, match="outside"):
        wm.advance(client, 2017, [2025, 2026], "x", "y")


def test_set_by_hand_keeps_the_reason_and_the_scope():
    store = {}
    client = _client(store)
    body = wm.set_watermark(client, 2024, "replay 2025 as the new year")
    assert body["set_by_hand"] is True and body["previous_committed_through_year"] is None
    assert wm.set_watermark(client, 2025, "r")["previous_committed_through_year"] == 2024
    with pytest.raises(ValueError, match="outside"):
        wm.set_watermark(client, 2026, "r")
    with pytest.raises(ValueError, match="reason"):
        wm.set_watermark(client, 2024, " ")


# --- DAG -------------------------------------------------------------------------------


def test_incremental_dag_order_and_settings(dag_bag):
    dag = dag_bag.dags["weather_incremental_dag"]
    assert dag.catchup is False and dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is True and dag.schedule_interval == "@daily"
    down = {t.task_id: t.downstream_task_ids for t in dag.tasks}
    assert down == {
        "plan_window": {"run_window"},
        "run_window": {"advance_watermark"},
        "advance_watermark": set(),
    }
    trigger = dag.get_task("run_window")
    assert trigger.trigger_dag_id == "weather_bronze_to_dw_dag" and trigger.wait_for_completion
    assert trigger.allowed_states == ["success"] and trigger.failed_states == ["failed"]
    advance = dag.get_task("advance_watermark")
    assert advance.trigger_rule == "all_success" and advance.retries == 0
    assert dag.get_task("plan_window").ignore_downstream_trigger_rules is True


def _ti(xcoms):
    ti = Mock()
    ti.xcom_pull.side_effect = lambda task_ids, key: xcoms[(task_ids, key)]
    return ti


def test_plan_task_returns_the_pipeline_conf_or_skips(dag_bag, monkeypatch):
    store = {}
    monkeypatch.setattr(wm, "get_minio_client", lambda: _client(store))
    task = dag_bag.dags["weather_incremental_dag"].get_task("plan_window")
    params = {"lookback_years": 1, "max_years": 1}

    wm.set_watermark(_client(store), 2024, "test")
    ti = Mock()
    assert task.python_callable(params=params, ti=ti) == {
        "start_year": 2024,
        "end_year": 2025,
        "location": "",
    }
    assert ti.xcom_push.call_args.kwargs["value"]["base_committed_through_year"] == 2024

    wm.set_watermark(_client(store), 2025, "test")
    assert task.python_callable(params=params, ti=Mock()) is False


def test_advance_task_records_the_pipeline_run(dag_bag, monkeypatch):
    store = {}
    monkeypatch.setattr(wm, "get_minio_client", lambda: _client(store))
    wm.set_watermark(_client(store), 2024, "test")
    task = dag_bag.dags["weather_incremental_dag"].get_task("advance_watermark")
    window = {"start_year": 2024, "end_year": 2025, "base_committed_through_year": 2024}
    ti = _ti(
        {
            ("plan_window", "plan"): window,
            ("run_window", "trigger_run_id"): "incremental__manual__x",
        }
    )
    task.python_callable(ti=ti, run_id="manual__x")
    body = wm.read_watermark(_client(store))
    assert body["committed_through_year"] == 2025
    assert (
        body["pipeline_run_id"] == "incremental__manual__x" and body["advanced_by"] == "manual__x"
    )
