"""Tests for scripts/publish_predictions.py (workbook ID 32).

Unit tests use synthetic frames. Integration tests reuse the ID 31 fixture: migrations
reapplied, one transaction rolled back at the end, so the shared test database keeps its rows.
"""

from datetime import date
from unittest.mock import Mock

import pandas as pd
import pytest
from load_predictions import load_predictions
from publish_predictions import check, compare, publish, read_dw_scope
from test_load_predictions import EVALUATION, PREDS, REPORT, _NoCommit, pred_db  # noqa: F401

JAN1, JAN2 = date(2025, 1, 1), date(2025, 1, 2)


def _gold(preds=PREDS, sha=EVALUATION["model_sha256"]):
    """The ID 31 fixture predictions in the Gold layout written by score_cart.py."""
    return preds.assign(
        model_name=REPORT["model_name"],
        model_sha256=sha,
        feature_source="Open-Meteo",
        evaluation_split="test",
    ).reset_index(drop=True)


def _dw(gold):
    return pd.DataFrame(
        {
            "location_code": gold["location_code"],
            "origin_utc": gold["origin_utc"],
            "target_start_utc": gold["target_start_utc"],
            "target_end_utc": gold["target_end_utc"],
            "rain_probability": [round(float(p), 4) for p in gold["probability"]],
            "rain_next_24h": gold["predicted_rain"],
            "probability_threshold": gold["probability_threshold"],
            "evaluation_split": "test",
            "horizon_hours": 24,
        }
    )


def test_compare_finds_missing_extra_and_changed_rows():
    gold = _gold().assign(probability=[0.123456, 0.1, 0.39, 0.6])
    assert compare(gold, _dw(gold))["mismatches"] == dict.fromkeys(
        ("probability", "class", "threshold", "window", "split"), 0
    )
    dw = _dw(gold)
    dw.loc[1, "rain_probability"] = 0.1001
    dw.loc[2, "rain_next_24h"] = True
    extra = dw.iloc[[3]].assign(origin_utc=dw["origin_utc"].iloc[3] + pd.Timedelta(hours=1))
    out = compare(gold.iloc[:3], pd.concat([dw, extra]).iloc[1:])
    assert out["missing_in_dw"] == 1 and out["extra_in_dw"] == 2
    assert out["mismatches"]["probability"] == 1 and out["mismatches"]["class"] == 1


# --- integration ------------------------------------------------------------------------


class _Savepoint(_NoCommit):
    """Commit is a no-op and rollback returns to a savepoint, so a refused or dry-run
    publish keeps the fixture rows of the surrounding transaction."""

    def __init__(self, conn):
        super().__init__(conn)
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT publish")

    def rollback(self):
        with self._conn.cursor() as cur:
            cur.execute("ROLLBACK TO SAVEPOINT publish")


def _tuples(cur):
    """Row identity and physical location: an UPDATE moves a row to a new ctid."""
    cur.execute(
        "SELECT prediction_id, ctid::text, predicted_at FROM fact_weather_prediction ORDER BY 1"
    )
    return cur.fetchall()


def _registered(conn):
    return load_predictions(_NoCommit(conn), REPORT, EVALUATION, PREDS)["model_key"]


@pytest.mark.integration
def test_unchanged_rerun_writes_nothing(pred_db):  # noqa: F811
    conn, cur = pred_db
    model_key = _registered(conn)
    before = _tuples(cur)

    out = publish(_Savepoint(conn), _gold(), JAN1, JAN2)
    assert (out["rows"], out["unchanged"], out["inserted"], out["updated"]) == (4, 4, 0, 0)
    assert out["retracted"] == 0 and out["model_key"] == model_key
    assert _tuples(cur) == before  # no row rewritten

    changed = _gold().assign(probability=[0.9, 0.1, 0.41, 0.6], predicted_rain=[1, 0, 1, 1])
    changed["predicted_rain"] = changed["predicted_rain"].astype(bool)
    dry = publish(_Savepoint(conn), changed, JAN1, JAN2, dry_run=True)
    assert dry["updated"] == 1 and dry["dry_run"] is True
    assert _tuples(cur) == before  # rolled back

    client = Mock()
    assert check(_Savepoint(conn), client, _gold(), JAN1, JAN2)["passed"] is True
    assert client.put_object.call_args.args[1].startswith("quality_reports/prediction_reconcile_")


@pytest.mark.integration
def test_changed_and_missing_rows_stay_inside_the_window(pred_db):  # noqa: F811
    conn, cur = pred_db
    model_key = _registered(conn)
    # Window Jan 1 only: A (00Z) and C (17Z); B and D are Jan 2 origins outside the scope
    jan1 = _gold().iloc[[0, 2]].reset_index(drop=True)
    jan1.loc[1, ["probability", "predicted_rain"]] = [0.45, True]  # C changes
    out = publish(_Savepoint(conn), jan1, JAN1, JAN1)
    assert (out["updated"], out["unchanged"], out["inserted"], out["retracted"]) == (1, 1, 0, 0)

    out = publish(_Savepoint(conn), jan1.iloc[[0]], JAN1, JAN1)  # C is no longer scored
    assert out["retracted"] == 1
    dw = read_dw_scope(cur, model_key, JAN1, JAN2)
    assert len(dw) == 3  # A on Jan 1, B and D on Jan 2 untouched
    assert compare(_gold().iloc[[0, 1, 3]], dw)["mismatches"]["probability"] == 0


@pytest.mark.integration
def test_unregistered_or_different_artifact_is_refused(pred_db):  # noqa: F811
    conn, cur = pred_db
    with pytest.raises(RuntimeError, match="not in dim_model"):
        publish(_Savepoint(conn), _gold(), JAN1, JAN2)
    _registered(conn)
    before = _tuples(cur)
    with pytest.raises(ValueError, match="SHA-256"):
        publish(_Savepoint(conn), _gold(sha="b" * 64), JAN1, JAN2)
    mixed = _gold().assign(model_sha256=["a" * 64] * 3 + ["b" * 64])
    with pytest.raises(ValueError, match="mixes models"):
        publish(_Savepoint(conn), mixed, JAN1, JAN2)
    assert _tuples(cur) == before
