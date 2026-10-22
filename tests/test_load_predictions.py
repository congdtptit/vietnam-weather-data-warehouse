"""Tests for sql/04_predictions.sql and scripts/load_predictions.py (workbook ID 31).

Unit tests use synthetic frames. Integration tests run in one transaction that is rolled
back, so the shared test database keeps its original schema.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import pytest
from load_predictions import load_predictions, model_row, prediction_records

ROOT = Path(__file__).resolve().parents[1]
UTC = timezone.utc

REPORT = {
    "model_name": "DecisionTree_CART",
    "model_version": "v1",
    "label_version": "rain_next24h_v0.2",
    "feature_version": "v1",
    "feature_source": "Open-Meteo",
    "label_threshold_mm": 1.0,
    "probability_threshold": 0.4,
    "train_window_utc": ["2016-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"],
    "selected_params": {"max_depth": 8},
    "random_state": 42,
    "objects": {"model": "gold/ml_models/cart_rain_next24h_v1/model.joblib"},
    "validation_metrics_at_threshold": {"f1": 0.786},
    "availability": "retrospective",
}
EVALUATION = {
    "model_version": "v1",
    "probability_threshold": 0.4,
    "model_sha256": "a" * 64,
    "test_metrics_at_threshold": {"precision": 0.756, "recall": 0.795, "f1": 0.775},
}


def _preds(rows):
    """rows: (origin as UTC datetime, probability)."""
    df = pd.DataFrame(
        {
            "location_code": "HAN",
            "origin_utc": pd.to_datetime([r[0] for r in rows], utc=True),
            "probability": [r[1] for r in rows],
        }
    )
    df["target_start_utc"] = df["origin_utc"]
    df["target_end_utc"] = df["origin_utc"] + pd.Timedelta(hours=24)
    df["predicted_rain"] = df["probability"] >= 0.4
    df["model_version"] = "v1"
    df["probability_threshold"] = 0.4
    return df


A = datetime(2025, 1, 1, 0, tzinfo=UTC)  # window (Jan 1 00Z, Jan 2 00Z]
B = datetime(2025, 1, 2, 0, tzinfo=UTC)  # window (Jan 2 00Z, Jan 3 00Z]
C = datetime(2025, 1, 1, 17, tzinfo=UTC)  # Vietnam 2025-01-02 00:00
D = datetime(2025, 1, 2, 12, tzinfo=UTC)  # window runs past the last observation
PREDS = _preds([(A, 0.9), (B, 0.1), (C, 0.39), (D, 0.6)])


def test_records_use_vietnam_local_keys():
    recs = prediction_records(PREDS, model_key=7, location_keys={"HAN": 1}, report=REPORT)
    by_origin = {r[9]: r for r in recs}
    c = by_origin[C]
    assert (c[0], c[1], c[2]) == (20250102, 0, 20250103)  # origin and target end in UTC+7
    a = by_origin[A]
    assert (a[0], a[1], a[2]) == (20250101, 700, 20250102)
    assert c[6] is False and a[6] is True and c[7] == 0.39 and c[8] == 7


@pytest.mark.parametrize(
    "change, message",
    [
        (lambda d: d.assign(predicted_rain=~d["predicted_rain"]), "probability >= threshold"),
        (lambda d: d.assign(probability_threshold=0.5), "threshold differs"),
        (lambda d: d.assign(location_code="XXX"), "missing in dim_location"),
        (lambda d: pd.concat([d, d.iloc[[0]]]), "duplicate"),
    ],
)
def test_records_reject_inconsistent_input(change, message):
    with pytest.raises(ValueError, match=message):
        prediction_records(change(PREDS), 1, {"HAN": 1}, REPORT)


def test_model_row_requires_matching_versions_and_threshold():
    with pytest.raises(ValueError, match="different model versions"):
        model_row(REPORT, {**EVALUATION, "model_version": "v2"})
    with pytest.raises(ValueError, match="threshold"):
        model_row(REPORT, {**EVALUATION, "probability_threshold": 0.5})
    assert model_row(REPORT, EVALUATION)[14] == "a" * 64


# --- integration ------------------------------------------------------------------------


class _NoCommit:
    """Connection proxy: the loader's commit becomes a no-op so the test can roll back."""

    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        pass

    def rollback(self):
        self._conn.rollback()


@pytest.fixture
def pred_db(pg_connection):
    cur = pg_connection.cursor()
    try:
        migration = (ROOT / "sql" / "04_predictions.sql").read_text()
        cur.execute(migration)
        cur.execute(migration)  # idempotent
        cur.execute(
            """
            INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month,
                                  month, month_name, quarter, year, is_weekend, is_rainy_season)
            SELECT to_char(d, 'YYYYMMDD')::int, d::date, extract(isodow FROM d)::int,
                   to_char(d, 'FMDay'), extract(day FROM d)::int, extract(month FROM d)::int,
                   to_char(d, 'FMMonth'), extract(quarter FROM d)::int, extract(year FROM d)::int,
                   extract(isodow FROM d) IN (6, 7), false
            FROM generate_series('2024-12-30'::date, '2025-01-05'::date, interval '1 day') AS d
            ON CONFLICT (date_key) DO NOTHING;
            """
        )
        cur.execute("SELECT source_key FROM dim_source WHERE source_name = 'Open-Meteo'")
        source_key = cur.fetchone()[0]
        cur.execute("SELECT location_key FROM dim_location WHERE location_code = 'HAN'")
        location_key = cur.fetchone()[0]
        # Hourly Open-Meteo rain, 2024-12-31 00Z .. 2025-01-03 00Z: 5 mm stamped exactly at A
        # (outside A's window) and 1 mm stamped at B (the last hour of A's window).
        ts = datetime(2024, 12, 31, tzinfo=UTC)
        while ts <= datetime(2025, 1, 3, tzinfo=UTC):
            local = ts + timedelta(hours=7)
            rain = 5.0 if ts == A else 1.0 if ts == B else 0.0
            cur.execute(
                """
                INSERT INTO fact_weather_observation (date_key, time_key, location_key,
                    source_key, timestamp_utc, timestamp_local, precipitation_mm)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    int(local.strftime("%Y%m%d")),
                    local.hour * 100,
                    location_key,
                    source_key,
                    ts,
                    local.replace(tzinfo=None),
                    rain,
                ),
            )
            ts += timedelta(hours=1)
        yield pg_connection, cur
    finally:
        pg_connection.rollback()
        cur.close()


def _count(cur):
    cur.execute("SELECT count(*) FROM fact_weather_prediction")
    return cur.fetchone()[0]


@pytest.mark.integration
def test_load_is_idempotent_and_retracts_missing_rows(pred_db):
    conn, cur = pred_db
    first = load_predictions(_NoCommit(conn), REPORT, EVALUATION, PREDS)
    assert first["rows_upserted"] == 4 and _count(cur) == 4

    again = load_predictions(_NoCommit(conn), REPORT, EVALUATION, PREDS)
    assert again["model_key"] == first["model_key"] and _count(cur) == 4
    cur.execute("SELECT count(*) FROM dim_model")
    assert cur.fetchone()[0] == 1

    subset = load_predictions(_NoCommit(conn), REPORT, EVALUATION, PREDS.iloc[:3])
    assert subset["rows_retracted"] == 1 and _count(cur) == 3


@pytest.mark.integration
def test_view_reconciles_actual_rain_over_the_same_window(pred_db):
    conn, cur = pred_db
    load_predictions(_NoCommit(conn), REPORT, EVALUATION, PREDS)
    cur.execute(
        """
        SELECT origin_utc, actual_valid_hours, actual_rain_mm, actual_rain, outcome
        FROM vw_prediction_vs_actual ORDER BY origin_utc
        """
    )
    rows = {r[0]: r[1:] for r in cur.fetchall()}
    assert (
        rows[A][0] == 24 and float(rows[A][1]) == 1.0 and rows[A][3] == "TP"
    )  # 5 mm at A excluded
    assert rows[B][3] == "TN" and float(rows[B][1]) == 0.0  # 1 mm at B belongs to A's window
    assert rows[C][3] == "FN"  # p 0.39 < 0.40, 1 mm at B inside (C, C+24h]
    assert rows[D][0] == 12 and rows[D][1] is None and rows[D][3] is None  # incomplete: unknown


@pytest.mark.integration
def test_constraints_reject_duplicates_and_bad_windows(pred_db):
    conn, cur = pred_db
    load_predictions(_NoCommit(conn), REPORT, EVALUATION, PREDS)
    cur.execute("SAVEPOINT s")
    with pytest.raises(psycopg2.errors.UniqueViolation):
        cur.execute(
            """
            INSERT INTO fact_weather_prediction (prediction_date_key, prediction_time_key,
                target_date_key, location_key, model_version, rain_next_24h, rain_probability,
                model_key, origin_utc, target_start_utc, target_end_utc, horizon_hours,
                probability_threshold, evaluation_split)
            SELECT prediction_date_key, prediction_time_key, target_date_key, location_key,
                model_version, rain_next_24h, rain_probability, model_key, origin_utc,
                target_start_utc, target_end_utc, horizon_hours, probability_threshold,
                evaluation_split
            FROM fact_weather_prediction LIMIT 1
            """
        )
    cur.execute("ROLLBACK TO SAVEPOINT s")
    with pytest.raises(psycopg2.errors.CheckViolation):
        cur.execute(
            "UPDATE fact_weather_prediction SET target_end_utc = target_end_utc + interval '1 hour'"
        )
    cur.execute("ROLLBACK TO SAVEPOINT s")
