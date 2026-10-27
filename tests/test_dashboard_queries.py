"""Unit tests for scripts/dashboard_queries.py (the web dashboard that replaces Power BI).

Synthetic frames only; the SQL itself runs against the test database in
tests/test_olap_views.py::test_dashboard_queries_on_the_warehouse.
"""

import math
from decimal import Decimal

import dashboard_queries as dq
import numpy as np
import pandas as pd
import pytest


def _month(location, year, month, rain, rainy, complete=30, hot=0, temp=(750.0, 30)):
    return {
        "location_code": location,
        "location_name": location,
        "year": year,
        "month": month,
        "days_in_month": 30,
        "record_hours": 720,
        "rain_mm": rain,
        "rain_valid_hours": 0 if rain is None else 720,
        "temp_sum": temp[0],
        "temp_valid_hours": temp[1],
        "temp_min_c": 20.0,
        "temp_max_c": 36.0,
        "complete_rain_days": complete,
        "rainy_days": rainy,
        "complete_temp_days": 30,
        "hot_days": hot,
    }


def _frame(rows):
    return pd.DataFrame(rows).astype({"rain_mm": "float64", "rainy_days": "float64"})


def test_vn_number_uses_vietnamese_separators_and_a_dash_for_unknown():
    assert dq.vn_number(2660.5) == "2.660,5"
    assert dq.vn_number(0.7766, 3) == "0,777"
    assert dq.vn_number(1490237, 0) == "1.490.237"
    assert dq.vn_number(24.62, 2, "°C") == "24,62 °C"
    assert dq.vn_number(0.0) == "0,0"  # zero stays zero
    for unknown in (None, math.nan, np.nan, pd.NA):
        assert dq.vn_number(unknown) == "—"


def test_frame_turns_numeric_into_float_and_null_into_nan():
    df = dq.frame(["a", "b", "c"], [(Decimal("1.5"), 2, "x"), (None, None, "y")])
    assert df["a"].dtype == "float64" and math.isnan(df["a"][1]) and df["a"][0] == 1.5
    assert df["b"].dtype == "float64" and math.isnan(df["b"][1])
    assert list(df["c"]) == ["x", "y"]


def test_summary_rain_and_days_are_means_per_location_year_never_sums():
    rows = [
        _month("HAN", 2024, 7, 300.0, 20),
        _month("HAN", 2024, 8, 100.0, 10),
        _month("LCA", 2024, 7, 500.0, 25),
        _month("LCA", 2024, 8, 300.0, 15),
    ]
    s = dq.summary(_frame(rows), n_locations=2, hours_per_location=1440.0)
    assert s["rain_mm"] == pytest.approx((400 + 800) / 2)  # mean of location totals
    assert s["rainy_days"] == pytest.approx((30 + 40) / 2)
    assert s["temp_mean_c"] == pytest.approx(25.0)  # sum / valid hours
    assert s["rain_coverage"] == pytest.approx(1.0)

    # Over two years the measures are per year, not ten-year totals
    two_years = rows + [dict(r, year=2025) for r in rows]
    s2 = dq.summary(_frame(two_years), n_locations=2, hours_per_location=2880.0)
    assert s2["rain_mm"] == pytest.approx(600.0) and s2["rainy_days"] == pytest.approx(35.0)


def test_summary_keeps_unknown_rain_and_days_unknown():
    # Meteostat: no rain values at all and no complete rain day
    rows = [_month("HAN", 2024, m, None, None, complete=0) for m in (1, 2)]
    s = dq.summary(_frame(rows), n_locations=1, hours_per_location=1440.0)
    assert math.isnan(s["rain_mm"]) and math.isnan(s["rainy_days"])
    assert s["rain_coverage"] == 0.0 and s["hot_days"] == 0.0

    # A location without rows still counts in the coverage denominator
    s8 = dq.summary(_frame([_month("HAN", 2024, 1, 10.0, 1)]), 8, 720.0)
    assert s8["rain_coverage"] == pytest.approx(1 / 8)


def test_day_counts_are_zero_inside_the_completeness_guard():
    # Complete days but no hot day: 0, not unknown (March 2024 gave 0.50 per location only
    # because locations without a hot day were averaged in as 0)
    rows = [
        _month("HAN", 2024, 3, 50.0, 5, hot=4),
        _month("LCA", 2024, 3, 60.0, 6, hot=0),
    ]
    s = dq.summary(_frame(rows), n_locations=2, hours_per_location=720.0)
    assert s["hot_days"] == pytest.approx(2.0)


def test_by_location_and_by_month():
    rows = [
        _month("HAN", 2024, 7, 300.0, 20),
        _month("HAN", 2025, 7, 100.0, 10),
        _month("LCA", 2024, 7, 500.0, 25),
    ]
    loc = dq.by_location(_frame(rows), hours_per_location=1440.0).set_index("location_code")
    assert loc.loc["HAN", "rain_mm"] == pytest.approx(200.0)  # mean per year
    assert loc.loc["HAN", "rain_coverage"] == pytest.approx(1.0)
    assert loc.loc["LCA", "rain_coverage"] == pytest.approx(0.5)  # one of two years present
    month = dq.by_month(_frame(rows)).set_index("month")
    assert month.loc[7, "rain_mm"] == pytest.approx(300.0)


def test_month_pivot_row_total_and_location_mean():
    rows = [
        _month("HAN", 2024, 7, 300.0, 20),
        _month("HAN", 2025, 7, 100.0, 10),
        _month("HAN", 2024, 8, 50.0, 5),
        _month("HAN", 2025, 8, 50.0, 5),
        _month("LCA", 2024, 7, 400.0, 25),
        _month("LCA", 2024, 8, 200.0, 15),
    ]
    grid = dq.month_pivot(_frame(rows))
    assert grid.loc["HAN", "T7"] == pytest.approx(200.0)
    assert grid.loc["HAN", "Cả kỳ"] == pytest.approx(250.0)  # mean yearly total
    assert grid.loc["TB các điểm", "T7"] == pytest.approx(300.0)  # mean, not sum
    assert list(grid.columns) == ["T7", "T8", "Cả kỳ"]


def test_season_days_split_by_local_month():
    rows = [
        _month("HAN", 2024, 1, 20.0, 3),
        _month("HAN", 2024, 7, 300.0, 20, hot=5),
    ]
    days = dq.season_days(_frame(rows)).set_index("season")
    assert days.loc["Mùa mưa (5–10)", "rainy_days"] == 20
    assert days.loc["Mùa khô (11–4)", "rainy_days"] == 3
    assert days.loc["Mùa mưa (5–10)", "hot_days"] == 5


def _predictions():
    return pd.DataFrame(
        {
            "location_code": ["HAN", "HAN", "HAN", "LCA", "LCA", "LCA"],
            "month": [1, 1, 7, 7, 7, 7],
            "rain_probability": [0.38, 0.45, 0.90, 0.10, 0.39, 0.70],
            "probability_threshold": [0.40] * 6,
            "predicted_rain": [False, True, True, False, False, True],
            "actual_rain": [True, False, True, None, True, True],
        }
    )


def test_classification_counts_use_stored_classes_at_the_model_threshold():
    df = _predictions()
    stored = dq.classification_counts(df)
    assert stored == {"tp": 2, "fp": 1, "tn": 0, "fn": 2}  # the row without actual is out
    assert dq.classification_counts(df, 0.40) == stored
    lower = dq.classification_counts(df, 0.36)  # reclassifies 0.38 and 0.39 as rain
    assert lower == {"tp": 4, "fp": 1, "tn": 0, "fn": 0}


def test_metrics_undefined_without_a_denominator():
    m = dq.classification_metrics({"tp": 0, "fp": 0, "tn": 5, "fn": 0})
    assert m["n"] == 5 and m["accuracy"] == 1.0
    assert math.isnan(m["precision"]) and math.isnan(m["recall"]) and math.isnan(m["f1"])
    m = dq.classification_metrics({"tp": 27467, "fp": 8569, "tn": 26617, "fn": 7235})
    assert round(m["f1"], 3) == 0.777 and round(m["accuracy"], 3) == 0.774


def test_metrics_by_group_come_from_group_counts():
    by = dq.metrics_by(_predictions(), "location_code").set_index("location_code")
    assert by.loc["HAN", "tp"] == 1 and by.loc["HAN", "fn"] == 1 and by.loc["HAN", "fp"] == 1
    assert by.loc["LCA", "n"] == 2  # the row without an actual is not counted
    assert dq.season_of_month(5) == "Mùa mưa (5–10)" and dq.season_of_month(11) == "Mùa khô (11–4)"


def test_station_summary_counts_rainy_days_and_complete_model_days():
    rows = pd.DataFrame(
        {
            "location_code": ["HAN"] * 3,
            "location_name": ["Hà Nội"] * 3,
            "station_id": ["48825"] * 3,
            "station_name": ["Hà Đông"] * 3,
            "station_role": ["nearest_available"] * 3,
            "station_rain_mm": [5.0, 0.2, 12.0],
            "station_is_rainy": [True, False, True],
            "model_rain_mm": [4.0, np.nan, 10.0],
            "model_is_rainy": [True, None, True],
        }
    )
    s = dq.station_summary(rows).iloc[0]
    assert s["compared_days"] == 3 and s["station_rainy_days"] == 2
    assert s["model_rain_mm"] == pytest.approx(14.0) and s["model_rainy_days"] == 2


def test_coverage_and_missing_percentages():
    cov = pd.DataFrame(
        {
            "expected_hours": [8784],
            "record_hours": [8784],
            "rain_valid_hours": [0],
            "temp_valid_hours": [8767],
        }
    )
    pct = dq.coverage_pct(cov).iloc[0]
    assert pct["record_pct"] == 100.0 and pct["rain_pct"] == 0.0
    assert round(pct["temp_pct"], 2) == 99.81
    counts = pd.DataFrame(
        {"source_name": ["Meteostat"], "rows": [100], **{c: [100] for c in dq.FACT_MEASURES}}
    ).assign(precipitation_mm=0, cloud_cover_percent=17)
    missing = dq.column_missing_pct(counts).iloc[0]
    assert missing["precipitation_mm"] == 100.0 and missing["cloud_cover_percent"] == 83.0
    assert missing["temperature_c"] == 0.0


def test_boundary_lines_cover_polygons_and_multipolygons():
    square = [[[105.0, 21.0], [106.0, 21.0], [106.0, 22.0], [105.0, 21.0]]]
    geo = {
        "features": [
            {"geometry": {"type": "Polygon", "coordinates": square}},
            {"geometry": {"type": "MultiPolygon", "coordinates": [square, square]}},
        ]
    }
    lons, lats = dq.boundary_lines(geo)
    assert lons.count(None) == lats.count(None) == 3  # one break after each ring
    assert len(lons) == 3 * (4 + 1) and lons[0] == 105.0 and lats[2] == 22.0


def test_boundary_key_matches_the_ingestion_script():
    from ingest_boundaries import SILVER_KEY

    assert dq.BOUNDARY_KEY == SILVER_KEY


class _FakeCursor:
    def __init__(self, log):
        self.log, self.description = log, [("level",), ("rain_mm",)]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self):
        pass

    def mogrify(self, sql, params=None):
        return f"{sql} -- {params}".encode()

    def execute(self, sql, params=None):
        self.log.append(sql)

    def fetchall(self):
        return [("year", Decimal("12.5"))]


class _FakeConnection:
    def __init__(self):
        self.log = []

    def cursor(self):
        return _FakeCursor(self.log)


def test_run_and_olap_return_the_sql_that_was_sent():
    conn = _FakeConnection()
    df, sql = dq.run(conn, "SELECT %s", (1,))
    assert sql == "SELECT %s -- (1,)" and df["rain_mm"][0] == 12.5

    df, sql = dq.olap(conn, "cube_rainy_days", "Open-Meteo", (2016, 2025))
    assert "CUBE (location_code, season)" in sql and "('Open-Meteo', 2016, 2025)" in sql
    assert conn.log[-1].strip().startswith("SELECT source_name")


def test_timed_query_only_accepts_the_view_or_the_cube():
    with pytest.raises(KeyError):
        dq.timed_monthly_query(_FakeConnection(), "fact_weather_observation; --", "x", "y", 1)
