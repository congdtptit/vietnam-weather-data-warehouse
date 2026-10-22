"""Unit tests for scripts/rain_eda.py (workbook ID 21). Synthetic hourly data only."""

import numpy as np
import pandas as pd
import pytest
from rain_eda import (
    add_origin_variables,
    binned_positive_rate,
    class_balance,
    missingness,
    month_location_matrix,
    strong_pairs,
    variable_correlation,
    variable_separation,
)
from rain_labels import assign_splits, build_rain_labels


def _hourly(loc="HAN", start="2023-12-30", hours=24 * 6, rain_hours=(30, 31), humid_hours=None):
    humid_hours = rain_hours if humid_hours is None else humid_hours
    ts = pd.date_range(start, periods=hours, freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "location_code": loc,
            "timestamp_utc": ts,
            "precipitation_mm": [2.0 if i in rain_hours else 0.0 for i in range(hours)],
            "temperature_c": 20.0 + np.arange(hours) % 24 / 10,
            "dew_point_c": 15.0,
            "humidity_percent": [95.0 if i in humid_hours else 60.0 for i in range(hours)],
            "pressure_hpa": 1010.0 - np.arange(hours) / 100,
            "cloud_cover_percent": 50.0,
            "wind_speed_ms": 2.0,
            "solar_radiation_wm2": 0.0,
        }
    )


def _frame(hourly):
    labels = assign_splits(
        build_rain_labels(hourly[["location_code", "timestamp_utc", "precipitation_mm"]])
    )
    return add_origin_variables(labels, hourly)


def test_origin_variables_use_only_past_and_present_hours():
    hourly = _hourly()
    df = _frame(hourly).set_index("origin_utc")
    t = pd.Timestamp("2023-12-31 07:00", tz="UTC")  # hour index 31
    assert df.loc[t, "rain_past_24h_mm"] == pytest.approx(4.0)  # hours 30 and 31, both <= t
    assert df.loc[t, "precipitation_mm"] == 2.0
    assert df.loc[t, "dew_point_depression_c"] == pytest.approx(df.loc[t, "temperature_c"] - 15.0)
    assert df.loc[t, "pressure_change_24h_hpa"] == pytest.approx(-0.24)
    # The first 23 origins have no complete trailing window
    assert df["rain_past_24h_mm"].iloc[:23].isna().all()

    # Changing data after t must not change anything observed at t
    later = hourly.copy()
    later.loc[later["timestamp_utc"] > t, ["precipitation_mm", "pressure_hpa"]] = 99.0
    df2 = _frame(later).set_index("origin_utc")
    cols = [
        "rain_past_24h_mm",
        "pressure_change_24h_hpa",
        "precipitation_mm",
        "dew_point_depression_c",
    ]
    assert df2.loc[t, cols].equals(df.loc[t, cols])


def test_gap_in_hourly_data_makes_trailing_window_missing():
    hourly = _hourly().drop(index=40)
    df = _frame(hourly).set_index("origin_utc")
    t = pd.Timestamp("2023-12-31 20:00", tz="UTC")  # window includes the removed hour 40
    assert np.isnan(df.loc[t, "rain_past_24h_mm"])


def test_summaries_respect_splits_and_report_missingness():
    # Rain on 2023-12-30 10:00-11:00, humid in the hours before it; train origins end at
    # 2023-12-30 23:00 because later targets reach the 2024 split boundary.
    hourly = pd.concat(
        [
            _hourly("HAN", rain_hours=(10, 11), humid_hours=range(4, 11)),
            _hourly("LCA", rain_hours=()),
        ],
        ignore_index=True,
    )
    df = _frame(hourly)
    balance = class_balance(df)
    assert list(balance.index) == ["train", "validation", "test"]
    assert balance.loc["train", "rows"] > 0 and pd.isna(balance.loc["test", "rows"])

    matrix = month_location_matrix(df)  # train only: December 2023
    assert list(matrix.columns) == [12] and matrix.loc["LCA", 12] == 0.0

    sep = variable_separation(df, ["humidity_percent", "cloud_cover_percent"])
    assert sep.index[0] == "humidity_percent" and sep.loc["humidity_percent", "smd"] > 0
    assert np.isnan(sep.loc["cloud_cover_percent", "smd"])  # constant: no separation

    rates = binned_positive_rate(df, "humidity_percent", [0, 80, 101])
    assert len(rates) == 2

    miss = missingness(df, ["rain_past_24h_mm", "humidity_percent"])
    assert miss["humidity_percent"] == 0.0 and miss["rain_past_24h_mm"] > 0


def test_duplicate_hours_are_rejected():
    hourly = _hourly()
    with pytest.raises(ValueError, match="duplicate"):
        add_origin_variables(pd.DataFrame(), pd.concat([hourly, hourly.iloc[[0]]]))


def test_month_matrix_uses_vietnam_local_month():
    # Origins from 2023-11-30 17:00 UTC are already December in Vietnam (UTC+7)
    df = _frame(_hourly("HAN", start="2023-11-29", hours=24 * 3, rain_hours=()))
    u = df[df["exclusion_reason"].isna()]
    late = u["origin_utc"] >= pd.Timestamp("2023-11-30 17:00", tz="UTC")
    assert late.any() and (~late).any()
    assert list(month_location_matrix(df).columns) == [11, 12]


def test_correlation_is_computed_on_train_and_flags_strong_pairs():
    df = _frame(_hourly("HAN", rain_hours=(10, 11), humid_hours=range(4, 11)))
    cols = ["temperature_c", "pressure_hpa", "humidity_percent"]
    corr = variable_correlation(df, cols)
    assert list(corr.columns) == cols and np.allclose(np.diag(corr), 1.0)
    pairs = strong_pairs(corr, 0.8)
    assert set(pairs.columns) == {"variable_a", "variable_b", "correlation"}
    assert (pairs["correlation"].abs() >= 0.8).all()
