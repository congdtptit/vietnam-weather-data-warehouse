"""Unit tests for scripts/analyze_cart_thresholds.py (workbook ID 36). Synthetic data."""

import numpy as np
import pandas as pd
import pytest
from analyze_cart_thresholds import GRID, errors, f1_rule, recall_rule, scan


def _rows(values):
    return [{"threshold": t, "precision": p, "recall": r, "f1": f} for t, p, r, f in values]


def test_grid_is_the_model_threshold_rule_grid():
    assert GRID[0] == 0.30 and GRID[-1] == 0.70 and len(GRID) == 21
    assert 0.36 in GRID and 0.40 in GRID


def test_f1_rule_takes_the_lowest_threshold_on_a_tie():
    rows = _rows([(0.38, 0.7, 0.9, 0.79), (0.40, 0.75, 0.8, 0.79), (0.42, 0.8, 0.7, 0.75)])
    assert f1_rule(rows) == 0.38


def test_recall_rule_is_the_highest_precision_reaching_the_target():
    rows = _rows([(0.34, 0.70, 0.90, 0.79), (0.36, 0.71, 0.88, 0.79), (0.38, 0.74, 0.84, 0.79)])
    assert recall_rule(rows, 0.85) == 0.36
    assert recall_rule(rows, 0.95) is None


def test_scan_counts_match_the_threshold():
    y = np.array([True, True, False, False])
    p = np.array([0.9, 0.35, 0.37, 0.1])
    at_036, at_040 = scan(y, p, [0.36, 0.40])
    assert (at_036["tp"], at_036["fp"]) == (1, 1) and (at_040["tp"], at_040["fp"]) == (1, 0)
    assert at_036["predicted_rain_share"] == 0.5


def test_error_groups_separate_dry_trace_and_rain_before_the_origin():
    df = pd.DataFrame(
        {
            "location_code": "HAN",
            "month_local": [1, 7, 7, 7, 1, 1],
            "hour_local": [0, 6, 12, 18, 3, 9],
            "rain_past_24h_mm": [0.0, 0.0, 0.4, 5.0, 2.0, 0.0],
            "rain_next24h": [True, True, False, True, False, False],
            "rain_next24h_mm": [1.5, 25.0, 0.6, 8.0, 0.0, 0.2],
            "probability": [0.2, 0.7, 0.6, 0.9, 0.1, 0.45],
        }
    )
    out = errors(df.assign(pred=df["probability"] >= 0.4))
    past = out["by_rain_in_past_24h"]
    assert past["dry past 24 h (0 mm)"]["cases"] == 3  # exactly 0 mm is dry, not trace
    assert past["trace past 24 h (<1 mm)"]["cases"] == 1
    assert past["rain past 24 h (>=1 mm)"]["cases"] == 2
    missed = out["missed_rain_by_amount"]
    assert missed["1-2 mm"] == {"rain_cases": 1, "missed": 1, "missed_share": 1.0}
    assert missed[">=20 mm"]["missed"] == 0
    fa = out["false_alarms_by_observed_rain"]
    assert fa["0.5-1 mm"]["false_alarms"] == 1 and fa["0-0.5 mm"]["false_alarms"] == 1
    assert fa["0 mm"]["false_alarms"] == 0 and out["false_alarms_total"] == 2
    assert sum(c["cases"] for c in out["calibration"]) == 6


@pytest.mark.parametrize("threshold", [0.36, 0.40])
def test_scan_handles_a_split_without_predicted_rain(threshold):
    y = np.array([True, False])
    (row,) = scan(y, np.array([0.1, 0.2]), [threshold])
    assert row["precision"] is None and row["recall"] == 0.0
