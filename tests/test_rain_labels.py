"""Unit tests for RainNext24h labels and split purging (docs/rain-label.md v0.2).

Synthetic hourly sequences cover rain at t, t+1h, t+24h, t+25h, missing hours, the end of
series, and split boundaries.
"""

import pandas as pd
import pytest
from rain_labels import assign_splits, build_rain_labels

T0 = pd.Timestamp("2024-03-01 00:00", tz="UTC")


def _series(hours: int, rain_at: dict[int, float] | None = None, start=T0, loc="HAN"):
    rain_at = rain_at or {}
    ts = pd.date_range(start, periods=hours, freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "location_code": loc,
            "timestamp_utc": ts,
            "precipitation_mm": [rain_at.get(i, 0.0) for i in range(hours)],
        }
    )


def _label_at(labels, origin, loc="HAN"):
    row = labels[(labels["location_code"] == loc) & (labels["origin_utc"] == origin)]
    assert len(row) == 1
    return row.iloc[0]


def test_window_is_t_plus_1_to_t_plus_24():
    """Rain at t and t+25h is outside (t, t+24h]; rain at t+1h and t+24h is inside."""
    hours = 60
    for offset, expected in [(0, False), (1, True), (24, True), (25, False)]:
        labels = build_rain_labels(_series(hours, {offset: 2.0}))
        row = _label_at(labels, T0)
        assert row["valid_hours"] == 24
        assert bool(row["rain_next24h"]) is expected, offset


def test_threshold_is_inclusive_and_summed_over_window():
    labels = build_rain_labels(_series(48, {3: 0.4, 10: 0.6}), threshold_mm=1.0)
    row = _label_at(labels, T0)
    assert row["rain_next24h_mm"] == pytest.approx(1.0)
    assert bool(row["rain_next24h"]) is True

    labels = build_rain_labels(_series(48, {3: 0.4, 10: 0.5}), threshold_mm=1.0)
    assert bool(_label_at(labels, T0)["rain_next24h"]) is False


def test_missing_hour_makes_label_unavailable_not_no():
    """An absent row or a null value inside the window yields no label, never No."""
    df = _series(60)
    absent = df.drop(index=5)  # hour t+5 absent entirely
    labels = build_rain_labels(absent)
    row = _label_at(labels, T0)
    assert row["valid_hours"] == 23
    assert pd.isna(row["rain_next24h"])

    null_value = _series(60)
    null_value.loc[30, "precipitation_mm"] = None
    labels = build_rain_labels(null_value)
    assert pd.isna(_label_at(labels, T0 + pd.Timedelta(hours=10))["rain_next24h"])
    # Origins whose window ends before the gap are unaffected
    assert not pd.isna(_label_at(labels, T0 + pd.Timedelta(hours=5))["rain_next24h"])


def test_end_of_series_window_shorter_than_24h_is_invalid():
    labels = build_rain_labels(_series(30))
    last_complete = T0 + pd.Timedelta(hours=5)
    assert not pd.isna(_label_at(labels, last_complete)["rain_next24h"])
    for h in range(6, 30):
        assert pd.isna(_label_at(labels, T0 + pd.Timedelta(hours=h))["rain_next24h"])


def test_locations_are_labelled_independently():
    df = pd.concat([_series(48, {1: 5.0}, loc="HAN"), _series(48, loc="LCA")], ignore_index=True)
    labels = build_rain_labels(df)
    assert bool(_label_at(labels, T0, "HAN")["rain_next24h"]) is True
    assert bool(_label_at(labels, T0, "LCA")["rain_next24h"]) is False


def test_rejects_naive_timestamps_and_duplicates():
    df = _series(30)
    naive = df.assign(timestamp_utc=df["timestamp_utc"].dt.tz_localize(None))
    with pytest.raises(ValueError, match="timezone-aware"):
        build_rain_labels(naive)
    with pytest.raises(ValueError, match="Duplicate"):
        build_rain_labels(pd.concat([df, df.iloc[[0]]], ignore_index=True))


@pytest.mark.parametrize(
    "boundary, prev_split, next_split",
    [
        (pd.Timestamp("2024-01-01", tz="UTC"), "train", "validation"),
        (pd.Timestamp("2025-01-01", tz="UTC"), "validation", "test"),
    ],
)
def test_split_boundary_purge(boundary, prev_split, next_split):
    """Labels whose target reaches the next split are purged; later origins are kept."""
    start = boundary - pd.Timedelta(hours=48)
    labels = assign_splits(build_rain_labels(_series(120, start=start)))

    def at(hours_before):
        return _label_at(labels, boundary - pd.Timedelta(hours=hours_before))

    kept = at(25)  # target_end = boundary - 1h
    assert kept["split"] == prev_split and pd.isna(kept["exclusion_reason"])
    for h in (24, 12, 1):  # target_end >= boundary
        row = at(h)
        assert row["split"] == prev_split
        assert row["exclusion_reason"] == "boundary_purge"
    first_next = at(0)
    assert first_next["split"] == next_split and pd.isna(first_next["exclusion_reason"])


def test_test_split_end_and_out_of_scope():
    start = pd.Timestamp("2025-12-30 00:00", tz="UTC")
    labels = assign_splits(build_rain_labels(_series(96, start=start)))
    last_test = _label_at(labels, pd.Timestamp("2025-12-30 23:00", tz="UTC"))
    assert last_test["split"] == "test" and pd.isna(last_test["exclusion_reason"])
    purged = _label_at(labels, pd.Timestamp("2025-12-31 00:00", tz="UTC"))
    assert purged["exclusion_reason"] == "boundary_purge"
    outside = _label_at(labels, pd.Timestamp("2026-01-01 00:00", tz="UTC"))
    assert outside["exclusion_reason"] == "out_of_scope"


def test_missing_label_reason_inside_split():
    df = _series(60, start=pd.Timestamp("2024-06-01", tz="UTC")).drop(index=10)
    labels = assign_splits(build_rain_labels(df))
    row = _label_at(labels, pd.Timestamp("2024-06-01", tz="UTC"))
    assert row["split"] == "validation"
    assert row["exclusion_reason"] == "missing_label"
