"""Unit tests for scripts/build_rain_features.py (workbook ID 23). Synthetic hours, mock MinIO."""

import io
import json
import re
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
from build_rain_features import (
    FEATURES,
    build_feature_dataset,
    compute_features,
    export_features,
    features_keys,
)
from minio.error import S3Error
from rain_labels import assign_splits, build_rain_labels


def _hourly(loc="HAN", start="2023-12-30", hours=24 * 4, offset=0.0):
    ts = pd.date_range(start, periods=hours, freq="h", tz="UTC")
    i = np.arange(hours, dtype=float)
    return pd.DataFrame(
        {
            "location_code": loc,
            "timestamp_utc": ts,
            "temperature_c": 20.0 + offset + i / 10,
            "dew_point_c": 15.0 + offset,
            "humidity_percent": 50.0 + offset + i,
            "pressure_hpa": 1000.0 + offset + i / 100,
            "cloud_cover_percent": offset + i % 100,
            "wind_speed_ms": 2.0,
            "precipitation_mm": 0.5 * (i % 2) + offset,  # 0.5 mm on odd hours
        }
    )


def _feature_at(feats, loc, ts):
    row = feats[(feats["location_code"] == loc) & (feats["origin_utc"] == pd.Timestamp(ts))]
    assert len(row) == 1
    return row.iloc[0]


def test_features_use_only_past_and_present_hours():
    hourly = _hourly()
    t = pd.Timestamp("2023-12-31 06:00", tz="UTC")  # hour index 30
    row = _feature_at(compute_features(hourly), "HAN", t)
    assert row["humidity_percent"] == 80.0
    assert row["humidity_percent_lag3h"] == 77.0 and row["humidity_percent_lag6h"] == 74.0
    assert row["rain_past_3h_mm"] == pytest.approx(0.5)  # hours 28, 29, 30 -> 0 + 0.5 + 0
    assert row["rain_past_24h_mm"] == pytest.approx(6.0)
    assert row["humidity_mean_6h_percent"] == pytest.approx(77.5)  # hours 25..30
    assert row["pressure_change_3h_hpa"] == pytest.approx(0.03)
    assert row["pressure_change_24h_hpa"] == pytest.approx(0.24)
    assert row["dew_point_depression_c"] == pytest.approx(23.0 - 15.0)

    # Changing every value after t must not change any feature at t
    later = hourly.copy()
    after = later["timestamp_utc"] > t
    later.loc[after, later.columns[2:]] = 999.0
    row_later = _feature_at(compute_features(later), "HAN", t)
    assert row_later[list(FEATURES)].equals(row[list(FEATURES)])


def test_incomplete_history_and_gaps_give_missing_not_filled_values():
    hourly = _hourly().drop(index=40)  # 2023-12-31 16:00 is absent
    feats = compute_features(hourly)

    first = _feature_at(feats, "HAN", "2023-12-30 00:00+00:00")
    assert np.isnan(first["humidity_percent_lag3h"]) and np.isnan(first["rain_past_3h_mm"])

    # The grid keeps the missing hour as a row of NaN
    gap = _feature_at(feats, "HAN", "2023-12-31 16:00+00:00")
    assert np.isnan(gap["humidity_percent"])

    t = _feature_at(feats, "HAN", "2023-12-31 18:00+00:00")  # 3h window includes hour 40
    assert np.isnan(t["rain_past_3h_mm"]) and np.isnan(t["humidity_mean_6h_percent"])
    assert t["humidity_percent_lag3h"] == 50.0 + 39  # lag 3h points at hour 39, present
    assert np.isnan(_feature_at(feats, "HAN", "2023-12-31 19:00+00:00")["humidity_percent_lag3h"])


def test_lags_never_cross_locations():
    hourly = pd.concat([_hourly("HAN"), _hourly("LCA", offset=500.0)], ignore_index=True)
    feats = compute_features(hourly)
    lca_first = feats[feats["location_code"] == "LCA"].sort_values("origin_utc").iloc[:3]
    assert lca_first["humidity_percent_lag3h"].isna().all()
    lca = _feature_at(feats, "LCA", "2023-12-31 06:00+00:00")
    assert lca["humidity_percent_lag3h"] == 500.0 + 50.0 + 27


def test_calendar_uses_vietnam_local_time():
    feats = compute_features(_hourly())
    # 2023-12-31 18:00 UTC is 2024-01-01 01:00 in Vietnam
    row = _feature_at(feats, "HAN", "2023-12-31 18:00+00:00")
    assert row["hour_local"] == 1 and row["month_local"] == 1
    assert _feature_at(feats, "HAN", "2023-12-31 16:00+00:00")["month_local"] == 12


def test_dataset_keeps_usable_labels_only():
    hourly = _hourly(hours=24 * 6)
    labels = assign_splits(
        build_rain_labels(hourly[["location_code", "timestamp_utc", "precipitation_mm"]])
    )
    data = build_feature_dataset(labels, hourly)
    assert len(data) == int(labels["exclusion_reason"].isna().sum())
    assert data["rain_next24h"].dtype == bool
    assert "rain_next24h_mm" not in FEATURES  # target value is never a feature
    assert list(data.columns[5 : 5 + len(FEATURES)]) == list(FEATURES)

    with pytest.raises(ValueError, match="rain_next24h_v0.2"):
        build_feature_dataset(labels.assign(label_version="old"), hourly)


# --- export with a mock MinIO -------------------------------------------------------------

SILVER_KEY = re.compile(
    r"weather_hourly/year=(\d{4})/month=(\d{2})/weather_hourly_([A-Z]{3})_\d{6}\.parquet"
)


def _client(store):
    client = Mock()

    def get(bucket, key):
        if bucket == "gold" and key in store:
            body = store[key]
        else:
            m = SILVER_KEY.fullmatch(key)
            if m is None:
                raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)
            year, month, loc = int(m.group(1)), int(m.group(2)), m.group(3)
            start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
            end = start + pd.offsets.MonthBegin(1)
            hours = int((end - start) / pd.Timedelta(hours=1))
            df = _hourly(loc, start=start.isoformat(), hours=hours).assign(source_name="Open-Meteo")
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            body = buf.getvalue()
        return SimpleNamespace(read=lambda: body, close=lambda: None, release_conn=lambda: None)

    def put(bucket, key, data, length, content_type=None):
        store[key] = data.read()

    client.get_object.side_effect = get
    client.put_object.side_effect = put

    def stat(bucket, key):
        if key not in store:
            raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)

    client.stat_object.side_effect = stat
    return client


def test_export_writes_split_files_and_manifest():
    import export_rain_labels as erl

    store: dict[str, bytes] = {}
    client = _client(store)
    erl.export_labels(2024, 2024, client=client)
    manifest = export_features(2024, 2024, client=client)

    split_keys, manifest_key = features_keys()
    assert split_keys["validation"] in store
    assert split_keys["train"] not in store and split_keys["test"] not in store  # out of scope
    val = pd.read_parquet(io.BytesIO(store[split_keys["validation"]]))
    assert len(val) == 8 * (8784 - 24)  # usable validation labels in 2024
    assert set(val["split"]) == {"validation"}

    saved = json.loads(store[manifest_key])
    assert saved == manifest
    assert [f["name"] for f in saved["features"]] == list(FEATURES)
    assert saved["by_split"]["validation"]["rows"] == len(val)
    # Without 2023 history the first 23 origins per location miss the 24 h features
    assert saved["missing_values_per_feature"]["rain_past_24h_mm"] == 8 * 23


def test_trailing_rain_sums_are_free_of_float_error():
    hourly = _hourly().assign(precipitation_mm=0.1)
    row = _feature_at(compute_features(hourly), "HAN", "2023-12-31 06:00+00:00")
    assert (
        row["rain_past_24h_mm"] == 2.4 and row["rain_past_3h_mm"] == 0.3
    )  # exact, not 2.4000000000000004


def test_export_stamps_requested_version_and_refuses_overwrite():
    import export_rain_labels as erl

    store: dict[str, bytes] = {}
    client = _client(store)
    erl.export_labels(2024, 2024, client=client)
    export_features(2024, 2024, version="vtest", client=client)
    split_keys, _ = features_keys("vtest")
    val = pd.read_parquet(io.BytesIO(store[split_keys["validation"]]))
    assert set(val["feature_version"]) == {"vtest"}

    with pytest.raises(RuntimeError, match="already exist"):
        export_features(2024, 2024, version="vtest", client=client)
