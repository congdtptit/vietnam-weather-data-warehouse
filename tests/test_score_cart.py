"""Unit tests for scripts/score_cart.py (workbook ID 32). Synthetic Silver, mock MinIO."""

import hashlib
import io
import json
from types import SimpleNamespace
from unittest.mock import Mock

import joblib
import numpy as np
import pandas as pd
import pytest
from build_rain_features import FEATURES, compute_features, features_keys
from evaluate_cart_test import evaluation_keys
from minio.error import S3Error
from score_cart import check_window, prediction_key, score, silver_key
from station_config import CANONICAL_LOCATIONS
from train_cart import make_pipeline, model_keys

TEST_WINDOW = ["2025-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"]
LOCS = sorted(CANONICAL_LOCATIONS)
D1, D2 = pd.Timestamp("2025-01-01").date(), pd.Timestamp("2025-01-02").date()


def _silver(seed=0):
    """Hourly Open-Meteo rows for every location, December 2024 to January 2025."""
    rng = np.random.default_rng(seed)
    ts = pd.date_range("2024-12-01", "2025-01-31 23:00", freq="h", tz="UTC")
    frames = []
    for loc in LOCS:
        n = len(ts)
        frames.append(
            pd.DataFrame(
                {
                    "location_code": loc,
                    "timestamp_utc": ts,
                    "temperature_c": rng.normal(20, 3, n),
                    "dew_point_c": rng.normal(16, 3, n),
                    "humidity_percent": rng.uniform(40, 100, n),
                    "pressure_hpa": rng.normal(1010, 3, n),
                    "cloud_cover_percent": rng.uniform(0, 100, n),
                    "wind_speed_ms": rng.uniform(0, 8, n),
                    "precipitation_mm": rng.choice([0.0, 0.0, 0.3, 2.0], n),
                    "source_name": "Open-Meteo",
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


def _parquet(df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _put_silver(store, silver):
    for (loc, year, month), g in silver.groupby(
        [silver["location_code"], silver["timestamp_utc"].dt.year, silver["timestamp_utc"].dt.month]
    ):
        store[silver_key(loc, year, month)] = _parquet(g)


def _store(silver=None):
    silver = _silver() if silver is None else silver
    store = {}
    _put_silver(store, silver)
    feats = compute_features(silver.drop(columns="source_name")).dropna()
    model = make_pipeline(list(FEATURES), {"max_depth": 3})
    model.fit(feats[[*FEATURES, "location_code"]], feats["humidity_percent"] > 75)
    buf = io.BytesIO()
    joblib.dump(model, buf)
    model_key, report_key = model_keys()
    store[model_key] = buf.getvalue()
    store[report_key] = json.dumps(
        {
            "model_name": "DecisionTree_CART",
            "model_version": "v1",
            "feature_version": "v1",
            "feature_source": "Open-Meteo",
            "probability_threshold": 0.4,
            "features": [{"name": n} for n in FEATURES],
            "availability": "retrospective",
        }
    ).encode()
    store[evaluation_keys()[0]] = json.dumps(
        {"model_sha256": hashlib.sha256(store[model_key]).hexdigest()}
    ).encode()
    store[features_keys("v1")[1]] = json.dumps({"splits_utc": {"test": TEST_WINDOW}}).encode()
    return store, model


def _client(store):
    def missing():
        return S3Error("NoSuchKey", "missing", "/r", "req", "host", None)

    def get(bucket, key):
        if key not in store:
            raise missing()
        body = store[key]
        return SimpleNamespace(read=lambda: body, close=lambda: None, release_conn=lambda: None)

    def put(bucket, key, data, length, content_type=None):
        store[key] = data.read()

    def stat(bucket, key):
        if key not in store:
            raise missing()

    client = Mock()
    client.get_object.side_effect = get
    client.put_object.side_effect = put
    client.stat_object.side_effect = stat
    return client


def _read(store, day):
    return pd.read_parquet(io.BytesIO(store[prediction_key(day)]))


def test_scores_every_origin_and_rewrites_nothing_on_rerun():
    store, model = _store()
    client = _client(store)
    result = score(D1, D2, client=client)
    assert result["origins_in_window"] == result["scored"] == 2 * 24 * len(LOCS)
    assert result["objects"] == {"written": 2, "replaced": 0, "unchanged": 0}

    day = _read(store, D1)
    assert len(day) == 24 * len(LOCS) and set(day["evaluation_split"]) == {"test"}
    assert (day["target_end_utc"] - day["origin_utc"] == pd.Timedelta(hours=24)).all()
    assert (day["predicted_rain"] == (day["probability"] >= 0.4)).all()
    # Same probabilities as the model applied to features of the full Silver series
    silver = _silver().drop(columns="source_name")
    full = compute_features(silver).merge(day[["location_code", "origin_utc"]])
    full = full.sort_values(["origin_utc", "location_code"]).reset_index(drop=True)
    expected = model.predict_proba(full[[*FEATURES, "location_code"]])[:, 1]
    assert np.array_equal(day["probability"].to_numpy(), expected)

    again = score(D1, D2, client=client)
    assert again["objects"] == {"written": 0, "replaced": 0, "unchanged": 2}


def test_features_at_an_origin_ignore_later_values():
    store, _ = _store()
    client = _client(store)
    score(D1, D1, client=client)
    before = _read(store, D1)

    silver = _silver()
    later = silver["timestamp_utc"] > pd.Timestamp("2025-01-01 12:00", tz="UTC")
    silver.loc[later, "humidity_percent"] = 99.0
    silver.loc[later, "precipitation_mm"] = 30.0
    _put_silver(store, silver)
    result = score(D1, D1, client=client)
    after = _read(store, D1)

    early = before["origin_utc"] <= pd.Timestamp("2025-01-01 12:00", tz="UTC")
    pd.testing.assert_frame_equal(before[early], after[early])
    assert result["objects"]["replaced"] == 1  # later origins see the new values


def test_missing_hour_fails_unless_allowed():
    silver = _silver()
    gap = (silver["location_code"] == "HAN") & (
        silver["timestamp_utc"] == pd.Timestamp("2025-01-01 10:00", tz="UTC")
    )
    store, _ = _store(silver[~gap])
    client = _client(store)
    with pytest.raises(RuntimeError, match="missing features"):
        score(D1, D2, client=client)
    assert prediction_key(D1) not in store

    result = score(D1, D2, allow_missing=True, client=client)
    missing = result["not_scored_missing_features"]
    # HAN origins whose 24 h trailing windows include 10:00 (10:00 .. 09:00 next day), plus
    # 10:00 next day, whose 24 h pressure change needs the missing hour
    assert missing["total"] == 25 and missing["by_location"] == {"HAN": 25}
    assert missing["origins_without_silver_row"] == 1
    assert result["scored"] == 2 * 24 * len(LOCS) - 25


def test_missing_silver_object_fails():
    store, _ = _store()
    del store[silver_key("LCA", 2024, 12)]  # the history before the window
    with pytest.raises(RuntimeError, match="Silver Open-Meteo objects missing"):
        score(D1, D1, client=_client(store))


def test_changed_artifact_is_refused_before_scoring():
    store, _ = _store()
    store[evaluation_keys()[0]] = json.dumps({"model_sha256": "0" * 64}).encode()
    with pytest.raises(ValueError, match="SHA-256"):
        score(D1, D1, client=_client(store))
    assert prediction_key(D1) not in store


def test_day_of_another_model_version_is_not_overwritten():
    store, _ = _store()
    client = _client(store)
    score(D1, D1, client=client)
    other = _read(store, D1).assign(model_version="v0")
    store[prediction_key(D1)] = _parquet(other)
    with pytest.raises(RuntimeError, match="another model version"):
        score(D1, D1, client=client)


@pytest.mark.parametrize(
    "start, end, message",
    [
        ("2024-12-31", "2025-01-01", "outside the test split"),
        ("2025-12-01", "2025-12-31", "outside the 2016-2025 scope"),
        ("2025-03-02", "2025-03-01", "reversed"),
    ],
)
def test_windows_outside_the_published_origins_are_refused(start, end, message):
    with pytest.raises(ValueError, match=message):
        check_window(pd.Timestamp(start).date(), pd.Timestamp(end).date(), TEST_WINDOW)


def test_last_origin_day_is_accepted():
    check_window(pd.Timestamp("2025-12-30").date(), pd.Timestamp("2025-12-30").date(), TEST_WINDOW)
