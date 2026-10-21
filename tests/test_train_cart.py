"""Unit tests for scripts/train_cart.py (workbook ID 28). Synthetic features, mock MinIO."""

import io
import json
from types import SimpleNamespace
from unittest.mock import Mock

import joblib
import numpy as np
import pandas as pd
import pytest
from build_rain_features import FEATURES, features_keys
from minio.error import S3Error
from train_cart import (
    choose_threshold,
    classification_metrics,
    model_keys,
    read_split,
    select_config,
    train_cart,
)

LOCS = ["HAN", "LCA"]


def _split(split, start, hours, seed):
    """Rain whenever humidity at t is above 80 %, with 5 % label noise."""
    rng = np.random.default_rng(seed)
    rows = []
    for loc in LOCS:
        ts = pd.date_range(start, periods=hours, freq="h", tz="UTC")
        df = pd.DataFrame({name: rng.normal(size=hours) for name in FEATURES})
        df["humidity_percent"] = rng.uniform(40, 100, hours)
        df["rain_past_24h_mm"] = rng.uniform(0, 5, hours)
        df["month_local"] = ts.month
        y = (df["humidity_percent"] > 80) ^ (rng.uniform(size=hours) < 0.05)
        df.insert(0, "location_code", loc)
        df.insert(1, "origin_utc", ts)
        df["split"] = split
        df["rain_next24h"] = y.to_numpy()
        df["rain_next24h_mm"] = np.where(y, 3.0, 0.0)
        df["threshold_mm"] = 1.0
        rows.append(df)
    return pd.concat(rows, ignore_index=True)


def _manifest():
    return {
        "label_version": "rain_next24h_v0.2",
        "feature_source": "Open-Meteo",
        "availability": "retrospective",
        "features": [{"name": n, "unit": u} for n, u in FEATURES.items()],
        "categorical_features": [{"name": "location_code", "note": ""}],
        "splits_utc": {"train": ["2016", "2024"], "validation": ["2024", "2025"]},
    }


def _client():
    split_keys, manifest_key = features_keys()
    train = _split("train", "2023-01-01", 2000, seed=1)
    train.loc[0, "rain_past_3h_mm"] = np.nan  # one row with a missing feature
    store = {manifest_key: json.dumps(_manifest()).encode()}
    for split, df in (("train", train), ("validation", _split("validation", "2024-01-01", 800, 2))):
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        store[split_keys[split]] = buf.getvalue()
    client = Mock()

    def get(bucket, key):
        if key not in store:
            raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)
        body = store[key]
        return SimpleNamespace(read=lambda: body, close=lambda: None, release_conn=lambda: None)

    def put(bucket, key, data, length, content_type=None):
        store[key] = data.read()

    client.get_object.side_effect = get
    client.put_object.side_effect = put
    return client, store


GRID = {"max_depth": [2, 4], "min_samples_leaf": [5], "class_weight": [None], "ccp_alpha": [0.0]}


def test_metrics_report_undefined_values_as_none():
    m = classification_metrics([True, False, True], [True, True, False])
    assert (m["tp"], m["fp"], m["tn"], m["fn"]) == (1, 1, 0, 1)
    assert m["precision"] == 0.5 and m["recall"] == 0.5 and m["f1"] == 0.5
    always_no = classification_metrics([True, False], [False, False])
    assert always_no["precision"] is None and always_no["recall"] == 0.0
    assert always_no["f1"] is None


def test_selection_prefers_shallow_tree_within_tolerance_and_threshold_near_half():
    results = [
        {"f1": 0.800, "depth": 12, "leaves": 900},
        {"f1": 0.797, "depth": 6, "leaves": 60},
        {"f1": 0.797, "depth": 6, "leaves": 40},
        {"f1": 0.700, "depth": 2, "leaves": 4},
    ]
    assert select_config(results) == results[2]

    y = np.array([True, False])
    proba = np.array([0.9, 0.1])  # every threshold in the scan gives the same F1
    threshold, scan = choose_threshold(y, proba)
    assert threshold == 0.5 and len(scan) == 21


def test_test_split_is_never_read():
    client, _ = _client()
    with pytest.raises(ValueError, match="must not be read"):
        read_split(client, "test")

    train_cart(grid=GRID, n_jobs=1, client=client)
    read_keys = [c.args[1] for c in client.get_object.call_args_list]
    assert features_keys()[0]["test"] not in read_keys


def test_training_is_reproducible_and_the_saved_model_reloads():
    client, store = _client()
    report = train_cart(grid=GRID, n_jobs=1, client=client)
    model_key, report_key = model_keys()

    assert json.loads(store[report_key]) == report
    assert report["test_split_read"] is False
    assert report["rows"]["train_dropped_missing_features"] == 1
    assert report["selected_params"]["max_depth"] in (2, 4)
    assert report["validation_metrics_at_threshold"]["recall"] > 0.8  # learnable rule
    assert report["feature_importances"][0]["feature"] == "humidity_percent"
    assert set(report["validation_baselines"]) == {
        "always_no",
        "always_yes",
        "persistence_rain_past_24h",
        "climatology_location_month",
    }

    model = joblib.load(io.BytesIO(store[model_key]))
    val = pd.read_parquet(io.BytesIO(store[features_keys()[0]["validation"]]))
    x = val[[*FEATURES, "location_code"]]
    first = model.predict_proba(x)[:, 1]

    client2, store2 = _client()
    train_cart(grid=GRID, n_jobs=1, client=client2)
    again = joblib.load(io.BytesIO(store2[model_key])).predict_proba(x)[:, 1]
    np.testing.assert_array_equal(first, again)
