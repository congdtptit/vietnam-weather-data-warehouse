"""Unit tests for scripts/evaluate_cart_test.py (workbook ID 29). Synthetic data, mock MinIO."""

import io
import json

import joblib
import pandas as pd
import pytest
from build_rain_features import FEATURES, features_keys
from evaluate_cart_test import evaluate_test, evaluation_keys
from minio.error import S3Error
from sklearn.dummy import DummyClassifier
from test_train_cart import GRID, _client, _split
from train_cart import classification_metrics, make_pipeline, model_keys, train_cart


def _trained_client():
    client, store = _client()
    split_keys, manifest_key = features_keys()
    buf = io.BytesIO()
    _split("test", "2025-01-01", 600, seed=3).to_parquet(buf, index=False)
    store[split_keys["test"]] = buf.getvalue()
    manifest = json.loads(store[manifest_key])
    manifest["splits_utc"]["test"] = ["2025", "2026"]
    store[manifest_key] = json.dumps(manifest).encode()

    def stat(bucket, key):
        if key not in store:
            raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)

    client.stat_object.side_effect = stat
    train_cart(grid=GRID, n_jobs=1, client=client)
    client.get_object.reset_mock()
    return client, store


def _read_keys(client):
    return [c.args[1] for c in client.get_object.call_args_list]


def test_evaluation_uses_frozen_threshold_and_runs_only_once():
    client, store = _trained_client()
    report = json.loads(store[model_keys()[1]])
    evaluation = evaluate_test(client=client)

    eval_key, pred_key = evaluation_keys()
    assert json.loads(store[eval_key]) == evaluation
    assert evaluation["probability_threshold"] == report["probability_threshold"]
    assert evaluation["validation_reproduced"] is True
    assert evaluation["coverage"]["evaluated"] == 2 * 600
    assert evaluation["coverage"]["not_covered_missing_features"] == 0

    preds = pd.read_parquet(io.BytesIO(store[pred_key]))
    recomputed = classification_metrics(
        preds["rain_next24h"], preds["probability"] >= evaluation["probability_threshold"]
    )
    assert recomputed == evaluation["test_metrics_at_threshold"]
    assert (
        preds["predicted_rain"] == (preds["probability"] >= report["probability_threshold"])
    ).all()
    assert set(evaluation["breakdowns"]["by_location"]) == {"HAN", "LCA"}

    with pytest.raises(RuntimeError, match="evaluated only once"):
        evaluate_test(client=client)


def test_swapped_model_is_rejected_before_test_is_read():
    client, store = _trained_client()
    model_key, _ = model_keys()
    # Replace the selected model with one that always predicts Rain
    swapped = make_pipeline(list(FEATURES), {"max_depth": 1})
    swapped.steps[-1] = ("tree", DummyClassifier(strategy="constant", constant=True))
    val = pd.read_parquet(io.BytesIO(store[features_keys()[0]["validation"]]))
    swapped.fit(val[[*FEATURES, "location_code"]], val["rain_next24h"])
    buf = io.BytesIO()
    joblib.dump(swapped, buf)
    store[model_key] = buf.getvalue()

    with pytest.raises(ValueError, match="does not reproduce"):
        evaluate_test(client=client)
    assert features_keys()[0]["test"] not in _read_keys(client)
    assert evaluation_keys()[0] not in store


def test_feature_mismatch_is_rejected():
    client, store = _trained_client()
    _, manifest_key = features_keys()
    manifest = json.loads(store[manifest_key])
    manifest["features"] = manifest["features"][::-1]
    store[manifest_key] = json.dumps(manifest).encode()
    with pytest.raises(ValueError, match="feature manifest does not match"):
        evaluate_test(client=client)
