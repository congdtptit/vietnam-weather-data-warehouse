"""Unit tests for scripts/rescore_cart_corrected_labels.py. Synthetic data, mock MinIO."""

import io
import json

import pandas as pd
import pytest
import train_cart as train_cart_module
from build_rain_features import features_keys
from evaluate_cart_test import evaluate_test, evaluation_keys
from rescore_cart_corrected_labels import corrected_evaluation_key, rescore
from test_evaluate_cart_test import _trained_client


@pytest.fixture(autouse=True)
def no_release_pins(monkeypatch):
    """Synthetic models reuse the name v1; the pins of the real release are tested in
    tests/test_model_release.py."""
    monkeypatch.setattr(train_cart_module, "RELEASED_MODELS", {})


def _relabel(store, key, flip_rows):
    """Rewrite a feature file with corrected labels (rain_next24h flipped on some rows)."""
    df = pd.read_parquet(io.BytesIO(store[key]))
    df["label_version"] = "rain_next24h_v0.2.1"
    df.loc[flip_rows, "rain_next24h"] = ~df.loc[flip_rows, "rain_next24h"]
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    store[key] = buf.getvalue()


def _evaluated_client():
    client, store = _trained_client()
    evaluate_test(client=client)
    split_keys, _ = features_keys()
    for split in ("train", "validation", "test"):
        _relabel(store, split_keys[split], flip_rows=[0, 1, 2] if split == "test" else [])
    return client, store


def test_rescore_keeps_predictions_and_reports_corrected_metrics():
    client, store = _evaluated_client()
    result = rescore(feature_version="v2", client=client)
    assert result["test_labels_changed"] == 3
    assert result["model_or_threshold_changed"] is False
    saved = json.loads(store[corrected_evaluation_key()])
    assert saved == result
    original = json.loads(store[evaluation_keys()[0]])
    assert (
        saved["test_metrics_at_threshold_original_labels"] == original["test_metrics_at_threshold"]
    )
    assert saved["test_metrics_at_threshold"] != original["test_metrics_at_threshold"]
    with pytest.raises(RuntimeError, match="already exists"):
        rescore(feature_version="v2", client=client)


def test_rescore_refuses_changed_features_or_old_labels():
    client, store = _evaluated_client()
    key = features_keys()[0]["test"]
    df = pd.read_parquet(io.BytesIO(store[key]))
    df["humidity_percent"] = 0.0  # inputs changed: predictions would differ
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    store[key] = buf.getvalue()
    with pytest.raises(ValueError, match="probabilities changed|classes changed"):
        rescore(feature_version="v2", client=client)

    client, store = _evaluated_client()
    df = pd.read_parquet(io.BytesIO(store[key])).assign(label_version="rain_next24h_v0.2")
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    store[key] = buf.getvalue()
    with pytest.raises(ValueError, match="v0.2.1"):
        rescore(feature_version="v2", client=client)


def test_rescore_refuses_a_model_that_is_not_the_evaluated_one():
    from train_cart import model_keys

    client, store = _evaluated_client()
    model_key, _ = model_keys()
    store[model_key] = store[model_key] + b"\0"  # different bytes, different SHA-256
    with pytest.raises(ValueError, match="differs from the one evaluated"):
        rescore(feature_version="v2", client=client)
    assert corrected_evaluation_key() not in store
