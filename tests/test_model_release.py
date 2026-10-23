"""Release pins and the model release bundle (found by the clean-machine rerun, ID 34)."""

import hashlib
import json

import evaluate_cart_test
import model_release
import pytest
import train_cart
from test_evaluate_cart_test import _trained_client
from test_train_cart import GRID, _client
from train_cart import check_release, model_keys


def _pin(monkeypatch, version, feature_version, sha):
    monkeypatch.setattr(
        train_cart,
        "RELEASED_MODELS",
        {version: {"feature_version": feature_version, "model_sha256": sha}},
    )


def test_released_name_cannot_be_trained_on_other_features(monkeypatch):
    _pin(monkeypatch, "v1", "v1", "0" * 64)
    check_release("v9", "v2")  # not released: no constraint
    client, store = _client()  # synthetic feature files are v2
    before = set(store)
    with pytest.raises(ValueError, match="trained on features v1"):
        train_cart.train_cart(grid=GRID, n_jobs=1, client=client)  # defaults: v2 as "v1"
    assert set(store) == before


def test_released_name_is_written_only_when_the_bytes_reproduce(monkeypatch):
    monkeypatch.setattr(train_cart, "RELEASED_MODELS", {})
    client, store = _client()
    train_cart.train_cart(grid=GRID, n_jobs=1, client=client, version="probe")
    sha = hashlib.sha256(store[model_keys("probe")[0]]).hexdigest()

    _pin(monkeypatch, "v1", "v2", "0" * 64)
    with pytest.raises(ValueError, match="nothing written"):
        train_cart.train_cart(grid=GRID, n_jobs=1, client=client)
    assert model_keys("v1")[0] not in store

    _pin(monkeypatch, "v1", "v2", sha)
    report = train_cart.train_cart(grid=GRID, n_jobs=1, client=client)
    assert report["objects"]["model_sha256"] == sha
    assert report["objects"]["reproduces_released_version"] == "v1"


def test_test_evaluation_refuses_another_model_under_a_released_name(monkeypatch):
    monkeypatch.setattr(train_cart, "RELEASED_MODELS", {})
    client, store = _trained_client()
    _pin(monkeypatch, "v1", "v2", "0" * 64)
    with pytest.raises(ValueError, match="differ from released v1"):
        evaluate_cart_test.evaluate_test(client=client)
    assert evaluate_cart_test.evaluation_keys()[0] not in store


def _release_store(monkeypatch):
    """A Gold store holding every v1 release object, with the model pinned."""
    client, store = _client()
    for i, key in enumerate(model_release.RELEASE_OBJECTS["v1"]):
        store[key] = f"object {i}".encode()
    sha = hashlib.sha256(store[model_keys("v1")[0]]).hexdigest()
    _pin(monkeypatch, "v1", "v1", sha)
    return client, store


def test_export_then_import_into_an_empty_lake(monkeypatch, tmp_path):
    client, store = _release_store(monkeypatch)
    release = model_release.export_release("v1", tmp_path, client=client)
    assert len(release["objects"]) == 12
    assert (
        json.loads((tmp_path / "release.json").read_text())["model_sha256"]
        == release["model_sha256"]
    )

    empty, target = _client()
    target.clear()
    assert model_release.import_release(tmp_path, client=empty) == {
        "uploaded": 12,
        "identical_skipped": 0,
    }
    assert all(target[k] == store[k] for k in model_release.RELEASE_OBJECTS["v1"])
    assert model_release.import_release(tmp_path, client=empty)["identical_skipped"] == 12


def test_import_refuses_a_changed_file_or_a_conflicting_object(monkeypatch, tmp_path):
    client, store = _release_store(monkeypatch)
    model_release.export_release("v1", tmp_path, client=client)

    other, target = _client()
    target.clear()
    key = "ml_models/cart_rain_next24h_v1/report.json"
    target[key] = b"another report"
    with pytest.raises(RuntimeError, match="other bytes"):
        model_release.import_release(tmp_path, client=other)
    assert set(target) == {key}  # nothing uploaded

    (tmp_path / "ml_features/cart_rain_val_2024_v1.parquet").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="does not match release.json"):
        model_release.import_release(tmp_path, client=other)


def test_export_refuses_a_model_that_is_not_the_release(monkeypatch, tmp_path):
    client, store = _release_store(monkeypatch)
    store[model_keys("v1")[0]] = b"retrained"
    with pytest.raises(ValueError, match="differs from the released v1"):
        model_release.export_release("v1", tmp_path, client=client)
