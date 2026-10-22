"""Behavior of existing ingestion, with API and object storage mocked."""

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
import requests


def response(status, payload=None):
    content = json.dumps(payload).encode() if payload is not None else b""
    return SimpleNamespace(status_code=status, text="synthetic response", content=content)


def _year_payload(year=2024):
    """A complete synthetic one-year response (every UTC hour)."""
    hours = pd.date_range(f"{year}-01-01", f"{year}-12-31 23:00", freq="h")
    return {
        "latitude": 21.0,
        "longitude": 105.875,
        "elevation": 19.0,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "generationtime_ms": 1.5,
        "hourly_units": {"time": "iso8601", "precipitation": "mm"},
        "hourly": {
            "time": [h.strftime("%Y-%m-%dT%H:%M") for h in hours],
            "precipitation": [0.0] * len(hours),
        },
    }


def _store_client(store):
    """Mock MinIO backed by a dict of key -> bytes."""
    from minio.error import S3Error

    client = Mock()

    def get(bucket, key):
        if key not in store:
            raise S3Error("NoSuchKey", "missing", "/", "req", "host", None)
        body = store[key]
        return SimpleNamespace(read=lambda: body, close=lambda: None, release_conn=lambda: None)

    def put(bucket_name, object_name, data, length, content_type=None):
        store[object_name] = data.read()

    client.get_object.side_effect = get
    client.put_object.side_effect = put
    return client


def test_request_uses_utc_and_explicit_units(ingestion, hourly_payload, monkeypatch):
    get = Mock(return_value=response(200, hourly_payload))
    monkeypatch.setattr(ingestion.requests, "get", get)
    result = json.loads(ingestion.fetch_open_meteo_year(21.0285, 105.8542, 2024))
    params = get.call_args.kwargs["params"]
    assert params["timezone"] == "UTC"
    assert params["wind_speed_unit"] == "ms"
    assert params["precipitation_unit"] == "mm"
    assert (params["start_date"], params["end_date"]) == ("2024-01-01", "2024-12-31")
    assert get.call_args.kwargs["timeout"] == 30
    # Preserve missing precipitation in the received payload.
    assert result["hourly"]["precipitation"] == [0.0, None, 1.2]


@pytest.mark.parametrize("first", [response(429), requests.Timeout("synthetic timeout")])
def test_transient_failure_is_retried(ingestion, hourly_payload, monkeypatch, first):
    get = Mock(side_effect=[first, response(200, hourly_payload)])
    monkeypatch.setattr(ingestion.requests, "get", get)
    raw = ingestion.fetch_open_meteo_year(21.0, 105.0, 2024, max_retries=2)
    assert json.loads(raw) == hourly_payload
    assert get.call_count == 2


def test_retry_exhaustion_raises(ingestion, monkeypatch):
    get = Mock(return_value=response(503))
    monkeypatch.setattr(ingestion.requests, "get", get)
    with pytest.raises(RuntimeError, match="2"):
        ingestion.fetch_open_meteo_year(21.0, 105.0, 2024, max_retries=2)
    assert get.call_count == 2


def test_bronze_keeps_received_bytes_and_writes_a_manifest(ingestion, monkeypatch):
    raw = json.dumps(_year_payload(2024)).encode()
    store = {}
    fetch = Mock(return_value=raw)
    monkeypatch.setattr(ingestion, "get_minio_client", lambda: _store_client(store))
    monkeypatch.setattr(ingestion, "fetch_open_meteo_year", fetch)
    ingestion.ingest_data(2023, 2025, target_location="han", target_split="validation")
    fetch.assert_called_once_with(lat=21.0285, lon=105.8542, year=2024)
    data_key, manifest_key = ingestion.bronze_keys("HAN", 2024)
    assert data_key == "open_meteo/hourly/year=2024/open_meteo_HAN_hourly_20240101_20241231.json"
    assert store[data_key] == raw  # unchanged bytes
    manifest = json.loads(store[manifest_key])
    assert manifest["sha256"] == hashlib.sha256(raw).hexdigest()
    assert manifest["coverage"]["complete"] is True and manifest["coverage"]["hour_count"] == 8784
    assert manifest["response_metadata"]["utc_offset_seconds"] == 0
    assert manifest["request_params"]["timezone"] == "UTC"

    # A rerun skips by manifest and checksum; a corrupted object is downloaded again
    fetch.reset_mock()
    ingestion.ingest_data(2024, 2024, target_location="HAN")
    fetch.assert_not_called()
    store[data_key] = raw[:-1]
    ingestion.ingest_data(2024, 2024, target_location="HAN")
    fetch.assert_called_once()
    assert store[data_key] == raw


def test_incomplete_response_is_a_failure_and_not_stored(ingestion, hourly_payload, monkeypatch):
    store = {}
    monkeypatch.setattr(ingestion, "get_minio_client", lambda: _store_client(store))
    monkeypatch.setattr(
        ingestion, "fetch_open_meteo_year", Mock(return_value=json.dumps(hourly_payload).encode())
    )
    with pytest.raises(RuntimeError, match="incomplete response coverage"):
        ingestion.ingest_data(2024, 2024, target_location="HAN")
    assert store == {}


def test_legacy_object_is_kept_or_replaced_only_by_identical_data(ingestion, monkeypatch):
    payload = _year_payload(2024)
    legacy = json.dumps({**payload, "_pipeline_metadata": {"year": 2024}}).encode()
    data_key, manifest_key = ingestion.bronze_keys("HAN", 2024)
    store = {data_key: legacy}
    raw = json.dumps({**payload, "generationtime_ms": 9.9}).encode()  # differs only in timing
    fetch = Mock(return_value=raw)
    monkeypatch.setattr(ingestion, "get_minio_client", lambda: _store_client(store))
    monkeypatch.setattr(ingestion, "fetch_open_meteo_year", fetch)

    ingestion.ingest_data(2024, 2024, target_location="HAN")
    fetch.assert_not_called()
    assert store[data_key] == legacy

    changed = dict(payload, hourly=dict(payload["hourly"], precipitation=[0.1] * 8784))
    fetch.return_value = json.dumps(changed).encode()
    with pytest.raises(RuntimeError, match="differ from the legacy"):
        ingestion.ingest_data(2024, 2024, target_location="HAN", replace_legacy=True)
    assert store[data_key] == legacy and manifest_key not in store

    fetch.return_value = raw
    counts = ingestion.ingest_data(2024, 2024, target_location="HAN", replace_legacy=True)
    assert counts["replaced_legacy"] == 1
    assert store[data_key] == raw and manifest_key in store


def test_unknown_location_is_rejected(ingestion, monkeypatch):
    fetch = Mock()
    monkeypatch.setattr(ingestion, "get_minio_client", Mock())
    monkeypatch.setattr(ingestion, "fetch_open_meteo_year", fetch)
    with pytest.raises(ValueError):
        ingestion.ingest_data(2024, 2024, target_location="UNKNOWN")
    fetch.assert_not_called()


def test_failed_file_is_reported_after_the_rest_are_attempted(ingestion, monkeypatch):
    store = {}
    fetch = Mock(
        side_effect=[RuntimeError("synthetic outage"), json.dumps(_year_payload(2025)).encode()]
    )
    monkeypatch.setattr(ingestion, "get_minio_client", lambda: _store_client(store))
    monkeypatch.setattr(ingestion, "fetch_open_meteo_year", fetch)
    with pytest.raises(RuntimeError, match="completed with 1 failure"):
        ingestion.ingest_data(2024, 2025, target_location="HAN")
    assert fetch.call_count == 2
    assert ingestion.bronze_keys("HAN", 2025)[0] in store  # 2025 still stored
    assert ingestion.bronze_keys("HAN", 2024)[0] not in store


def test_client_error_is_not_retried(ingestion, monkeypatch):
    get = Mock(return_value=response(400))
    monkeypatch.setattr(ingestion.requests, "get", get)
    with pytest.raises(RuntimeError, match="HTTP 400"):
        ingestion.fetch_open_meteo_year(21.0, 105.0, 2024, max_retries=3)
    assert get.call_count == 1
