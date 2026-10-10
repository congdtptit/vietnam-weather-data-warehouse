"""Behavior of existing ingestion, with API and object storage mocked."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests


def response(status, payload=None):
    return SimpleNamespace(status_code=status, text="synthetic response", json=lambda: payload)


def test_request_uses_utc_and_explicit_units(ingestion, hourly_payload, monkeypatch):
    get = Mock(return_value=response(200, hourly_payload))
    monkeypatch.setattr(ingestion.requests, "get", get)
    result = ingestion.fetch_open_meteo_year(21.0285, 105.8542, 2024)
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
    assert ingestion.fetch_open_meteo_year(21.0, 105.0, 2024, max_retries=2) == hourly_payload
    assert get.call_count == 2


def test_retry_exhaustion_raises(ingestion, monkeypatch):
    get = Mock(return_value=response(503))
    monkeypatch.setattr(ingestion.requests, "get", get)
    with pytest.raises(RuntimeError, match="2"):
        ingestion.fetch_open_meteo_year(21.0, 105.0, 2024, max_retries=2)
    assert get.call_count == 2


def test_location_and_split_limit_uploaded_objects(ingestion, hourly_payload, monkeypatch):
    from minio.error import S3Error

    client = Mock()
    client.stat_object.side_effect = S3Error("NoSuchKey", "missing", "/", "req", "host", None)
    fetch = Mock(return_value=hourly_payload)
    monkeypatch.setattr(ingestion, "get_minio_client", lambda: client)
    monkeypatch.setattr(ingestion, "fetch_open_meteo_year", fetch)
    ingestion.ingest_data(2023, 2025, target_location="han", target_split="validation")
    fetch.assert_called_once_with(lat=21.0285, lon=105.8542, year=2024)
    client.put_object.assert_called_once()
    upload = client.put_object.call_args.kwargs
    assert upload["bucket_name"] == "bronze"
    assert (
        upload["object_name"]
        == "open_meteo/hourly/year=2024/open_meteo_HAN_hourly_20240101_20241231.json"
    )
    data = json.loads(upload["data"].getvalue())
    assert data["hourly"] == hourly_payload["hourly"]
    assert data["_pipeline_metadata"]["location_code"] == "HAN"


def test_existing_object_is_skipped_by_current_size_rule(ingestion, monkeypatch):
    # Current behavior only; size is not evidence of coverage or validity.
    client = Mock()
    client.stat_object.return_value = SimpleNamespace(size=2001)
    fetch = Mock()
    monkeypatch.setattr(ingestion, "get_minio_client", lambda: client)
    monkeypatch.setattr(ingestion, "fetch_open_meteo_year", fetch)
    ingestion.ingest_data(2024, 2024, target_location="HAN")
    fetch.assert_not_called()
    client.put_object.assert_not_called()


def test_unknown_location_is_rejected(ingestion, monkeypatch):
    fetch = Mock()
    monkeypatch.setattr(ingestion, "get_minio_client", Mock())
    monkeypatch.setattr(ingestion, "fetch_open_meteo_year", fetch)
    with pytest.raises(ValueError):
        ingestion.ingest_data(2024, 2024, target_location="UNKNOWN")
    fetch.assert_not_called()
