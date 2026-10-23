"""Meteostat Bronze with manifests (workbook ID 9). Synthetic payloads, mock MinIO, no network."""

import gzip
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import Mock

import ingest_meteostat as ms
import pytest
import transform_meteostat_to_silver as ms_silver
from minio.error import S3Error

CSV = (
    b"year,month,day,hour,temp,temp_source,rhum,rhum_source\n"
    b"2024,5,1,0,24.0,metar,80,metar\n"
    b"2024,5,1,1,24.5,metar,78,metar\n"
)
GZ = gzip.compress(CSV, mtime=0)
DATA, MANIFEST, LEGACY = ms.bronze_keys("48820", 2024)


def _missing():
    return S3Error("NoSuchKey", "missing", "/r", "req", "host", None)


def _client(store):
    def get(bucket, key):
        if key not in store:
            raise _missing()
        body = store[key]
        return SimpleNamespace(read=lambda: body, close=lambda: None, release_conn=lambda: None)

    def put(*args, **kwargs):  # positional (ingester) or keyword (Silver transform) calls
        call = dict(zip(("bucket_name", "object_name", "data", "length"), args)) | kwargs
        store[call["object_name"]] = call["data"].read()

    def stat(bucket, key):
        if key not in store:
            raise _missing()
        return SimpleNamespace(size=len(store[key]))

    client = Mock()
    client.get_object.side_effect = get
    client.put_object.side_effect = put
    client.stat_object.side_effect = stat
    client.remove_object.side_effect = lambda bucket, key: store.pop(key)
    return client


@pytest.fixture
def fetch(monkeypatch):
    calls = []
    response = {"value": (200, GZ, "https://data.meteostat.net/hourly/2024/48820.csv.gz", "x")}

    def fake(station_id, year):
        calls.append((station_id, year))
        return response["value"]

    monkeypatch.setattr(ms, "fetch_meteostat_station_year", fake)
    monkeypatch.setattr(ms.time, "sleep", lambda s: None)
    return SimpleNamespace(calls=calls, response=response)


def test_stores_received_bytes_with_manifest_and_skips_on_rerun(fetch):
    store = {}
    client = _client(store)
    assert ms.ingest_station_year(client, "48820", 2024) == "stored"
    assert store[DATA] == GZ  # received bytes, not decompressed
    manifest = json.loads(store[MANIFEST])
    assert manifest["sha256"] == hashlib.sha256(GZ).hexdigest()
    assert manifest["csv_sha256"] == hashlib.sha256(CSV).hexdigest()
    assert manifest["payload_key"] == DATA and manifest["http_status"] == 200

    assert ms.ingest_station_year(client, "48820", 2024) == "skipped"
    assert len(fetch.calls) == 1  # the rerun made no request


def test_missing_station_year_is_recorded_and_not_requested_again(fetch):
    fetch.response["value"] = (
        404,
        None,
        "https://data.meteostat.net/hourly/2024/48820.csv.gz",
        None,
    )
    store = {}
    client = _client(store)
    assert ms.ingest_station_year(client, "48820", 2024) == "not_found"
    assert json.loads(store[MANIFEST])["http_status"] == 404 and DATA not in store
    assert ms.ingest_station_year(client, "48820", 2024) == "not_found_known"
    assert len(fetch.calls) == 1
    assert ms.ingest_station_year(client, "48820", 2024, recheck_missing=True) == "not_found"
    assert len(fetch.calls) == 2


def test_identical_legacy_csv_is_replaced_by_the_received_file(fetch):
    store = {LEGACY: CSV}
    client = _client(store)
    assert ms.ingest_station_year(client, "48820", 2024) == "replaced_legacy"
    assert LEGACY not in store and store[DATA] == GZ and MANIFEST in store


@pytest.mark.parametrize(
    "response, message",
    [
        ((200, gzip.compress(CSV + b"2024,5,1,2,25.0,metar,75,metar\n"), "u", None), "differs"),
        ((404, None, "u", None), "no longer has"),
    ],
)
def test_legacy_csv_is_kept_when_upstream_changed(fetch, response, message):
    fetch.response["value"] = response
    store = {LEGACY: CSV}
    client = _client(store)
    with pytest.raises(RuntimeError, match=message):
        ms.ingest_station_year(client, "48820", 2024)
    assert store == {LEGACY: CSV}  # nothing written, nothing removed


def test_batch_reports_known_404_without_failing(fetch, monkeypatch):
    fetch.response["value"] = (404, None, "u", None)
    store = {}
    monkeypatch.setattr(ms, "get_minio_client", lambda: _client(store))
    first = ms.ingest_meteostat_data(2024, 2025, ["48820"])
    again = ms.ingest_meteostat_data(2024, 2025, ["48820"])
    assert first["not_found_count"] == 2 and first["not_found_known_count"] == 0
    assert again["not_found_known_count"] == 2 and again["failure_count"] == 0
    assert len(fetch.calls) == 2


# --- Silver reads Bronze through the manifest ------------------------------------------


def test_silver_reads_the_verified_csv(fetch):
    store = {}
    client = _client(store)
    ms.ingest_station_year(client, "48820", 2024)
    assert ms_silver.read_bronze_csv(client, "48820", 2024) == CSV


def test_silver_rejects_a_changed_object_or_a_missing_manifest(fetch):
    store = {}
    client = _client(store)
    ms.ingest_station_year(client, "48820", 2024)
    store[DATA] = gzip.compress(CSV.replace(b"24.5", b"34.5"))
    with pytest.raises(ValueError, match="SHA-256"):
        ms_silver.read_bronze_csv(client, "48820", 2024)
    del store[MANIFEST]
    with pytest.raises(RuntimeError, match="missing"):
        ms_silver.read_bronze_csv(client, "48820", 2024)


def test_silver_skips_a_station_year_recorded_as_404(fetch, monkeypatch):
    fetch.response["value"] = (404, None, "u", None)
    store = {}
    client = _client(store)
    ms.ingest_station_year(client, "48820", 2024)
    assert ms_silver.read_bronze_csv(client, "48820", 2024) is None
    monkeypatch.setattr(ms_silver, "get_minio_client", lambda: client)
    dq = ms_silver.process_meteostat_to_silver(2024, 2024, ["48820"], overwrite=True)
    assert dq["failures"] == [] and dq["total_files_transformed"] == 0
