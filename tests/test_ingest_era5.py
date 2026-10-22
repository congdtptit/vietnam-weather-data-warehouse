"""Unit tests for scripts/ingest_era5.py (workbook IDs 5, 14). Synthetic NetCDF, mock CDS/MinIO."""

import hashlib
import io
import json
import zipfile
from types import SimpleNamespace
from unittest.mock import Mock

import ingest_era5 as era5
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from minio.error import S3Error


def _client():
    store = {}
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


class FakeCds:
    """Writes a small NetCDF shaped like a CDS download."""

    def __init__(self, hours=None, drop=None, zipped=False):
        self.calls, self.hours, self.drop, self.zipped = [], hours, drop, zipped

    def _write(self, path, request, names):
        n = self.hours or len(request["day"]) * 24
        times = pd.date_range(f"{request['year'][0]}-{request['month'][0]}-01", periods=n, freq="h")
        lat, lon = np.array([23.5, 21.5, 19.5]), np.array([102.0, 104.75, 107.5])
        data = {
            short: (("valid_time", "latitude", "longitude"), np.zeros((n, 3, 3), "float32"))
            for short in names
            if short != self.drop
        }
        ds = xr.Dataset(data, coords={"valid_time": times, "latitude": lat, "longitude": lon})
        if "t2m" in ds:
            ds["t2m"].attrs["units"] = "K"
        ds.to_netcdf(path, engine="netcdf4")

    def retrieve(self, dataset, request):
        self.calls.append((dataset, request))
        url = "https://cds.example/api/retrieve/v1/jobs/req-123/results"
        return SimpleNamespace(url=url, download=lambda target: self._download(request, target))

    def _download(self, request, target):
        names = list(era5.VARIABLES.values())
        if not self.zipped:
            self._write(target, request, names)
        else:  # like the CDS: instantaneous and accumulated variables in separate files
            parts = {
                "data_stream-oper_stepType-instant.nc": [n for n in names if n != "tp"],
                "data_stream-oper_stepType-accum.nc": ["tp"],
            }
            with zipfile.ZipFile(target, "w") as zf:
                for member, member_names in parts.items():
                    member_path = f"{target}.{member}"
                    self._write(member_path, request, member_names)
                    zf.write(member_path, member)
        return target


def test_dry_run_builds_one_request_per_month_without_network(capsys):
    result = era5.ingest_era5(2024, 2024, months=[2, 3], dry_run=True)
    assert result == {"requests": 2, "dry_run": True}
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    feb = lines[0]["request"]
    assert lines[0]["dataset"] == "reanalysis-era5-single-levels"
    assert feb["day"][-1] == "29" and len(feb["time"]) == 24  # leap year
    assert feb["area"] == era5.AREA and feb["data_format"] == "netcdf"
    assert set(feb["variable"]) == set(era5.VARIABLES)


def test_month_is_checked_stored_with_manifest_and_skipped_on_rerun():
    client, store = _client()
    cds = FakeCds()
    era5.ingest_era5(2024, 2024, months=[2], cds_client=cds, client=client)
    data_key, manifest_key = era5.bronze_keys(2024, 2)
    manifest = json.loads(store[manifest_key])
    assert manifest["sha256"] == hashlib.sha256(store[data_key]).hexdigest()
    assert manifest["content"]["format"] == "netcdf" and manifest["object_key"] == data_key
    assert manifest["content"]["members"][0]["time_steps"] == 29 * 24
    assert manifest["request_id"] == "req-123" and manifest["request"]["month"] == ["02"]
    assert manifest["content"]["members"][0]["units"]["t2m"] == "K"

    counts = era5.ingest_era5(2024, 2024, months=[2], cds_client=cds, client=client)
    assert counts == {"stored": 0, "skipped": 1} and len(cds.calls) == 1


@pytest.mark.parametrize(
    "cds, message", [(FakeCds(hours=24), "time steps"), (FakeCds(drop="tp"), "missing")]
)
def test_file_not_matching_the_request_is_a_failure_and_not_stored(cds, message):
    client, store = _client()
    with pytest.raises(RuntimeError, match=f"1 failure.*{message}"):
        era5.ingest_era5(2024, 2024, months=[1], cds_client=cds, client=client)
    assert store == {}


def test_missing_credentials_are_reported(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("CDSAPI_KEY", raising=False)
    client, _ = _client()
    with pytest.raises(RuntimeError, match="no CDS credentials"):
        era5.ingest_era5(2024, 2024, months=[1], client=client)


def test_zip_of_netcdf_files_is_stored_as_received_with_members_listed():
    client, store = _client()
    era5.ingest_era5(2024, 2024, months=[1], cds_client=FakeCds(zipped=True), client=client)
    zip_key, manifest_key = era5.bronze_keys(2024, 1, "zip")
    manifest = json.loads(store[manifest_key])
    assert manifest["object_key"] == zip_key and zipfile.is_zipfile(io.BytesIO(store[zip_key]))
    assert manifest["sha256"] == hashlib.sha256(store[zip_key]).hexdigest()
    members = {m["name"]: m for m in manifest["content"]["members"]}
    assert members["data_stream-oper_stepType-accum.nc"]["variables"] == ["tp"]
    assert all(m["time_steps"] == 31 * 24 for m in members.values())
    assert era5.bronze_keys(2024, 1)[0] not in store  # no .nc object for a zip
    counts = era5.ingest_era5(2024, 2024, months=[1], cds_client=FakeCds(), client=client)
    assert counts == {"stored": 0, "skipped": 1}
