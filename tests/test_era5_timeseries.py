"""Unit tests for ERA5 point time series: Bronze (IDs 5, 14) and Silver (ID 15). Synthetic data."""

import hashlib
import io
import json
import zipfile
from types import SimpleNamespace
from unittest.mock import Mock

import ingest_era5_timeseries as ts
import numpy as np
import pandas as pd
import pytest
import transform_era5_to_silver as silver
import xarray as xr
from minio.error import S3Error


def _client():
    store = {}
    client = Mock()

    def get(bucket, key):
        if (bucket, key) not in store:
            raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)
        body = store[(bucket, key)]
        return SimpleNamespace(read=lambda: body, close=lambda: None, release_conn=lambda: None)

    def put(bucket, key, data, length, content_type=None):
        store[(bucket, key)] = data.read()

    def stat(bucket, key):
        if (bucket, key) not in store:
            raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)

    client.get_object.side_effect = get
    client.put_object.side_effect = put
    client.stat_object.side_effect = stat
    return client, store


VALUES = {  # hour index -> (t2m K, d2m K, tp m, sp Pa, u10, v10, tcc)
    0: (300.0, 300.0, 0.001, 100000.0, 0.0, -5.0, 0.5),  # saturated, wind from the north
    1: (300.0, 300.005, 0.0, 100000.0, -5.0, 0.0, 0.0),  # d2m 0.005 K above: saturation
    2: (300.0, 290.0, 0.0, 100000.0, 5.0, 0.0, 1.0),  # wind from the west
}


class FakeCds:
    def __init__(self, hours=None, grid=(21.0, 105.75)):
        self.calls, self.hours, self.grid = [], hours, grid

    def retrieve(self, dataset, request):
        self.calls.append((dataset, request))
        url = "https://cds.example/api/retrieve/v1/jobs/ts-1/results"
        return SimpleNamespace(url=url, download=lambda target: self._download(request, target))

    def _download(self, request, target):
        first, last = request["date"][0].split("/")
        times = pd.date_range(first, f"{last} 23:00", freq="h")[: self.hours]
        n = len(times)
        data = {
            k: np.full(n, v, "float32")
            for k, v in zip(
                ("t2m", "d2m", "tp", "sp", "u10", "v10", "tcc"),
                (295.0, 290.0, 0.0, 100500.0, 1.0, 1.0, 0.2),
            )
        }
        for i, row in VALUES.items():
            for k, v in zip(("t2m", "d2m", "tp", "sp", "u10", "v10", "tcc"), row):
                data[k][i] = v
        ds = xr.Dataset(
            {k: ("valid_time", v) for k, v in data.items()},
            coords={"valid_time": times, "latitude": self.grid[0], "longitude": self.grid[1]},
        )
        nc = f"{target}.nc"
        ds.to_netcdf(nc, engine="netcdf4")
        with zipfile.ZipFile(target, "w") as zf:
            zf.write(nc, "reanalysis-era5-single-levels-timeseries-x.nc")
        return target


def test_dry_run_builds_one_request_per_location(capsys):
    assert ts.ingest_era5_timeseries(2016, 2025, dry_run=True) == {"requests": 8, "dry_run": True}
    first = json.loads(capsys.readouterr().out.splitlines()[0])
    assert first["dataset"] == "reanalysis-era5-single-levels-timeseries"
    assert first["request"]["date"] == ["2016-01-01/2025-12-31"]
    assert first["request"]["location"] == {"latitude": 21.0285, "longitude": 105.8542}  # HAN


def test_location_is_stored_as_received_with_manifest_and_skipped_on_rerun():
    client, store = _client()
    cds = FakeCds()
    ts.ingest_era5_timeseries(2024, 2024, location="HAN", cds_client=cds, client=client)
    data_key, manifest_key = ts.bronze_keys("HAN", 2024, 2024)
    assert data_key == "era5/timeseries/location=HAN/era5_timeseries_HAN_20240101_20241231.zip"
    manifest = json.loads(store[("bronze", manifest_key)])
    assert manifest["sha256"] == hashlib.sha256(store[("bronze", data_key)]).hexdigest()
    member = manifest["content"]["members"][0]
    assert member["time_steps"] == 8784 and member["grid_latitude"] == 21.0
    assert manifest["request_id"] == "ts-1"
    again = ts.ingest_era5_timeseries(2024, 2024, location="HAN", cds_client=cds, client=client)
    assert again == {"stored": 0, "skipped": 1} and len(cds.calls) == 1


@pytest.mark.parametrize(
    "cds, message", [(FakeCds(hours=100), "time steps"), (FakeCds(grid=(21.5, 105.75)), "nearest")]
)
def test_download_not_matching_the_request_is_a_failure_and_not_stored(cds, message):
    client, store = _client()
    with pytest.raises(RuntimeError, match="1 failure"):
        ts.ingest_era5_timeseries(2024, 2024, location="HAN", cds_client=cds, client=client)
    assert store == {}


def _silver_store():
    client, store = _client()
    ts.ingest_era5_timeseries(2024, 2024, location="HAN", cds_client=FakeCds(), client=client)
    return client, store


def _month(store, month):
    body = store[("silver", silver.silver_key("HAN", 2024, month))]
    return pd.read_parquet(io.BytesIO(body))


def test_silver_converts_units_and_derives_humidity_and_wind():
    client, store = _silver_store()
    report = silver.transform_era5(2024, 2024, location="HAN", client=client)
    (loc,) = report["locations"]
    assert loc["rows"] == 8784 and loc["written"] == 12 and loc["humidity_set_to_saturation"] == 1
    jan = _month(store, 1)
    assert list(jan.columns) == silver.COLUMNS and len(jan) == 744
    first, second, third = jan.iloc[0], jan.iloc[1], jan.iloc[2]
    assert first["temperature_c"] == 26.85 and first["humidity_percent"] == 100.0
    assert first["precipitation_mm"] == 1.0 and first["pressure_hpa"] == 1000.0
    assert first["wind_speed_ms"] == 5.0 and first["wind_direction_deg"] == 0.0  # from north
    assert first["cloud_cover_percent"] == 50.0 and first["pressure_type"] == "surface"
    assert second["humidity_percent"] == 100.0 and second["wind_direction_deg"] == 90.0  # east
    assert third["wind_direction_deg"] == 270.0  # from west
    assert third["humidity_percent"] == pytest.approx(
        100 * np.exp(17.625 * 16.85 / (243.04 + 16.85) - 17.625 * 26.85 / (243.04 + 26.85)),
        abs=0.05,
    )
    assert str(first["timestamp_utc"]) == "2024-01-01 00:00:00+00:00"
    assert str(first["timestamp_local"]) == "2024-01-01 07:00:00"
    assert first["grid_latitude"] == 21.0 and first["grid_longitude"] == 105.75
    assert (
        "silver",
        "weather_hourly/year=2024/month=01/weather_hourly_HAN_202401.parquet",
    ) not in store
    assert any(k[1].startswith("quality_reports/dq_report_era5_") for k in store)


def test_silver_keeps_existing_objects_unless_overwrite():
    client, store = _silver_store()
    silver.transform_era5(2024, 2024, location="HAN", client=client)
    again = silver.transform_era5(2024, 2024, location="HAN", client=client)
    assert again["locations"][0]["written"] == 0 and again["locations"][0]["skipped_existing"] == 12
    forced = silver.transform_era5(2024, 2024, location="HAN", overwrite=True, client=client)
    assert forced["locations"][0]["written"] == 12


def test_silver_refuses_bronze_that_does_not_match_its_manifest():
    client, store = _silver_store()
    data_key, _ = ts.bronze_keys("HAN", 2024, 2024)
    store[("bronze", data_key)] += b"tampered"
    with pytest.raises(RuntimeError, match="1 failure"):
        silver.transform_era5(2024, 2024, location="HAN", client=client)
    assert not any(k[0] == "silver" and k[1].startswith("era5/") for k in store)


@pytest.mark.parametrize(
    "u, v, expected", [(0.0, -1.0, 0.0), (-1.0, 0.0, 90.0), (0.0, 1.0, 180.0), (1.0, 0.0, 270.0)]
)
def test_wind_direction_is_where_the_wind_comes_from(u, v, expected):
    speed, direction = silver.wind_from_uv(pd.Series([u]), pd.Series([v]))
    assert speed.iloc[0] == 1.0 and direction.iloc[0] == pytest.approx(expected)
    assert np.isnan(silver.wind_from_uv(pd.Series([0.0]), pd.Series([0.0]))[1].iloc[0])  # calm


def test_comparison_pairs_only_hours_present_in_both_sources():
    import compare_era5_open_meteo as cmp

    a = pd.Series([1.0, 2.0, np.nan, 4.0])
    b = pd.Series([0.0, 1.0, 5.0, np.nan])
    m = cmp.metrics(a, b)
    assert m["pairs"] == 2 and m["bias"] == 1.0 and m["mae"] == 1.0 and m["rmse"] == 1.0
    assert cmp.metrics(pd.Series([np.nan]), pd.Series([1.0])) == {"pairs": 0}  # no pairs, no score


def test_comparison_reports_missing_source_instead_of_agreement():
    import compare_era5_open_meteo as cmp

    client, _ = _client()
    row = cmp.compare_location_year(client, "HAN", 2024)
    assert row["pairs"] == 0 and "no Silver data" in row["note"]


def test_rerun_over_current_objects_needs_no_credentials(monkeypatch):
    client, _ = _client()
    ts.ingest_era5_timeseries(2024, 2024, location="HAN", cds_client=FakeCds(), client=client)
    monkeypatch.setattr(ts, "make_cds_client", Mock(side_effect=RuntimeError("no CDS credentials")))
    assert ts.ingest_era5_timeseries(2024, 2024, location="HAN", client=client) == {
        "stored": 0,
        "skipped": 1,
    }
