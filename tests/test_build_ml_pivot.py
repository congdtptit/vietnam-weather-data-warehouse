"""Unit tests for scripts/build_ml_pivot.py (workbook ID 16). Synthetic Gold rows, mock MinIO."""

import io
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import build_ml_pivot as bp
import numpy as np
import pandas as pd
import pytest
from minio.error import S3Error

TS = pd.date_range("2024-01-01", periods=3, freq="h", tz="UTC")


def _long():
    """HAN: Open-Meteo and ERA5 every hour, Meteostat only at the first; LCA: Open-Meteo only."""
    rows = []
    for i, ts in enumerate(TS):
        rows.append(("Open-Meteo", "HAN", ts, 20.0 + i, 1.0))
        rows.append(("ERA5", "HAN", ts, 19.0 + i, 2.0))
        rows.append(("Open-Meteo", "LCA", ts, 15.0, 0.5))
    rows.append(("Meteostat", "HAN", TS[0], 21.5, np.nan))
    df = pd.DataFrame(
        rows,
        columns=[
            "source_name",
            "location_code",
            "timestamp_utc",
            "temperature_c",
            "precipitation_mm",
        ],
    )
    for v in bp.VARIABLES:
        if v not in df:
            df[v] = np.nan
    local = df["timestamp_utc"] + pd.Timedelta(hours=7)
    df["date_key"] = local.dt.strftime("%Y%m%d").astype(int)
    df["time_key"] = local.dt.hour * 100
    return df


def test_one_row_per_location_hour_and_one_column_per_source():
    wide = bp.pivot(_long())
    assert len(wide) == 6 and not wide.duplicated(bp.KEY).any()
    assert wide.columns[:4].tolist() == ["location_code", "timestamp_utc", "date_key", "time_key"]
    assert len(wide.columns) == 4 + len(bp.SOURCES) * len(bp.VARIABLES)
    han = wide[wide["location_code"] == "HAN"].reset_index(drop=True)
    # Rain is kept per source, never summed across sources
    assert han["precipitation_mm__open_meteo"].tolist() == [1.0, 1.0, 1.0]
    assert han["precipitation_mm__era5"].tolist() == [2.0, 2.0, 2.0]
    # Meteostat has one row: the other hours stay missing, not filled
    assert han["temperature_c__meteostat"].iloc[0] == 21.5
    assert han["temperature_c__meteostat"].iloc[1:].isna().all()
    lca = wide[wide["location_code"] == "LCA"]
    assert lca["temperature_c__era5"].isna().all()  # ERA5 absent at LCA in this window
    assert wide.loc[0, "date_key"] == 20240101 and wide.loc[0, "time_key"] == 700


def test_reconcile_counts_and_sums_per_source():
    long = _long()
    checks = bp.reconcile(long, bp.pivot(long))
    assert checks["Meteostat"]["gold_rows"] == 1
    assert checks["ERA5"]["variables"]["precipitation_mm"] == {"non_null": 3, "sum": 6.0}
    assert checks["Open-Meteo"]["pivot_rows_with_values"] == 6

    wide = bp.pivot(long)
    wide.loc[0, "precipitation_mm__era5"] = 5.0
    with pytest.raises(ValueError, match="precipitation_mm__era5"):
        bp.reconcile(long, wide)


@pytest.mark.parametrize(
    "change, message",
    [
        (lambda df: df.assign(source_name=df["source_name"].replace("ERA5", "NOAA")), "policy"),
        (lambda df: pd.concat([df, df.iloc[[0]]]), "duplicate"),
    ],
)
def test_unknown_source_or_duplicate_rows_are_rejected(change, message):
    with pytest.raises(ValueError, match=message):
        bp.pivot(change(_long()))


def _client(store):
    def get(bucket, key):
        if key not in store:
            raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)
        body = store[key]
        return SimpleNamespace(read=lambda: body, close=lambda: None, release_conn=lambda: None)

    def put(bucket, key, data, length, content_type=None):
        store[key] = data.read()

    def stat(bucket, key):
        if key not in store:
            raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)

    client = Mock()
    client.get_object.side_effect = get
    client.put_object.side_effect = put
    client.stat_object.side_effect = stat
    return client


def _gold_store():
    store, long = {}, _long()
    for month in range(1, 13):
        part = long if month == 1 else long.iloc[0:0]
        buf = io.BytesIO()
        part.to_parquet(buf, index=False)
        store[bp.gold_key(2024, month)] = buf.getvalue()
    return store


def _conn(counts):
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value.fetchall.return_value = counts
    return conn


DW_OK = [("Open-Meteo", 2024, 6), ("ERA5", 2024, 3), ("Meteostat", 2024, 1)]


def test_build_writes_the_year_and_manifest_once():
    store = _gold_store()
    manifest = bp.build(2024, 2024, client=_client(store), conn=_conn(DW_OK))
    data_key, manifest_key = bp.pivot_keys(2024)
    assert len(pd.read_parquet(io.BytesIO(store[data_key]))) == 6
    assert json.loads(store[manifest_key])["rows"] == manifest["rows"] == 6
    pressure = {c["name"]: c.get("pressure_type") for c in manifest["columns"]}
    assert pressure["pressure_hpa__meteostat"] == "sea_level"
    assert pressure["pressure_hpa__era5"] == "surface"
    assert len(manifest["lineage"]["objects"]) == 12

    with pytest.raises(RuntimeError, match="already exist"):
        bp.build(2024, 2024, client=_client(store), conn=_conn(DW_OK))


def test_dw_mismatch_fails_before_anything_is_written():
    store = _gold_store()
    before = set(store)
    with pytest.raises(ValueError, match="DW 2"):
        bp.build(
            2024, 2024, client=_client(store), conn=_conn([*DW_OK[:2], ("Meteostat", 2024, 2)])
        )
    assert set(store) == before


def test_window_outside_scope_is_refused():
    with pytest.raises(ValueError, match="outside"):
        bp.build(2025, 2026, client=Mock(), conn=Mock())
