"""Unit tests for the Gold ML label export (workbook ID 22). Mock MinIO, synthetic hours."""

import io
import json
import re
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
from minio.error import S3Error

KEY = re.compile(
    r"weather_hourly/year=(\d{4})/month=(\d{2})/weather_hourly_([A-Z]{3})_\d{6}\.parquet"
)


def _silver_object(year, month, loc):
    start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
    hours = pd.date_range(start, start + pd.offsets.MonthBegin(1), freq="h", inclusive="left")
    # Rain only on the 10th of each month at HAN; nothing elsewhere
    rain = [1.5 if (loc == "HAN" and h.day == 10 and h.hour == 6) else 0.0 for h in hours]
    df = pd.DataFrame(
        {
            "source_name": "Open-Meteo",
            "location_code": loc,
            "timestamp_utc": hours,
            "precipitation_mm": rain,
        }
    )
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _client(missing=()):
    store: dict[str, bytes] = {}
    client = Mock()

    def get(bucket, key):
        m = KEY.fullmatch(key)
        if key in missing or m is None:
            raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)
        body = _silver_object(int(m.group(1)), int(m.group(2)), m.group(3))
        return SimpleNamespace(read=lambda: body, close=lambda: None, release_conn=lambda: None)

    def put(bucket, key, data, length, content_type=None):
        store[key] = data.read()

    client.get_object.side_effect = get
    client.put_object.side_effect = put

    def stat(bucket, key):
        if key not in store:
            raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)

    client.stat_object.side_effect = stat
    return client, store


def test_export_writes_labels_and_manifest():
    import export_rain_labels as erl

    client, store = _client()
    manifest = erl.export_labels(2024, 2024, client=client)

    parquet_key, manifest_key = erl.labels_keys()
    labels = pd.read_parquet(io.BytesIO(store[parquet_key]))
    assert len(labels) == 8 * 8784  # every location x hour of 2024
    assert (labels["label_source"] == "Open-Meteo").all()
    assert set(labels["split"].dropna()) == {"validation"}

    usable = labels[labels["exclusion_reason"].isna()]
    # 2024 is the validation year: the last 24 origins per location reach 2025 and are purged
    assert len(usable) == 8 * (8784 - 24)
    han = usable[usable["location_code"] == "HAN"].set_index("origin_utc")["rain_next24h"]
    # 1.5 mm at 2024-01-10 06:00: origins 2024-01-09 06:00 .. 2024-01-10 05:00 are Yes
    assert bool(han[pd.Timestamp("2024-01-09 06:00", tz="UTC")]) is True
    assert bool(han[pd.Timestamp("2024-01-10 05:00", tz="UTC")]) is True
    assert bool(han[pd.Timestamp("2024-01-10 06:00", tz="UTC")]) is False
    assert bool(han[pd.Timestamp("2024-01-09 05:00", tz="UTC")]) is False

    saved = json.loads(store[manifest_key])
    assert saved == manifest
    assert saved["rows_usable"] == len(usable)
    assert saved["excluded"] == {"boundary_purge": 8 * 24}
    assert saved["by_location"]["LCA"]["positive_rate"] == 0.0
    assert saved["threshold_mm"] == 1.0 and saved["label_source"] == "Open-Meteo"


def test_export_refuses_incomplete_silver_snapshot():
    import export_rain_labels as erl

    client, store = _client(
        missing={"weather_hourly/year=2024/month=03/weather_hourly_PTO_202403.parquet"}
    )
    with pytest.raises(RuntimeError, match="1 Silver Open-Meteo objects missing"):
        erl.export_labels(2024, 2024, client=client)
    assert store == {}  # nothing published


def test_export_refuses_to_overwrite_an_existing_version():
    import export_rain_labels as erl

    client, store = _client()
    erl.export_labels(2024, 2024, client=client)
    parquet_key, _ = erl.labels_keys()
    saved = store[parquet_key]
    with pytest.raises(RuntimeError, match="already exist"):
        erl.export_labels(2024, 2024, client=client)
    assert store[parquet_key] == saved
    erl.export_labels(2024, 2024, client=client, overwrite=True)
