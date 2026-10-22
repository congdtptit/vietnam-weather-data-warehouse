"""Unit tests for scripts/reconcile_dw.py (workbook ID 17). Synthetic Silver, fake DW cursor."""

import io
import json
from collections import Counter
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
import reconcile_dw as rec


def _parquet(rows):
    buf = io.BytesIO()
    pd.DataFrame(rows, columns=["source_name", "location_code", "timestamp_utc"]).to_parquet(buf)
    return buf.getvalue()


def _client(objects):
    store = {}
    client = Mock()
    client.list_objects.side_effect = lambda bucket, prefix, recursive: [
        SimpleNamespace(object_name=k) for k in objects if k.startswith(prefix)
    ]
    client.get_object.side_effect = lambda bucket, key: SimpleNamespace(
        read=lambda: objects[key], close=lambda: None, release_conn=lambda: None
    )
    client.put_object.side_effect = lambda bucket, key, data, length, ct: store.update(
        {key: data.read()}
    )
    return client, store


def _conn(rows):
    cur = Mock()
    cur.fetchall.return_value = rows
    conn = Mock()
    conn.cursor.return_value.__enter__ = lambda self: cur
    conn.cursor.return_value.__exit__ = lambda self, *a: None
    return conn


TS = [pd.Timestamp("2024-03-01 00:00", tz="UTC"), pd.Timestamp("2024-03-01 01:00", tz="UTC")]
SILVER = {
    "weather_hourly/year=2024/month=03/weather_hourly_HAN_202403.parquet": _parquet(
        [("Open-Meteo", "HAN", t) for t in TS]
    ),
    "weather_hourly/year=2024/month=03/weather_hourly_meteostat_48820_202403.parquet": _parquet(
        [("Meteostat", "HAN", TS[0]), ("Meteostat", "XYZ", TS[1])]  # XYZ is not canonical
    ),
    "weather_hourly/year=2024/month=03/notes.txt": b"",
}


def test_matching_counts_pass_and_write_a_report():
    client, store = _client(SILVER)
    conn = _conn([("Open-Meteo", "HAN", 2024, 2), ("Meteostat", "HAN", 2024, 1)])
    report = rec.reconcile(2024, 2024, client=client, conn=conn)
    assert report["passed"] and report["silver_rows"] == report["dw_rows"] == 3
    (key,) = store
    assert (
        key.startswith("quality_reports/reconcile_2024_2024_") and json.loads(store[key])["passed"]
    )


def test_missing_or_extra_groups_fail():
    client, _ = _client(SILVER)
    conn = _conn([("Open-Meteo", "HAN", 2024, 1), ("Meteostat", "PTO", 2024, 5)])
    with pytest.raises(RuntimeError, match="3 group"):
        rec.reconcile(2024, 2024, client=client, conn=conn)


def test_compare_reports_both_sides():
    out = rec.compare(Counter({("A", "HAN", 2024): 2}), Counter({("A", "HAN", 2025): 1}))
    assert {(m["year"], m["silver"], m["dw"]) for m in out} == {(2024, 2, 0), (2025, 0, 1)}
