"""Tests for NOAA GHCNh daily station rainfall (workbook ID 9): ingest, Silver, DW load, view.

Synthetic PSV payloads and mocks only; integration tests roll back every change.
"""

import hashlib
import io
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
from minio.error import S3Error

ROOT = Path(__file__).resolve().parents[1]
HEADER = (
    "STATION|DATE|temperature|precipitation_24_hour|precipitation_24_hour_Measurement_Code|"
    "precipitation_24_hour_Quality_Code|precipitation_24_hour_Report_Type|"
    "precipitation_24_hour_Source_Code"
)


def _psv(rows):
    lines = [HEADER]
    for when, value, measurement, quality in rows:
        lines.append(f"VMM00048825|{when}|25.0|{value}|{measurement}|{quality}|FM12|220")
    return ("\n".join(lines) + "\n").encode()


SAMPLE = _psv(
    [
        ("2024-01-02T12:00:00", "5.2", "", "1"),  # clean
        ("2024-01-02T06:00:00", "", "", ""),  # no 24 h value (temperature-only row)
        ("2024-01-03T12:00:00", "0.0", "T", "1"),  # trace, kept
        ("2024-01-04T12:00:00", "9.0", "", "2"),  # suspect quality, rejected
        ("2024-01-05T12:00:00", "30.0", "4", "1"),  # end of accumulated period, rejected
        ("2024-01-06T12:00:00", "999.1", "", "1"),  # out of range, rejected
        ("2024-01-07T18:00:00", "3.0", "", "1"),  # off schedule, ignored
        ("2024-01-08T12:00:00", "4.0", "", "1"),  # conflicting duplicate
        ("2024-01-08T12:00:00", "6.0", "", "1"),  # conflicting duplicate
        ("2024-01-09T12:00:00", "1.0", "", "1"),  # identical duplicate
        ("2024-01-09T12:00:00", "1.0", "", "1"),
    ]
)


def _not_found():
    return S3Error("NoSuchKey", "missing", "/res", "req", "host", None)


# -----------------------------------------------------------------------------
# Silver
# -----------------------------------------------------------------------------
def test_clean_daily_rain_rules_and_window():
    from transform_ghcnh_to_silver import clean_ghcnh_daily_rain

    clean, conflicts, stats = clean_ghcnh_daily_rain(SAMPLE, "48825", 2024)

    assert list(clean["rain_day_utc"]) == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 9)]
    assert list(clean["rain_24h_mm"]) == [5.2, 0.0, 1.0]
    assert list(clean["is_trace"]) == [False, True, False]
    assert (clean["location_code"] == "HAN").all()
    assert (clean["station_role"] == "nearest_available").all()
    first = clean.iloc[0]
    assert first["window_end_utc"] == pd.Timestamp("2024-01-02 12:00", tz="UTC")
    assert first["window_start_utc"] == pd.Timestamp("2024-01-01 12:00", tz="UTC")

    assert stats["quality_rejected"] == {"2": 1}
    assert stats["measurement_rejected"] == {"4": 1}
    assert stats["out_of_range_rejected"] == 1
    assert stats["off_schedule_rows_ignored"] == 1
    assert stats["identical_duplicates_removed"] == 1
    assert stats["conflicting_rows_quarantined"] == 2
    assert sorted(conflicts["rain_24h_mm"]) == [4.0, 6.0]
    assert set(conflicts["quarantine_reason"]) == {"conflicting_duplicate"}
    assert stats["clean_days"] == 3 and stats["expected_days"] == 366
    assert stats["missing_days"] == 363  # a day without a usable report is missing, not 0 mm


def test_unsupported_extreme_is_quarantined_supported_extreme_kept():
    from transform_ghcnh_to_silver import clean_ghcnh_daily_rain

    header = HEADER + "|precipitation_6_hour|precipitation_12_hour"
    rows = [
        # 700 mm with no sub-daily rain in its window: quarantined
        "VMM00048823|2023-10-13T12:00:00|25.0|700.0||1|FM12|220||",
        # 338 mm supported by a 12-hour report of 188 mm inside the window: kept
        "VMM00048823|2023-10-13T18:00:00|25.0||||||188.0",
        "VMM00048823|2023-10-14T12:00:00|25.0|338.0||1|FM12|220||50.0",
        # 400 mm: the 50 mm report at 2023-10-14 12:00 ends the previous window, so no support
        "VMM00048823|2023-10-15T12:00:00|25.0|400.0||1|FM12|220||",
    ]
    raw = ("\n".join([header, *rows]) + "\n").encode()
    clean, quarantined, stats = clean_ghcnh_daily_rain(raw, "48823", 2023)

    assert list(clean["rain_24h_mm"]) == [338.0]
    assert sorted(quarantined["rain_24h_mm"]) == [400.0, 700.0]
    assert set(quarantined["quarantine_reason"]) == {"unsupported_extreme"}
    assert stats["unsupported_extreme_quarantined"] == 2


def test_clean_rejects_unknown_station_and_missing_columns():
    from transform_ghcnh_to_silver import clean_ghcnh_daily_rain

    with pytest.raises(ValueError, match="GHCNH_RAIN_STATIONS"):
        clean_ghcnh_daily_rain(SAMPLE, "48820", 2024)
    with pytest.raises(ValueError, match="missing columns"):
        clean_ghcnh_daily_rain(b"STATION|DATE\nX|2024-01-01T12:00:00\n", "48825", 2024)


def test_station_mapping_is_separate_from_meteostat():
    from station_config import GHCNH_RAIN_STATIONS, get_canonical_stations

    assert {s: m["location_code"] for s, m in GHCNH_RAIN_STATIONS.items()} == {
        "48826": "HPH",
        "48831": "TNG",
        "48803": "LCA",
        "48825": "HAN",
        "48823": "NBI",
    }
    # Hà Đông and Nam Định must not become Meteostat stations (HAN keeps one Meteostat station)
    assert "48825" not in get_canonical_stations() and "48823" not in get_canonical_stations()


# -----------------------------------------------------------------------------
# Ingest
# -----------------------------------------------------------------------------
def _client_with_store():
    store: dict[str, bytes] = {}
    client = Mock()

    def put(bucket, key, data, length, content_type=None):
        store[key] = data.read()

    def get(bucket, key):
        if key not in store:
            raise _not_found()
        return SimpleNamespace(
            read=lambda: store[key], close=lambda: None, release_conn=lambda: None
        )

    def stat(bucket, key):
        if key not in store:
            raise _not_found()
        return SimpleNamespace(size=len(store[key]))

    client.put_object.side_effect = put
    client.get_object.side_effect = get
    client.stat_object.side_effect = stat
    return client, store


def test_ingest_stores_raw_bytes_manifest_and_skips_rerun(monkeypatch):
    import ingest_ghcnh

    calls = []

    def fake_get(url, timeout):
        calls.append(url)
        if "48823" in url:
            return SimpleNamespace(status_code=404, content=b"", headers={})
        return SimpleNamespace(status_code=200, content=SAMPLE, headers={})

    monkeypatch.setattr(ingest_ghcnh.requests, "get", fake_get)
    client, store = _client_with_store()

    summary = ingest_ghcnh.ingest_ghcnh(2024, 2024, ["48825", "48823"], client=client)
    data_key, manifest_key = ingest_ghcnh.bronze_keys("48825", 2024)
    assert store[data_key] == SAMPLE  # unchanged bytes
    manifest = json.loads(store[manifest_key])
    assert manifest["sha256"] == hashlib.sha256(SAMPLE).hexdigest()
    assert manifest["byte_size"] == len(SAMPLE) and manifest["data_rows"] == 11
    assert summary["stored"] == 1 and summary["not_found"] == [
        {"station_id": "48823", "year": 2024}
    ]

    calls.clear()
    rerun = ingest_ghcnh.ingest_ghcnh(2024, 2024, ["48825"], client=client)
    assert rerun["skipped_existing"] == 1 and calls == []


def test_ingest_retries_transient_and_fails_on_permanent(monkeypatch):
    import ingest_ghcnh

    monkeypatch.setattr(ingest_ghcnh.time, "sleep", lambda s: None)
    responses = iter(
        [
            SimpleNamespace(status_code=503, content=b"", headers={}),
            SimpleNamespace(status_code=200, content=SAMPLE, headers={}),
        ]
    )
    monkeypatch.setattr(ingest_ghcnh.requests, "get", lambda url, timeout: next(responses))
    status, body, _ = ingest_ghcnh.fetch_station_year("48825", 2024)
    assert status == 200 and body == SAMPLE

    monkeypatch.setattr(
        ingest_ghcnh.requests,
        "get",
        lambda url, timeout: SimpleNamespace(status_code=403, content=b"", headers={}),
    )
    client, _ = _client_with_store()
    with pytest.raises(RuntimeError, match="failure"):
        ingest_ghcnh.ingest_ghcnh(2024, 2024, ["48825"], client=client)


def test_silver_process_writes_parquet_and_fails_on_missing_bronze_with_manifest():
    import ingest_ghcnh
    import transform_ghcnh_to_silver as tg

    client, store = _client_with_store()
    client.bucket_exists.return_value = True
    data_key, manifest_key = ingest_ghcnh.bronze_keys("48825", 2024)
    store[data_key] = SAMPLE
    store[manifest_key] = b"{}"

    report = tg.process_ghcnh_to_silver(2024, 2024, ["48825"], overwrite=True, client=client)
    clean_key, conflict_key = tg.silver_keys("48825", 2024)
    assert len(pd.read_parquet(io.BytesIO(store[clean_key]))) == 3
    assert len(pd.read_parquet(io.BytesIO(store[conflict_key]))) == 2
    assert report["station_years"]["48825_2024"]["clean_days"] == 3

    del store[data_key]
    with pytest.raises(RuntimeError, match="failure"):
        tg.process_ghcnh_to_silver(2024, 2024, ["48825"], overwrite=True, client=client)


# -----------------------------------------------------------------------------
# DW load and comparison view (integration, rolled back)
# -----------------------------------------------------------------------------
class _NoCommit:
    """Connection proxy: the loader's commit becomes a no-op so the test can roll back."""

    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        pass

    def rollback(self):
        self._conn.rollback()


@pytest.fixture
def rain_db(pg_connection):
    cur = pg_connection.cursor()
    try:
        for sql in ("02_olap_views.sql", "03_station_daily_rain.sql", "03_station_daily_rain.sql"):
            cur.execute((ROOT / "sql" / sql).read_text())
        cur.execute(
            """
            INSERT INTO dim_station (station_id, station_name, wmo_id)
            VALUES ('48825', 'Hà Đông (Hà Nội)', '48825')
            ON CONFLICT (station_id) DO NOTHING;
            INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month,
                                  month, month_name, quarter, year, is_weekend, is_rainy_season)
            SELECT to_char(d, 'YYYYMMDD')::int, d::date, extract(isodow FROM d)::int,
                   to_char(d, 'FMDay'), extract(day FROM d)::int, extract(month FROM d)::int,
                   to_char(d, 'FMMonth'), extract(quarter FROM d)::int, extract(year FROM d)::int,
                   extract(isodow FROM d) IN (6, 7), false
            FROM generate_series('2023-12-31'::date, '2024-01-04'::date, interval '1 day') AS d
            ON CONFLICT (date_key) DO NOTHING;
            """
        )
        cur.execute("SELECT source_key FROM dim_source WHERE source_name = 'GHCNh'")
        source_key = cur.fetchone()[0]
        cur.execute("SELECT station_key FROM dim_station WHERE station_id = '48825'")
        station_key = cur.fetchone()[0]
        cur.execute("SELECT location_key FROM dim_location WHERE location_code = 'HAN'")
        location_key = cur.fetchone()[0]
        yield SimpleNamespace(
            conn=_NoCommit(pg_connection),
            cur=cur,
            source_key=source_key,
            station_key=station_key,
            location_key=location_key,
        )
    finally:
        pg_connection.rollback()
        cur.close()


def _silver_frame(days_and_mm):
    rows = []
    for day, mm in days_and_mm:
        end = datetime(day.year, day.month, day.day, 12, tzinfo=timezone.utc)
        rows.append(
            {
                "rain_day_utc": day,
                "window_start_utc": pd.Timestamp(end - timedelta(hours=24)),
                "window_end_utc": pd.Timestamp(end),
                "rain_24h_mm": mm,
                "is_trace": False,
                "station_role": "nearest_available",
                "quality_code": "1",
                "measurement_code": None,
                "report_type": "FM12",
                "source_code": "220",
            }
        )
    return pd.DataFrame(rows)


@pytest.mark.integration
def test_load_rerun_and_retraction(rain_db):
    from load_station_daily_rain import load_station_year

    db = rain_db
    args = (db.conn, db.source_key, db.station_key, db.location_key, 2024)
    frame = _silver_frame([(date(2024, 1, 2), 5.2), (date(2024, 1, 3), 0.0)])
    assert load_station_year(*args, frame) == 2
    assert load_station_year(*args, frame) == 2  # rerun: same keys
    db.cur.execute("SELECT count(*), sum(rain_24h_mm)::float FROM fact_station_daily_rain")
    assert db.cur.fetchone() == (2, pytest.approx(5.2))

    load_station_year(*args, frame.iloc[[0]])  # day 3 removed from Silver
    db.cur.execute("SELECT rain_day_utc FROM fact_station_daily_rain ORDER BY 1")
    assert [r[0] for r in db.cur.fetchall()] == [date(2024, 1, 2)]

    load_station_year(*args, pd.DataFrame(columns=["rain_day_utc"]))  # no usable day left
    db.cur.execute("SELECT count(*) FROM fact_station_daily_rain")
    assert db.cur.fetchone()[0] == 0


@pytest.mark.integration
def test_comparison_view_uses_the_same_12_utc_window(rain_db):
    from load_station_daily_rain import load_station_year

    db = rain_db
    load_station_year(
        db.conn,
        db.source_key,
        db.station_key,
        db.location_key,
        2024,
        _silver_frame([(date(2024, 1, 2), 10.0), (date(2024, 1, 3), 2.0)]),
    )
    db.cur.execute("SELECT source_key FROM dim_source WHERE source_name = 'Open-Meteo'")
    om_key = db.cur.fetchone()[0]
    vn = timezone(timedelta(hours=7))
    # Hours stamped 2024-01-01 12:00 (previous window, 100 mm) through 2024-01-02 12:00:
    # the 24 hours 13:00..12:00 belong to rain day 2024-01-02 and sum to 12.0 mm.
    for i in range(25):
        ts = datetime(2024, 1, 1, 12, tzinfo=timezone.utc) + timedelta(hours=i)
        local = ts.astimezone(vn)
        db.cur.execute(
            """INSERT INTO fact_weather_observation (date_key, time_key, location_key, source_key,
                   timestamp_utc, timestamp_local, precipitation_mm)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (
                int(local.strftime("%Y%m%d")),
                local.hour * 100,
                db.location_key,
                om_key,
                ts,
                local.replace(tzinfo=None),
                100.0 if i == 0 else 0.5,
            ),
        )
    db.cur.execute(
        """SELECT rain_day_utc, station_rain_mm::float, model_rain_mm::float, model_valid_hours,
                  model_is_rainy
           FROM vw_rain_station_vs_model WHERE location_code = 'HAN' ORDER BY 1"""
    )
    rows = db.cur.fetchall()
    assert rows[0][:4] == (date(2024, 1, 2), pytest.approx(10.0), pytest.approx(12.0), 24)
    assert rows[0][4] is True
    # 2024-01-03 has no model hours in this fixture: model rain is unknown, not 0
    assert rows[1][0] == date(2024, 1, 3) and rows[1][2] is None


def test_extreme_support_exactly_at_the_ratio_is_accepted():
    from transform_ghcnh_to_silver import extreme_is_supported

    assert 0.1 * 338.0 > 33.8  # the float product that used to reject an exact support
    assert extreme_is_supported(33.8, 338.0)
    assert not extreme_is_supported(33.7, 338.0)
    assert extreme_is_supported(49.2, 491.3) and not extreme_is_supported(49.1, 491.3)
    assert not extreme_is_supported(float("nan"), 700.0)  # no sub-daily report
