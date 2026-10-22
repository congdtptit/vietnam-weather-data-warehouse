"""Unit tests for scripts/quality_gate.py (workbook ID 24). Synthetic Silver, fake MinIO."""

import io
import json
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
import quality_gate as qg
from minio.error import S3Error

OM_KEY = "weather_hourly/year=2024/month={m:02d}/weather_hourly_HAN_2024{m:02d}.parquet"
MS_KEY = "weather_hourly/year=2024/month={m:02d}/weather_hourly_meteostat_48820_2024{m:02d}.parquet"
RAIN_KEY = "ghcnh/station_daily_rain/year=2024/ghcnh_daily_rain_48825_2024.parquet"


def _parquet(df):
    buf = io.BytesIO()
    df.to_parquet(buf)
    return buf.getvalue()


def _hourly(source, month, hours=None):
    start = pd.Timestamp(f"2024-{month:02d}-01", tz="UTC")
    ts = pd.date_range(start, start + pd.offsets.MonthBegin(1), freq="h", inclusive="left")
    ts = ts[:hours] if hours else ts
    df = pd.DataFrame(
        {
            "source_name": source,
            "location_code": "HAN",
            "timestamp_utc": ts,
            "pressure_type": "surface" if source == "Open-Meteo" else "sea_level",
        }
    )
    for col in qg.MEASUREMENTS:
        df[col] = PLAIN.get(col, 10.0)
    return df


PLAIN = {"temperature_c": 25.0, "humidity_percent": 80.0, "pressure_hpa": 1005.0}


def _rain(days=366):
    day = pd.date_range("2024-01-01", periods=days, freq="D")
    end = day.tz_localize("UTC") + pd.Timedelta(hours=12)
    return pd.DataFrame(
        {
            "station_id": "48825",
            "rain_day_utc": day.date,
            "window_start_utc": end - pd.Timedelta(hours=24),
            "window_end_utc": end,
            "rain_24h_mm": 1.5,
        }
    )


def _silver(**changes):
    """A complete HAN 2024 Silver: 12 Open-Meteo and Meteostat months, 366 rain days."""
    frames = {OM_KEY.format(m=m): _hourly("Open-Meteo", m) for m in range(1, 13)}
    frames.update({MS_KEY.format(m=m): _hourly("Meteostat", m) for m in range(1, 13)})
    frames[RAIN_KEY] = _rain()
    frames.update(changes)
    return {k: _parquet(v) for k, v in frames.items() if v is not None}


def _client(objects):
    store = {}
    client = Mock()
    client.list_objects.side_effect = lambda bucket, prefix, recursive: [
        SimpleNamespace(object_name=k) for k in objects if k.startswith(prefix)
    ]

    def get(bucket, key):
        if key not in objects:
            raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)
        return SimpleNamespace(
            read=lambda: objects[key], close=lambda: None, release_conn=lambda: None
        )

    client.get_object.side_effect = get
    client.put_object.side_effect = lambda bucket, key, data, length, ct: store.update(
        {key: data.read()}
    )
    return client, store


def _failed(objects, check_id):
    client, store = _client(objects)
    with pytest.raises(RuntimeError, match="quality check"):
        qg.quality_gate(2024, 2024, "HAN", client=client)
    (report,) = (json.loads(v) for v in store.values())
    assert not report["passed"]
    return [c for c in report["checks"] if c["status"] == "FAIL" and c["check_id"] == check_id]


def _checks(report, check_id, source=None):
    return [
        c
        for c in report["checks"]
        if c["check_id"] == check_id and (source is None or c.get("source") == source)
    ]


def _run(objects):
    client, _ = _client(objects)
    return qg.quality_gate(2024, 2024, "HAN", client=client)


def test_complete_silver_passes_without_warnings_and_writes_a_report():
    client, store = _client(_silver())
    report = qg.quality_gate(2024, 2024, "HAN", client=client)
    assert report["passed"] and report["by_status"]["FAIL"] == 0 and report["warnings"] == 0
    (key,) = store
    assert key.startswith("quality_reports/quality_gate_2024_2024_HAN_")
    (om,) = _checks(report, "H07_hourly_coverage", "Open-Meteo")
    assert om["status"] == "PASS" and om["severity"] == "ERROR"
    (ms,) = _checks(report, "H07_hourly_coverage", "Meteostat")
    assert ms["status"] == "PASS" and ms["severity"] == "WARN" and ms["threshold"] == 95.0
    assert _checks(report, "H00_objects_read")[0]["rows_in_scope"] == 2 * 8784
    (rain,) = _checks(report, "D06_day_coverage")
    assert rain["status"] == "PASS" and rain["rows"] == 366 == rain["days_in_year"]


def test_low_station_coverage_warns_without_blocking(capsys):
    partial = {MS_KEY.format(m=m): None for m in range(2, 13)}
    report = _run(_silver(**partial, **{RAIN_KEY: _rain(days=300)}))
    # Missing hours also count as missing values: coverage and H08 both warn for Meteostat.
    assert report["passed"] and report["warnings"] == 3
    assert _checks(report, "H08_missing_values", "Meteostat")[0]["status"] == "WARN"
    (ms,) = _checks(report, "H07_hourly_coverage", "Meteostat")
    assert ms["status"] == "WARN" and ms["coverage_pct"] == round(744 / 8784 * 100, 2)
    (rain,) = _checks(report, "D06_day_coverage")
    assert rain["status"] == "WARN" and rain["coverage_pct"] == round(300 / 366 * 100, 2)
    assert capsys.readouterr().out.count("[WARN] ") == 3


def test_missing_open_meteo_month_blocks_the_load():
    (fail,) = _failed(_silver(**{OM_KEY.format(m=2): None}), "H07_hourly_coverage")
    assert fail["source"] == "Open-Meteo" and fail["missing_hours"] == 29 * 24  # leap year


def test_missing_open_meteo_location_year_blocks_the_load():
    no_om = {OM_KEY.format(m=m): None for m in range(1, 13)}
    (fail,) = _failed(_silver(**no_om), "H07_hourly_coverage")
    assert fail["rows"] == 0 and "no Silver rows" in fail["note"]


def test_duplicate_hour_blocks_the_load():
    jan = _hourly("Open-Meteo", 1)
    (fail,) = _failed(
        _silver(**{OM_KEY.format(m=1): pd.concat([jan, jan.iloc[:2]])}), "H05_unique_key"
    )
    assert fail["actual"] == 4


@pytest.mark.parametrize(
    "column, value, violation",
    [
        ("humidity_percent", 101.0, "out_of_range_humidity"),
        ("precipitation_mm", -0.1, "negative_precipitation"),
        ("wind_direction_deg", 361.0, "invalid_wind_direction"),
    ],
)
def test_value_outside_the_contract_range_blocks_the_load(column, value, violation):
    jan = _hourly("Open-Meteo", 1)
    jan.loc[5, column] = value
    (fail,) = _failed(_silver(**{OM_KEY.format(m=1): jan}), "H06_valid_ranges")
    assert fail["violations"] == {violation: 1}


def test_missing_values_warn_by_source_limit():
    om, ms = _hourly("Open-Meteo", 1), _hourly("Meteostat", 1)
    om.loc[:9, "pressure_hpa"] = np.nan  # Open-Meteo: any missing value warns
    ms.loc[:9, "humidity_percent"] = np.nan  # Meteostat: 10 h of 8,784 is under 5%
    ms["cloud_cover_percent"] = np.nan  # not limited for Meteostat: reported only
    report = _run(_silver(**{OM_KEY.format(m=1): om, MS_KEY.format(m=1): ms}))
    assert report["passed"] and report["warnings"] == 1
    (om_check,) = _checks(report, "H08_missing_values", "Open-Meteo")
    assert om_check["status"] == "WARN"
    assert om_check["exceeded"] == {"pressure_hpa": round(10 / 8784 * 100, 3)}
    (ms_check,) = _checks(report, "H08_missing_values", "Meteostat")
    assert ms_check["status"] == "PASS" and ms_check["missing_pct"]["cloud_cover_percent"] > 8

    ms.loc[:500, "humidity_percent"] = np.nan  # 501 of 8,784 h = 5.7%
    report = _run(_silver(**{MS_KEY.format(m=1): ms}))
    (ms_check,) = _checks(report, "H08_missing_values", "Meteostat")
    assert ms_check["status"] == "WARN" and list(ms_check["exceeded"]) == ["humidity_percent"]


@pytest.mark.parametrize(
    "column, value",
    [("temperature_c", 45.5), ("temperature_c", -0.5), ("pressure_hpa", 940.0)]
    + [("precipitation_mm", 120.0)],
)
def test_implausible_value_warns_without_blocking(column, value):
    jan = _hourly("Open-Meteo", 1)
    jan.loc[7, column] = value
    report = _run(_silver(**{OM_KEY.format(m=1): jan}))
    assert report["passed"] and report["warnings"] == 1
    (plausible,) = _checks(report, "H09_plausible_values", "Open-Meteo")
    assert plausible["status"] == "WARN" and plausible["outside"] == {column: 1}
    assert plausible["sample_keys"][column] == [["2024-01-01T07:00:00+00:00", value]]


def test_extreme_rain_warns_unless_already_reviewed():
    rain = _rain()
    rain.loc[rain["rain_day_utc"].astype(str) == "2024-07-01", "rain_24h_mm"] = 320.0
    report = _run(_silver(**{RAIN_KEY: rain}))
    assert report["passed"] and report["warnings"] == 1
    (extreme,) = _checks(report, "D07_extreme_rain")
    assert extreme["status"] == "WARN" and extreme["unreviewed_days"] == ["2024-07-01"]

    reviewed = ("48825", "2024-07-01")
    qg.REVIEWED_EXTREME_RAIN_DAYS.add(reviewed)
    try:
        (extreme,) = _checks(_run(_silver(**{RAIN_KEY: rain})), "D07_extreme_rain")
    finally:
        qg.REVIEWED_EXTREME_RAIN_DAYS.discard(reviewed)
    assert extreme["status"] == "PASS" and extreme["reviewed_days"] == ["2024-07-01"]


def test_timestamp_outside_its_partition_or_naive_blocks_the_load():
    jan = _hourly("Open-Meteo", 1)
    jan.loc[0, "timestamp_utc"] = pd.Timestamp("2024-02-01 00:00", tz="UTC")
    (fail,) = _failed(_silver(**{OM_KEY.format(m=1): jan}), "H04_timestamps_in_partition")
    assert fail["actual"] == 1 and fail["partition"] == [2024, 1]

    naive = _hourly("Open-Meteo", 1)
    naive["timestamp_utc"] = naive["timestamp_utc"].dt.tz_localize(None)
    (fail,) = _failed(_silver(**{OM_KEY.format(m=1): naive}), "H03_utc_timestamps")


def test_missing_key_column_blocks_the_load():
    jan = _hourly("Open-Meteo", 1).drop(columns=["pressure_type"])
    (fail,) = _failed(_silver(**{OM_KEY.format(m=1): jan}), "H01_columns")
    assert fail["missing_columns"] == ["pressure_type"]


@pytest.mark.parametrize(
    "change, check_id",
    [
        (lambda df: df.assign(rain_24h_mm=[1.0, 999.0, 0.0]), "D05_rain_range"),
        (lambda df: pd.concat([df, df.iloc[:1]]), "D03_unique_station_day"),
        (
            lambda df: df.assign(window_end_utc=df["window_end_utc"] + pd.Timedelta(hours=1)),
            "D04_rain_window",
        ),
    ],
)
def test_invalid_daily_rain_blocks_the_load(change, check_id):
    assert _failed(_silver(**{RAIN_KEY: change(_rain(days=3))}), check_id)


def test_cli_exits_1_when_a_check_fails(monkeypatch):
    client, _ = _client(_silver(**{OM_KEY.format(m=3): None}))
    monkeypatch.setattr(qg, "get_minio_client", lambda: client)
    monkeypatch.setattr(
        "sys.argv",
        ["quality_gate.py", "--start-year", "2024", "--end-year", "2024", "--location", "HAN"],
    )
    assert qg.main() == 1
