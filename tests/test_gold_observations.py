"""Unit tests for Gold observations curation and Star Schema dimension mapping.

Tests:
- Date key and Time key mapping using Vietnam Local Time (UTC+7 / Asia/Ho_Chi_Minh)
- 17:00 UTC boundary becoming 00:00 next day in local time
- Canonical location filtering (excluding unmapped regional codes)
- Dimension surrogate key resolution (dim_location, dim_source, dim_station)
- Fact grain deduplication
- Parquet serialization and PyArrow deserialization consistency
"""

import io
from unittest.mock import Mock

import pandas as pd
import pyarrow.parquet as pq
from load_gold_observations import (
    clean_row_value,
    process_month_observations,
    save_gold_parquet,
)
from station_config import CANONICAL_LOCATIONS


def test_vietnam_local_calendar_basis_and_day_boundary():
    """Verify that date_key and time_key follow Vietnam local time (UTC+7).

    Specifically, 17:00 UTC on 2024-05-01 must become 00:00 on 2024-05-02 in local time.
    """
    ts_utc = pd.Timestamp("2024-05-01 17:00:00", tz="UTC")

    # Mock silver dataframe
    df_silver = pd.DataFrame(
        [
            {
                "source_name": "Open-Meteo",
                "location_code": "HAN",
                "timestamp_utc": ts_utc,
                "temperature_c": 28.5,
                "humidity_percent": 80.0,
                "pressure_hpa": 1008.0,
                "precipitation_mm": 0.0,
                "wind_speed_ms": 2.5,
                "wind_direction_deg": 120.0,
                "cloud_cover_percent": 50.0,
                "solar_radiation_wm2": 0.0,
                "pressure_type": "surface",
                "observation_type": "model_reanalysis",
            }
        ]
    )

    buf = io.BytesIO()
    df_silver.to_parquet(buf, index=False)
    parquet_bytes = buf.getvalue()

    # Mock MinIO client
    mock_minio = Mock()
    mock_obj = Mock()
    mock_obj.object_name = "weather_hourly/year=2024/month=05/weather_hourly_HAN_202405.parquet"
    mock_minio.list_objects.return_value = [mock_obj]

    mock_resp = Mock()
    mock_resp.read.return_value = parquet_bytes
    mock_minio.get_object.return_value = mock_resp

    loc_map = {"HAN": 1}
    source_map = {"Open-Meteo": 1}
    station_map = {}

    df_gold = process_month_observations(
        minio_client=mock_minio,
        year=2024,
        month=5,
        loc_map=loc_map,
        source_map=source_map,
        station_map=station_map,
    )

    assert len(df_gold) == 1
    row = df_gold.iloc[0]

    # Verification of calendar boundary:
    # 2024-05-01 17:00 UTC + 7h = 2024-05-02 00:00:00 Vietnam Local Time
    assert row["date_key"] == 20240502
    assert row["time_key"] == 0
    assert row["location_key"] == 1
    assert row["source_key"] == 1
    assert pd.isna(row["station_key"])  # grid source: no station


def test_canonical_location_filtering_excludes_unmapped():
    """Verify that unmapped regional locations (e.g. NW, NE) are filtered out of canonical Gold."""
    df_silver = pd.DataFrame(
        [
            {
                "source_name": "Meteostat",
                "location_code": "HAN",  # Canonical
                "station_id": "48820",
                "timestamp_utc": pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
                "temperature_c": 20.0,
            },
            {
                "source_name": "Meteostat",
                "location_code": "NW",  # Unmapped regional
                "station_id": "48806",
                "timestamp_utc": pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
                "temperature_c": 18.0,
            },
        ]
    )

    buf = io.BytesIO()
    df_silver.to_parquet(buf, index=False)
    parquet_bytes = buf.getvalue()

    mock_minio = Mock()
    mock_obj = Mock()
    mock_obj.object_name = "weather_hourly/year=2024/month=01/weather_hourly_sample.parquet"
    mock_minio.list_objects.return_value = [mock_obj]

    mock_resp = Mock()
    mock_resp.read.return_value = parquet_bytes
    mock_minio.get_object.return_value = mock_resp

    loc_map = {"HAN": 1}
    source_map = {"Meteostat": 3}
    station_map = {"48820": 1, "48806": 9}

    df_gold = process_month_observations(
        minio_client=mock_minio,
        year=2024,
        month=1,
        loc_map=loc_map,
        source_map=source_map,
        station_map=station_map,
    )

    # Only HAN should be retained
    assert len(df_gold) == 1
    assert df_gold.iloc[0]["location_code"] == "HAN"
    assert df_gold.iloc[0]["station_key"] == 1


def test_gold_parquet_serialization_round_trip():
    """Verify Gold fact observation Parquet export and round-trip read."""
    df_test = pd.DataFrame(
        [
            {
                "date_key": 20240101,
                "time_key": 700,
                "location_key": 1,
                "source_key": 1,
                "station_key": None,
                "location_code": "HAN",
                "source_name": "Open-Meteo",
                "timestamp_utc": pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
                "timestamp_local": pd.Timestamp("2024-01-01 07:00:00"),
                "temperature_c": 22.5,
                "humidity_percent": 75.0,
                "pressure_hpa": 1012.0,
                "precipitation_mm": 0.0,
                "wind_speed_ms": 3.2,
                "wind_direction_deg": 180.0,
                "cloud_cover_percent": 30.0,
                "solar_radiation_wm2": 150.0,
                "pressure_type": "surface",
                "observation_type": "model_reanalysis",
            }
        ]
    )

    mock_minio = Mock()
    uploaded_data = None

    def capture_put(bucket_name, object_name, data, length, content_type):
        nonlocal uploaded_data
        uploaded_data = data.read()

    mock_minio.put_object.side_effect = capture_put

    gold_key = save_gold_parquet(
        minio_client=mock_minio,
        df=df_test,
        year=2024,
        month=1,
        overwrite=True,
    )

    assert (
        gold_key
        == "fact_weather_observation/year=2024/month=01/fact_weather_observation_202401.parquet"
    )
    assert uploaded_data is not None

    # Read back with pyarrow
    table = pq.read_table(io.BytesIO(uploaded_data))
    df_read = table.to_pandas()

    assert len(df_read) == 1
    assert df_read.iloc[0]["date_key"] == 20240101
    assert df_read.iloc[0]["time_key"] == 700
    assert df_read.iloc[0]["temperature_c"] == 22.5
    assert df_read.iloc[0]["pressure_type"] == "surface"


def test_clean_row_value_types():
    """Verify native type conversions for psycopg2 SQL parameter binding."""
    import numpy as np

    assert clean_row_value(None) is None
    assert clean_row_value(float("nan")) is None
    assert clean_row_value(np.nan) is None
    assert clean_row_value(np.float64(24.5678)) == 24.57
    assert clean_row_value(np.int64(20240501)) == 20240501
    assert clean_row_value("string") == "string"


def _listing(*names):
    return [Mock(object_name=n) for n in names]


def test_retraction_scope_comes_from_the_request_not_the_incoming_rows():
    from load_gold_observations import retraction_scope

    loc_map = {code: i for i, code in enumerate(sorted(CANONICAL_LOCATIONS), start=1)}
    source_map = {"Open-Meteo": 1, "ERA5": 2, "Meteostat": 3, "NOAA": 4, "GHCNh": 5}
    sources, locations = retraction_scope(loc_map, source_map)
    assert sources == [1, 2, 3]  # Meteostat stays in scope even in a month without its rows
    assert locations == list(range(1, 9))
    assert retraction_scope(loc_map, source_map, "han")[1] == [loc_map["HAN"]]


def test_missing_era5_location_object_is_reported_and_keys_match_the_transform():
    import transform_era5_to_silver as era5
    from load_gold_observations import era5_silver_key, missing_era5_objects

    assert era5_silver_key("HAN", 2024, 6) == era5.silver_key("HAN", 2024, 6)
    present = [era5_silver_key(c, 2024, 6) for c in sorted(CANONICAL_LOCATIONS) if c != "LCA"]
    client = Mock()
    client.list_objects.return_value = _listing(*present)
    assert missing_era5_objects(client, 2024, 6, sorted(CANONICAL_LOCATIONS)) == [
        era5_silver_key("LCA", 2024, 6)
    ]


def test_missing_open_meteo_location_object_is_reported():
    from load_gold_observations import missing_open_meteo_objects, open_meteo_silver_key

    present = [open_meteo_silver_key(c, 2024, 6) for c in sorted(CANONICAL_LOCATIONS) if c != "PTO"]
    client = Mock()
    client.list_objects.return_value = _listing(
        *present, "weather_hourly/year=2024/month=06/weather_hourly_meteostat_48820_202406.parquet"
    )
    missing = missing_open_meteo_objects(client, 2024, 6, sorted(CANONICAL_LOCATIONS))
    assert missing == [open_meteo_silver_key("PTO", 2024, 6)]


def test_pipeline_fails_the_month_instead_of_retracting_a_missing_location(monkeypatch):
    import load_gold_observations as gold

    loc_map = {code: i for i, code in enumerate(sorted(CANONICAL_LOCATIONS), start=1)}
    client = Mock()
    client.list_objects.return_value = _listing(gold.open_meteo_silver_key("HAN", 2024, 1))
    load = Mock()
    monkeypatch.setattr(gold, "get_minio_client", lambda: client)
    monkeypatch.setattr(gold, "get_db_connection", Mock)
    monkeypatch.setattr(
        gold, "load_dimension_lookups", lambda conn: (loc_map, {"Open-Meteo": 1}, {})
    )
    monkeypatch.setattr(gold, "load_dwh_fact_observations", load)
    try:
        gold.run_gold_pipeline(2024, 2024)
    except RuntimeError as ex:
        assert "12 failure" in str(ex)
    else:
        raise AssertionError("missing Silver objects must fail the run")
    load.assert_not_called()


def test_gold_station_key_is_a_nullable_integer():
    df_silver = pd.DataFrame(
        {
            "source_name": ["Meteostat", "Open-Meteo"],
            "location_code": ["HAN", "HAN"],
            "station_id": ["48820", None],
            "timestamp_utc": [pd.Timestamp("2024-05-01 00:00", tz="UTC")] * 2,
            "temperature_c": [28.0, 27.0],
        }
    )
    buf = io.BytesIO()
    df_silver.to_parquet(buf, index=False)
    body = buf.getvalue()
    client = Mock()
    client.list_objects.return_value = _listing(
        "weather_hourly/year=2024/month=05/weather_hourly_HAN_202405.parquet"
    )
    client.get_object.return_value = Mock(read=lambda: body)
    df = process_month_observations(
        client, 2024, 5, {"HAN": 1}, {"Open-Meteo": 1, "Meteostat": 3}, {"48820": 7}
    )
    assert str(df["station_key"].dtype) == "Int64"
    keys = df.set_index("source_name")["station_key"]
    assert keys["Meteostat"] == 7 and pd.isna(keys["Open-Meteo"])


def test_a_month_with_open_meteo_and_era5_loads_each_source_once():
    from types import SimpleNamespace

    import transform_era5_to_silver as era5
    from load_gold_observations import era5_silver_key, open_meteo_silver_key

    ts = pd.Timestamp("2024-05-01 00:00:00", tz="UTC")
    base = {
        "location_code": "HAN",
        "timestamp_utc": ts,
        "temperature_c": 25.0,
        "pressure_type": "surface",
        "observation_type": "model_reanalysis",
    }
    bodies = {}
    for key, source in (
        (open_meteo_silver_key("HAN", 2024, 5), "Open-Meteo"),
        (era5_silver_key("HAN", 2024, 5), "ERA5"),
    ):
        buf = io.BytesIO()
        pd.DataFrame([{**base, "source_name": source}]).to_parquet(buf, index=False)
        bodies[key] = buf.getvalue()
    client = Mock()
    client.list_objects.side_effect = lambda bucket, prefix, recursive=False: [
        SimpleNamespace(object_name=k) for k in bodies if k.startswith(prefix)
    ]
    client.get_object.side_effect = lambda bucket, key: SimpleNamespace(
        read=lambda: bodies[key], close=lambda: None, release_conn=lambda: None
    )
    df = process_month_observations(client, 2024, 5, {"HAN": 1}, {"Open-Meteo": 1, "ERA5": 2}, {})
    assert sorted(df["source_key"]) == [1, 2]  # one row per source, nothing read twice
    assert df["station_key"].isna().all() and set(df["pressure_type"]) == {"surface"}
    assert era5_silver_key("HAN", 2024, 5) == era5.silver_key("HAN", 2024, 5)
