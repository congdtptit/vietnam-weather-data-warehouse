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
    assert row["station_key"] is None


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
