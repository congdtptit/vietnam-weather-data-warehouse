"""Regression tests covering PR #5 review findings.

Defects tested:
1. S3Error 404 in Request ID (server error false classification)
2. Location-filtered rerun preserving unaffected locations in Gold Parquet and DW
3. Silver read / Parquet parse failure blocking publication
4. Conflicting duplicates rejected while identical duplicates collapse cleanly
5. SQL transaction recovery via rollback between months
6. UTC-to-local deterministic derivation and grid validation

Re-review probes (PR #5 re-review, 2026-10-17):
7. S3Error NoSuchKey on already-listed Silver object blocks publication (SilverReadError)
8. Retracted Silver rows removed from both Gold and DW on location-filtered rerun
9. Naive timestamp (no tzinfo) rejected before UTC normalization
10. Out-of-month aware timestamp rejected during month curation
"""

import io
from types import SimpleNamespace
from unittest.mock import Mock

import ingest_meteostat
import load_gold_observations as gold_obs
import pandas as pd
import pytest
import transform_meteostat_to_silver as ms_silver
import transform_open_meteo_to_silver as om_silver
from minio.error import S3Error


# ---------------------------------------------------------------------------
# Defect 1: S3Error 404 in Request ID
# ---------------------------------------------------------------------------
def test_s3_error_404_in_request_id_fails_in_ingestion_and_silver(monkeypatch):
    """Verify S3Error('InternalError', ...) with '404' in request-id fails as server error."""
    internal_err = S3Error(
        "InternalError",
        "Backend unavailable",
        "/object",
        "request-404-id",
        "host",
        None,
    )

    # 1. Ingestion: must fail with RuntimeError, not skip
    client_ingest = Mock()
    client_ingest.stat_object.side_effect = internal_err
    monkeypatch.setattr(ingest_meteostat, "get_minio_client", lambda: client_ingest)

    with pytest.raises(RuntimeError, match="completed with 1 failure"):
        ingest_meteostat.ingest_meteostat_data(
            start_year=2024,
            end_year=2024,
            station_ids=["48820"],
        )

    # 2. Meteostat Silver: must fail with RuntimeError
    client_ms = Mock()
    client_ms.get_object.side_effect = internal_err
    monkeypatch.setattr(ms_silver, "get_minio_client", lambda: client_ms)

    with pytest.raises(RuntimeError, match="Meteostat transformation encountered 1 failure"):
        ms_silver.process_meteostat_to_silver(
            start_year=2024,
            end_year=2024,
            station_ids=["48820"],
        )

    # 3. Open-Meteo Silver: must fail with RuntimeError
    client_om = Mock()
    client_om.get_object.side_effect = internal_err
    monkeypatch.setattr(om_silver, "get_minio_client", lambda: client_om)

    with pytest.raises(RuntimeError, match="Open-Meteo transformation encountered 1 failure"):
        om_silver.process_open_meteo_to_silver(
            start_year=2024,
            end_year=2024,
            target_location="HAN",
        )


# ---------------------------------------------------------------------------
# Defect 3: Do Not Swallow Silver Read / Parquet Exceptions
# ---------------------------------------------------------------------------
def test_silver_read_failure_blocks_publication_and_records_failure():
    """Verify OSError or corrupted Parquet raises SilverReadError and blocks publication."""
    mock_minio = Mock()
    mock_obj = Mock()
    mock_obj.object_name = "weather_hourly/year=2024/month=05/weather_hourly_HAN_202405.parquet"
    mock_minio.list_objects.return_value = [mock_obj]

    # Corrupt Parquet bytes
    mock_resp = Mock()
    mock_resp.read.side_effect = OSError("Disk read failed")
    mock_minio.get_object.return_value = mock_resp

    loc_map = {"HAN": 1}
    source_map = {"Open-Meteo": 1}
    station_map = {}

    with pytest.raises(gold_obs.SilverReadError) as exc_info:
        gold_obs.process_month_observations(
            minio_client=mock_minio,
            year=2024,
            month=5,
            loc_map=loc_map,
            source_map=source_map,
            station_map=station_map,
        )

    assert "weather_hourly_HAN_202405.parquet" in exc_info.value.key
    assert "Disk read failed" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Defect 4: Quarantine or Reject Conflicting Duplicate Observations
# ---------------------------------------------------------------------------
def test_identical_duplicates_collapse_conflicting_duplicates_rejected():
    """Identical rows collapse cleanly; conflicting measurements on same grain raise ValueError."""
    ts_utc = pd.Timestamp("2024-05-01 10:00:00", tz="UTC")
    loc_map = {"HAN": 1}
    source_map = {"Open-Meteo": 1}
    station_map = {}

    # Case A: Identical duplicates (same grain, same measurements)
    df_identical = pd.DataFrame(
        [
            {
                "source_name": "Open-Meteo",
                "location_code": "HAN",
                "timestamp_utc": ts_utc,
                "temperature_c": 28.0,
                "humidity_percent": 80.0,
                "precipitation_mm": 1.0,
            },
            {
                "source_name": "Open-Meteo",
                "location_code": "HAN",
                "timestamp_utc": ts_utc,
                "temperature_c": 28.0,
                "humidity_percent": 80.0,
                "precipitation_mm": 1.0,
            },
        ]
    )

    buf_a = io.BytesIO()
    df_identical.to_parquet(buf_a, index=False)
    bytes_a = buf_a.getvalue()

    mock_minio_a = Mock()
    mock_obj_a = Mock()
    mock_obj_a.object_name = "weather_hourly/year=2024/month=05/weather_hourly_HAN_202405.parquet"
    mock_minio_a.list_objects.return_value = [mock_obj_a]
    mock_minio_a.get_object.return_value = SimpleNamespace(
        read=lambda: bytes_a, close=lambda: None, release_conn=lambda: None
    )

    result_a = gold_obs.process_month_observations(
        minio_client=mock_minio_a,
        year=2024,
        month=5,
        loc_map=loc_map,
        source_map=source_map,
        station_map=station_map,
    )
    assert len(result_a) == 1
    assert result_a.iloc[0]["precipitation_mm"] == 1.0

    # Case B: Conflicting duplicates (same grain, differing measurements: 1.0 vs 99.0)
    df_conflicting = pd.DataFrame(
        [
            {
                "source_name": "Open-Meteo",
                "location_code": "HAN",
                "timestamp_utc": ts_utc,
                "temperature_c": 28.0,
                "humidity_percent": 80.0,
                "precipitation_mm": 1.0,
            },
            {
                "source_name": "Open-Meteo",
                "location_code": "HAN",
                "timestamp_utc": ts_utc,
                "temperature_c": 28.0,
                "humidity_percent": 80.0,
                "precipitation_mm": 99.0,
            },
        ]
    )

    buf_b = io.BytesIO()
    df_conflicting.to_parquet(buf_b, index=False)
    bytes_b = buf_b.getvalue()

    mock_minio_b = Mock()
    mock_obj_b = Mock()
    mock_obj_b.object_name = "weather_hourly/year=2024/month=05/weather_hourly_HAN_202405.parquet"
    mock_minio_b.list_objects.return_value = [mock_obj_b]
    mock_minio_b.get_object.return_value = SimpleNamespace(
        read=lambda: bytes_b, close=lambda: None, release_conn=lambda: None
    )

    with pytest.raises(ValueError, match="Conflicting duplicate observations detected"):
        gold_obs.process_month_observations(
            minio_client=mock_minio_b,
            year=2024,
            month=5,
            loc_map=loc_map,
            source_map=source_map,
            station_map=station_map,
        )


# ---------------------------------------------------------------------------
# Defect 6: UTC-to-Local Derivation & Grid Validation
# ---------------------------------------------------------------------------
def test_off_grid_utc_and_conflicting_local_timestamps_rejected():
    """Verify off-grid UTC timestamps and conflicting local timestamps are rejected."""
    loc_map = {"HAN": 1}
    source_map = {"Open-Meteo": 1}
    station_map = {}

    # Case 1: Off-grid UTC timestamp (minute=30)
    df_off_grid = pd.DataFrame(
        [
            {
                "source_name": "Open-Meteo",
                "location_code": "HAN",
                "timestamp_utc": pd.Timestamp("2024-05-01 10:30:00", tz="UTC"),
                "temperature_c": 25.0,
            }
        ]
    )
    buf1 = io.BytesIO()
    df_off_grid.to_parquet(buf1, index=False)
    mock_minio1 = Mock()
    mock_obj1 = Mock()
    mock_obj1.object_name = "weather_hourly/year=2024/month=05/part1.parquet"
    mock_minio1.list_objects.return_value = [mock_obj1]
    mock_minio1.get_object.return_value = SimpleNamespace(
        read=lambda: buf1.getvalue(), close=lambda: None, release_conn=lambda: None
    )

    with pytest.raises(ValueError, match="off-grid UTC timestamps"):
        gold_obs.process_month_observations(
            minio_client=mock_minio1,
            year=2024,
            month=5,
            loc_map=loc_map,
            source_map=source_map,
            station_map=station_map,
        )

    # Case 2: Conflicting local timestamp (Jan 1 UTC + 7h should be Jan 1 17:00, but given Jan 2 10:00)
    df_conflict_local = pd.DataFrame(
        [
            {
                "source_name": "Open-Meteo",
                "location_code": "HAN",
                "timestamp_utc": pd.Timestamp("2024-01-01 10:00:00", tz="UTC"),
                "timestamp_local": pd.Timestamp("2024-01-02 10:00:00"),
                "temperature_c": 20.0,
            }
        ]
    )
    buf2 = io.BytesIO()
    df_conflict_local.to_parquet(buf2, index=False)
    mock_minio2 = Mock()
    mock_obj2 = Mock()
    mock_obj2.object_name = "weather_hourly/year=2024/month=01/part2.parquet"
    mock_minio2.list_objects.return_value = [mock_obj2]
    mock_minio2.get_object.return_value = SimpleNamespace(
        read=lambda: buf2.getvalue(), close=lambda: None, release_conn=lambda: None
    )

    with pytest.raises(ValueError, match="Supplied 'timestamp_local' conflicts with UTC-derived"):
        gold_obs.process_month_observations(
            minio_client=mock_minio2,
            year=2024,
            month=1,
            loc_map=loc_map,
            source_map=source_map,
            station_map=station_map,
        )

    # Case 3: Partial null in timestamp_local
    df_partial_null = pd.DataFrame(
        [
            {
                "source_name": "Open-Meteo",
                "location_code": "HAN",
                "timestamp_utc": pd.Timestamp("2024-01-01 10:00:00", tz="UTC"),
                "timestamp_local": pd.Timestamp("2024-01-01 17:00:00"),
                "temperature_c": 20.0,
            },
            {
                "source_name": "Open-Meteo",
                "location_code": "HAN",
                "timestamp_utc": pd.Timestamp("2024-01-01 11:00:00", tz="UTC"),
                "timestamp_local": pd.NaT,
                "temperature_c": 21.0,
            },
        ]
    )
    buf3 = io.BytesIO()
    df_partial_null.to_parquet(buf3, index=False)
    mock_minio3 = Mock()
    mock_obj3 = Mock()
    mock_obj3.object_name = "weather_hourly/year=2024/month=01/part3.parquet"
    mock_minio3.list_objects.return_value = [mock_obj3]
    mock_minio3.get_object.return_value = SimpleNamespace(
        read=lambda: buf3.getvalue(), close=lambda: None, release_conn=lambda: None
    )

    with pytest.raises(ValueError, match="partial nulls"):
        gold_obs.process_month_observations(
            minio_client=mock_minio3,
            year=2024,
            month=1,
            loc_map=loc_map,
            source_map=source_map,
            station_map=station_map,
        )


# ---------------------------------------------------------------------------
# Defect 2: Location-Filtered Rerun Idempotency (Integration Test)
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_location_filtered_rerun_preserves_other_locations(minio_client, pg_connection):
    """Publish HAN and PTO, rerun for HAN only, assert both remain in Gold and DW."""
    # Ensure seed dimensions are present
    schema_sql = """
        INSERT INTO dim_date (
            date_key, full_date, day_of_week, day_name, day_of_month,
            month, month_name, quarter, year, is_weekend, is_rainy_season
        ) VALUES (
            20240601, '2024-06-01', 6, 'Thứ Bảy', 1,
            6, 'Tháng 6', 2, 2024, true, true
        ) ON CONFLICT (date_key) DO NOTHING;
    """
    with pg_connection.cursor() as cur:
        cur.execute(schema_sql)
    pg_connection.commit()

    # Create synthetic initial month dataframe with HAN and PTO
    df_initial = pd.DataFrame(
        [
            {
                "date_key": 20240601,
                "time_key": 700,
                "location_key": 1,  # HAN
                "source_key": 1,  # Open-Meteo
                "station_key": None,
                "location_code": "HAN",
                "source_name": "Open-Meteo",
                "timestamp_utc": pd.Timestamp("2024-06-01 00:00:00", tz="UTC"),
                "timestamp_local": pd.Timestamp("2024-06-01 07:00:00"),
                "temperature_c": 28.0,
                "humidity_percent": 80.0,
                "precipitation_mm": 0.0,
                "wind_speed_ms": 2.0,
                "wind_direction_deg": 90.0,
                "pressure_hpa": 1010.0,
                "cloud_cover_percent": 20.0,
                "solar_radiation_wm2": 300.0,
                "pressure_type": "surface",
            },
            {
                "date_key": 20240601,
                "time_key": 700,
                "location_key": 2,  # PTO
                "source_key": 1,
                "station_key": None,
                "location_code": "PTO",
                "source_name": "Open-Meteo",
                "timestamp_utc": pd.Timestamp("2024-06-01 00:00:00", tz="UTC"),
                "timestamp_local": pd.Timestamp("2024-06-01 07:00:00"),
                "temperature_c": 27.5,
                "humidity_percent": 85.0,
                "precipitation_mm": 0.5,
                "wind_speed_ms": 1.5,
                "wind_direction_deg": 100.0,
                "pressure_hpa": 1009.0,
                "cloud_cover_percent": 40.0,
                "solar_radiation_wm2": 250.0,
                "pressure_type": "surface",
            },
        ]
    )

    # 1. Publish initial state (both HAN and PTO)
    gold_obs.save_gold_parquet(
        minio_client=minio_client,
        df=df_initial,
        year=2024,
        month=6,
        overwrite=True,
    )
    gold_obs.load_dwh_fact_observations(pg_connection, df_initial)

    # 2. Prepare rerun for HAN only (with updated temperature 29.5)
    df_han_update = pd.DataFrame(
        [
            {
                "date_key": 20240601,
                "time_key": 700,
                "location_key": 1,
                "source_key": 1,
                "station_key": None,
                "location_code": "HAN",
                "source_name": "Open-Meteo",
                "timestamp_utc": pd.Timestamp("2024-06-01 00:00:00", tz="UTC"),
                "timestamp_local": pd.Timestamp("2024-06-01 07:00:00"),
                "temperature_c": 29.5,
                "humidity_percent": 78.0,
                "precipitation_mm": 0.0,
                "wind_speed_ms": 2.5,
                "wind_direction_deg": 90.0,
                "pressure_hpa": 1010.0,
                "cloud_cover_percent": 20.0,
                "solar_radiation_wm2": 350.0,
                "pressure_type": "surface",
            }
        ]
    )

    # Rerun save_gold_parquet with location_filter="HAN"
    gold_obs.save_gold_parquet(
        minio_client=minio_client,
        df=df_han_update,
        year=2024,
        month=6,
        overwrite=True,
        location_filter="HAN",
    )
    gold_obs.load_dwh_fact_observations(pg_connection, df_han_update)

    # 3. Assert Gold Parquet in MinIO still contains both HAN and PTO
    gold_key = "fact_weather_observation/year=2024/month=06/fact_weather_observation_202406.parquet"
    resp = minio_client.get_object("gold", gold_key)
    try:
        df_gold_read = pd.read_parquet(io.BytesIO(resp.read()))
    finally:
        resp.close()
        resp.release_conn()

    assert len(df_gold_read) == 2
    assert set(df_gold_read["location_code"]) == {"HAN", "PTO"}
    han_row = df_gold_read[df_gold_read["location_code"] == "HAN"].iloc[0]
    assert han_row["temperature_c"] == 29.5
    pto_row = df_gold_read[df_gold_read["location_code"] == "PTO"].iloc[0]
    assert pto_row["temperature_c"] == 27.5

    # 4. Assert PostgreSQL DW table still contains both HAN and PTO
    with pg_connection.cursor() as cur:
        cur.execute(
            """
            SELECT l.location_code, f.temperature_c
            FROM fact_weather_observation f
            JOIN dim_location l ON f.location_key = l.location_key
            WHERE f.date_key = 20240601 AND f.time_key = 700;
            """
        )
        db_rows = dict(cur.fetchall())

    assert set(db_rows.keys()) == {"HAN", "PTO"}
    assert float(db_rows["HAN"]) == 29.5
    assert float(db_rows["PTO"]) == 27.5


# ---------------------------------------------------------------------------
# Defect 5: SQL Transaction Recovery Between Months (Integration Test)
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_sql_transaction_recovery_on_failed_month(pg_connection):
    """Verify conn.rollback() recovers transaction state so Month 2 succeeds after Month 1 fails."""
    # Ensure seed date is present
    with pg_connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dim_date (
                date_key, full_date, day_of_week, day_name, day_of_month,
                month, month_name, quarter, year, is_weekend, is_rainy_season
            ) VALUES
                (20240701, '2024-07-01', 1, 'Thứ Hai', 1, 7, 'Tháng 7', 3, 2024, false, true),
                (20240801, '2024-08-01', 4, 'Thứ Năm', 1, 8, 'Tháng 8', 3, 2024, false, true)
            ON CONFLICT (date_key) DO NOTHING;
            """
        )
    pg_connection.commit()

    # Month 1 dataframe has invalid location_key=9999 (violates foreign key constraint)
    df_month1_invalid = pd.DataFrame(
        [
            {
                "date_key": 20240701,
                "time_key": 0,
                "location_key": 9999,  # Invalid FK
                "source_key": 1,
                "station_key": None,
                "timestamp_utc": pd.Timestamp("2024-07-01 00:00:00", tz="UTC"),
                "timestamp_local": pd.Timestamp("2024-07-01 07:00:00"),
                "temperature_c": 25.0,
                "humidity_percent": 80.0,
                "pressure_hpa": 1010.0,
                "precipitation_mm": 0.0,
                "wind_speed_ms": 2.0,
                "wind_direction_deg": 90.0,
                "cloud_cover_percent": 20.0,
                "solar_radiation_wm2": 0.0,
            }
        ]
    )

    # Month 2 dataframe has valid location_key=1
    df_month2_valid = pd.DataFrame(
        [
            {
                "date_key": 20240801,
                "time_key": 0,
                "location_key": 1,
                "source_key": 1,
                "station_key": None,
                "timestamp_utc": pd.Timestamp("2024-08-01 00:00:00", tz="UTC"),
                "timestamp_local": pd.Timestamp("2024-08-01 07:00:00"),
                "temperature_c": 26.0,
                "humidity_percent": 82.0,
                "pressure_hpa": 1011.0,
                "precipitation_mm": 0.0,
                "wind_speed_ms": 2.2,
                "wind_direction_deg": 100.0,
                "cloud_cover_percent": 25.0,
                "solar_radiation_wm2": 0.0,
            }
        ]
    )

    # Load Month 1 -> fails with ForeignKeyViolation, should be rolled back
    with pytest.raises(Exception):
        gold_obs.load_dwh_fact_observations(pg_connection, df_month1_invalid)

    # Connection should be clean (NOT InFailedSqlTransaction)
    # Load Month 2 -> should succeed!
    loaded = gold_obs.load_dwh_fact_observations(pg_connection, df_month2_valid)
    assert loaded == 1

    with pg_connection.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM fact_weather_observation WHERE date_key = 20240801 AND time_key = 0;"
        )
        assert cur.fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Re-review Probe 1: S3Error NoSuchKey on listed Silver object blocks curation
# ---------------------------------------------------------------------------
def test_rereview_nosuchkey_during_read_raises_silver_read_error():
    """list_objects returns good.parquet and missing.parquet.

    Reading missing.parquet raises S3Error('NoSuchKey') -> must raise SilverReadError,
    not skip the file and return an incomplete dataset.
    """
    # Prepare a valid Parquet buffer for good.parquet
    df_good = pd.DataFrame(
        [
            {
                "source_name": "Open-Meteo",
                "location_code": "HAN",
                "timestamp_utc": pd.Timestamp("2024-09-01 10:00:00", tz="UTC"),
                "temperature_c": 30.0,
                "humidity_percent": 70.0,
                "precipitation_mm": 0.0,
            }
        ]
    )
    buf_good = io.BytesIO()
    df_good.to_parquet(buf_good, index=False)
    good_bytes = buf_good.getvalue()

    nosuchkey_err = S3Error(
        "NoSuchKey",
        "The specified key does not exist.",
        "silver/weather_hourly/year=2024/month=09/missing.parquet",
        "req-id-001",
        "host-id-001",
        None,
    )

    # Two listed objects
    mock_obj_good = Mock()
    mock_obj_good.object_name = "weather_hourly/year=2024/month=09/good.parquet"
    mock_obj_missing = Mock()
    mock_obj_missing.object_name = "weather_hourly/year=2024/month=09/missing.parquet"

    mock_minio = Mock()
    mock_minio.list_objects.return_value = [mock_obj_good, mock_obj_missing]

    def _get_object(_bucket, key):
        if "good" in key:
            return SimpleNamespace(
                read=lambda: good_bytes,
                close=lambda: None,
                release_conn=lambda: None,
            )
        raise nosuchkey_err

    mock_minio.get_object.side_effect = _get_object

    loc_map = {"HAN": 1}
    source_map = {"Open-Meteo": 1}
    station_map = {}

    with pytest.raises(gold_obs.SilverReadError) as exc_info:
        gold_obs.process_month_observations(
            minio_client=mock_minio,
            year=2024,
            month=9,
            loc_map=loc_map,
            source_map=source_map,
            station_map=station_map,
        )

    assert "missing.parquet" in exc_info.value.key
    assert isinstance(exc_info.value.original_error, S3Error)


# ---------------------------------------------------------------------------
# Re-review Probe 3: Naive timestamp rejected
# ---------------------------------------------------------------------------
def test_rereview_naive_timestamp_rejected():
    """Naive timestamp '2024-09-01 16:00:00' (no tz) in Silver must fail validation."""
    df_naive = pd.DataFrame(
        [
            {
                "source_name": "Open-Meteo",
                "location_code": "HAN",
                # Naive datetime — no timezone info
                "timestamp_utc": pd.Timestamp("2024-09-01 16:00:00"),
                "temperature_c": 30.0,
            }
        ]
    )
    buf = io.BytesIO()
    df_naive.to_parquet(buf, index=False)
    naive_bytes = buf.getvalue()

    mock_minio = Mock()
    mock_obj = Mock()
    mock_obj.object_name = "weather_hourly/year=2024/month=09/naive.parquet"
    mock_minio.list_objects.return_value = [mock_obj]
    mock_minio.get_object.return_value = SimpleNamespace(
        read=lambda: naive_bytes, close=lambda: None, release_conn=lambda: None
    )

    loc_map = {"HAN": 1}
    source_map = {"Open-Meteo": 1}
    station_map = {}

    with pytest.raises(ValueError, match="timezone-naive"):
        gold_obs.process_month_observations(
            minio_client=mock_minio,
            year=2024,
            month=9,
            loc_map=loc_map,
            source_map=source_map,
            station_map=station_map,
        )


# ---------------------------------------------------------------------------
# Re-review Probe 4: Out-of-month aware timestamp rejected
# ---------------------------------------------------------------------------
def test_rereview_out_of_month_aware_timestamp_rejected():
    """Aware timestamp 2024-10-01T00:00:00Z during 2024-09 curation must fail validation."""
    df_out_of_month = pd.DataFrame(
        [
            {
                "source_name": "Open-Meteo",
                "location_code": "HAN",
                # October 1st 00:00 UTC — not in September
                "timestamp_utc": pd.Timestamp("2024-10-01 00:00:00", tz="UTC"),
                "temperature_c": 28.0,
            }
        ]
    )
    buf = io.BytesIO()
    df_out_of_month.to_parquet(buf, index=False)
    oom_bytes = buf.getvalue()

    mock_minio = Mock()
    mock_obj = Mock()
    mock_obj.object_name = "weather_hourly/year=2024/month=09/october_leak.parquet"
    mock_minio.list_objects.return_value = [mock_obj]
    mock_minio.get_object.return_value = SimpleNamespace(
        read=lambda: oom_bytes, close=lambda: None, release_conn=lambda: None
    )

    loc_map = {"HAN": 1}
    source_map = {"Open-Meteo": 1}
    station_map = {}

    with pytest.raises(ValueError, match="outside the requested UTC month"):
        gold_obs.process_month_observations(
            minio_client=mock_minio,
            year=2024,
            month=9,
            loc_map=loc_map,
            source_map=source_map,
            station_map=station_map,
        )


# ---------------------------------------------------------------------------
# Re-review Probe 2: Retracted Silver row removed from Gold and DW
# ---------------------------------------------------------------------------
@pytest.mark.integration
def test_rereview_retracted_silver_row_removed_from_gold_and_dw(minio_client, pg_connection):
    """Publish HAN@16:00, HAN@17:00, PTO@16:00.  Remove HAN@17:00 from Silver, rerun HAN.

    After rerun, both Gold and DW must contain exactly 2 keys:
    HAN@16:00 and PTO@16:00.  HAN@17:00 must be gone from both sinks.
    """
    from datetime import datetime as dt
    from datetime import timezone as tz

    # Seed dim_date for the two local dates:
    # 2024-09-01 16:00 UTC = 2024-09-01 23:00 local -> date_key 20240901
    # 2024-09-01 17:00 UTC = 2024-09-02 00:00 local -> date_key 20240902
    with pg_connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dim_date (
                date_key, full_date, day_of_week, day_name, day_of_month,
                month, month_name, quarter, year, is_weekend, is_rainy_season
            ) VALUES
                (20240901, '2024-09-01', 7, 'Chủ Nhật', 1, 9, 'Tháng 9', 3, 2024, true, true),
                (20240902, '2024-09-02', 1, 'Thứ Hai', 2, 9, 'Tháng 9', 3, 2024, false, true)
            ON CONFLICT (date_key) DO NOTHING;
            """
        )
    pg_connection.commit()

    # --- Phase 1: Publish all 3 observations ---
    df_all = pd.DataFrame(
        [
            {
                "date_key": 20240901,
                "time_key": 2300,  # 16:00 UTC -> 23:00 local
                "location_key": 1,  # HAN
                "source_key": 1,  # Open-Meteo
                "station_key": None,
                "location_code": "HAN",
                "source_name": "Open-Meteo",
                "timestamp_utc": pd.Timestamp("2024-09-01 16:00:00", tz="UTC"),
                "timestamp_local": pd.Timestamp("2024-09-01 23:00:00"),
                "temperature_c": 28.0,
                "humidity_percent": 80.0,
                "precipitation_mm": 0.0,
                "wind_speed_ms": 2.0,
                "wind_direction_deg": 90.0,
                "pressure_hpa": 1010.0,
                "cloud_cover_percent": 20.0,
                "solar_radiation_wm2": 0.0,
            },
            {
                "date_key": 20240902,
                "time_key": 0,  # 17:00 UTC -> 00:00 local
                "location_key": 1,  # HAN
                "source_key": 1,
                "station_key": None,
                "location_code": "HAN",
                "source_name": "Open-Meteo",
                "timestamp_utc": pd.Timestamp("2024-09-01 17:00:00", tz="UTC"),
                "timestamp_local": pd.Timestamp("2024-09-02 00:00:00"),
                "temperature_c": 27.0,
                "humidity_percent": 82.0,
                "precipitation_mm": 1.0,
                "wind_speed_ms": 1.5,
                "wind_direction_deg": 100.0,
                "pressure_hpa": 1009.0,
                "cloud_cover_percent": 40.0,
                "solar_radiation_wm2": 0.0,
            },
            {
                "date_key": 20240901,
                "time_key": 2300,
                "location_key": 2,  # PTO
                "source_key": 1,
                "station_key": None,
                "location_code": "PTO",
                "source_name": "Open-Meteo",
                "timestamp_utc": pd.Timestamp("2024-09-01 16:00:00", tz="UTC"),
                "timestamp_local": pd.Timestamp("2024-09-01 23:00:00"),
                "temperature_c": 26.0,
                "humidity_percent": 75.0,
                "precipitation_mm": 0.0,
                "wind_speed_ms": 1.0,
                "wind_direction_deg": 80.0,
                "pressure_hpa": 1011.0,
                "cloud_cover_percent": 10.0,
                "solar_radiation_wm2": 0.0,
            },
        ]
    )

    gold_obs.save_gold_parquet(
        minio_client=minio_client, df=df_all, year=2024, month=9, overwrite=True
    )
    gold_obs.load_dwh_fact_observations(
        pg_connection,
        df_all,
        source_keys=[1],
        location_keys=[1, 2],
        month_start_utc=dt(2024, 9, 1, tzinfo=tz.utc),
        next_month_start_utc=dt(2024, 10, 1, tzinfo=tz.utc),
    )

    # Verify initial state: 3 rows in DW
    with pg_connection.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM fact_weather_observation
            WHERE timestamp_utc >= '2024-09-01T00:00:00Z'
              AND timestamp_utc < '2024-10-01T00:00:00Z';
            """
        )
        assert cur.fetchone()[0] == 3

    # --- Phase 2: Rerun for HAN only, with HAN@17:00 removed ---
    df_han_rerun = pd.DataFrame(
        [
            {
                "date_key": 20240901,
                "time_key": 2300,
                "location_key": 1,
                "source_key": 1,
                "station_key": None,
                "location_code": "HAN",
                "source_name": "Open-Meteo",
                "timestamp_utc": pd.Timestamp("2024-09-01 16:00:00", tz="UTC"),
                "timestamp_local": pd.Timestamp("2024-09-01 23:00:00"),
                "temperature_c": 28.0,
                "humidity_percent": 80.0,
                "precipitation_mm": 0.0,
                "wind_speed_ms": 2.0,
                "wind_direction_deg": 90.0,
                "pressure_hpa": 1010.0,
                "cloud_cover_percent": 20.0,
                "solar_radiation_wm2": 0.0,
            }
        ]
    )

    # Gold: merge with filter so PTO is preserved
    gold_obs.save_gold_parquet(
        minio_client=minio_client,
        df=df_han_rerun,
        year=2024,
        month=9,
        overwrite=True,
        location_filter="HAN",
    )
    # DW: transactional retraction scoped to HAN only
    gold_obs.load_dwh_fact_observations(
        pg_connection,
        df_han_rerun,
        source_keys=[1],
        location_keys=[1],  # HAN only
        month_start_utc=dt(2024, 9, 1, tzinfo=tz.utc),
        next_month_start_utc=dt(2024, 10, 1, tzinfo=tz.utc),
    )

    # --- Phase 3: Verify Gold Parquet ---
    gold_key = "fact_weather_observation/year=2024/month=09/fact_weather_observation_202409.parquet"
    resp = minio_client.get_object("gold", gold_key)
    try:
        df_gold = pd.read_parquet(io.BytesIO(resp.read()))
    finally:
        resp.close()
        resp.release_conn()

    assert len(df_gold) == 2, f"Expected 2 rows in Gold, got {len(df_gold)}"
    gold_keys = set(
        zip(
            df_gold["location_code"],
            df_gold["timestamp_utc"].astype(str),
        )
    )
    assert ("HAN", "2024-09-01 16:00:00+00:00") in gold_keys
    assert ("PTO", "2024-09-01 16:00:00+00:00") in gold_keys
    # HAN@17:00 must be gone
    assert ("HAN", "2024-09-01 17:00:00+00:00") not in gold_keys

    # --- Phase 4: Verify DW ---
    with pg_connection.cursor() as cur:
        cur.execute(
            """
            SELECT l.location_code, f.timestamp_utc
            FROM fact_weather_observation f
            JOIN dim_location l ON f.location_key = l.location_key
            WHERE f.timestamp_utc >= '2024-09-01T00:00:00Z'
              AND f.timestamp_utc < '2024-10-01T00:00:00Z'
            ORDER BY l.location_code, f.timestamp_utc;
            """
        )
        dw_rows = cur.fetchall()

    # Exactly 2 rows: HAN@16:00, PTO@16:00
    assert len(dw_rows) == 2, f"Expected 2 rows in DW, got {len(dw_rows)}: {dw_rows}"
    dw_keys = {(row[0], row[1].strftime("%Y-%m-%dT%H:%M:%SZ")) for row in dw_rows}
    assert ("HAN", "2024-09-01T16:00:00Z") in dw_keys
    assert ("PTO", "2024-09-01T16:00:00Z") in dw_keys
    # HAN@17:00 must be retracted
    assert ("HAN", "2024-09-01T17:00:00Z") not in dw_keys
