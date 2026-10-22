"""Unit tests for Meteostat & Open-Meteo Silver transformations, quality checks, and ingestion.

Tests all requirements using mock objects and small synthetic fixtures:
- Conflict detection and order-invariance on grain (source x location x timestamp_utc)
- Hourly grid coverage on [start, end) including leap year 2024 and empty data
- Range validation without clipping (negative rain -> NaN, out-of-bounds humidity -> NaN)
- Pressure type differentiation (surface vs sea_level) and provenance
- Station synchronization and unmapped location validation
- Error classification: S3 AccessDenied vs NoSuchKey, HTTP 404 handling, non-zero exit status
- Parquet round-trip consistency
"""

import io
import json
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
from minio.error import S3Error
from quality_checks import (
    compute_hourly_coverage,
    resolve_duplicates_with_quarantine,
    validate_and_nullify_ranges,
)
from station_config import (
    CANONICAL_LOCATIONS,
    STATION_REGISTRY,
    get_canonical_stations,
    validate_station_location_mapping,
)
from transform_meteostat_to_silver import (
    clean_and_standardize_meteostat,
    is_s3_not_found_error,
)
from transform_open_meteo_to_silver import clean_and_standardize_open_meteo


# -----------------------------------------------------------------------------
# 1. Meteostat station uniqueness & grain conflict tests
# -----------------------------------------------------------------------------
def test_meteostat_station_dedup_and_grain_conflict():
    """Conflicting records for the same (source, location, timestamp_utc) must be quarantined.

    Verifies that differing values (e.g. from two stations or sensor disagreement)
    are detected as conflicting and not silently resolved by keeping the first or averaging.
    """
    key_cols = ["source_name", "location_code", "timestamp_utc"]
    measurement_cols = ["temperature_c", "precipitation_mm"]

    ts = pd.Timestamp("2024-05-01 12:00:00", tz="UTC")
    df = pd.DataFrame(
        [
            {
                "source_name": "Meteostat",
                "station_id": "48825",
                "location_code": "HPH",
                "timestamp_utc": ts,
                "temperature_c": 26.5,
                "precipitation_mm": 1.0,
            },
            {
                "source_name": "Meteostat",
                "station_id": "48826",
                "location_code": "HPH",
                "timestamp_utc": ts,
                "temperature_c": 28.0,
                "precipitation_mm": 5.0,
            },
        ]
    )

    clean_df, quarantine_df, stats = resolve_duplicates_with_quarantine(
        df=df,
        key_cols=key_cols,
        measurement_cols=measurement_cols,
    )

    # Both conflicting rows must be quarantined; 0 published to clean
    assert len(clean_df) == 0
    assert len(quarantine_df) == 2
    assert stats["conflicting_groups"] == 1
    assert stats["conflicting_duplicates_count"] == 2
    assert stats["identical_duplicates_removed"] == 0
    # Station IDs and provenance are retained in quarantined data
    assert set(quarantine_df["station_id"]) == {"48825", "48826"}


def test_duplicate_precipitation_1_and_9_order_invariant():
    """Conflicting duplicates with precipitation 1.0 and 9.0 must be quarantined order-invariantly."""
    key_cols = ["source_name", "location_code", "timestamp_utc"]
    measurement_cols = ["precipitation_mm"]

    ts = pd.Timestamp("2024-01-01 01:00:00", tz="UTC")
    row_a = {
        "source_name": "Open-Meteo",
        "location_code": "HAN",
        "timestamp_utc": ts,
        "precipitation_mm": 1.0,
    }
    row_b = {
        "source_name": "Open-Meteo",
        "location_code": "HAN",
        "timestamp_utc": ts,
        "precipitation_mm": 9.0,
    }

    df_order_1 = pd.DataFrame([row_a, row_b])
    clean_1, quar_1, stats_1 = resolve_duplicates_with_quarantine(
        df_order_1, key_cols, measurement_cols
    )

    df_order_2 = pd.DataFrame([row_b, row_a])
    clean_2, quar_2, stats_2 = resolve_duplicates_with_quarantine(
        df_order_2, key_cols, measurement_cols
    )

    # Order inversion must yield identical results: 0 clean, 2 quarantined
    assert len(clean_1) == 0
    assert len(clean_2) == 0
    assert len(quar_1) == 2
    assert len(quar_2) == 2
    assert stats_1["conflicting_groups"] == stats_2["conflicting_groups"] == 1


def test_identical_duplicates_are_deduplicated_deterministically():
    """Identical duplicates are deterministically collapsed into a single clean row."""
    key_cols = ["source_name", "location_code", "timestamp_utc"]
    measurement_cols = ["temperature_c", "precipitation_mm"]

    ts = pd.Timestamp("2024-06-01 00:00:00", tz="UTC")
    df = pd.DataFrame(
        [
            {
                "source_name": "Meteostat",
                "location_code": "HAN",
                "timestamp_utc": ts,
                "temperature_c": 30.0,
                "precipitation_mm": 0.0,
            },
            {
                "source_name": "Meteostat",
                "location_code": "HAN",
                "timestamp_utc": ts,
                "temperature_c": 30.0,
                "precipitation_mm": 0.0,
            },
        ]
    )

    clean_df, quarantine_df, stats = resolve_duplicates_with_quarantine(
        df=df,
        key_cols=key_cols,
        measurement_cols=measurement_cols,
    )

    assert len(clean_df) == 1
    assert len(quarantine_df) == 0
    assert stats["identical_duplicates_removed"] == 1
    assert stats["conflicting_duplicates_count"] == 0


# -----------------------------------------------------------------------------
# 2. Expected hourly grid coverage & missing hours tests
# -----------------------------------------------------------------------------
def test_coverage_file_with_only_two_hours_in_2024():
    """A file with only two consecutive hours in 2024 must NOT report 0 missing hours.

    Year 2024 is a leap year with 366 days * 24 = 8,784 hours.
    Two hours must report 8,782 missing hours and ~0.02% coverage.
    """
    start_window = "2024-01-01 00:00:00+00:00"
    end_window = "2025-01-01 00:00:00+00:00"

    df_two_hours = pd.DataFrame(
        {
            "timestamp_utc": [
                pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 01:00:00", tz="UTC"),
            ],
            "temperature_c": [20.0, 20.5],
        }
    )

    cov = compute_hourly_coverage(
        df=df_two_hours,
        start_time=start_window,
        end_time=end_window,
        measurement_cols=["temperature_c"],
    )

    assert cov["expected_hours"] == 8784
    assert cov["present_expected_hours"] == 2
    assert cov["missing_hours"] == 8782
    assert cov["record_coverage_pct"] < 0.1
    assert cov["end_gap_hours"] == 8782


def test_coverage_gap_at_start_and_end_of_year():
    """Missing hours at the start and end of interval are correctly counted."""
    # Window: 24 hours of 2024-01-01
    start_window = "2024-01-01 00:00:00+00:00"
    end_window = "2024-01-02 00:00:00+00:00"

    # Data only present from 05:00 to 20:00 (16 hours present)
    # Start gap: 00:00 to 04:00 (5 hours)
    # End gap: 21:00 to 23:00 (3 hours)
    # Total missing = 8 hours
    ts_range = pd.date_range("2024-01-01 05:00:00", "2024-01-01 20:00:00", freq="h", tz="UTC")
    df = pd.DataFrame({"timestamp_utc": ts_range})

    cov = compute_hourly_coverage(df, start_window, end_window)

    assert cov["expected_hours"] == 24
    assert cov["present_expected_hours"] == 16
    assert cov["missing_hours"] == 8
    assert cov["start_gap_hours"] == 5
    assert cov["end_gap_hours"] == 3


def test_coverage_mid_interval_gaps():
    """Gaps inside the interval are detected."""
    start_window = "2024-01-01 00:00:00+00:00"
    end_window = "2024-01-01 10:00:00+00:00"  # 10 expected hours

    # Omit hours 03:00 and 04:00
    hours = [0, 1, 2, 5, 6, 7, 8, 9]
    ts_list = [pd.Timestamp(f"2024-01-01 {h:02d}:00:00", tz="UTC") for h in hours]
    df = pd.DataFrame({"timestamp_utc": ts_list})

    cov = compute_hourly_coverage(df, start_window, end_window)

    assert cov["expected_hours"] == 10
    assert cov["present_expected_hours"] == 8
    assert cov["missing_hours"] == 2
    assert "2024-01-01T03:00:00+00:00" in cov["sample_missing_timestamps"]
    assert "2024-01-01T04:00:00+00:00" in cov["sample_missing_timestamps"]


def test_coverage_empty_data():
    """Empty DataFrame is handled cleanly without exceptions, reporting 100% missing."""
    start_window = "2024-01-01 00:00:00+00:00"
    end_window = "2024-01-02 00:00:00+00:00"

    cov = compute_hourly_coverage(pd.DataFrame(), start_window, end_window, ["temperature_c"])
    assert cov["expected_hours"] == 24
    assert cov["actual_records"] == 0
    assert cov["missing_hours"] == 24
    assert cov["record_coverage_pct"] == 0.0


def test_coverage_leap_year_2024_vs_non_leap_2023():
    """Leap year 2024 has 8784 hours, non-leap year 2023 has 8760 hours."""
    cov_2024 = compute_hourly_coverage(
        pd.DataFrame(), "2024-01-01 00:00:00+00:00", "2025-01-01 00:00:00+00:00"
    )
    cov_2023 = compute_hourly_coverage(
        pd.DataFrame(), "2023-01-01 00:00:00+00:00", "2024-01-01 00:00:00+00:00"
    )

    assert cov_2024["expected_hours"] == 8784
    assert cov_2023["expected_hours"] == 8760


def test_record_coverage_differs_from_value_completeness():
    """Sufficient timestamp records does not equate to complete non-null data."""
    start_window = "2024-01-01 00:00:00+00:00"
    end_window = "2024-01-01 04:00:00+00:00"  # 4 hours

    ts_list = pd.date_range("2024-01-01 00:00:00", "2024-01-01 03:00:00", freq="h", tz="UTC")
    df = pd.DataFrame(
        {
            "timestamp_utc": ts_list,
            # Only 1 out of 4 rows has precipitation
            "precipitation_mm": [0.0, np.nan, np.nan, np.nan],
        }
    )

    cov = compute_hourly_coverage(df, start_window, end_window, ["precipitation_mm"])

    # Record presence coverage is 100%
    assert cov["record_coverage_pct"] == 100.0
    assert cov["missing_hours"] == 0

    # Value completeness for precipitation is 25%
    prcp_stat = cov["value_completeness"]["precipitation_mm"]
    assert prcp_stat["non_null_count"] == 1
    assert prcp_stat["null_count"] == 3
    assert prcp_stat["completeness_pct_of_expected"] == 25.0


# -----------------------------------------------------------------------------
# 3. Range validation tests (no clipping)
# -----------------------------------------------------------------------------
def test_range_validation_nullifies_negative_rain():
    """Negative precipitation must be set to NaN, never clipped to 0.0 ('no rain')."""
    df = pd.DataFrame(
        {
            "precipitation_mm": [-1.5, 0.0, 5.2, -0.1],
            "humidity_percent": [50.0, 80.0, 90.0, 70.0],
        }
    )

    clean_df, violations = validate_and_nullify_ranges(df)

    # Negative values must become NaN
    assert np.isnan(clean_df.loc[0, "precipitation_mm"])
    assert np.isnan(clean_df.loc[3, "precipitation_mm"])
    assert clean_df.loc[1, "precipitation_mm"] == 0.0
    assert clean_df.loc[2, "precipitation_mm"] == 5.2
    assert violations["negative_precipitation"] == 2


def test_range_validation_nullifies_out_of_range_humidity_and_cloud():
    """Humidity and cloud cover outside [0, 100] must be set to NaN, not clipped."""
    df = pd.DataFrame(
        {
            "humidity_percent": [-5.0, 50.0, 100.0, 105.0],
            "cloud_cover_percent": [0.0, 120.0, 80.0, -10.0],
        }
    )

    clean_df, violations = validate_and_nullify_ranges(df)

    assert np.isnan(clean_df.loc[0, "humidity_percent"])
    assert np.isnan(clean_df.loc[3, "humidity_percent"])
    assert clean_df.loc[1, "humidity_percent"] == 50.0
    assert clean_df.loc[2, "humidity_percent"] == 100.0
    assert violations["out_of_range_humidity"] == 2

    assert np.isnan(clean_df.loc[1, "cloud_cover_percent"])
    assert np.isnan(clean_df.loc[3, "cloud_cover_percent"])
    assert violations["out_of_range_cloud_cover"] == 2


# -----------------------------------------------------------------------------
# 4. Pressure type and provenance distinction tests
# -----------------------------------------------------------------------------
def test_pressure_type_and_provenance():
    """Open-Meteo has pressure_type='surface', Meteostat has pressure_type='sea_level'."""
    # Open-Meteo payload
    open_meteo_raw = {
        "hourly": {
            "time": ["2024-01-01T00:00"],
            "temperature_2m": [22.0],
            "relative_humidity_2m": [80.0],
            "surface_pressure": [1008.5],
            "precipitation": [0.0],
            "wind_speed_10m": [3.2],
            "wind_direction_10m": [120.0],
            "cloud_cover": [40.0],
            "shortwave_radiation": [0.0],
        }
    }
    om_clean, _, _ = clean_and_standardize_open_meteo(
        raw_json=open_meteo_raw,
        location_code="HAN",
        start_time="2024-01-01 00:00:00+00:00",
        end_time="2024-01-01 01:00:00+00:00",
    )

    assert om_clean.loc[0, "pressure_type"] == "surface"
    assert om_clean.loc[0, "pressure_hpa"] == 1008.5
    assert om_clean.loc[0, "observation_type"] == "model_reanalysis"

    # Meteostat CSV payload
    meteostat_csv = (
        b"year,month,day,hour,temp,temp_source,rhum,rhum_source,pres,pres_source\n"
        b"2024,1,1,0,21.5,isd_lite,85,isd_lite,1015.2,metar\n"
    )
    ms_clean, _, _ = clean_and_standardize_meteostat(
        raw_csv_bytes=meteostat_csv,
        station_id="48820",
        start_time="2024-01-01 00:00:00+00:00",
        end_time="2024-01-01 01:00:00+00:00",
    )

    assert ms_clean.loc[0, "pressure_type"] == "sea_level"
    assert ms_clean.loc[0, "pressure_hpa"] == 1015.2
    assert ms_clean.loc[0, "station_id"] == "48820"
    assert ms_clean.loc[0, "observation_type"] == "station_reported"


def test_meteostat_model_values_removed_by_source():
    """Model-sourced values are nulled per variable; model-only rows are dropped."""
    csv = (
        b"year,month,day,hour,temp,temp_source,prcp,prcp_source,rhum,rhum_source\n"
        b"2024,1,1,0,21.5,isd_lite,0.4,dwd_mosmix,85,metar\n"
        b"2024,1,1,1,21.0,metno_forecast,1.2,metno_forecast,80,metno_forecast\n"
        b"2024,1,1,2,20.5,isd_lite,,,79,some_new_model\n"
    )
    clean, _, dq = clean_and_standardize_meteostat(
        raw_csv_bytes=csv,
        station_id="48820",
        start_time="2024-01-01 00:00:00+00:00",
        end_time="2024-01-01 03:00:00+00:00",
    )

    # Hour 1 had only model values and is dropped; hours 0 and 2 remain
    assert list(clean["timestamp_utc"].dt.hour) == [0, 2]
    assert clean.loc[0, "temperature_c"] == 21.5
    assert pd.isna(clean.loc[0, "precipitation_mm"])  # dwd_mosmix removed
    assert clean.loc[0, "humidity_percent"] == 85
    assert pd.isna(clean.loc[1, "humidity_percent"])  # unknown source treated as non-observation
    assert (clean["observation_type"] == "station_reported").all()

    sf = dq["source_filter"]
    assert sf["source_columns_present"] is True
    assert sf["model_values_removed"] == {"temp": 1, "rhum": 2, "prcp": 2}
    assert sf["rows_without_observation_dropped"] == 1
    assert dq["hourly_coverage"]["missing_hours"] == 1


def test_meteostat_all_model_payload_yields_no_rows():
    csv = (
        b"year,month,day,hour,temp,temp_source,prcp,prcp_source\n"
        b"2024,1,1,0,21.5,metno_forecast,0.4,metno_forecast\n"
    )
    clean, _, dq = clean_and_standardize_meteostat(
        raw_csv_bytes=csv,
        station_id="48826",
        start_time="2024-01-01 00:00:00+00:00",
        end_time="2024-01-01 01:00:00+00:00",
    )
    assert clean.empty
    assert dq["source_filter"]["rows_without_observation_dropped"] == 1


def test_meteostat_legacy_payload_without_sources_is_unverified():
    csv = b"year,month,day,hour,temp,prcp\n2024,1,1,0,21.5,0.0\n"
    clean, _, dq = clean_and_standardize_meteostat(
        raw_csv_bytes=csv,
        station_id="48820",
        start_time="2024-01-01 00:00:00+00:00",
        end_time="2024-01-01 01:00:00+00:00",
    )
    assert clean.loc[0, "temperature_c"] == 21.5
    assert clean.loc[0, "observation_type"] == "unverified_source"
    assert dq["source_filter"]["source_columns_present"] is False


# -----------------------------------------------------------------------------
# 5. Station configuration synchronization & validation
# -----------------------------------------------------------------------------
def test_all_canonical_stations_have_valid_mappings():
    """Verified stations map 1:1 to canonical locations; locations without one stay unmapped."""
    canonical = get_canonical_stations()
    assert set(canonical) == {"48820", "48826", "48831", "48803"}

    mapped_locations = [s["location_code"] for s in canonical.values()]
    assert len(mapped_locations) == len(set(mapped_locations))
    assert set(mapped_locations) == {"HAN", "HPH", "TNG", "LCA"}
    assert set(mapped_locations) <= CANONICAL_LOCATIONS

    for sid, info in canonical.items():
        is_valid, loc, msg = validate_station_location_mapping(sid)
        assert is_valid, msg
        assert loc in CANONICAL_LOCATIONS
        assert info["latitude"] is not None and info["longitude"] is not None


def test_misassigned_table6_station_ids_are_rejected():
    """Ids whose verified identity is another station must not publish under a research location."""
    for sid in ["48825", "48830", "48838", "48840", "48816"]:
        is_valid, loc, msg = validate_station_location_mapping(sid)
        assert not is_valid
        assert loc is None
        assert "Pending team decision" in msg


def test_unmapped_stations_are_rejected_not_unknown():
    """Stations without confirmed canonical mappings are rejected, not published as UNKNOWN."""
    # 48806 (Sơn La) and 48808 (Cao Bằng)
    for sid in ["48806", "48808"]:
        is_valid, loc, msg = validate_station_location_mapping(sid)
        assert not is_valid
        assert loc is None
        assert "Pending team decision" in msg

    # Transformation must raise ValueError when given an unmapped station
    with pytest.raises(ValueError, match="mapping error"):
        clean_and_standardize_meteostat(
            raw_csv_bytes=b"year,month,day,hour\n2024,1,1,0\n",
            station_id="48806",
            start_time="2024-01-01 00:00:00+00:00",
            end_time="2024-01-01 01:00:00+00:00",
        )


# -----------------------------------------------------------------------------
# 6. Error handling & exit status (mock tests)
# -----------------------------------------------------------------------------
def test_is_s3_not_found_error_distinction():
    """S3Error distinction: NoSuchKey is missing object, AccessDenied is failure.

    Verifies authoritative code attribute or response status 404.
    Rejects server errors that happen to have 404 in their request-id.
    """
    no_such_key = S3Error(
        "NoSuchKey", "The specified key does not exist", "/res", "req", "host", None
    )
    access_denied = S3Error("AccessDenied", "Access Denied", "/res", "req", "host", None)

    assert is_s3_not_found_error(no_such_key) is True
    assert is_s3_not_found_error(access_denied) is False

    class MockResponse404:
        status = 404

    class MockS3Error404(Exception):
        code = None
        response = MockResponse404()

    class MockS3ErrorAccessDenied:
        code = "AccessDenied"

    # Regression: S3Error with '404' in request_id must NOT be classified as not found
    internal_with_404_req_id = S3Error(
        "InternalError", "Backend unavailable", "/object", "request-404-id", "host", None
    )

    assert is_s3_not_found_error(MockS3Error404()) is True
    assert is_s3_not_found_error(MockS3ErrorAccessDenied()) is False
    assert is_s3_not_found_error(internal_with_404_req_id) is False
    assert is_s3_not_found_error(None) is False


def test_ingest_raises_on_access_denied(monkeypatch):
    """Meteostat ingestion raises RuntimeError when MinIO returns AccessDenied."""
    import ingest_meteostat

    client = Mock()
    client.stat_object.side_effect = S3Error(
        "AccessDenied", "Access Denied", "/res", "req", "host", None
    )

    monkeypatch.setattr(ingest_meteostat, "get_minio_client", lambda: client)

    with pytest.raises(RuntimeError, match="completed with 1 failure"):
        ingest_meteostat.ingest_meteostat_data(
            start_year=2024,
            end_year=2024,
            station_ids=["48820"],
        )


def test_ingest_handles_404_as_allowed_skip(monkeypatch):
    """Meteostat ingestion treats upstream HTTP 404 as allowed skip (no data), not failure."""
    import ingest_meteostat

    client = Mock()
    client.stat_object.side_effect = S3Error("NoSuchKey", "missing", "/res", "req", "host", None)

    monkeypatch.setattr(ingest_meteostat, "get_minio_client", lambda: client)
    # Mock HTTP 404 return
    monkeypatch.setattr(
        ingest_meteostat, "fetch_meteostat_station_year", lambda station_id, year: None
    )

    summary = ingest_meteostat.ingest_meteostat_data(
        start_year=2024,
        end_year=2024,
        station_ids=["48820"],
    )

    assert summary["failure_count"] == 0
    assert summary["not_found_count"] == 1
    assert summary["success_count"] == 0


# -----------------------------------------------------------------------------
# 7. Parquet round-trip & small pipeline slice tests
# -----------------------------------------------------------------------------
def test_parquet_round_trip_consistency():
    """Parquet round-trip preserves UTC timestamps, nullable types, and metadata."""
    import pyarrow.parquet as pq

    ts_range = pd.date_range("2024-01-01 00:00:00", periods=5, freq="h", tz="UTC")
    df = pd.DataFrame(
        {
            "source_name": ["Open-Meteo"] * 5,
            "location_code": ["HAN"] * 5,
            "timestamp_utc": ts_range,
            "temperature_c": [20.1, 20.5, np.nan, 21.0, 21.2],
            "precipitation_mm": [0.0, np.nan, 1.2, 0.0, 0.0],
            "pressure_type": ["surface"] * 5,
        }
    )

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)

    table = pq.read_table(buf)
    df_read = table.to_pandas()

    assert len(df_read) == 5
    assert df_read["timestamp_utc"].dt.tz is not None
    assert df_read.loc[2, "temperature_c"] is np.nan or pd.isna(df_read.loc[2, "temperature_c"])
    assert df_read.loc[2, "precipitation_mm"] == 1.2
    assert (df_read["pressure_type"] == "surface").all()


def test_process_open_meteo_to_silver_small_mock_run(monkeypatch, hourly_payload):
    """Small-scope slice verification of Open-Meteo Silver transformer using mock MinIO."""
    import transform_open_meteo_to_silver as om_silver

    client = Mock()
    # Mock bronze read
    payload_bytes = json.dumps(hourly_payload).encode("utf-8")
    client.get_object.return_value = SimpleNamespace(read=lambda: payload_bytes)
    # Mock stat_object to indicate objects don't exist yet
    client.stat_object.side_effect = S3Error("NoSuchKey", "missing", "/res", "req", "host", None)

    uploaded_objects: dict[str, bytes] = {}

    def mock_put(bucket_name, object_name, data, length, content_type):
        uploaded_objects[object_name] = (
            data.getvalue() if hasattr(data, "getvalue") else data.read()
        )

    client.put_object.side_effect = mock_put

    monkeypatch.setattr(om_silver, "get_minio_client", lambda: client)

    dq_result = om_silver.process_open_meteo_to_silver(
        start_year=2024,
        end_year=2024,
        target_location="HAN",
        overwrite=True,
    )

    assert dq_result["total_files_transformed"] == 1
    assert dq_result["total_records_processed"] == 3
    assert len(dq_result["failures"]) == 0

    # Verify at least one monthly parquet was uploaded and is readable by pyarrow
    parquet_keys = [k for k in uploaded_objects if k.endswith(".parquet")]
    assert len(parquet_keys) > 0
    first_parquet = parquet_keys[0]
    table = pq.read_table(io.BytesIO(uploaded_objects[first_parquet]))
    df_res = table.to_pandas()
    assert "pressure_type" in df_res.columns
    assert (df_res["pressure_type"] == "surface").all()
    assert (df_res["location_code"] == "HAN").all()


def test_process_meteostat_to_silver_small_mock_run(monkeypatch):
    """Small-scope slice verification of Meteostat Silver transformer using mock MinIO."""
    import transform_meteostat_to_silver as ms_silver

    raw_csv = (
        b"year,month,day,hour,temp,rhum,pres,prcp,wspd,wdir,cldc,coco\n"
        b"2024,5,1,0,24.0,80,1012.0,0.0,10.0,90,4,1\n"
        b"2024,5,1,1,24.5,78,1011.5,1.5,12.0,95,5,2\n"
        b"2024,5,1,2,25.0,75,1011.0,0.0,8.0,100,3,1\n"
    )

    client = Mock()
    client.get_object.return_value = SimpleNamespace(read=lambda: raw_csv)
    client.stat_object.side_effect = S3Error("NoSuchKey", "missing", "/res", "req", "host", None)

    uploaded_objects: dict[str, bytes] = {}

    def mock_put(bucket_name, object_name, data, length, content_type):
        uploaded_objects[object_name] = (
            data.getvalue() if hasattr(data, "getvalue") else data.read()
        )

    client.put_object.side_effect = mock_put

    monkeypatch.setattr(ms_silver, "get_minio_client", lambda: client)

    dq_result = ms_silver.process_meteostat_to_silver(
        start_year=2024,
        end_year=2024,
        station_ids=["48820"],
        overwrite=True,
    )

    assert dq_result["total_files_transformed"] == 1
    assert dq_result["total_records_processed"] == 3
    assert len(dq_result["failures"]) == 0

    # Verify stations metadata table was created
    assert "stations/stations_metadata_cleaned.parquet" in uploaded_objects
    st_table = pq.read_table(
        io.BytesIO(uploaded_objects["stations/stations_metadata_cleaned.parquet"])
    )
    assert len(st_table) == len(STATION_REGISTRY)

    # Verify monthly parquet table
    parquet_keys = [
        k for k in uploaded_objects if k.startswith("weather_hourly/") and k.endswith(".parquet")
    ]
    assert len(parquet_keys) == 1
    obs_table = pq.read_table(io.BytesIO(uploaded_objects[parquet_keys[0]]))
    df_obs = obs_table.to_pandas()
    assert len(df_obs) == 3
    assert (df_obs["pressure_type"] == "sea_level").all()
    assert (df_obs["station_id"] == "48820").all()
    assert (df_obs["location_code"] == "HAN").all()


def test_process_meteostat_overwrite_removes_stale_silver_months(monkeypatch):
    """With --overwrite, months that no longer have observations lose their old Silver object."""
    import transform_meteostat_to_silver as ms_silver

    raw_csv = (
        b"year,month,day,hour,temp,temp_source,prcp,prcp_source\n"
        b"2024,5,1,0,24.0,isd_lite,0.0,dwd_mosmix\n"
        b"2024,6,1,0,25.0,metno_forecast,1.0,metno_forecast\n"
    )
    existing = {
        "weather_hourly/year=2024/month=05/weather_hourly_meteostat_48820_202405.parquet",
        "weather_hourly/year=2024/month=06/weather_hourly_meteostat_48820_202406.parquet",
    }

    def mock_stat(bucket_name, object_name):
        if object_name in existing:
            return SimpleNamespace(size=1000)
        raise S3Error("NoSuchKey", "missing", "/res", "req", "host", None)

    client = Mock()
    client.get_object.return_value = SimpleNamespace(read=lambda: raw_csv)
    client.stat_object.side_effect = mock_stat
    uploaded: dict[str, bytes] = {}
    client.put_object.side_effect = lambda bucket_name, object_name, data, length, content_type: (
        uploaded.__setitem__(object_name, data.getvalue())
    )
    monkeypatch.setattr(ms_silver, "get_minio_client", lambda: client)

    dq = ms_silver.process_meteostat_to_silver(
        start_year=2024, end_year=2024, station_ids=["48820"], overwrite=True
    )

    removed = [c.args[1] for c in client.remove_object.call_args_list]
    assert removed == [
        "weather_hourly/year=2024/month=06/weather_hourly_meteostat_48820_202406.parquet"
    ]
    assert dq["stale_monthly_parquet_removed"] == 1
    assert dq["total_records_processed"] == 1
    assert dq["failures"] == []
    assert "weather_hourly/year=2024/month=05/weather_hourly_meteostat_48820_202405.parquet" in (
        uploaded
    )


def test_missing_bronze_file_fails_unless_allowed(monkeypatch):
    import transform_open_meteo_to_silver as om_silver

    client = Mock()
    client.get_object.side_effect = S3Error("NoSuchKey", "missing", "/res", "req", "host", None)
    client.stat_object.side_effect = S3Error("NoSuchKey", "missing", "/res", "req", "host", None)
    monkeypatch.setattr(om_silver, "get_minio_client", lambda: client)

    with pytest.raises(RuntimeError, match="encountered 1 failure"):
        om_silver.process_open_meteo_to_silver(2024, 2024, target_location="HAN")
    result = om_silver.process_open_meteo_to_silver(
        2024, 2024, target_location="HAN", allow_missing=True
    )
    assert result["failures"] == [] and result["total_files_transformed"] == 0
