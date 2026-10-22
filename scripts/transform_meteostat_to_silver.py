#!/usr/bin/env python3
"""scripts/transform_meteostat_to_silver.py.

Clean and standardize Meteostat weather station observations from Bronze (CSV)
to Silver (Parquet) according to docs/data-contract.md and docs/NAMING_CONVENTIONS.md.

Silver storage structure:
  silver/weather_hourly/year=YYYY/month=MM/weather_hourly_meteostat_<station_id>_<YYYYMM>.parquet
  silver/stations/stations_metadata_cleaned.parquet
Quality reports:
  silver/quality_reports/dq_report_meteostat_<timestamp>.json
"""

import argparse
import io
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from minio import Minio
from minio.error import S3Error
from quality_checks import (
    compute_hourly_coverage,
    resolve_duplicates_with_quarantine,
    validate_and_nullify_ranges,
)
from station_config import (
    STATION_REGISTRY,
    get_canonical_stations,
    validate_station_location_mapping,
)

# Configure UTF-8 for Windows console
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Load environment variables from .env
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if os.path.exists(env_file):
        with open(env_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip("'\""))

# MinIO configuration
MINIO_HOST = os.getenv("MINIO_EXTERNAL_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")

SILVER_STANDARD_COLUMNS = [
    "source_name",
    "station_id",
    "location_code",
    "timestamp_utc",
    "timestamp_local",
    "temperature_c",
    "humidity_percent",
    "pressure_hpa",
    "pressure_type",
    "precipitation_mm",
    "wind_speed_ms",
    "wind_direction_deg",
    "cloud_cover_percent",
    "solar_radiation_wm2",
    "wind_gusts_ms",
    "weather_condition_code",
    "observation_type",
]

# Meteostat bulk CSVs tag every value with `<variable>_source`. Only these sources are
# station observations; model sources such as `dwd_mosmix` and `metno_forecast` are not.
# Values from any other source (including unknown names) are removed at Silver.
OBSERVATION_SOURCES = frozenset({"isd_lite", "metar", "synop", "national"})
RAW_VARIABLES = ("temp", "rhum", "pres", "prcp", "wspd", "wdir", "cldc", "wpgt", "coco")

MEASUREMENT_COLUMNS = [
    "temperature_c",
    "humidity_percent",
    "pressure_hpa",
    "precipitation_mm",
    "wind_speed_ms",
    "wind_direction_deg",
    "cloud_cover_percent",
    "solar_radiation_wm2",
    "wind_gusts_ms",
]


def get_minio_client() -> Minio:
    """Initialize MinIO client with fallback endpoint."""
    endpoint = MINIO_HOST
    try:
        client = Minio(
            endpoint=endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        if not client.bucket_exists(BUCKET_SILVER):
            client.make_bucket(BUCKET_SILVER)
        return client
    except Exception:
        alt_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        client = Minio(
            endpoint=alt_endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        if not client.bucket_exists(BUCKET_SILVER):
            client.make_bucket(BUCKET_SILVER)
        return client


def is_s3_not_found_error(err: Any) -> bool:
    """Return True if the error corresponds to an S3 'NoSuchKey' / HTTP 404 (object missing).

    Checks authoritative S3 error code or HTTP response status 404.
    Does NOT match arbitrary substrings in messages, resources, or request IDs.
    """
    if err is None:
        return False

    code = getattr(err, "code", None)
    if code in ("NoSuchKey", "NoSuchBucket", "NoSuchObject", "ResourceNotFound"):
        return True

    resp = getattr(err, "response", None)
    if resp is not None:
        status = getattr(resp, "status", getattr(resp, "status_code", None))
        if status == 404:
            return True

    return False


def remove_non_observation_values(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Null every raw value whose `<variable>_source` is not a station observation source.

    Rows left without any raw measurement are dropped, because they carry no observation.
    When a CSV has no `<variable>_source` columns at all (legacy format), values are kept
    and the caller marks them `unverified_source`.

    Returns the filtered frame and statistics for the DQ report.
    """
    present = [v for v in RAW_VARIABLES if v in df.columns]
    source_cols = [f"{v}_source" for v in present if f"{v}_source" in df.columns]
    stats: dict[str, Any] = {
        "source_columns_present": bool(source_cols),
        "observation_sources": sorted(OBSERVATION_SOURCES),
        "values_by_source": {},
        "model_values_removed": {},
        "rows_without_observation_dropped": 0,
    }
    if not source_cols:
        return df, stats

    out = df.copy()
    for var in present:
        src_col = f"{var}_source"
        has_value = out[var].notna()
        if src_col not in out.columns:
            # A value without a source tag cannot be verified as an observation.
            keep = pd.Series(False, index=out.index)
        else:
            keep = out[src_col].isin(OBSERVATION_SOURCES)
            counts = out.loc[has_value, src_col].fillna("missing").value_counts()
            stats["values_by_source"][var] = {str(k): int(v) for k, v in counts.items()}
        removed = has_value & ~keep
        stats["model_values_removed"][var] = int(removed.sum())
        out.loc[removed, var] = np.nan

    no_observation = out[present].isna().all(axis=1)
    stats["rows_without_observation_dropped"] = int(no_observation.sum())
    out = out.loc[~no_observation].reset_index(drop=True)
    return out, stats


def clean_and_standardize_meteostat(
    raw_csv_bytes: bytes,
    station_id: str,
    start_time: datetime | pd.Timestamp | str,
    end_time: datetime | pd.Timestamp | str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Clean and standardize Meteostat raw observations.

    Rules:
    1. Validate station and canonical location mapping before processing.
    2. Parse year/month/day/hour to canonical UTC and Asia/Ho_Chi_Minh local time.
    3. Explicit pressure type: Meteostat 'pres' is sea-level pressure ('sea_level').
    4. Unit conversions: wind speed km/h -> m/s, cloud cover okta -> %.
    5. Range validation without clipping: negative rain or out-of-range humidity set to NaN.
    6. Unique grain deduplication on (source_name, location_code, timestamp_utc):
       identicals are kept deterministically; conflicting records are quarantined.
    7. Expected hourly grid evaluated on [start_time, end_time).
    """
    sid_str = str(station_id).strip()
    is_valid, loc_code, map_msg = validate_station_location_mapping(sid_str)
    if not is_valid or loc_code is None:
        raise ValueError(f"Station {sid_str} mapping error: {map_msg}")

    df = pd.read_csv(io.BytesIO(raw_csv_bytes))
    initial_rows = len(df)

    if df.empty:
        coverage_stats = compute_hourly_coverage(
            df=pd.DataFrame(),
            start_time=start_time,
            end_time=end_time,
            measurement_cols=MEASUREMENT_COLUMNS,
        )
        empty_clean = pd.DataFrame(columns=SILVER_STANDARD_COLUMNS)
        dq_stats = {
            "initial_rows": 0,
            "clean_rows": 0,
            "quarantined_rows": 0,
            "station_id": sid_str,
            "location_code": loc_code,
            "hourly_coverage": coverage_stats,
            "deduplication": {
                "identical_duplicates_removed": 0,
                "conflicting_duplicates_count": 0,
                "conflicting_groups": 0,
                "conflicting_details": [],
            },
            "range_violations": {},
            "provenance_notes": "Empty raw payload.",
        }
        return empty_clean, pd.DataFrame(), dq_stats

    # 1. Remove model-derived values; keep only station observations
    df, source_stats = remove_non_observation_values(df)

    # 1b. Parse timestamps
    df["timestamp_utc"] = pd.to_datetime(df[["year", "month", "day", "hour"]], utc=True)
    df["timestamp_local"] = (
        df["timestamp_utc"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
    )

    # 2. Assign standard grain identifiers
    df["source_name"] = "Meteostat"
    df["station_id"] = sid_str
    df["location_code"] = loc_code

    # 3. Standardize variables and units
    df["temperature_c"] = pd.to_numeric(df.get("temp"), errors="coerce").astype("float64")
    df["humidity_percent"] = pd.to_numeric(df.get("rhum"), errors="coerce").astype("float64")

    # Sea-level pressure in hPa (explicit pressure_type distinguishes from surface pressure)
    df["pressure_hpa"] = pd.to_numeric(df.get("pres"), errors="coerce").astype("float64")
    df["pressure_type"] = "sea_level"

    df["precipitation_mm"] = pd.to_numeric(df.get("prcp"), errors="coerce").astype("float64")
    df["wind_speed_ms"] = (
        (pd.to_numeric(df.get("wspd"), errors="coerce") / 3.6).round(2).astype("float64")
    )
    df["wind_direction_deg"] = pd.to_numeric(df.get("wdir"), errors="coerce").astype("float64")

    # Cloud cover: okta (0-8) -> percent [0, 100]%
    cldc_numeric = pd.to_numeric(df.get("cldc"), errors="coerce")
    df["cloud_cover_percent"] = (cldc_numeric / 8.0 * 100.0).round(1).astype("float64")

    df["solar_radiation_wm2"] = pd.Series([np.nan] * len(df), dtype="float64")

    wpgt_numeric = pd.to_numeric(df.get("wpgt"), errors="coerce")
    df["wind_gusts_ms"] = (wpgt_numeric / 3.6).round(2).astype("float64")
    df["weather_condition_code"] = pd.to_numeric(df.get("coco"), errors="coerce").astype("float64")

    # Provenance: retained values passed the per-variable source filter in step 1
    df["observation_type"] = (
        "station_reported" if source_stats["source_columns_present"] else "unverified_source"
    )

    # 4. Range validation without clipping
    df_valid, range_violations = validate_and_nullify_ranges(df)

    # 5. Order-invariant deduplication on target observation grain:
    # (source_name, location_code, timestamp_utc)
    key_cols = ["source_name", "location_code", "timestamp_utc"]
    clean_df, quarantine_df, dedup_stats = resolve_duplicates_with_quarantine(
        df=df_valid,
        key_cols=key_cols,
        measurement_cols=MEASUREMENT_COLUMNS,
    )

    # 6. Chronological grid coverage check on requested [start, end) window
    coverage_stats = compute_hourly_coverage(
        df=clean_df,
        start_time=start_time,
        end_time=end_time,
        measurement_cols=MEASUREMENT_COLUMNS,
    )

    clean_standard = clean_df[SILVER_STANDARD_COLUMNS].copy()
    if not quarantine_df.empty:
        quarantine_standard = quarantine_df[
            [c for c in SILVER_STANDARD_COLUMNS if c in quarantine_df.columns]
        ].copy()
    else:
        quarantine_standard = pd.DataFrame(columns=SILVER_STANDARD_COLUMNS)

    dq_stats = {
        "initial_rows": initial_rows,
        "clean_rows": len(clean_standard),
        "quarantined_rows": len(quarantine_standard),
        "station_id": sid_str,
        "location_code": loc_code,
        "hourly_coverage": coverage_stats,
        "deduplication": dedup_stats,
        "range_violations": range_violations,
        "null_counts": {k: int(v) for k, v in clean_standard.isnull().sum().items()},
        "source_filter": source_stats,
        "provenance_notes": (
            "Meteostat pressure is sea-level pressure (pressure_type='sea_level'). "
            "Values whose <variable>_source is not a station observation source "
            f"({sorted(OBSERVATION_SOURCES)}) were removed; rows left without any "
            "measurement were dropped (docs/data-contract.md)."
        ),
    }

    return clean_standard, quarantine_standard, dq_stats


def sync_stations_metadata(client: Minio) -> None:
    """Save cleaned station metadata table to silver/stations/stations_metadata_cleaned.parquet."""
    rows = []
    for sid, info in STATION_REGISTRY.items():
        rows.append(
            {
                "station_id": sid,
                "station_name": info["station_name"],
                "location_code": info["location_code"],
                "latitude": info["latitude"],
                "longitude": info["longitude"],
                "elevation_m": info["elevation_m"],
                "source_name": "Meteostat",
                "status": info["status"],
                "notes": info.get("notes", ""),
                "updated_at": datetime.now(timezone.utc),
            }
        )
    df_st = pd.DataFrame(rows)
    buf = io.BytesIO()
    df_st.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    pbytes = buf.getvalue()

    target_key = "stations/stations_metadata_cleaned.parquet"
    client.put_object(
        bucket_name=BUCKET_SILVER,
        object_name=target_key,
        data=io.BytesIO(pbytes),
        length=len(pbytes),
        content_type="application/octet-stream",
    )
    print(f"[✓] Published cleaned station metadata: silver/{target_key} ({len(df_st)} stations)")


def process_meteostat_to_silver(
    start_year: int = 2016,
    end_year: int = 2025,
    station_ids: list[str] | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Execute transformation pipeline from Meteostat Bronze to Silver Parquet."""
    client = get_minio_client()

    if station_ids:
        selected_stations = [s.strip() for s in station_ids]
    else:
        selected_stations = list(get_canonical_stations().keys())

    years = list(range(start_year, end_year + 1))

    print("=" * 80)
    print(" STARTING TRANSFORMATION: METEOSTAT BRONZE -> SILVER PARQUET")
    print("=" * 80)
    print(f"[*] Year range: {start_year} - {end_year} ({len(years)} years)")
    print(f"[*] Station list: {', '.join(selected_stations)}")
    print(
        "[*] Partition pattern: silver/weather_hourly/year=YYYY/month=MM/weather_hourly_meteostat_<id>_<YYYYMM>.parquet"
    )
    print("=" * 80)

    # 1. Update station metadata
    sync_stations_metadata(client)

    overall_dq: dict[str, Any] = {
        "pipeline": "Meteostat Bronze to Silver",
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "years": years,
        "stations": selected_stations,
        "total_files_transformed": 0,
        "total_monthly_parquet_created": 0,
        "stale_monthly_parquet_removed": 0,
        "total_model_values_removed": 0,
        "total_rows_without_observation_dropped": 0,
        "total_records_processed": 0,
        "total_identical_duplicates_removed": 0,
        "total_conflicting_duplicates_quarantined": 0,
        "total_missing_hours": 0,
        "details_by_station_year": {},
        "failures": [],
    }

    total_tasks = len(selected_stations) * len(years)
    current_task = 0

    for sid in selected_stations:
        is_valid, loc_code, val_msg = validate_station_location_mapping(sid)
        if not is_valid or loc_code is None:
            print(f"\n[!] Station {sid} cannot be published: {val_msg}")
            overall_dq["failures"].append(
                {
                    "station_id": sid,
                    "stage": "station_validation",
                    "error": val_msg,
                }
            )
            continue

        st_info = STATION_REGISTRY.get(sid, {})
        st_name = st_info.get("station_name", f"Station {sid}")
        print(f"\n>>> Processing station [{sid}] - {st_name} (Location: {loc_code})")

        for year in years:
            current_task += 1
            bronze_key = (
                f"meteostat/station_hourly/year={year}/meteostat_station_{sid}_hourly_{year}.csv"
            )
            start_window = f"{year}-01-01 00:00:00+00:00"
            end_window = f"{year + 1}-01-01 00:00:00+00:00"

            # Read CSV from Bronze
            try:
                obj = client.get_object(BUCKET_BRONZE, bronze_key)
                raw_bytes = obj.read()
            except S3Error as s3_err:
                if is_s3_not_found_error(s3_err):
                    print(
                        f"  [{current_task}/{total_tasks}] Year {year}: Bronze file not found (station had no 404 hourly data)"
                    )
                    continue
                else:
                    print(
                        f"  [{current_task}/{total_tasks}] Year {year}: MinIO S3Error reading {bronze_key}: {s3_err}"
                    )
                    overall_dq["failures"].append(
                        {
                            "station_id": sid,
                            "year": year,
                            "stage": "read_bronze_s3",
                            "error": str(s3_err),
                        }
                    )
                    continue
            except Exception as e:
                print(
                    f"  [{current_task}/{total_tasks}] Year {year}: Unexpected error reading Bronze: {e}"
                )
                overall_dq["failures"].append(
                    {
                        "station_id": sid,
                        "year": year,
                        "stage": "read_bronze",
                        "error": str(e),
                    }
                )
                continue

            # Clean & standardize
            try:
                clean_df, quarantine_df, dq_stats = clean_and_standardize_meteostat(
                    raw_csv_bytes=raw_bytes,
                    station_id=sid,
                    start_time=start_window,
                    end_time=end_window,
                )
            except Exception as e:
                print(f"  [{current_task}/{total_tasks}] Year {year}: Transformation error: {e}")
                overall_dq["failures"].append(
                    {
                        "station_id": sid,
                        "year": year,
                        "stage": "transformation",
                        "error": str(e),
                    }
                )
                continue

            task_key = f"{sid}_{year}"
            overall_dq["details_by_station_year"][task_key] = dq_stats
            overall_dq["total_files_transformed"] += 1
            overall_dq["total_records_processed"] += len(clean_df)
            overall_dq["total_identical_duplicates_removed"] += dq_stats["deduplication"][
                "identical_duplicates_removed"
            ]
            overall_dq["total_conflicting_duplicates_quarantined"] += dq_stats["deduplication"][
                "conflicting_duplicates_count"
            ]
            overall_dq["total_missing_hours"] += dq_stats["hourly_coverage"]["missing_hours"]
            source_filter = dq_stats.get("source_filter", {})
            overall_dq["total_model_values_removed"] += sum(
                source_filter.get("model_values_removed", {}).values()
            )
            overall_dq["total_rows_without_observation_dropped"] += source_filter.get(
                "rows_without_observation_dropped", 0
            )

            # On overwrite, remove Silver months this run no longer produces (for example,
            # months whose values were all model-derived), so Gold cannot reload them.
            if overwrite:
                produced = (
                    set(clean_df["timestamp_utc"].dt.strftime("%Y%m"))
                    if not clean_df.empty
                    else set()
                )
                for month in range(1, 13):
                    ym = f"{year}{month:02d}"
                    if ym in produced:
                        continue
                    stale_key = (
                        f"weather_hourly/year={year}/month={month:02d}/"
                        f"weather_hourly_meteostat_{sid}_{ym}.parquet"
                    )
                    try:
                        client.stat_object(BUCKET_SILVER, stale_key)
                    except S3Error as s3_err:
                        if not is_s3_not_found_error(s3_err):
                            overall_dq["failures"].append(
                                {
                                    "station_id": sid,
                                    "year": year,
                                    "stage": "stat_stale_silver",
                                    "error": str(s3_err),
                                }
                            )
                        continue
                    try:
                        client.remove_object(BUCKET_SILVER, stale_key)
                        overall_dq["stale_monthly_parquet_removed"] += 1
                    except Exception as rm_err:
                        overall_dq["failures"].append(
                            {
                                "station_id": sid,
                                "year": year,
                                "stage": "remove_stale_silver",
                                "error": str(rm_err),
                            }
                        )

            # Partition by month and write to Parquet
            if not clean_df.empty:
                clean_df["year_month"] = clean_df["timestamp_utc"].dt.strftime("%Y%m")
                months_grouped = clean_df.groupby("year_month")

                created_months = 0
                for ym_str, group in months_grouped:
                    y_str = ym_str[:4]
                    m_str = ym_str[4:]
                    silver_key = f"weather_hourly/year={y_str}/month={m_str}/weather_hourly_meteostat_{sid}_{ym_str}.parquet"

                    if not overwrite:
                        try:
                            stat = client.stat_object(BUCKET_SILVER, silver_key)
                            if stat.size > 500:
                                created_months += 1
                                continue
                        except S3Error as s3_err:
                            if not is_s3_not_found_error(s3_err):
                                overall_dq["failures"].append(
                                    {
                                        "station_id": sid,
                                        "year": year,
                                        "stage": "stat_silver",
                                        "error": str(s3_err),
                                    }
                                )
                                continue

                    parquet_buf = io.BytesIO()
                    export_df = group.drop(columns=["year_month"]).reset_index(drop=True)
                    export_df.to_parquet(
                        parquet_buf, index=False, engine="pyarrow", compression="snappy"
                    )
                    pbytes = parquet_buf.getvalue()

                    try:
                        client.put_object(
                            bucket_name=BUCKET_SILVER,
                            object_name=silver_key,
                            data=io.BytesIO(pbytes),
                            length=len(pbytes),
                            content_type="application/octet-stream",
                        )
                        created_months += 1
                        overall_dq["total_monthly_parquet_created"] += 1
                    except Exception as upload_err:
                        print(f"  [!] Failed to upload {silver_key}: {upload_err}")
                        overall_dq["failures"].append(
                            {
                                "station_id": sid,
                                "year": year,
                                "stage": "upload_silver",
                                "error": str(upload_err),
                            }
                        )

                cov = dq_stats["hourly_coverage"]
                print(
                    f"  [{current_task}/{total_tasks}] Year {year}: Done ({len(clean_df):,} records, "
                    f"coverage: {cov['record_coverage_pct']}% of {cov['expected_hours']}h, "
                    f"missing: {cov['missing_hours']}h, quarantined: {dq_stats['quarantined_rows']})"
                )
            else:
                print(
                    f"  [{current_task}/{total_tasks}] Year {year}: Clean records is 0 (all quarantined or empty)."
                )

    # Write Data Quality report to Silver
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    dq_report_key = f"quality_reports/dq_report_meteostat_{now_str}.json"
    dq_json_bytes = json.dumps(overall_dq, indent=2, ensure_ascii=False).encode("utf-8")
    try:
        client.put_object(
            bucket_name=BUCKET_SILVER,
            object_name=dq_report_key,
            data=io.BytesIO(dq_json_bytes),
            length=len(dq_json_bytes),
            content_type="application/json",
        )
    except Exception as dq_err:
        overall_dq["failures"].append({"stage": "write_dq_report", "error": str(dq_err)})

    print("\n" + "=" * 80)
    print(" METEOSTAT SILVER TRANSFORMATION SUMMARY:")
    print(f"  + Bronze files processed          : {overall_dq['total_files_transformed']}")
    print(f"  + Silver monthly Parquet files     : {overall_dq['total_monthly_parquet_created']}")
    print(f"  + Hourly observation records       : {overall_dq['total_records_processed']:,}")
    print(
        f"  + Identical duplicates removed     : {overall_dq['total_identical_duplicates_removed']}"
    )
    print(
        f"  + Conflicting records quarantined  : {overall_dq['total_conflicting_duplicates_quarantined']}"
    )
    print(f"  + Total missing hours across years : {overall_dq['total_missing_hours']}")
    print(f"  + Model-derived values removed     : {overall_dq['total_model_values_removed']:,}")
    print(
        f"  + Rows without observation dropped : {overall_dq['total_rows_without_observation_dropped']:,}"
    )
    print(f"  + Stale Silver months removed      : {overall_dq['stale_monthly_parquet_removed']}")
    print(f"  + Failures encountered             : {len(overall_dq['failures'])}")
    print(f"  + Data Quality report              : silver/{dq_report_key}")
    print("=" * 80)

    if overall_dq["failures"]:
        raise RuntimeError(
            f"Meteostat transformation encountered {len(overall_dq['failures'])} failure(s). "
            "Failing process to trigger Airflow retry."
        )

    return overall_dq


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform Meteostat Bronze CSV to Silver Parquet")
    parser.add_argument("--start-year", type=int, default=2016, help="Start year (default: 2016)")
    parser.add_argument("--end-year", type=int, default=2025, help="End year (default: 2025)")
    parser.add_argument("--stations", type=str, default=None, help="Comma-separated station IDs")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing Silver files")

    args = parser.parse_args()
    st_list = [s.strip() for s in args.stations.split(",")] if args.stations else None

    try:
        process_meteostat_to_silver(
            start_year=args.start_year,
            end_year=args.end_year,
            station_ids=st_list,
            overwrite=args.overwrite,
        )
        sys.exit(0)
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n[!] Error: {e}", file=sys.stderr)
        sys.exit(1)
