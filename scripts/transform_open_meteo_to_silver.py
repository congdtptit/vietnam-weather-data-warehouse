#!/usr/bin/env python3
"""scripts/transform_open_meteo_to_silver.py.

Clean and standardize Open-Meteo historical weather data from Bronze (JSON)
to Silver (Parquet) according to docs/data-contract.md and docs/NAMING_CONVENTIONS.md.

Silver storage structure:
  silver/weather_hourly/year=YYYY/month=MM/weather_hourly_<location>_<YYYYMM>.parquet
Quality reports:
  silver/quality_reports/dq_report_open_meteo_<timestamp>.json
"""

import argparse
import hashlib
import io
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from minio import Minio
from minio.error import S3Error
from quality_checks import (
    compute_hourly_coverage,
    resolve_duplicates_with_quarantine,
    validate_and_nullify_ranges,
)
from station_config import CANONICAL_LOCATIONS

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

NORTH_LOCATIONS = sorted(CANONICAL_LOCATIONS)

SILVER_STANDARD_COLUMNS = [
    "source_name",
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
    "dew_point_c",
    "apparent_temperature_c",
    "weather_code",
    "wind_gusts_ms",
    "observation_type",
]

MEASUREMENT_COLUMNS = [
    "temperature_c",
    "humidity_percent",
    "pressure_hpa",
    "precipitation_mm",
    "wind_speed_ms",
    "wind_direction_deg",
    "cloud_cover_percent",
    "solar_radiation_wm2",
    "dew_point_c",
    "apparent_temperature_c",
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


def clean_and_standardize_open_meteo(
    raw_json: dict[str, Any],
    location_code: str,
    start_time: datetime | pd.Timestamp | str,
    end_time: datetime | pd.Timestamp | str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Clean and standardize Open-Meteo hourly JSON into Silver schema.

    Rules:
    1. Validate canonical location code.
    2. Parse timestamp string to UTC and Asia/Ho_Chi_Minh local time.
    3. Pressure type: Open-Meteo provides surface_pressure, stored as pressure_type='surface'.
    4. Range validation without clipping: negative precipitation and out-of-range humidity set to NaN.
    5. Deduplication: deterministic on (source_name, location_code, timestamp_utc).
       Identical duplicates are deduplicated; conflicting duplicates are quarantined.
    6. Chronological hourly coverage calculated on [start_time, end_time).
    """
    loc_clean = location_code.strip().upper()
    if loc_clean not in CANONICAL_LOCATIONS:
        raise ValueError(
            f"Invalid location code '{location_code}'. Must be one of canonical {sorted(CANONICAL_LOCATIONS)}"
        )

    hourly = raw_json.get("hourly", {})
    if not hourly or "time" not in hourly:
        raise ValueError("JSON payload does not contain valid 'hourly.time' data")

    df = pd.DataFrame(hourly)
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
            "location_code": loc_clean,
            "hourly_coverage": coverage_stats,
            "deduplication": {
                "identical_duplicates_removed": 0,
                "conflicting_duplicates_count": 0,
                "conflicting_groups": 0,
                "conflicting_details": [],
            },
            "range_violations": {},
            "provenance_notes": "Empty hourly payload.",
        }
        return empty_clean, pd.DataFrame(), dq_stats

    # 1. Parse timestamps
    df["timestamp_utc"] = pd.to_datetime(df["time"], utc=True)
    df["timestamp_local"] = (
        df["timestamp_utc"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
    )

    # 2. Add canonical grain identifiers
    df["source_name"] = "Open-Meteo"
    df["location_code"] = loc_clean

    # 3. Rename columns per data contract
    column_mapping = {
        "temperature_2m": "temperature_c",
        "relative_humidity_2m": "humidity_percent",
        "dew_point_2m": "dew_point_c",
        "apparent_temperature": "apparent_temperature_c",
        "surface_pressure": "pressure_hpa",
        "precipitation": "precipitation_mm",
        "wind_speed_10m": "wind_speed_ms",
        "wind_direction_10m": "wind_direction_deg",
        "cloud_cover": "cloud_cover_percent",
        "shortwave_radiation": "solar_radiation_wm2",
        "wind_gusts_10m": "wind_gusts_ms",
    }
    df = df.rename(columns=column_mapping)

    # Pressure type: explicit surface pressure
    df["pressure_type"] = "surface"
    df["observation_type"] = "model_reanalysis"

    # Ensure numeric types
    for col in MEASUREMENT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # 4. Range validation without clipping
    df_valid, range_violations = validate_and_nullify_ranges(df)

    # 5. Order-invariant deduplication on target observation grain
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

    available_clean_cols = [c for c in SILVER_STANDARD_COLUMNS if c in clean_df.columns]
    clean_standard = clean_df[available_clean_cols].copy()

    if not quarantine_df.empty:
        available_q_cols = [c for c in SILVER_STANDARD_COLUMNS if c in quarantine_df.columns]
        quarantine_standard = quarantine_df[available_q_cols].copy()
    else:
        quarantine_standard = pd.DataFrame(columns=SILVER_STANDARD_COLUMNS)

    dq_stats = {
        "initial_rows": initial_rows,
        "clean_rows": len(clean_standard),
        "quarantined_rows": len(quarantine_standard),
        "location_code": loc_clean,
        "hourly_coverage": coverage_stats,
        "deduplication": dedup_stats,
        "range_violations": range_violations,
        "null_counts": {k: int(v) for k, v in clean_standard.isnull().sum().items()},
        "provenance_notes": (
            "Open-Meteo pressure is surface pressure (pressure_type='surface'). "
            "Downstream consumers must not equate surface pressure with sea-level pressure."
        ),
    }

    return clean_standard, quarantine_standard, dq_stats


def process_open_meteo_to_silver(
    start_year: int = 2016,
    end_year: int = 2025,
    target_location: str | None = None,
    overwrite: bool = False,
    allow_missing: bool = False,
) -> dict[str, Any]:
    """Execute transformation pipeline from Open-Meteo Bronze to Silver Parquet.

    A requested Bronze file that does not exist is a failure unless allow_missing is set,
    so an incomplete ingestion cannot pass as a complete Silver run.
    """
    client = get_minio_client()

    if target_location:
        loc_upper = target_location.strip().upper()
        if loc_upper not in CANONICAL_LOCATIONS:
            raise ValueError(
                f"Requested location '{target_location}' is not a canonical research location: {sorted(CANONICAL_LOCATIONS)}"
            )
        locations = [loc_upper]
    else:
        locations = NORTH_LOCATIONS

    years = list(range(start_year, end_year + 1))
    total_tasks = len(locations) * len(years)

    print("=" * 80)
    print(" STARTING TRANSFORMATION: OPEN-METEO BRONZE -> SILVER PARQUET")
    print("=" * 80)
    print(f"[*] Year range: {start_year} - {end_year} ({len(years)} years)")
    print(f"[*] Locations: {', '.join(locations)}")
    print(
        "[*] Partition pattern: silver/weather_hourly/year=YYYY/month=MM/weather_hourly_<loc>_<YYYYMM>.parquet"
    )
    print("=" * 80)

    overall_dq: dict[str, Any] = {
        "pipeline": "Open-Meteo Bronze to Silver",
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "years": years,
        "locations": locations,
        "total_files_transformed": 0,
        "total_monthly_parquet_created": 0,
        "total_records_processed": 0,
        "total_identical_duplicates_removed": 0,
        "total_conflicting_duplicates_quarantined": 0,
        "total_missing_hours": 0,
        "details_by_location_year": {},
        "bronze_verified_by_manifest": 0,
        "bronze_without_manifest": 0,
        "failures": [],
    }

    current_task = 0

    for loc in locations:
        print(f"\n>>> Processing location: [{loc}]")

        for year in years:
            current_task += 1
            bronze_key = (
                f"open_meteo/hourly/year={year}/open_meteo_{loc}_hourly_{year}0101_{year}1231.json"
            )
            start_window = f"{year}-01-01 00:00:00+00:00"
            end_window = f"{year + 1}-01-01 00:00:00+00:00"

            # 1. Read Bronze JSON
            try:
                obj = client.get_object(BUCKET_BRONZE, bronze_key)
                raw_bytes = obj.read()
                raw_json = json.loads(raw_bytes.decode("utf-8"))
            except S3Error as s3_err:
                if is_s3_not_found_error(s3_err):
                    print(
                        f"  [{current_task}/{total_tasks}] Year {year}: Bronze file not found: {bronze_key}"
                    )
                    if not allow_missing:
                        overall_dq["failures"].append(
                            {
                                "location_code": loc,
                                "year": year,
                                "stage": "missing_bronze",
                                "error": f"requested Bronze object not found: {bronze_key}",
                            }
                        )
                    continue
                else:
                    print(
                        f"  [{current_task}/{total_tasks}] Year {year}: S3Error reading Bronze {bronze_key}: {s3_err}"
                    )
                    overall_dq["failures"].append(
                        {
                            "location_code": loc,
                            "year": year,
                            "stage": "read_bronze_s3",
                            "error": str(s3_err),
                        }
                    )
                    continue
            except Exception as e:
                print(f"  [{current_task}/{total_tasks}] Year {year}: Error reading Bronze: {e}")
                overall_dq["failures"].append(
                    {
                        "location_code": loc,
                        "year": year,
                        "stage": "read_bronze",
                        "error": str(e),
                    }
                )
                continue

            # 1b. Verify the stored bytes against the Bronze manifest (objects written since
            # 2026-10-22); legacy objects without a manifest are counted, not rejected.
            manifest_key = bronze_key.replace("open_meteo/hourly/", "open_meteo/manifests/", 1)
            try:
                man = client.get_object(BUCKET_BRONZE, manifest_key)
                try:
                    manifest = json.loads(man.read())
                finally:
                    man.close()
                    man.release_conn()
            except S3Error as s3_err:
                if not is_s3_not_found_error(s3_err):
                    overall_dq["failures"].append(
                        {
                            "location_code": loc,
                            "year": year,
                            "stage": "read_manifest",
                            "error": str(s3_err),
                        }
                    )
                    continue
                manifest = None
            if manifest is None:
                overall_dq["bronze_without_manifest"] += 1
            elif hashlib.sha256(raw_bytes).hexdigest() != manifest.get("sha256"):
                overall_dq["failures"].append(
                    {
                        "location_code": loc,
                        "year": year,
                        "stage": "manifest_checksum",
                        "error": f"{bronze_key} does not match its manifest SHA-256",
                    }
                )
                continue
            else:
                overall_dq["bronze_verified_by_manifest"] += 1

            # 2. Clean & standardize
            try:
                clean_df, quarantine_df, dq_stats = clean_and_standardize_open_meteo(
                    raw_json=raw_json,
                    location_code=loc,
                    start_time=start_window,
                    end_time=end_window,
                )
            except Exception as e:
                print(f"  [{current_task}/{total_tasks}] Year {year}: Transformation error: {e}")
                overall_dq["failures"].append(
                    {
                        "location_code": loc,
                        "year": year,
                        "stage": "transformation",
                        "error": str(e),
                    }
                )
                continue

            task_key = f"{loc}_{year}"
            overall_dq["details_by_location_year"][task_key] = dq_stats
            overall_dq["total_files_transformed"] += 1
            overall_dq["total_records_processed"] += len(clean_df)
            overall_dq["total_identical_duplicates_removed"] += dq_stats["deduplication"][
                "identical_duplicates_removed"
            ]
            overall_dq["total_conflicting_duplicates_quarantined"] += dq_stats["deduplication"][
                "conflicting_duplicates_count"
            ]
            overall_dq["total_missing_hours"] += dq_stats["hourly_coverage"]["missing_hours"]

            # 3. Partition by month and write to Parquet
            if not clean_df.empty:
                clean_df["year_month"] = clean_df["timestamp_utc"].dt.strftime("%Y%m")
                months_grouped = clean_df.groupby("year_month")

                created_months = 0
                for ym_str, group in months_grouped:
                    y_str = ym_str[:4]
                    m_str = ym_str[4:]
                    silver_key = f"weather_hourly/year={y_str}/month={m_str}/weather_hourly_{loc}_{ym_str}.parquet"

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
                                        "location_code": loc,
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
                    parquet_bytes = parquet_buf.getvalue()

                    try:
                        client.put_object(
                            bucket_name=BUCKET_SILVER,
                            object_name=silver_key,
                            data=io.BytesIO(parquet_bytes),
                            length=len(parquet_bytes),
                            content_type="application/octet-stream",
                        )
                        created_months += 1
                        overall_dq["total_monthly_parquet_created"] += 1
                    except Exception as upload_err:
                        print(f"  [!] Failed to upload {silver_key}: {upload_err}")
                        overall_dq["failures"].append(
                            {
                                "location_code": loc,
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

    # 4. Write DQ report to Silver
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    dq_report_key = f"quality_reports/dq_report_open_meteo_{now_str}.json"
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
    print(" OPEN-METEO SILVER TRANSFORMATION SUMMARY:")
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
    print(f"  + Failures encountered             : {len(overall_dq['failures'])}")
    print(f"  + Data Quality report              : silver/{dq_report_key}")
    print("=" * 80)

    if overall_dq["failures"]:
        raise RuntimeError(
            f"Open-Meteo transformation encountered {len(overall_dq['failures'])} failure(s). "
            "Failing process to trigger Airflow retry."
        )

    return overall_dq


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transform Open-Meteo Bronze JSON to Silver Parquet"
    )
    parser.add_argument("--start-year", type=int, default=2016, help="Start year (default: 2016)")
    parser.add_argument("--end-year", type=int, default=2025, help="End year (default: 2025)")
    parser.add_argument(
        "--location",
        type=str,
        default=None,
        help="Canonical location code (e.g. HAN) or omit for all 8 locations",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing Silver files")
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Skip requested Bronze files that do not exist instead of failing",
    )

    args = parser.parse_args()

    try:
        process_open_meteo_to_silver(
            start_year=args.start_year,
            end_year=args.end_year,
            target_location=args.location,
            overwrite=args.overwrite,
            allow_missing=args.allow_missing,
        )
        sys.exit(0)
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\n[!] Error: {e}", file=sys.stderr)
        sys.exit(1)
