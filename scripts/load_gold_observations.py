#!/usr/bin/env python3
"""scripts/load_gold_observations.py.

Curate, standardize, and load weather observations from Silver Parquet into:
1. MinIO Gold Bucket: Star Schema Curated Parquet
   Path: gold/fact_weather_observation/year=YYYY/month=MM/fact_weather_observation_<YYYYMM>.parquet
2. PostgreSQL Data Warehouse (weather_dwh):
   Table: fact_weather_observation

According to docs/warehouse-schema.md, docs/data-contract.md, and docs/NAMING_CONVENTIONS.md Table 5.
"""

import argparse
import io
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
from minio import Minio
from minio.error import S3Error
from psycopg2.extras import execute_values
from station_config import CANONICAL_LOCATIONS

# Configure UTF-8 for Windows console
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Load environment variables
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
BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")
BUCKET_GOLD = os.getenv("MINIO_BUCKET_GOLD", "gold")

# PostgreSQL configuration
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DWH_DB = os.getenv("DWH_DB", "weather_dwh")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5434"))

FACT_MEASUREMENT_COLS = [
    "temperature_c",
    "humidity_percent",
    "pressure_hpa",
    "precipitation_mm",
    "wind_speed_ms",
    "wind_direction_deg",
    "cloud_cover_percent",
    "solar_radiation_wm2",
]


def get_minio_client() -> Minio:
    """Initialize MinIO client with fallback endpoint."""
    endpoints = [MINIO_HOST, os.getenv("MINIO_ENDPOINT", "minio:9000"), "localhost:9000"]
    for ep in endpoints:
        try:
            client = Minio(
                endpoint=ep,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE,
            )
            if not client.bucket_exists(BUCKET_GOLD):
                client.make_bucket(BUCKET_GOLD)
            return client
        except Exception:
            continue
    raise RuntimeError("Failed to connect to MinIO after trying available endpoints.")


def get_db_connection():
    """Initialize PostgreSQL DWH connection with host/container fallback."""
    connection_attempts = [
        {"host": POSTGRES_HOST, "port": POSTGRES_PORT},
        {"host": "localhost", "port": 5434},
        {"host": "postgres", "port": 5432},
        {"host": "localhost", "port": 5432},
    ]
    last_err = None
    for attempt in connection_attempts:
        try:
            conn = psycopg2.connect(
                host=attempt["host"],
                port=attempt["port"],
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=DWH_DB,
                connect_timeout=5,
            )
            return conn
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Failed to connect to PostgreSQL DWH ({DWH_DB}): {last_err}")


def load_dimension_lookups(conn) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    """Load surrogate keys for locations, sources, and stations from PostgreSQL."""
    with conn.cursor() as cur:
        cur.execute("SELECT location_key, location_code FROM dim_location;")
        loc_map = {row[1]: row[0] for row in cur.fetchall()}

        cur.execute("SELECT source_key, source_name FROM dim_source;")
        source_map = {row[1]: row[0] for row in cur.fetchall()}

        cur.execute("SELECT station_key, station_id FROM dim_station;")
        station_map = {str(row[1]): row[0] for row in cur.fetchall()}

    return loc_map, source_map, station_map


class SilverReadError(RuntimeError):
    """Exception raised when reading or parsing a Silver Parquet object fails."""

    def __init__(self, key: str, original_error: Exception):
        super().__init__(f"Error reading/parsing Silver Parquet object '{key}': {original_error}")
        self.key = key
        self.original_error = original_error


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


def clean_row_value(val: Any) -> Any:
    """Convert numpy/pandas types to python native types for psycopg2/SQL."""
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, (np.floating, float)):
        return round(float(val), 2)
    if isinstance(val, (np.integer, int)):
        return int(val)
    return val


def process_month_observations(
    minio_client: Minio,
    year: int,
    month: int,
    loc_map: dict[str, int],
    source_map: dict[str, int],
    station_map: dict[str, int],
    location_filter: str | None = None,
) -> pd.DataFrame:
    """Read, combine, and resolve keys for all Silver Parquet files in a given year/month."""
    m_str = f"{month:02d}"
    prefix = f"weather_hourly/year={year}/month={m_str}/"

    objects = list(minio_client.list_objects(BUCKET_SILVER, prefix=prefix, recursive=True))
    if not objects:
        return pd.DataFrame()

    dfs: list[pd.DataFrame] = []
    for obj in objects:
        if not obj.object_name.endswith(".parquet"):
            continue
        try:
            resp = minio_client.get_object(BUCKET_SILVER, obj.object_name)
            try:
                data = resp.read()
            finally:
                resp.close()
                resp.release_conn()
            df_part = pd.read_parquet(io.BytesIO(data))
            if not df_part.empty:
                dfs.append(df_part)
        except S3Error as s3_err:
            if is_s3_not_found_error(s3_err):
                print(f"  [-] Silver file not found: {obj.object_name}")
                continue
            raise SilverReadError(obj.object_name, s3_err) from s3_err
        except Exception as e:
            raise SilverReadError(obj.object_name, e) from e

    if not dfs:
        return pd.DataFrame()

    combined_df = pd.concat(dfs, ignore_index=True)

    # Filter canonical locations only
    combined_df["location_code"] = combined_df["location_code"].astype(str).str.strip().str.upper()
    valid_mask = combined_df["location_code"].isin(CANONICAL_LOCATIONS)
    if location_filter:
        valid_mask = valid_mask & (combined_df["location_code"] == location_filter.strip().upper())
    combined_df = combined_df[valid_mask].copy()

    if combined_df.empty:
        return pd.DataFrame()

    # Part 1, Item 6: Validate canonical UTC timestamp and exact hourly grid
    if "timestamp_utc" not in combined_df.columns:
        raise ValueError(
            f"Month {year}-{m_str}: 'timestamp_utc' column missing from Silver observations."
        )

    try:
        combined_df["timestamp_utc"] = pd.to_datetime(combined_df["timestamp_utc"], utc=True)
    except Exception as e:
        raise ValueError(
            f"Month {year}-{m_str}: Failed to parse 'timestamp_utc' as UTC: {e}"
        ) from e

    if combined_df["timestamp_utc"].isna().any():
        null_count = int(combined_df["timestamp_utc"].isna().sum())
        raise ValueError(
            f"Month {year}-{m_str}: Found {null_count} null/NaT 'timestamp_utc' values."
        )

    # Validate exact hourly grid (minute == 0, second == 0, microsecond == 0, nanosecond == 0)
    off_grid_mask = (
        (combined_df["timestamp_utc"].dt.minute != 0)
        | (combined_df["timestamp_utc"].dt.second != 0)
        | (combined_df["timestamp_utc"].dt.microsecond != 0)
        | (combined_df["timestamp_utc"].dt.nanosecond != 0)
    )
    if off_grid_mask.any():
        off_grid_count = int(off_grid_mask.sum())
        samples = combined_df.loc[off_grid_mask, "timestamp_utc"].head(3).tolist()
        raise ValueError(
            f"Month {year}-{m_str}: Found {off_grid_count} off-grid UTC timestamps (expected HH:00:00). "
            f"Samples: {samples}"
        )

    # Always derive local timestamps deterministically from canonical UTC
    derived_local = (
        combined_df["timestamp_utc"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
    )

    # If supplied timestamp_local is present in input, validate against derived local time
    if "timestamp_local" in combined_df.columns and not combined_df["timestamp_local"].isna().all():
        supplied_local_series = combined_df["timestamp_local"]
        if supplied_local_series.isna().any():
            null_count = int(supplied_local_series.isna().sum())
            raise ValueError(
                f"Month {year}-{m_str}: Supplied 'timestamp_local' contains {null_count} partial nulls."
            )

        supplied_local = pd.to_datetime(supplied_local_series)
        if supplied_local.dt.tz is not None:
            supplied_local = supplied_local.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)

        mismatches = supplied_local != derived_local
        if mismatches.any():
            mismatch_count = int(mismatches.sum())
            sample_mismatches = (
                combined_df.loc[mismatches, ["timestamp_utc", "timestamp_local"]]
                .head(3)
                .to_dict("records")
            )
            raise ValueError(
                f"Month {year}-{m_str}: Supplied 'timestamp_local' conflicts with UTC-derived local time for {mismatch_count} rows. "
                f"Samples: {sample_mismatches}"
            )

    combined_df["timestamp_local"] = derived_local
    combined_df["date_key"] = derived_local.dt.strftime("%Y%m%d").astype(int)
    combined_df["time_key"] = derived_local.dt.hour * 100

    # Foreign key lookups
    combined_df["location_key"] = combined_df["location_code"].map(loc_map)
    combined_df["source_key"] = combined_df["source_name"].map(source_map)

    if combined_df["location_key"].isna().any():
        missing_locs = (
            combined_df[combined_df["location_key"].isna()]["location_code"].unique().tolist()
        )
        raise ValueError(
            f"Month {year}-{m_str}: Unknown location codes with no dimension key: {missing_locs}"
        )
    if combined_df["source_key"].isna().any():
        missing_srcs = (
            combined_df[combined_df["source_key"].isna()]["source_name"].unique().tolist()
        )
        raise ValueError(
            f"Month {year}-{m_str}: Unknown source names with no dimension key: {missing_srcs}"
        )

    combined_df["location_key"] = combined_df["location_key"].astype(int)
    combined_df["source_key"] = combined_df["source_key"].astype(int)

    # Station key lookup (nullable for model/grid sources like Open-Meteo)
    if "station_id" in combined_df.columns:
        combined_df["station_key"] = (
            combined_df["station_id"].astype(str).str.strip().map(station_map)
        )
    else:
        combined_df["station_key"] = None

    # Ensure all measurement columns exist
    for col in FACT_MEASUREMENT_COLS:
        if col not in combined_df.columns:
            combined_df[col] = np.nan
        else:
            combined_df[col] = pd.to_numeric(combined_df[col], errors="coerce")

    # Part 1, Item 4: Quarantine or reject conflicting duplicate observations
    # Grain columns: (date_key, time_key, location_key, source_key)
    grain_cols = ["date_key", "time_key", "location_key", "source_key"]
    full_cols = grain_cols + FACT_MEASUREMENT_COLS

    # Collapse duplicates ONLY when both observation key AND all measurement values are identical
    dedup_df = combined_df.drop_duplicates(subset=full_cols, keep="first").copy()

    # Reject conflicting duplicates where keys match but measurements differ
    conflict_mask = dedup_df.duplicated(subset=grain_cols, keep=False)
    if conflict_mask.any():
        num_conflicts = int(conflict_mask.sum())
        sample_conflicts = (
            dedup_df.loc[conflict_mask, grain_cols + FACT_MEASUREMENT_COLS]
            .head(4)
            .to_dict("records")
        )
        raise ValueError(
            f"Month {year}-{m_str}: Conflicting duplicate observations detected for grain {grain_cols} "
            f"({num_conflicts} rows). Samples: {sample_conflicts}"
        )

    # Sort deterministically
    dedup_df = dedup_df.sort_values(by=grain_cols).reset_index(drop=True)
    return dedup_df


def save_gold_parquet(
    minio_client: Minio,
    df: pd.DataFrame,
    year: int,
    month: int,
    overwrite: bool = False,
    location_filter: str | None = None,
) -> str | None:
    """Save consolidated monthly fact observations to MinIO Gold Parquet.

    When running with a location_filter, preserves unaffected locations from
    the existing Gold Parquet file if present.
    """
    m_str = f"{month:02d}"
    ym_str = f"{year}{m_str}"
    gold_key = f"fact_weather_observation/year={year}/month={m_str}/fact_weather_observation_{ym_str}.parquet"

    # If running with a location filter, inspect existing Gold object
    if location_filter:
        loc_code = location_filter.strip().upper()
        existing_df: pd.DataFrame | None = None
        try:
            resp = minio_client.get_object(BUCKET_GOLD, gold_key)
            try:
                existing_bytes = resp.read()
            finally:
                resp.close()
                resp.release_conn()
            existing_df = pd.read_parquet(io.BytesIO(existing_bytes))
        except S3Error as err:
            if is_s3_not_found_error(err):
                existing_df = None
            else:
                # Storage / server error: abort publication!
                raise RuntimeError(
                    f"Storage error reading existing Gold Parquet {gold_key}: {err}"
                ) from err
        except Exception as e:
            # Corrupt Parquet or unexpected parse error: abort publication!
            raise RuntimeError(
                f"Corrupt or unreadable existing Gold Parquet {gold_key}: {e}"
            ) from e

        if existing_df is not None and not existing_df.empty:
            # Preserve unaffected locations and replace target location
            unaffected_df = existing_df[existing_df["location_code"] != loc_code].copy()
            merged_df = pd.concat([unaffected_df, df], ignore_index=True)
            grain_cols = ["date_key", "time_key", "location_key", "source_key"]
            df_to_save = (
                merged_df.drop_duplicates(subset=grain_cols, keep="last")
                .sort_values(by=grain_cols)
                .reset_index(drop=True)
            )
        else:
            df_to_save = df
    else:
        if not overwrite:
            try:
                stat = minio_client.stat_object(BUCKET_GOLD, gold_key)
                if stat.size > 500:
                    return gold_key
            except S3Error as err:
                if not is_s3_not_found_error(err):
                    raise RuntimeError(
                        f"Storage error checking Gold Parquet {gold_key}: {err}"
                    ) from err
            except Exception as e:
                raise RuntimeError(f"Error checking Gold Parquet {gold_key}: {e}") from e

        df_to_save = df

    export_cols = [
        "date_key",
        "time_key",
        "location_key",
        "source_key",
        "station_key",
        "location_code",
        "source_name",
        "timestamp_utc",
        "timestamp_local",
    ] + FACT_MEASUREMENT_COLS

    if "pressure_type" in df_to_save.columns:
        export_cols.append("pressure_type")
    if "observation_type" in df_to_save.columns:
        export_cols.append("observation_type")

    available_cols = [c for c in export_cols if c in df_to_save.columns]
    out_df = df_to_save[available_cols].copy()

    buf = io.BytesIO()
    out_df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    parquet_bytes = buf.getvalue()

    minio_client.put_object(
        bucket_name=BUCKET_GOLD,
        object_name=gold_key,
        data=io.BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/octet-stream",
    )
    return gold_key


def load_dwh_fact_observations(conn, df: pd.DataFrame) -> int:
    """Insert or upsert fact observations into PostgreSQL fact_weather_observation table."""
    if df.empty:
        return 0

    records: list[tuple[Any, ...]] = []
    for _, row in df.iterrows():
        records.append(
            (
                clean_row_value(row["date_key"]),
                clean_row_value(row["time_key"]),
                clean_row_value(row["location_key"]),
                clean_row_value(row["source_key"]),
                clean_row_value(row["station_key"]),
                row["timestamp_utc"].to_pydatetime(),
                row["timestamp_local"].to_pydatetime(),
                clean_row_value(row["temperature_c"]),
                clean_row_value(row["humidity_percent"]),
                clean_row_value(row["pressure_hpa"]),
                clean_row_value(row["precipitation_mm"]),
                clean_row_value(row["wind_speed_ms"]),
                clean_row_value(row["wind_direction_deg"]),
                clean_row_value(row["cloud_cover_percent"]),
                clean_row_value(row["solar_radiation_wm2"]),
            )
        )

    upsert_sql = """
        INSERT INTO fact_weather_observation (
            date_key, time_key, location_key, source_key, station_key,
            timestamp_utc, timestamp_local,
            temperature_c, humidity_percent, pressure_hpa, precipitation_mm,
            wind_speed_ms, wind_direction_deg, cloud_cover_percent, solar_radiation_wm2
        ) VALUES %s
        ON CONFLICT (date_key, time_key, location_key, source_key)
        DO UPDATE SET
            temperature_c = EXCLUDED.temperature_c,
            humidity_percent = EXCLUDED.humidity_percent,
            pressure_hpa = EXCLUDED.pressure_hpa,
            precipitation_mm = EXCLUDED.precipitation_mm,
            wind_speed_ms = EXCLUDED.wind_speed_ms,
            wind_direction_deg = EXCLUDED.wind_direction_deg,
            cloud_cover_percent = EXCLUDED.cloud_cover_percent,
            solar_radiation_wm2 = EXCLUDED.solar_radiation_wm2,
            station_key = EXCLUDED.station_key,
            timestamp_utc = EXCLUDED.timestamp_utc,
            timestamp_local = EXCLUDED.timestamp_local;
    """

    try:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, records, page_size=2500)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return len(records)


def run_gold_pipeline(
    start_year: int = 2024,
    end_year: int = 2024,
    target_location: str | None = None,
    skip_dwh: bool = False,
    skip_gold_parquet: bool = False,
    overwrite: bool = True,
) -> dict[str, Any]:
    """Execute complete Gold pipeline: Silver Parquet -> Gold Parquet & PostgreSQL DWH."""
    t_start = time.time()
    minio_client = get_minio_client()
    conn = get_db_connection()

    loc_map, source_map, station_map = load_dimension_lookups(conn)

    print("=" * 80)
    print(" STARTING GOLD CURATION & DATA WAREHOUSE LOADING PIPELINE")
    print("=" * 80)
    print(f"[*] Scope Year Range : {start_year} - {end_year}")
    print(f"[*] Target Location  : {target_location or 'All 8 Canonical Locations'}")
    print(
        f"[*] Gold Destination : MinIO Gold ({not skip_gold_parquet}), PostgreSQL DWH ({not skip_dwh})"
    )
    print("[*] Calendar Basis   : Vietnam Local Time (UTC+7 / timestamp_local)")
    print(
        "[*] Active DWH Tables: dim_date, dim_time, dim_location, dim_source, dim_station -> fact_weather_observation"
    )
    print("=" * 80)

    stats: dict[str, Any] = {
        "pipeline": "Silver to Gold & PostgreSQL DWH",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "start_year": start_year,
        "end_year": end_year,
        "target_location": target_location,
        "total_months_processed": 0,
        "total_gold_parquet_created": 0,
        "total_gold_records": 0,
        "total_dwh_upserted": 0,
        "records_by_source": {},
        "records_by_location": {},
        "failures": [],
    }

    try:
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                m_str = f"{month:02d}"
                ym_str = f"{year}{m_str}"
                stats["total_months_processed"] += 1

                # 1. Process and curate observations for month
                try:
                    df_month = process_month_observations(
                        minio_client=minio_client,
                        year=year,
                        month=month,
                        loc_map=loc_map,
                        source_map=source_map,
                        station_map=station_map,
                        location_filter=target_location,
                    )
                except SilverReadError as sre:
                    print(f"  [!] Month {ym_str}: {sre}")
                    stats["failures"].append(
                        {
                            "month": ym_str,
                            "stage": "silver_read",
                            "failed_keys": [sre.key],
                            "error": str(sre.original_error),
                        }
                    )
                    continue
                except Exception as e:
                    print(f"  [!] Month {ym_str}: Error preparing observations: {e}")
                    stats["failures"].append({"month": ym_str, "stage": "curate", "error": str(e)})
                    continue

                if df_month.empty:
                    print(f"  [-] Month {ym_str}: No valid observations found.")
                    continue

                records_count = len(df_month)

                # 2. Save Gold Parquet to MinIO
                gold_key = None
                if not skip_gold_parquet:
                    try:
                        gold_key = save_gold_parquet(
                            minio_client=minio_client,
                            df=df_month,
                            year=year,
                            month=month,
                            overwrite=overwrite,
                            location_filter=target_location,
                        )
                        stats["total_gold_parquet_created"] += 1
                    except Exception as e:
                        print(f"  [!] Month {ym_str}: Error uploading Gold Parquet: {e}")
                        stats["failures"].append(
                            {"month": ym_str, "stage": "gold_upload", "error": str(e)}
                        )
                        continue

                # 3. Load into PostgreSQL DWH
                dwh_loaded = 0
                if not skip_dwh:
                    try:
                        dwh_loaded = load_dwh_fact_observations(conn, df_month)
                        stats["total_dwh_upserted"] += dwh_loaded
                    except Exception as e:
                        if conn:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                        print(f"  [!] Month {ym_str}: Error loading PostgreSQL DWH: {e}")
                        stats["failures"].append(
                            {"month": ym_str, "stage": "dwh_load", "error": str(e)}
                        )
                        continue

                stats["total_gold_records"] += records_count
                for src, cnt in df_month["source_name"].value_counts().items():
                    stats["records_by_source"][src] = stats["records_by_source"].get(src, 0) + int(
                        cnt
                    )
                for loc, cnt in df_month["location_code"].value_counts().items():
                    stats["records_by_location"][loc] = stats["records_by_location"].get(
                        loc, 0
                    ) + int(cnt)

                print(
                    f"  [+] Month {ym_str}: {records_count:,} records processed "
                    f"(Gold Parquet: {'OK' if gold_key else 'Skipped'}, DWH Upserted: {dwh_loaded:,})"
                )

        # 4. Save DQ & Reconciliation report to MinIO
        duration_s = round(time.time() - t_start, 2)
        stats["completed_at"] = datetime.now(timezone.utc).isoformat()
        stats["duration_seconds"] = duration_s

        now_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_key = f"quality_reports/dq_report_gold_load_{now_tag}.json"
        rep_bytes = json.dumps(stats, indent=2, ensure_ascii=False).encode("utf-8")
        minio_client.put_object(
            bucket_name=BUCKET_GOLD,
            object_name=report_key,
            data=io.BytesIO(rep_bytes),
            length=len(rep_bytes),
            content_type="application/json",
        )

        print("\n" + "=" * 80)
        print(" GOLD CURATION & DATA WAREHOUSE LOAD SUMMARY:")
        print(f"  + Months processed               : {stats['total_months_processed']}")
        print(f"  + Gold monthly Parquet files     : {stats['total_gold_parquet_created']}")
        print(f"  + Curated observation records    : {stats['total_gold_records']:,}")
        print(f"  + PostgreSQL DWH upserted rows   : {stats['total_dwh_upserted']:,}")
        print(f"  + Records by source              : {stats['records_by_source']}")
        print(f"  + Total duration                 : {duration_s} seconds")
        print(f"  + Failures encountered           : {len(stats['failures'])}")
        print(f"  + Reconciliation & DQ report     : gold/{report_key}")
        print("=" * 80)

        if stats["failures"]:
            raise RuntimeError(f"Gold loading encountered {len(stats['failures'])} failure(s).")

        return stats

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Load weather observations from Silver to Gold Parquet and PostgreSQL DWH"
    )
    parser.add_argument("--start-year", type=int, default=2024, help="Start year (default: 2024)")
    parser.add_argument("--end-year", type=int, default=2024, help="End year (default: 2024)")
    parser.add_argument(
        "--location",
        type=str,
        default=None,
        help="Canonical location code (e.g. HAN) or None for all 8 locations",
    )
    parser.add_argument("--skip-dwh", action="store_true", help="Skip loading to PostgreSQL DWH")
    parser.add_argument(
        "--skip-gold-parquet", action="store_true", help="Skip saving Gold Parquet to MinIO"
    )
    parser.add_argument(
        "--overwrite", action="store_true", default=True, help="Overwrite existing files"
    )

    args = parser.parse_args()

    try:
        run_gold_pipeline(
            start_year=args.start_year,
            end_year=args.end_year,
            target_location=args.location,
            skip_dwh=args.skip_dwh,
            skip_gold_parquet=args.skip_gold_parquet,
            overwrite=args.overwrite,
        )
        sys.exit(0)
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n[!] Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
