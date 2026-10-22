#!/usr/bin/env python3
"""
scripts/ingest_open_meteo.py
Download historical hourly weather from the Open-Meteo Archive API into Bronze (MinIO).

Year splits from the proposal:
- 2016 - 2023 : training
- 2024        : validation
- 2025        : testing

Eight northern locations:
HAN (Hà Nội), PTO (Phú Thọ), TNG (Thái Nguyên), TUQ (Tuyên Quang),
LCA (Lào Cai), HBI (Hòa Bình), HPH (Hải Phòng), NBI (Ninh Bình).

A requested file that cannot be downloaded or uploaded is a failure: the run continues
with the remaining files, then raises so the CLI exits 1 and Airflow can retry.
"""

import argparse
import io
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from minio import Minio
from minio.error import S3Error

# Use UTF-8 console output on Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Load .env when python-dotenv is not installed
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if os.path.exists(env_file):
        with open(env_file, "r", encoding="utf-8") as f:
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

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# The eight representative northern locations in the project scope
NORTH_LOCATIONS = {
    "HAN": {"name": "Hà Nội", "latitude": 21.0285, "longitude": 105.8542},
    "PTO": {"name": "Phú Thọ", "latitude": 21.3986, "longitude": 105.2280},
    "TNG": {"name": "Thái Nguyên", "latitude": 21.5942, "longitude": 105.8482},
    "TUQ": {"name": "Tuyên Quang", "latitude": 21.8236, "longitude": 105.2153},
    "LCA": {"name": "Lào Cai", "latitude": 22.4856, "longitude": 103.9707},
    "HBI": {"name": "Hòa Bình", "latitude": 20.8172, "longitude": 105.3376},
    "HPH": {"name": "Hải Phòng", "latitude": 20.8449, "longitude": 106.6881},
    "NBI": {"name": "Ninh Bình", "latitude": 20.2506, "longitude": 105.9745},
}

# Dataset split per year
YEAR_SPLITS = {
    2016: "training",
    2017: "training",
    2018: "training",
    2019: "training",
    2020: "training",
    2021: "training",
    2022: "training",
    2023: "training",
    2024: "validation",
    2025: "testing",
}

# Hourly variables requested
HOURLY_VARIABLES = [
    "temperature_2m",  # Air temperature at 2 m (°C)
    "relative_humidity_2m",  # Relative humidity (%)
    "dew_point_2m",  # Dew point (°C)
    "apparent_temperature",  # Apparent temperature (°C)
    "precipitation",  # Total precipitation (mm), RainNext24h label source
    "rain",  # Liquid rain (mm)
    "weather_code",  # WMO weather code
    "surface_pressure",  # Surface pressure (hPa)
    "cloud_cover",  # Total cloud cover (%)
    "wind_speed_10m",  # Wind speed at 10 m (m/s)
    "wind_direction_10m",  # Wind direction at 10 m (degrees)
    "wind_gusts_10m",  # Wind gusts (m/s)
    "direct_normal_irradiance",  # Direct normal irradiance (W/m²)
    "shortwave_radiation",  # Shortwave radiation (W/m²)
]


def get_minio_client() -> Minio:
    """Create a MinIO client and make sure the Bronze bucket exists."""
    endpoint = MINIO_HOST
    try:
        client = Minio(
            endpoint=endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        if not client.bucket_exists(BUCKET_BRONZE):
            client.make_bucket(BUCKET_BRONZE)
        return client
    except Exception:
        # Inside the Docker network
        alt_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        client = Minio(
            endpoint=alt_endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        if not client.bucket_exists(BUCKET_BRONZE):
            client.make_bucket(BUCKET_BRONZE)
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


def fetch_open_meteo_year(lat: float, lon: float, year: int, max_retries: int = 5) -> dict:
    """Fetch one calendar year of hourly data for one coordinate.

    Retries only transient failures (HTTP 429, 5xx, connection errors) with exponential
    backoff; any other HTTP status fails immediately.
    """
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": HOURLY_VARIABLES,
        "wind_speed_unit": "ms",
        "precipitation_unit": "mm",
        "timezone": "UTC",
    }

    delay = 2
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code != 429 and resp.status_code < 500:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:100]}")
            print(f"      [!] HTTP {resp.status_code}, retrying in {delay}s...")
        except requests.RequestException as ex:
            print(f"      [!] Connection error: {ex}, retrying in {delay}s...")
        if attempt < max_retries:
            time.sleep(delay)
            delay *= 2

    raise RuntimeError(f"Open-Meteo request for {year} failed after {max_retries} attempts")


def ingest_data(
    start_year: int = 2016,
    end_year: int = 2025,
    target_location: str = None,
    target_split: str = None,
    overwrite: bool = False,
):
    """Download the requested location-years into Bronze.

    Raises RuntimeError after the loop when any requested file failed.
    """
    client = get_minio_client()

    locations = NORTH_LOCATIONS
    if target_location:
        loc_code = target_location.upper()
        if loc_code not in NORTH_LOCATIONS:
            raise ValueError(
                f"Unknown location: {target_location}. Choose from: {list(NORTH_LOCATIONS.keys())}"
            )
        locations = {loc_code: NORTH_LOCATIONS[loc_code]}

    years = [y for y in range(start_year, end_year + 1) if y in YEAR_SPLITS]
    if target_split:
        target_split = target_split.lower()
        years = [y for y in years if YEAR_SPLITS.get(y) == target_split]

    total_tasks = len(locations) * len(years)
    current_idx = 0
    success_count = 0
    skipped_count = 0
    failures: list[str] = []

    print("=" * 80)
    print(" OPEN-METEO INGESTION -> MINIO BRONZE")
    print("=" * 80)
    print(f"[*] Years: {years}")
    print(f"[*] Locations: {len(locations)} ({', '.join(locations.keys())})")
    print(f"[*] Files requested: {total_tasks}")
    print(f"[*] Variables: {', '.join(HOURLY_VARIABLES)}")
    print("=" * 80)

    for loc_code, loc_info in locations.items():
        loc_name = loc_info["name"]
        lat = loc_info["latitude"]
        lon = loc_info["longitude"]

        print(f"\n>>> Location [{loc_code}] - {loc_name} ({lat}, {lon})")

        for year in years:
            current_idx += 1
            split_type = YEAR_SPLITS[year]
            object_path = f"open_meteo/hourly/year={year}/open_meteo_{loc_code}_hourly_{year}0101_{year}1231.json"

            # Skip objects that already exist (current size-based rule)
            if not overwrite:
                try:
                    stat = client.stat_object(BUCKET_BRONZE, object_path)
                    if stat.size > 2000:
                        print(
                            f"  [{current_idx}/{total_tasks}] (Split: {split_type.upper():<10}) {object_path} -> EXISTS ({stat.size} bytes), skipping."
                        )
                        skipped_count += 1
                        continue
                except S3Error as s3_err:
                    if is_s3_not_found_error(s3_err):
                        pass
                    else:
                        raise

            print(
                f"  [{current_idx}/{total_tasks}] (Split: {split_type.upper():<10}) Downloading {loc_code} {year} ...",
                end="",
                flush=True,
            )

            try:
                # 1. Call the Open-Meteo API
                data = fetch_open_meteo_year(lat=lat, lon=lon, year=year)

                # 2. Add pipeline metadata
                data["_pipeline_metadata"] = {
                    "source": "Open-Meteo Archive API",
                    "location_code": loc_code,
                    "location_name": loc_name,
                    "dataset_split": split_type,
                    "year": year,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "hourly_record_count": len(data.get("hourly", {}).get("time", [])),
                }

                # 3. Upload the JSON to MinIO
                json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
                client.put_object(
                    bucket_name=BUCKET_BRONZE,
                    object_name=object_path,
                    data=io.BytesIO(json_bytes),
                    length=len(json_bytes),
                    content_type="application/json",
                )

                records = data["_pipeline_metadata"]["hourly_record_count"]
                print(f" DONE ({records} hourly records, {len(json_bytes) / 1024:.1f} KB)")
                success_count += 1

                # Pause 1.2 s to respect the Open-Meteo fair-use policy
                time.sleep(1.2)

            except Exception as err:
                print(f" FAILED: {err}")
                failures.append(f"{object_path}: {err}")

    print("\n" + "=" * 80)
    print(" INGESTION RESULT:")
    print(f"  + Downloaded : {success_count} files")
    print(f"  + Skipped    : {skipped_count} files (already present)")
    print(f"  + Failed     : {len(failures)} files")
    print(f"  + Bronze     : {success_count + skipped_count} / {total_tasks}")
    print("=" * 80)
    if failures:
        raise RuntimeError(
            f"Open-Meteo ingestion completed with {len(failures)} failure(s): "
            + "; ".join(failures)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download Open-Meteo hourly weather into the Bronze layer"
    )
    parser.add_argument("--start-year", type=int, default=2016, help="First year (default: 2016)")
    parser.add_argument("--end-year", type=int, default=2025, help="Last year (default: 2025)")
    parser.add_argument(
        "--location",
        type=str,
        default=None,
        help="One location code (for example HAN, HPH); all eight when omitted",
    )
    parser.add_argument(
        "--split",
        type=str,
        default=None,
        choices=["training", "validation", "testing"],
        help="Only years of this split",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace existing objects")

    args = parser.parse_args()

    try:
        ingest_data(
            start_year=args.start_year,
            end_year=args.end_year,
            target_location=args.location,
            target_split=args.split,
            overwrite=args.overwrite,
        )
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\n[!] Error: {e}")
        sys.exit(1)
