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

Bronze keeps each response exactly as received, with a separate JSON manifest (request,
retrieval time, SHA-256, coverage, response metadata) under open_meteo/manifests/. A
requested file that cannot be downloaded or uploaded is a failure: the run continues with
the remaining files, then raises so the CLI exits 1 and Airflow can retry.
"""

import argparse
import calendar
import hashlib
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


def bronze_keys(location_code: str, year: int) -> tuple[str, str]:
    """Bronze object key and its manifest key for one location-year."""
    name = f"open_meteo_{location_code}_hourly_{year}0101_{year}1231"
    return (
        f"open_meteo/hourly/year={year}/{name}.json",
        f"open_meteo/manifests/year={year}/{name}.json",
    )


def request_params(lat: float, lon: float, year: int) -> dict[str, Any]:
    return {
        "latitude": lat,
        "longitude": lon,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "hourly": HOURLY_VARIABLES,
        "wind_speed_unit": "ms",
        "precipitation_unit": "mm",
        "timezone": "UTC",
    }


def fetch_open_meteo_year(lat: float, lon: float, year: int, max_retries: int = 5) -> bytes:
    """Fetch one calendar year of hourly data for one coordinate; returns the response bytes.

    Retries only transient failures (HTTP 429, 5xx, connection errors) with exponential
    backoff; any other HTTP status fails immediately.
    """
    params = request_params(lat, lon, year)
    delay = 2
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.content
            if resp.status_code != 429 and resp.status_code < 500:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:100]}")
            print(f"      [!] HTTP {resp.status_code}, retrying in {delay}s...")
        except requests.RequestException as ex:
            print(f"      [!] Connection error: {ex}, retrying in {delay}s...")
        if attempt < max_retries:
            time.sleep(delay)
            delay *= 2

    raise RuntimeError(f"Open-Meteo request for {year} failed after {max_retries} attempts")


def coverage(payload: dict[str, Any], year: int) -> dict[str, Any]:
    """Hourly coverage of a one-year response; complete means every UTC hour of the year."""
    times = payload.get("hourly", {}).get("time", [])
    expected = (366 if calendar.isleap(year) else 365) * 24
    return {
        "first_time": times[0] if times else None,
        "last_time": times[-1] if times else None,
        "hour_count": len(times),
        "expected_hours": expected,
        "complete": len(times) == expected
        and times[0] == f"{year}-01-01T00:00"
        and times[-1] == f"{year}-12-31T23:00",
    }


def hourly_equal(old: dict[str, Any], new: dict[str, Any]) -> bool:
    """Whether two responses carry the same hourly values and units."""
    return old.get("hourly") == new.get("hourly") and old.get("hourly_units") == new.get(
        "hourly_units"
    )


def build_manifest(
    location_code: str, year: int, raw: bytes, payload: dict[str, Any], retrieved_at: str
) -> dict[str, Any]:
    """Describe a stored response without modifying it."""
    loc = NORTH_LOCATIONS[location_code]
    return {
        "source": "Open-Meteo Archive API",
        "url": OPEN_METEO_ARCHIVE_URL,
        "request_params": request_params(loc["latitude"], loc["longitude"], year),
        "location_code": location_code,
        "location_name": loc["name"],
        "dataset_split": YEAR_SPLITS[year],
        "year": year,
        "retrieved_at_utc": retrieved_at,
        "http_status": 200,
        "bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "coverage": coverage(payload, year),
        "response_metadata": {
            k: payload.get(k)
            for k in (
                "latitude",
                "longitude",
                "elevation",
                "utc_offset_seconds",
                "timezone",
                "timezone_abbreviation",
                "hourly_units",
            )
        },
    }


def _read_bytes(client: Minio, key: str) -> bytes | None:
    try:
        resp = client.get_object(BUCKET_BRONZE, key)
    except S3Error as err:
        if is_s3_not_found_error(err):
            return None
        raise
    try:
        return resp.read()
    finally:
        resp.close()
        resp.release_conn()


def plan_object(
    client: Minio, data_key: str, manifest_key: str, overwrite: bool, replace_legacy: bool
) -> tuple[str, bytes | None]:
    """Decide what to do with one location-year: skip, skip_legacy, download, or replace.

    A stored object is current only when its manifest exists, its SHA-256 matches the
    manifest, and the manifest records complete coverage. An object without a manifest is a
    legacy object (written before 2026-10-22, payload modified); it is kept unless
    replace_legacy asks to replace it by an identical original response.
    """
    if overwrite:
        return "download", None
    stored = _read_bytes(client, data_key)
    manifest_bytes = _read_bytes(client, manifest_key)
    if stored is not None and manifest_bytes is not None:
        manifest = json.loads(manifest_bytes)
        if (
            hashlib.sha256(stored).hexdigest() == manifest.get("sha256")
            and manifest.get("coverage", {}).get("complete") is True
        ):
            return "skip", None
        return "download", None
    if stored is not None:
        return ("replace", stored) if replace_legacy else ("skip_legacy", None)
    return "download", None


def ingest_data(
    start_year: int = 2016,
    end_year: int = 2025,
    target_location: str = None,
    target_split: str = None,
    overwrite: bool = False,
    replace_legacy: bool = False,
):
    """Download the requested location-years into Bronze, unchanged, with a manifest each.

    Raises RuntimeError after the loop when any requested file failed. A response that does
    not cover the whole year is a failure and is not stored. With replace_legacy, a legacy
    object is replaced only if the new response has identical hourly values and units;
    otherwise the difference is reported as a failure and nothing is written.
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
    counts = {"downloaded": 0, "replaced_legacy": 0, "skipped": 0, "legacy_kept": 0}
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
        print(f"\n>>> Location [{loc_code}] - {loc_info['name']}")
        for year in years:
            current_idx += 1
            tag = f"  [{current_idx}/{total_tasks}] {loc_code} {year}"
            data_key, manifest_key = bronze_keys(loc_code, year)
            try:
                action, legacy = plan_object(
                    client, data_key, manifest_key, overwrite, replace_legacy
                )
                if action == "skip":
                    print(f"{tag}: current (manifest and checksum match), skipping")
                    counts["skipped"] += 1
                    continue
                if action == "skip_legacy":
                    print(f"{tag}: legacy object without manifest kept (use --replace-legacy)")
                    counts["legacy_kept"] += 1
                    continue

                raw = fetch_open_meteo_year(
                    lat=loc_info["latitude"], lon=loc_info["longitude"], year=year
                )
                retrieved_at = datetime.now(timezone.utc).isoformat()
                payload = json.loads(raw)
                cov = coverage(payload, year)
                if not cov["complete"]:
                    raise RuntimeError(f"incomplete response coverage {cov}")
                if action == "replace" and not hourly_equal(json.loads(legacy), payload):
                    raise RuntimeError(
                        "upstream hourly data differ from the legacy Bronze object; not replaced"
                    )

                client.put_object(
                    bucket_name=BUCKET_BRONZE,
                    object_name=data_key,
                    data=io.BytesIO(raw),
                    length=len(raw),
                    content_type="application/json",
                )
                manifest = json.dumps(
                    build_manifest(loc_code, year, raw, payload, retrieved_at), indent=2
                ).encode("utf-8")
                client.put_object(
                    bucket_name=BUCKET_BRONZE,
                    object_name=manifest_key,
                    data=io.BytesIO(manifest),
                    length=len(manifest),
                    content_type="application/json",
                )
                counts["replaced_legacy" if action == "replace" else "downloaded"] += 1
                print(f"{tag}: stored {cov['hour_count']} hours, {len(raw) / 1024:.1f} KB")

                # Pause 1.2 s to respect the Open-Meteo fair-use policy
                time.sleep(1.2)

            except Exception as err:
                print(f"{tag}: FAILED: {err}")
                failures.append(f"{data_key}: {err}")

    print("\n" + "=" * 80)
    print(" INGESTION RESULT:")
    for name, value in counts.items():
        print(f"  + {name:<16}: {value}")
    print(f"  + {'failed':<16}: {len(failures)}")
    print("=" * 80)
    if failures:
        raise RuntimeError(
            f"Open-Meteo ingestion completed with {len(failures)} failure(s): "
            + "; ".join(failures)
        )
    return counts


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
    parser.add_argument(
        "--overwrite", action="store_true", help="Download again even if the object is current"
    )
    parser.add_argument(
        "--replace-legacy",
        action="store_true",
        help="Replace objects without a manifest when the new hourly data are identical",
    )

    args = parser.parse_args()

    try:
        ingest_data(
            start_year=args.start_year,
            end_year=args.end_year,
            target_location=args.location,
            target_split=args.split,
            overwrite=args.overwrite,
            replace_legacy=args.replace_legacy,
        )
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\n[!] Error: {e}")
        sys.exit(1)
