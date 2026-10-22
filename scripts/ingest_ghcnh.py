#!/usr/bin/env python3
"""scripts/ingest_ghcnh.py.

Download NOAA GHCNh (Global Historical Climatology Network - hourly) station-year PSV
files and store the received bytes unchanged in the Bronze layer, with a separate JSON
manifest per object (workbook ID 9).

Bronze keys (per docs/NAMING_CONVENTIONS.md):
  ghcnh/station_hourly/year=YYYY/GHCNh_VMM000<wmo>_<YYYY>.psv
  ghcnh/manifests/year=YYYY/GHCNh_VMM000<wmo>_<YYYY>.json
"""

import argparse
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
from station_config import GHCNH_RAIN_STATIONS, resolve_ghcnh_stations

# Fallback load environment variables from .env
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

MINIO_HOST = os.getenv("MINIO_EXTERNAL_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

GHCNH_BY_YEAR_URL = (
    "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/"
    "by-year/{year}/psv/GHCNh_{station}_{year}.psv"
)
TRANSIENT_STATUS = (429, 500, 502, 503, 504)


def ghcnh_station_id(wmo_id: str) -> str:
    """Return the GHCN identifier used in file names (Vietnam WMO stations: VMM000<wmo>)."""
    return f"VMM000{wmo_id}"


def bronze_keys(wmo_id: str, year: int) -> tuple[str, str]:
    name = f"GHCNh_{ghcnh_station_id(wmo_id)}_{year}"
    return (
        f"ghcnh/station_hourly/year={year}/{name}.psv",
        f"ghcnh/manifests/year={year}/{name}.json",
    )


def get_minio_client() -> Minio:
    """Initialize MinIO client with fallback endpoint."""
    for endpoint in (MINIO_HOST, os.getenv("MINIO_ENDPOINT", "minio:9000")):
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
        except Exception as ex:
            last_err = ex
    raise RuntimeError(f"Cannot connect to MinIO: {last_err}")


def is_s3_not_found_error(err: Any) -> bool:
    """Return True only for an authoritative S3 missing-object error."""
    code = getattr(err, "code", None)
    if code in ("NoSuchKey", "NoSuchBucket", "NoSuchObject", "ResourceNotFound"):
        return True
    resp = getattr(err, "response", None)
    status = getattr(resp, "status", getattr(resp, "status_code", None)) if resp else None
    return status == 404


def fetch_station_year(
    wmo_id: str, year: int, max_retries: int = 4, timeout: int = 120
) -> tuple[int, bytes | None, str]:
    """Download one station-year file.

    Returns (http_status, body or None, url). HTTP 404 returns (404, None, url): the station
    has no file for that year. Transient errors (timeouts, 429, 5xx) are retried with
    exponential backoff; any other status raises immediately.
    """
    url = GHCNH_BY_YEAR_URL.format(year=year, station=ghcnh_station_id(wmo_id))
    delay = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
        except requests.RequestException as ex:
            if attempt == max_retries:
                raise RuntimeError(
                    f"GHCNh {wmo_id} {year}: network error after {attempt} attempts: {ex}"
                ) from ex
            time.sleep(delay)
            delay *= 2
            continue
        if resp.status_code == 200:
            return 200, resp.content, url
        if resp.status_code == 404:
            return 404, None, url
        if resp.status_code in TRANSIENT_STATUS and attempt < max_retries:
            retry_after = resp.headers.get("Retry-After")
            time.sleep(float(retry_after) if retry_after and retry_after.isdigit() else delay)
            delay *= 2
            continue
        raise RuntimeError(f"GHCNh {wmo_id} {year}: HTTP {resp.status_code} from {url}")
    raise RuntimeError(f"GHCNh {wmo_id} {year}: retries exhausted")


def build_manifest(
    wmo_id: str, year: int, url: str, body: bytes, retrieved_at: str
) -> dict[str, Any]:
    """Describe the stored object without modifying it."""
    lines = body.splitlines()
    return {
        "source": "NOAA NCEI GHCNh",
        "product_version_note": "GHCNh by-year PSV",
        "station_wmo_id": wmo_id,
        "ghcnh_station_id": ghcnh_station_id(wmo_id),
        "year": year,
        "url": url,
        "retrieved_at_utc": retrieved_at,
        "http_status": 200,
        "byte_size": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "data_rows": max(len(lines) - 1, 0),
    }


def read_manifest(client: Minio, key: str) -> dict[str, Any] | None:
    try:
        resp = client.get_object(BUCKET_BRONZE, key)
        try:
            return json.loads(resp.read())
        finally:
            resp.close()
            resp.release_conn()
    except S3Error as err:
        if is_s3_not_found_error(err):
            return None
        raise


def _object_matches(client: Minio, key: str, manifest: dict[str, Any]) -> bool:
    """True when the Bronze object exists with the byte size recorded in its manifest."""
    try:
        return client.stat_object(BUCKET_BRONZE, key).size == manifest.get("byte_size")
    except S3Error as err:
        if is_s3_not_found_error(err):
            return False
        raise


def ingest_ghcnh(
    start_year: int = 2016,
    end_year: int = 2025,
    station_ids: list[str] | None = None,
    overwrite: bool = False,
    client: Minio | None = None,
) -> dict[str, Any]:
    """Ingest GHCNh station-year files into Bronze. Raises RuntimeError on any failure."""
    client = client or get_minio_client()
    stations = station_ids or list(GHCNH_RAIN_STATIONS)
    unknown = [s for s in stations if s not in GHCNH_RAIN_STATIONS]
    if unknown:
        raise ValueError(f"Stations not in GHCNH_RAIN_STATIONS: {unknown}")

    summary: dict[str, Any] = {
        "stored": 0,
        "skipped_existing": 0,
        "not_found": [],
        "failures": [],
    }
    for wmo_id in stations:
        for year in range(start_year, end_year + 1):
            data_key, manifest_key = bronze_keys(wmo_id, year)
            try:
                if not overwrite:
                    manifest = read_manifest(client, manifest_key)
                    if manifest is not None and _object_matches(client, data_key, manifest):
                        summary["skipped_existing"] += 1
                        print(f"  {wmo_id} {year}: stored object matches manifest, skipping")
                        continue
                status, body, url = fetch_station_year(wmo_id, year)
                if status == 404:
                    summary["not_found"].append({"station_id": wmo_id, "year": year})
                    print(f"  {wmo_id} {year}: no file upstream (HTTP 404)")
                    continue
                retrieved_at = datetime.now(timezone.utc).isoformat()
                manifest = build_manifest(wmo_id, year, url, body, retrieved_at)
                client.put_object(
                    BUCKET_BRONZE, data_key, io.BytesIO(body), len(body), content_type="text/plain"
                )
                mbytes = json.dumps(manifest, indent=2).encode("utf-8")
                client.put_object(
                    BUCKET_BRONZE,
                    manifest_key,
                    io.BytesIO(mbytes),
                    len(mbytes),
                    content_type="application/json",
                )
                summary["stored"] += 1
                print(
                    f"  {wmo_id} {year}: stored {len(body):,} bytes ({manifest['data_rows']:,} rows)"
                )
            except Exception as ex:
                summary["failures"].append({"station_id": wmo_id, "year": year, "error": str(ex)})
                print(f"  [!] {wmo_id} {year}: {ex}")

    print(
        f"GHCNh ingestion: stored {summary['stored']}, skipped {summary['skipped_existing']}, "
        f"404 {len(summary['not_found'])}, failures {len(summary['failures'])}"
    )
    if summary["failures"]:
        raise RuntimeError(f"GHCNh ingestion had {len(summary['failures'])} failure(s)")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingest NOAA GHCNh station-year files into Bronze."
    )
    parser.add_argument("--start-year", type=int, required=True, help="First year (inclusive)")
    parser.add_argument("--end-year", type=int, required=True, help="Last year (inclusive)")
    parser.add_argument(
        "--stations",
        type=str,
        default=None,
        help="Comma-separated WMO ids (default: all GHCNH_RAIN_STATIONS)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Re-download existing objects")
    parser.add_argument(
        "--location", default=None, help="Only the GHCNh station(s) of this research location"
    )
    args = parser.parse_args()
    try:
        stations = resolve_ghcnh_stations(args.stations, args.location)
    except ValueError as ex:
        parser.error(str(ex))
    if stations == []:
        print(f"No GHCNh station is mapped to {args.location}; nothing to do")
        return 0
    try:
        ingest_ghcnh(args.start_year, args.end_year, stations, args.overwrite)
    except RuntimeError as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
