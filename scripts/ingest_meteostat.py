#!/usr/bin/env python3
"""scripts/ingest_meteostat.py.

Collect historical weather observation data from the Meteostat Bulk Data Archive
and store unaltered raw CSV bytes into the Bronze layer on MinIO Data Lake.

Bronze path structure (per docs/NAMING_CONVENTIONS.md):
  meteostat/station_hourly/year=YYYY/meteostat_station_<station_id>_hourly_<YYYY>.csv
"""

import argparse
import gzip
import io
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from minio import Minio
from minio.error import S3Error
from station_config import (
    STATION_REGISTRY,
    get_canonical_stations,
    validate_station_location_mapping,
)

# Configure UTF-8 on Windows console
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

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

# MinIO configuration
MINIO_HOST = os.getenv("MINIO_EXTERNAL_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

# Meteostat bulk hourly data endpoint
METEOSTAT_BULK_HOURLY_URL = "https://data.meteostat.net/hourly/{year}/{station}.csv.gz"


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
        if not client.bucket_exists(BUCKET_BRONZE):
            client.make_bucket(BUCKET_BRONZE)
        return client
    except Exception:
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


def fetch_meteostat_station_year(
    station_id: str,
    year: int,
    max_retries: int = 3,
    timeout: int = 20,
) -> bytes | None:
    """Download .csv.gz file from Meteostat bulk archive and decompress to raw CSV bytes.

    Returns None if upstream archive reports 404 (station has no hourly records for that year).
    Raises requests.RequestException or RuntimeError on connection, timeout, or server errors.
    """
    url = METEOSTAT_BULK_HOURLY_URL.format(year=year, station=station_id)
    delay = 2

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 200:
                return gzip.decompress(resp.content)
            elif resp.status_code == 404:
                # Allowed skip: station did not report hourly data for this year
                return None
            elif resp.status_code == 429:
                print(f"      [!] Rate limited (HTTP 429), retrying after {delay}s...")
                time.sleep(delay)
                delay *= 2
            elif resp.status_code in (500, 502, 503, 504):
                print(f"      [!] Server error HTTP {resp.status_code}, retrying after {delay}s...")
                time.sleep(delay)
                delay *= 2
            else:
                resp.raise_for_status()
        except (requests.RequestException, gzip.BadGzipFile) as ex:
            if attempt == max_retries:
                raise RuntimeError(
                    f"Failed to fetch Meteostat station {station_id} for year {year} "
                    f"after {max_retries} attempts: {ex}"
                ) from ex
            print(
                f"      [!] Fetch exception ({ex}), retrying {attempt}/{max_retries} after {delay}s..."
            )
            time.sleep(delay)
            delay *= 2

    raise RuntimeError(f"Exhausted retries fetching Meteostat station {station_id} year {year}")


def is_s3_not_found_error(err: S3Error) -> bool:
    """Check if S3Error indicates a genuinely missing object rather than an authorization error."""
    code = getattr(err, "code", None)
    return code in ("NoSuchKey", "ResourceNotFound")


def ingest_meteostat_data(
    start_year: int = 2016,
    end_year: int = 2025,
    station_ids: list[str] | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Ingest Meteostat raw data into MinIO Bronze layer.

    Aggregates successes, skips, 404s, and failures.
    Raises RuntimeError if any requested task encounters a real failure.
    """
    client = get_minio_client()

    selected_stations: dict[str, dict[str, Any]] = {}
    if station_ids:
        for sid in station_ids:
            sid_str = sid.strip()
            info = STATION_REGISTRY.get(sid_str)
            if info:
                selected_stations[sid_str] = info
            else:
                is_valid, loc, msg = validate_station_location_mapping(sid_str)
                selected_stations[sid_str] = {
                    "station_name": f"Station {sid_str}",
                    "location_code": loc,
                    "status": "CUSTOM",
                    "notes": msg,
                }
    else:
        # Default to canonical stations from station_config
        selected_stations = get_canonical_stations()

    years = list(range(start_year, end_year + 1))
    total_tasks = len(selected_stations) * len(years)

    print("=" * 80)
    print(" STARTING METEOSTAT STATION INGESTION -> MINIO BRONZE")
    print("=" * 80)
    print(f"[*] Year range: {start_year} - {end_year} ({len(years)} years)")
    print(f"[*] Stations count: {len(selected_stations)}")
    for sid, sinfo in selected_stations.items():
        print(
            f"    - Station {sid}: {sinfo.get('station_name')} (Location: {sinfo.get('location_code')}, Status: {sinfo.get('status')})"
        )
    print(f"[*] Total tasks to evaluate: {total_tasks}")
    print("=" * 80)

    current_idx = 0
    success_count = 0
    skipped_count = 0
    not_found_count = 0
    failures: list[dict[str, Any]] = []

    for sid, sinfo in selected_stations.items():
        st_name = sinfo.get("station_name", f"Station {sid}")
        print(f"\n>>> Processing station [{sid}] - {st_name}")

        for year in years:
            current_idx += 1
            object_path = (
                f"meteostat/station_hourly/year={year}/meteostat_station_{sid}_hourly_{year}.csv"
            )

            # Check if object already exists in Bronze
            if not overwrite:
                try:
                    stat = client.stat_object(BUCKET_BRONZE, object_path)
                    if stat.size > 200:
                        print(
                            f"  [{current_idx}/{total_tasks}] Year {year}: {object_path} -> EXISTS ({stat.size} bytes), skipping."
                        )
                        skipped_count += 1
                        continue
                except S3Error as s3_err:
                    if is_s3_not_found_error(s3_err):
                        pass  # Object not found: proceed to download
                    else:
                        print(
                            f"  [{current_idx}/{total_tasks}] Year {year}: MinIO stat_object error: {s3_err}"
                        )
                        failures.append(
                            {
                                "station_id": sid,
                                "year": year,
                                "stage": "stat_object",
                                "error": str(s3_err),
                            }
                        )
                        continue
                except Exception as ex:
                    print(
                        f"  [{current_idx}/{total_tasks}] Year {year}: Unexpected error during stat_object: {ex}"
                    )
                    failures.append(
                        {
                            "station_id": sid,
                            "year": year,
                            "stage": "stat_object",
                            "error": str(ex),
                        }
                    )
                    continue

            print(
                f"  [{current_idx}/{total_tasks}] Year {year}: Checking/fetching station {sid} ...",
                end="",
                flush=True,
            )

            try:
                csv_bytes = fetch_meteostat_station_year(station_id=sid, year=year)

                if csv_bytes is None:
                    print(" NO DATA (HTTP 404 - no hourly reports for this station-year)")
                    not_found_count += 1
                    continue

                # Preserve raw bytes and record provenance metadata in MinIO object
                metadata = {
                    "source": "Meteostat Bulk Archive",
                    "station_id": sid,
                    "year": str(year),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }

                client.put_object(
                    bucket_name=BUCKET_BRONZE,
                    object_name=object_path,
                    data=io.BytesIO(csv_bytes),
                    length=len(csv_bytes),
                    content_type="text/csv",
                    metadata=metadata,
                )

                line_count = csv_bytes.count(b"\n")
                print(f" OK ({line_count:,} CSV lines, {len(csv_bytes) / 1024:.1f} KB)")
                success_count += 1
                time.sleep(0.5)

            except Exception as err:
                print(f" FAILED: {err}")
                failures.append(
                    {
                        "station_id": sid,
                        "year": year,
                        "stage": "fetch_or_upload",
                        "error": str(err),
                    }
                )

    print("\n" + "=" * 80)
    print(" METEOSTAT INGESTION SUMMARY:")
    print(f"  + Newly ingested files      : {success_count}")
    print(f"  + Already existing (skipped): {skipped_count}")
    print(f"  + Upstream 404 (no data)    : {not_found_count}")
    print(f"  + Total failures encountered: {len(failures)}")
    print("=" * 80)

    summary = {
        "success_count": success_count,
        "skipped_count": skipped_count,
        "not_found_count": not_found_count,
        "failure_count": len(failures),
        "failures": failures,
    }

    if failures:
        print("\n[!] The batch encountered failures:")
        for f in failures:
            print(f"    - Station {f['station_id']} Year {f['year']} [{f['stage']}]: {f['error']}")
        raise RuntimeError(
            f"Meteostat ingestion completed with {len(failures)} failure(s). "
            "Failing batch so Airflow can detect and retry."
        )

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest Meteostat weather station data into MinIO Bronze layer"
    )
    parser.add_argument("--start-year", type=int, default=2016, help="Start year (default: 2016)")
    parser.add_argument("--end-year", type=int, default=2025, help="End year (default: 2025)")
    parser.add_argument(
        "--stations",
        type=str,
        default=None,
        help="Comma-separated station IDs (e.g. '48820,48825'), or omit for default canonical stations",
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite existing objects in Bronze"
    )

    args = parser.parse_args()
    station_list = [s.strip() for s in args.stations.split(",")] if args.stations else None

    try:
        ingest_meteostat_data(
            start_year=args.start_year,
            end_year=args.end_year,
            station_ids=station_list,
            overwrite=args.overwrite,
        )
        sys.exit(0)
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n[!] Error: {e}", file=sys.stderr)
        sys.exit(1)
