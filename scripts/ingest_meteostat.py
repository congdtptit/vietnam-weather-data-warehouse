#!/usr/bin/env python3
"""scripts/ingest_meteostat.py.

Collect hourly station data from the Meteostat Bulk Data Archive and store the received
.csv.gz bytes unchanged in the Bronze layer, with a JSON manifest per station-year (workbook
ID 9). A station-year the archive does not have (HTTP 404) also gets a manifest, so reruns
do not request it again (use --recheck-missing to ask again).

Bronze keys (per docs/NAMING_CONVENTIONS.md):
  meteostat/station_hourly/year=YYYY/meteostat_station_<station_id>_hourly_<YYYY>.csv.gz
  meteostat/manifests/year=YYYY/meteostat_station_<station_id>_hourly_<YYYY>.json

Until 2026-10-23 Bronze held the decompressed CSV (same name without .gz) and no manifest.
The first run after that date replaces such a file only when the download decompresses to
exactly the same bytes; otherwise it keeps the file and fails the station-year.
"""

import argparse
import gzip
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


def bronze_keys(station_id: str, year: int) -> tuple[str, str, str]:
    """(received .csv.gz, JSON manifest, legacy decompressed .csv written before 2026-10-23)."""
    name = f"meteostat_station_{station_id}_hourly_{year}"
    return (
        f"meteostat/station_hourly/year={year}/{name}.csv.gz",
        f"meteostat/manifests/year={year}/{name}.json",
        f"meteostat/station_hourly/year={year}/{name}.csv",
    )


def fetch_meteostat_station_year(
    station_id: str,
    year: int,
    max_retries: int = 3,
    timeout: int = 20,
) -> tuple[int, bytes | None, str, str | None]:
    """Download one station-year .csv.gz from the Meteostat bulk archive, unchanged.

    Returns (http_status, received bytes or None, url, Last-Modified header). HTTP 404
    returns (404, None, url, None): the station has no hourly records for that year.
    Raises RuntimeError on connection, timeout, or server errors after the retries, and on
    a body that is not gzip (so a broken download is never stored).
    """
    url = METEOSTAT_BULK_HOURLY_URL.format(year=year, station=station_id)
    delay = 2

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 200:
                gzip.decompress(resp.content)  # validate only; Bronze keeps the received bytes
                return 200, resp.content, url, resp.headers.get("Last-Modified")
            elif resp.status_code == 404:
                # Allowed skip: station did not report hourly data for this year
                return 404, None, url, None
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
        except (requests.RequestException, gzip.BadGzipFile, EOFError) as ex:
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


def build_manifest(
    station_id: str,
    year: int,
    url: str,
    status: int,
    body: bytes | None,
    last_modified: str | None,
) -> dict[str, Any]:
    """Describe a stored object (HTTP 200) or a station-year the archive does not have (404)."""
    manifest: dict[str, Any] = {
        "source": "Meteostat Bulk Archive",
        "product": "hourly station data (.csv.gz)",
        "station_id": station_id,
        "year": year,
        "url": url,
        "http_status": status,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    if status != 200 or body is None:
        return {**manifest, "payload_key": None}
    csv = gzip.decompress(body)
    return {
        **manifest,
        "upstream_last_modified": last_modified,
        "payload_key": bronze_keys(station_id, year)[0],
        "payload_encoding": "gzip (as received)",
        "byte_size": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "csv_byte_size": len(csv),
        "csv_sha256": hashlib.sha256(csv).hexdigest(),
        "csv_lines": csv.count(b"\n"),
    }


def read_manifest(client: Minio, key: str) -> dict[str, Any] | None:
    try:
        resp = client.get_object(BUCKET_BRONZE, key)
    except S3Error as err:
        if is_s3_not_found_error(err):
            return None
        raise
    try:
        return json.loads(resp.read())
    finally:
        resp.close()
        resp.release_conn()


def _read(client: Minio, key: str) -> bytes | None:
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


def _payload_matches(client: Minio, manifest: dict[str, Any]) -> bool:
    """True when the manifest's object exists with the recorded byte size."""
    try:
        return client.stat_object(BUCKET_BRONZE, manifest["payload_key"]).size == manifest.get(
            "byte_size"
        )
    except S3Error as err:
        if is_s3_not_found_error(err):
            return False
        raise


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


def ingest_station_year(
    client: Minio,
    sid: str,
    year: int,
    overwrite: bool = False,
    recheck_missing: bool = False,
) -> str:
    """Bring one station-year to a current Bronze state; return what happened.

    Outcomes: "skipped" (manifest and object current), "not_found_known" (a stored 404
    manifest; no request), "not_found" (HTTP 404, recorded), "stored", "replaced_legacy"
    (the received file decompresses to exactly the legacy CSV, which is then removed).
    A legacy CSV that differs from the download is kept, and the station-year fails, so
    changed upstream data never replaces Bronze without --overwrite.
    """
    data_key, manifest_key, legacy_key = bronze_keys(sid, year)
    if not overwrite:
        manifest = read_manifest(client, manifest_key)
        if manifest is not None:
            if manifest["http_status"] == 404 and not recheck_missing:
                return "not_found_known"
            if manifest["http_status"] == 200 and _payload_matches(client, manifest):
                return "skipped"

    status, body, url, last_modified = fetch_meteostat_station_year(station_id=sid, year=year)
    manifest = build_manifest(sid, year, url, status, body, last_modified)
    outcome = "not_found" if status == 404 else "stored"
    legacy = _read(client, legacy_key)
    if status == 404 and legacy is not None and not overwrite:
        raise RuntimeError(
            f"the archive no longer has {sid} {year} (HTTP 404), but {legacy_key} exists; "
            "kept unchanged. Pass --overwrite to record the station-year as missing"
        )
    if status == 200 and legacy is not None:
        if gzip.decompress(body) != legacy and not overwrite:
            raise RuntimeError(
                f"{legacy_key} differs from the current download; kept unchanged. "
                "Pass --overwrite to replace it with the new data"
            )
        outcome = "replaced_legacy"
    if status == 200:
        client.put_object(
            BUCKET_BRONZE,
            data_key,
            io.BytesIO(body),
            len(body),
            content_type="application/gzip",
        )
    mbytes = json.dumps(manifest, indent=2).encode("utf-8")
    client.put_object(
        BUCKET_BRONZE, manifest_key, io.BytesIO(mbytes), len(mbytes), "application/json"
    )
    if legacy is not None and status == 200:
        client.remove_object(BUCKET_BRONZE, legacy_key)
    return outcome


def ingest_meteostat_data(
    start_year: int = 2016,
    end_year: int = 2025,
    station_ids: list[str] | None = None,
    overwrite: bool = False,
    recheck_missing: bool = False,
) -> dict[str, Any]:
    """Ingest Meteostat station-years into the Bronze layer with a manifest each.

    Aggregates the outcomes of ingest_station_year. Raises RuntimeError if any requested
    station-year failed.
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

    counts = dict.fromkeys(
        ("stored", "replaced_legacy", "skipped", "not_found", "not_found_known"), 0
    )
    messages = {
        "stored": "stored with manifest",
        "replaced_legacy": "stored; identical legacy CSV removed",
        "skipped": "current (manifest and object match), skipping",
        "not_found": "NO DATA (HTTP 404), recorded in manifest",
        "not_found_known": "no data upstream (404 recorded earlier), not requested",
    }
    failures: list[dict[str, Any]] = []
    current_idx = 0
    for sid, sinfo in selected_stations.items():
        print(f"\n>>> Processing station [{sid}] - {sinfo.get('station_name', f'Station {sid}')}")
        for year in years:
            current_idx += 1
            try:
                outcome = ingest_station_year(client, sid, year, overwrite, recheck_missing)
            except Exception as err:
                print(f"  [{current_idx}/{total_tasks}] Year {year}: FAILED: {err}")
                failures.append(
                    {"station_id": sid, "year": year, "stage": "ingest", "error": str(err)}
                )
                continue
            counts[outcome] += 1
            print(f"  [{current_idx}/{total_tasks}] Year {year}: {messages[outcome]}")
            if outcome in ("stored", "replaced_legacy"):
                time.sleep(0.5)

    print("\n" + "=" * 80)
    print(" METEOSTAT INGESTION SUMMARY:")
    print(f"  + Newly stored files        : {counts['stored'] + counts['replaced_legacy']}")
    print(f"    (legacy CSV replaced)     : {counts['replaced_legacy']}")
    print(f"  + Already current (skipped) : {counts['skipped']}")
    print(f"  + Upstream 404 (no data)    : {counts['not_found'] + counts['not_found_known']}")
    print(f"    (known, not requested)    : {counts['not_found_known']}")
    print(f"  + Total failures encountered: {len(failures)}")
    print("=" * 80)

    summary = {
        "success_count": counts["stored"] + counts["replaced_legacy"],
        "replaced_legacy_count": counts["replaced_legacy"],
        "skipped_count": counts["skipped"],
        "not_found_count": counts["not_found"] + counts["not_found_known"],
        "not_found_known_count": counts["not_found_known"],
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
    parser.add_argument(
        "--recheck-missing",
        action="store_true",
        help="Request again station-years recorded as HTTP 404",
    )

    args = parser.parse_args()
    station_list = [s.strip() for s in args.stations.split(",")] if args.stations else None

    try:
        ingest_meteostat_data(
            start_year=args.start_year,
            end_year=args.end_year,
            station_ids=station_list,
            overwrite=args.overwrite,
            recheck_missing=args.recheck_missing,
        )
        sys.exit(0)
    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\n[!] Error: {e}", file=sys.stderr)
        sys.exit(1)
