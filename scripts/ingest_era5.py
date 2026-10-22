#!/usr/bin/env python3
"""scripts/ingest_era5.py.

Request ERA5 hourly single-level reanalysis for northern Viet Nam from the Copernicus Climate
Data Store (CDS) and store each monthly NetCDF unchanged in Bronze with a manifest
(workbook IDs 5 and 14; scope in docs/business-requirements.md section 5a).

Credentials are never in the repository: cdsapi reads ~/.cdsapirc or the CDSAPI_URL and
CDSAPI_KEY environment variables (passed to the Airflow containers from .env). Copy the URL
and key shown on your CDS account page and accept the dataset licence on its download page
first (unaccepted licences give HTTP 403).

Bronze (per docs/NAMING_CONVENTIONS.md), the received file unchanged:
  era5/reanalysis/year=YYYY/month=MM/era5_north_vietnam_hourly_<YYYYMM>.nc    single NetCDF
  era5/reanalysis/year=YYYY/month=MM/era5_north_vietnam_hourly_<YYYYMM>.zip   several NetCDF
  era5/manifests/year=YYYY/month=MM/era5_north_vietnam_hourly_<YYYYMM>.json

With NetCDF output the CDS splits variables by step type (instantaneous versus accumulated,
such as total precipitation) into separate files and returns them zipped, even when
`download_format` is `unarchived`; the zip is stored as received and its members are listed
in the manifest.

One request per month (CDS limits the number of fields per request). A month is current
when its manifest exists and the stored bytes match its SHA-256; it is then skipped. A file
whose variables, hourly time steps, or area do not match the request is a failure and is
not stored. Use --dry-run to print the requests without contacting CDS.
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import io
import json
import os
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from typing import Any

from ingest_ghcnh import BUCKET_BRONZE, get_minio_client, is_s3_not_found_error
from minio import Minio
from minio.error import S3Error

DATASET = "reanalysis-era5-single-levels"
# North, West, South, East in degrees: covers the 8 research points with a margin.
AREA = [23.5, 102.0, 19.5, 107.5]
VARIABLES = {  # CDS request name -> short name in the NetCDF file
    "2m_temperature": "t2m",
    "2m_dewpoint_temperature": "d2m",
    "total_precipitation": "tp",
    "surface_pressure": "sp",
    "10m_u_component_of_wind": "u10",
    "10m_v_component_of_wind": "v10",
    "total_cloud_cover": "tcc",
}
GRID_TOLERANCE_DEG = 0.25  # ERA5 single levels are on a 0.25 degree grid


def bronze_keys(year: int, month: int, extension: str = "nc") -> tuple[str, str]:
    name = f"era5_north_vietnam_hourly_{year}{month:02d}"
    part = f"year={year}/month={month:02d}"
    return f"era5/reanalysis/{part}/{name}.{extension}", f"era5/manifests/{part}/{name}.json"


def build_request(year: int, month: int) -> dict[str, Any]:
    days = calendar.monthrange(year, month)[1]
    return {
        "product_type": ["reanalysis"],
        "variable": list(VARIABLES),
        "year": [str(year)],
        "month": [f"{month:02d}"],
        "day": [f"{d:02d}" for d in range(1, days + 1)],
        "time": [f"{h:02d}:00" for h in range(24)],
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": AREA,
    }


def _inspect_one(path: str, year: int, month: int) -> dict[str, Any]:
    """Time steps and area of one NetCDF file; raise ValueError on a mismatch."""
    import xarray as xr

    with xr.open_dataset(path, engine="netcdf4") as ds:
        time_name = "valid_time" if "valid_time" in ds.coords else "time"
        times = ds[time_name].values
        expected = calendar.monthrange(year, month)[1] * 24
        if len(times) != expected:
            raise ValueError(f"{len(times)} time steps, expected {expected}")
        lat, lon = ds["latitude"].values, ds["longitude"].values
        north, west, south, east = AREA
        if (
            lat.max() > north + GRID_TOLERANCE_DEG
            or lat.min() < south - GRID_TOLERANCE_DEG
            or lon.min() < west - GRID_TOLERANCE_DEG
            or lon.max() > east + GRID_TOLERANCE_DEG
        ):
            raise ValueError("grid extends beyond the requested area")
        return {
            "variables": sorted(v for v in ds.data_vars if v in VARIABLES.values()),
            "time_coordinate": time_name,
            "first_time": str(times.min()),
            "last_time": str(times.max()),
            "time_steps": int(len(times)),
            "latitudes": [float(lat.min()), float(lat.max()), int(lat.size)],
            "longitudes": [float(lon.min()), float(lon.max()), int(lon.size)],
            "units": {v: ds[v].attrs.get("units") for v in ds.data_vars if v in VARIABLES.values()},
        }


def inspect_download(path: str, year: int, month: int) -> dict[str, Any]:
    """Check a download (one NetCDF or a zip of NetCDF files) against its request.

    Returns the format and, per NetCDF member, its name, bytes, SHA-256, variables, time
    steps, and grid. Every member must cover every hour of the month inside the area, and the
    members together must hold every requested variable exactly once.
    """
    if zipfile.is_zipfile(path):
        fmt, members = "zip", []
        with zipfile.ZipFile(path) as zf, tempfile.TemporaryDirectory() as tmp:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                if not info.filename.endswith(".nc"):
                    raise ValueError(f"unexpected member in archive: {info.filename}")
                body = zf.read(info)
                member_path = os.path.join(tmp, "member.nc")
                with open(member_path, "wb") as fh:
                    fh.write(body)
                members.append(
                    {
                        "name": info.filename,
                        "bytes": len(body),
                        "sha256": hashlib.sha256(body).hexdigest(),
                        **_inspect_one(member_path, year, month),
                    }
                )
    else:
        fmt = "netcdf"
        with open(path, "rb") as fh:
            body = fh.read()
        members = [
            {
                "name": None,
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                **_inspect_one(path, year, month),
            }
        ]
    found = [v for m in members for v in m["variables"]]
    missing = sorted(set(VARIABLES.values()) - set(found))
    if missing:
        raise ValueError(f"variables missing from the download: {missing}")
    if len(found) != len(set(found)):
        raise ValueError(f"a variable appears in more than one file: {sorted(found)}")
    return {"format": fmt, "members": members}


def request_id_of(result: Any) -> str | None:
    """CDS job id; the results URL is .../retrieve/v1/jobs/<id>/results."""
    parts = (getattr(result, "url", "") or "").split("/")
    return parts[parts.index("jobs") + 1] if "jobs" in parts[:-1] else None


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


def is_current(client: Minio, manifest_key: str) -> bool:
    """A month is current when its manifest exists and the object it names matches it."""
    manifest = _read(client, manifest_key)
    if manifest is None:
        return False
    meta = json.loads(manifest)
    stored = _read(client, meta.get("object_key", ""))
    return stored is not None and hashlib.sha256(stored).hexdigest() == meta.get("sha256")


def months_in(start_year: int, end_year: int, months: list[int] | None) -> list[tuple[int, int]]:
    return [(y, m) for y in range(start_year, end_year + 1) for m in (months or range(1, 13))]


def ingest_era5(
    start_year: int,
    end_year: int,
    months: list[int] | None = None,
    dry_run: bool = False,
    cds_client: Any = None,
    client: Minio | None = None,
) -> dict[str, Any]:
    """Request, check, and store each month; raise after the loop if any month failed."""
    scope = months_in(start_year, end_year, months)
    if dry_run:
        for year, month in scope:
            print(json.dumps({"dataset": DATASET, "request": build_request(year, month)}))
        return {"requests": len(scope), "dry_run": True}

    client = client or get_minio_client()
    if cds_client is None:
        if not (os.path.exists(os.path.expanduser("~/.cdsapirc")) or os.getenv("CDSAPI_KEY")):
            raise RuntimeError(
                "no CDS credentials: create ~/.cdsapirc or set CDSAPI_URL and CDSAPI_KEY"
            )
        import cdsapi

        cds_client = cdsapi.Client()

    counts = {"stored": 0, "skipped": 0}
    failures: list[str] = []
    for year, month in scope:
        _, manifest_key = bronze_keys(year, month)
        tag = f"  ERA5 {year}-{month:02d}"
        try:
            if is_current(client, manifest_key):
                print(f"{tag}: current (manifest and checksum match), skipping")
                counts["skipped"] += 1
                continue
            request = build_request(year, month)
            with tempfile.TemporaryDirectory() as tmp:
                target = os.path.join(tmp, "download.nc")
                result = cds_client.retrieve(DATASET, request)  # waits for the job
                result.download(target)
                retrieved_at = datetime.now(timezone.utc).isoformat()
                summary = inspect_download(target, year, month)
                with open(target, "rb") as fh:
                    raw = fh.read()
            is_zip = summary["format"] == "zip"
            data_key, _ = bronze_keys(year, month, "zip" if is_zip else "nc")
            client.put_object(
                BUCKET_BRONZE,
                data_key,
                io.BytesIO(raw),
                len(raw),
                content_type="application/zip" if is_zip else "application/x-netcdf",
            )
            manifest = {
                "source": "Copernicus Climate Data Store",
                "dataset": DATASET,
                "request": request,
                "request_id": request_id_of(result),
                "object_key": data_key,
                "retrieved_at_utc": retrieved_at,
                "bytes": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "content": summary,
                "licence_note": "Copernicus licence accepted on the CDS dataset page by the "
                "account owner; cite ERA5 (Hersbach et al., C3S)",
            }
            body = json.dumps(manifest, indent=2).encode("utf-8")
            client.put_object(
                BUCKET_BRONZE, manifest_key, io.BytesIO(body), len(body), "application/json"
            )
            counts["stored"] += 1
            hours = summary["members"][0]["time_steps"]
            print(f"{tag}: stored {summary['format']} ({hours} hours, {len(raw) / 1e6:.1f} MB)")
        except Exception as ex:
            print(f"{tag}: FAILED: {ex}")
            failures.append(f"{year}-{month:02d}: {ex}")

    print(json.dumps({**counts, "failed": len(failures)}))
    if failures:
        raise RuntimeError(f"ERA5 ingestion completed with {len(failures)} failure(s): {failures}")
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Request ERA5 monthly NetCDF into Bronze.")
    parser.add_argument("--start-year", type=int, required=True, help="First year (inclusive)")
    parser.add_argument("--end-year", type=int, required=True, help="Last year (inclusive)")
    parser.add_argument("--months", type=int, nargs="+", help="Only these months (1-12)")
    parser.add_argument("--dry-run", action="store_true", help="Print requests only")
    args = parser.parse_args()
    if not 2016 <= args.start_year <= args.end_year <= 2025:
        parser.error("the project scope is 2016-2025")
    try:
        ingest_era5(args.start_year, args.end_year, args.months, args.dry_run)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
