#!/usr/bin/env python3
"""scripts/ingest_era5_timeseries.py.

Request ERA5 hourly point time series for the 8 research points from the Copernicus Climate
Data Store (CDS) and store each response unchanged in Bronze with a manifest (workbook IDs 5
and 14; decided 2026-10-23, see docs/data-contract.md "ERA5 Bronze").

Product `reanalysis-era5-single-levels-timeseries`: the same ERA5 hourly single-level data as
scripts/ingest_era5.py, served per point from the nearest 0.25 degree grid cell. One request per
location covers the whole window (2016-2025 took about 25 s on 2026-10-22), whereas the gridded
monthly requests wait about two hours each in the CDS queue. Credentials and licence as in
scripts/ingest_era5.py. Requests are sent one at a time.

Bronze (per docs/NAMING_CONVENTIONS.md), the received file unchanged:
  era5/timeseries/location=<CODE>/era5_timeseries_<CODE>_<YYYYMMDD>_<YYYYMMDD>.<zip|nc>
  era5/manifests/timeseries/location=<CODE>/era5_timeseries_<CODE>_<YYYYMMDD>_<YYYYMMDD>.json

A location is current when its manifest exists and the stored bytes match its SHA-256; it is
then skipped. A file whose variables or hourly time steps do not match the request, or whose
grid cell is not the one nearest the point, is a failure and is not stored.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from typing import Any, Callable

from ingest_era5 import VARIABLES, is_current, make_cds_client, request_id_of
from ingest_ghcnh import BUCKET_BRONZE, get_minio_client
from ingest_open_meteo import NORTH_LOCATIONS
from minio import Minio

DATASET = "reanalysis-era5-single-levels-timeseries"
# Nearest 0.25 degree cell: each coordinate is at most 0.125 degrees from the point.
MAX_GRID_OFFSET_DEG = 0.125 + 1e-6


def window(start_year: int, end_year: int) -> tuple[str, str]:
    return f"{start_year}0101", f"{end_year}1231"


def bronze_keys(location: str, start_year: int, end_year: int, extension: str = "zip"):
    first, last = window(start_year, end_year)
    name = f"era5_timeseries_{location}_{first}_{last}"
    part = f"location={location}"
    return (
        f"era5/timeseries/{part}/{name}.{extension}",
        f"era5/manifests/timeseries/{part}/{name}.json",
    )


def build_request(location: str, start_year: int, end_year: int) -> dict[str, Any]:
    point = NORTH_LOCATIONS[location]
    return {
        "variable": list(VARIABLES),
        "location": {"latitude": point["latitude"], "longitude": point["longitude"]},
        "date": [f"{start_year}-01-01/{end_year}-12-31"],
        "data_format": "netcdf",
    }


def expected_hours(start_year: int, end_year: int) -> int:
    start = datetime(start_year, 1, 1, tzinfo=timezone.utc)
    end = datetime(end_year + 1, 1, 1, tzinfo=timezone.utc)
    return int((end - start).total_seconds() // 3600)


def _inspect_netcdf(path: str, request: dict[str, Any], hours: int) -> dict[str, Any]:
    import numpy as np
    import xarray as xr

    with xr.open_dataset(path, engine="netcdf4") as ds:
        time_name = "valid_time" if "valid_time" in ds.coords else "time"
        times = ds[time_name].values
        if len(times) != hours or len(np.unique(times)) != hours:
            raise ValueError(
                f"{len(times)} time steps ({len(np.unique(times))} unique), expected {hours}"
            )
        lat, lon = float(ds["latitude"].values), float(ds["longitude"].values)
        point = request["location"]
        if (
            abs(lat - point["latitude"]) > MAX_GRID_OFFSET_DEG
            or abs(lon - point["longitude"]) > MAX_GRID_OFFSET_DEG
        ):
            raise ValueError(f"grid cell ({lat}, {lon}) is not the nearest to {point}")
        names = sorted(v for v in ds.data_vars if v in VARIABLES.values())
        return {
            "variables": names,
            "time_coordinate": time_name,
            "first_time": str(times.min()),
            "last_time": str(times.max()),
            "time_steps": int(len(times)),
            "grid_latitude": lat,
            "grid_longitude": lon,
            "units": {v: ds[v].attrs.get("units") for v in names},
            "null_values": {v: int(ds[v].isnull().sum()) for v in names},
        }


def inspect_download(path: str, request: dict[str, Any], hours: int) -> dict[str, Any]:
    """Check a download (one NetCDF, or a zip of NetCDF files) against its request."""
    members = []
    if zipfile.is_zipfile(path):
        fmt = "zip"
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
                        **_inspect_netcdf(member_path, request, hours),
                    }
                )
    else:
        fmt = "netcdf"
        with open(path, "rb") as fh:
            body = fh.read()
        members.append(
            {
                "name": None,
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                **_inspect_netcdf(path, request, hours),
            }
        )
    found = [v for m in members for v in m["variables"]]
    missing = sorted(set(VARIABLES.values()) - set(found))
    if missing:
        raise ValueError(f"variables missing from the download: {missing}")
    if len(found) != len(set(found)):
        raise ValueError(f"a variable appears in more than one file: {sorted(found)}")
    return {"format": fmt, "members": members}


def ingest_location(
    location: str, start_year: int, end_year: int, get_cds: Callable[[], Any], client: Minio
) -> str:
    """Request, check, and store one location; return "stored" or "skipped", raise on failure.

    The CDS client is created only when a request is needed, so a rerun over current objects
    needs no credentials."""
    tag = f"  ERA5 time series {location} {start_year}-{end_year}"
    _, manifest_key = bronze_keys(location, start_year, end_year)
    if is_current(client, manifest_key):
        print(f"{tag}: current (manifest and checksum match), skipping", flush=True)
        return "skipped"
    request = build_request(location, start_year, end_year)
    with tempfile.TemporaryDirectory() as tmp:
        target = os.path.join(tmp, "download")
        result = get_cds().retrieve(DATASET, request)  # waits for the job
        result.download(target)
        retrieved_at = datetime.now(timezone.utc).isoformat()
        summary = inspect_download(target, request, expected_hours(start_year, end_year))
        with open(target, "rb") as fh:
            raw = fh.read()
    is_zip = summary["format"] == "zip"
    data_key, _ = bronze_keys(location, start_year, end_year, "zip" if is_zip else "nc")
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
        "location_code": location,
        "request": request,
        "request_id": request_id_of(result),
        "object_key": data_key,
        "retrieved_at_utc": retrieved_at,
        "bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "content": summary,
        "licence_note": "Copernicus licence accepted on the CDS dataset page by the account "
        "owner; cite ERA5 (Hersbach et al., C3S)",
    }
    body = json.dumps(manifest, indent=2).encode("utf-8")
    client.put_object(BUCKET_BRONZE, manifest_key, io.BytesIO(body), len(body), "application/json")
    member = summary["members"][0]
    print(
        f"{tag}: stored {summary['format']} ({member['time_steps']} hours, grid "
        f"{member['grid_latitude']}, {member['grid_longitude']}; {len(raw) / 1e6:.2f} MB)",
        flush=True,
    )
    return "stored"


def ingest_era5_timeseries(
    start_year: int,
    end_year: int,
    location: str | None = None,
    dry_run: bool = False,
    cds_client: Any = None,
    client: Minio | None = None,
) -> dict[str, Any]:
    """Request each location in turn; raise at the end if any location failed."""
    if location and location not in NORTH_LOCATIONS:
        raise ValueError(f"Unknown location: {location}")
    locations = [location] if location else sorted(NORTH_LOCATIONS)
    if dry_run:
        for loc in locations:
            print(
                json.dumps(
                    {"dataset": DATASET, "request": build_request(loc, start_year, end_year)}
                )
            )
        return {"requests": len(locations), "dry_run": True}
    client = client or get_minio_client()
    holder = {"cds": cds_client}

    def get_cds() -> Any:
        if holder["cds"] is None:
            holder["cds"] = make_cds_client()
        return holder["cds"]

    counts = {"stored": 0, "skipped": 0}
    failures: list[str] = []
    for loc in locations:
        try:
            counts[ingest_location(loc, start_year, end_year, get_cds, client)] += 1
        except Exception as ex:
            print(f"  ERA5 time series {loc}: FAILED: {ex}", flush=True)
            failures.append(f"{loc}: {ex}")
    print(json.dumps({**counts, "failed": len(failures)}))
    if failures:
        raise RuntimeError(
            f"ERA5 time series completed with {len(failures)} failure(s): {failures}"
        )
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Request ERA5 point time series into Bronze.")
    parser.add_argument("--start-year", type=int, required=True, help="First year (inclusive)")
    parser.add_argument("--end-year", type=int, required=True, help="Last year (inclusive)")
    parser.add_argument("--location", default=None, help="One canonical location code")
    parser.add_argument("--dry-run", action="store_true", help="Print requests only")
    args = parser.parse_args()
    if not 2016 <= args.start_year <= args.end_year <= 2025:
        parser.error("the project scope is 2016-2025")
    try:
        ingest_era5_timeseries(
            args.start_year, args.end_year, args.location and args.location.upper(), args.dry_run
        )
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
