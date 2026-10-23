#!/usr/bin/env python3
"""scripts/transform_era5_to_silver.py.

Turn the Bronze ERA5 point time series (scripts/ingest_era5_timeseries.py) into Silver hourly
observations in the standard columns (workbook ID 15; docs/data-contract.md "ERA5 Silver").

Conversions, per ERA5 single-level conventions (checked against the stored files):
- t2m, d2m K -> temperature_c, dew_point_c (minus 273.15);
- relative humidity from t2m and d2m with the Magnus formula (Alduchov and Eskridge 1996,
  a = 17.625, b = 243.04 degC). Where d2m exceeds t2m by at most SATURATION_TOLERANCE_K (float
  precision of the stored values) the air is saturated and humidity is 100%; such hours are
  counted. A larger excess is left to the range check, which nulls the value;
- sp Pa -> pressure_hpa (surface pressure, pressure_type `surface`);
- tp m -> precipitation_mm. ERA5 hourly tp is the total over the hour ending at valid_time,
  the same convention as Open-Meteo, so no de-accumulation is needed (unlike ERA5-Land);
- u10, v10 -> wind_speed_ms and the meteorological direction the wind blows from, in
  [0, 360) after rounding; null when the stored speed is 0.00 (calm);
- tcc (0-1) -> cloud_cover_percent.
The Bronze object is any current series covering the window (scripts/ingest_era5_timeseries.py,
`find_current`); every requested year is checked before any Silver object is written.
The value of the nearest grid cell is kept as `grid_latitude`, `grid_longitude`.

Silver keys (bucket silver), outside `weather_hourly/` (Open-Meteo and Meteostat); the Gold loader
reads both prefixes and loads ERA5 into the DW since 2026-10-23:
  era5/weather_hourly/year=YYYY/month=MM/era5_hourly_<LOCATION>_<YYYYMM>.parquet
  quality_reports/dq_report_era5_<timestamp>.json
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
from typing import Any

import numpy as np
import pandas as pd
from ingest_era5_timeseries import find_current
from ingest_ghcnh import BUCKET_BRONZE, get_minio_client, is_s3_not_found_error
from ingest_open_meteo import NORTH_LOCATIONS
from minio import Minio
from minio.error import S3Error
from quality_checks import compute_hourly_coverage, validate_and_nullify_ranges

BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")
SOURCE_NAME = "ERA5"
MAGNUS_A, MAGNUS_B = 17.625, 243.04
SATURATION_TOLERANCE_K = 0.01
MEASUREMENTS = [
    "temperature_c",
    "humidity_percent",
    "pressure_hpa",
    "precipitation_mm",
    "wind_speed_ms",
    "wind_direction_deg",
    "cloud_cover_percent",
]
COLUMNS = [
    "source_name",
    "location_code",
    "station_id",
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
    "observation_type",
    "grid_latitude",
    "grid_longitude",
]


def silver_key(location: str, year: int, month: int) -> str:
    return (
        f"era5/weather_hourly/year={year}/month={month:02d}/"
        f"era5_hourly_{location}_{year}{month:02d}.parquet"
    )


def relative_humidity(t_c: pd.Series, td_c: pd.Series) -> pd.Series:
    """Magnus formula; the ratio of saturation vapour pressures at dew point and temperature."""
    return 100.0 * np.exp(MAGNUS_A * td_c / (MAGNUS_B + td_c) - MAGNUS_A * t_c / (MAGNUS_B + t_c))


def wind_from_uv(u: pd.Series, v: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Speed (m/s, 2 decimals) and the direction the wind blows from (degrees in [0, 360), 1
    decimal; 0 = north, 90 = east). Direction is null when the stored speed is 0.00 (calm)."""
    speed = np.hypot(u, v).round(2)
    direction = ((np.degrees(np.arctan2(-u, -v)) + 360.0) % 360.0).round(1) % 360.0
    return speed, direction.where(speed > 0)


def to_silver(
    ds_frame: pd.DataFrame, location: str, grid: tuple[float, float]
) -> tuple[pd.DataFrame, int]:
    """Standard Silver columns from one location's ERA5 frame (valid_time + short names)."""
    t_c = ds_frame["t2m"].astype("float64") - 273.15
    td_c = ds_frame["d2m"].astype("float64") - 273.15
    rh = relative_humidity(t_c, td_c)
    excess = ds_frame["d2m"].astype("float64") - ds_frame["t2m"].astype("float64")
    saturated = (excess > 0) & (excess <= SATURATION_TOLERANCE_K)
    rh = rh.mask(saturated, 100.0)
    speed, direction = wind_from_uv(
        ds_frame["u10"].astype("float64"), ds_frame["v10"].astype("float64")
    )
    ts = pd.to_datetime(ds_frame["valid_time"])
    ts = ts.dt.tz_localize("UTC") if ts.dt.tz is None else ts.dt.tz_convert("UTC")
    out = pd.DataFrame(
        {
            "source_name": SOURCE_NAME,
            "location_code": location,
            "station_id": None,
            "timestamp_utc": ts,
            "timestamp_local": ts.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None),
            "temperature_c": t_c.round(2),
            "humidity_percent": rh.round(1),
            "pressure_hpa": (ds_frame["sp"].astype("float64") / 100.0).round(2),
            "pressure_type": "surface",
            "precipitation_mm": (ds_frame["tp"].astype("float64") * 1000.0).round(3),
            "wind_speed_ms": speed,
            "wind_direction_deg": direction,
            "cloud_cover_percent": (ds_frame["tcc"].astype("float64") * 100.0).round(1),
            "solar_radiation_wm2": np.nan,
            "dew_point_c": td_c.round(2),
            "observation_type": "model_reanalysis",
            "grid_latitude": grid[0],
            "grid_longitude": grid[1],
        }
    )
    return out[COLUMNS], int(saturated.sum())


def _read(client: Minio, bucket: str, key: str) -> bytes | None:
    try:
        resp = client.get_object(bucket, key)
    except S3Error as err:
        if is_s3_not_found_error(err):
            return None
        raise
    try:
        return resp.read()
    finally:
        resp.close()
        resp.release_conn()


def read_bronze(
    client: Minio, location: str, start_year: int, end_year: int
) -> tuple[pd.DataFrame, dict]:
    """The checked Bronze time series covering the window, as a frame, and its manifest."""
    manifest = find_current(client, location, start_year, end_year)
    if manifest is None:
        raise FileNotFoundError(
            f"no current Bronze ERA5 time series for {location} covering {start_year}-{end_year}"
        )
    raw = _read(client, BUCKET_BRONZE, manifest["object_key"])
    if raw is None:
        raise FileNotFoundError(f"Bronze object {manifest['object_key']} is missing")
    if hashlib.sha256(raw).hexdigest() != manifest["sha256"]:
        raise ValueError(f"{manifest['object_key']} does not match its manifest SHA-256")
    import xarray as xr

    frames = []
    with tempfile.TemporaryDirectory() as tmp:
        paths = []
        if zipfile.is_zipfile(io.BytesIO(raw)):
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                for i, name in enumerate(n for n in zf.namelist() if n.endswith(".nc")):
                    path = os.path.join(tmp, f"member{i}.nc")
                    with open(path, "wb") as fh:
                        fh.write(zf.read(name))
                    paths.append(path)
        else:
            path = os.path.join(tmp, "member.nc")
            with open(path, "wb") as fh:
                fh.write(raw)
            paths.append(path)
        grid = None
        for path in paths:
            with xr.open_dataset(path, engine="netcdf4") as ds:
                time_name = "valid_time" if "valid_time" in ds.coords else "time"
                grid = (float(ds["latitude"].values), float(ds["longitude"].values))
                names = [v for v in ("t2m", "d2m", "tp", "sp", "u10", "v10", "tcc") if v in ds]
                frame = ds[names].to_dataframe().reset_index()
                frames.append(
                    frame.rename(columns={time_name: "valid_time"})[["valid_time", *names]]
                )
    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on="valid_time", how="outer")
    merged.attrs["grid"] = grid
    return merged, manifest


def transform_location(
    client: Minio, location: str, start_year: int, end_year: int, overwrite: bool
) -> dict[str, Any]:
    frame, manifest = read_bronze(client, location, start_year, end_year)
    df, saturated = to_silver(frame, location, frame.attrs["grid"])
    df, violations = validate_and_nullify_ranges(df)
    duplicates = int(df.duplicated(["timestamp_utc"]).sum())
    if duplicates:
        raise ValueError(f"{duplicates} duplicate hours in the Bronze time series")
    result = {
        "location": location,
        "bronze_object": manifest["object_key"],
        "bronze_sha256": manifest["sha256"],
        "grid": list(frame.attrs["grid"]),
        "rows": int(len(df)),
        "humidity_set_to_saturation": saturated,
        "range_violations_nulled": {k: v for k, v in violations.items() if v},
        "years": {},
        "written": 0,
        "skipped_existing": 0,
    }
    parts = {}
    for year in range(start_year, end_year + 1):  # check every year before writing any
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        part = df[(df["timestamp_utc"] >= start) & (df["timestamp_utc"] < end)]
        cov = compute_hourly_coverage(part, start, end, MEASUREMENTS)
        result["years"][year] = {
            "rows": cov["actual_records"],
            "expected_hours": cov["expected_hours"],
            "missing_hours": cov["missing_hours"],
            "null_values": {
                c: m["null_count"] for c, m in cov["value_completeness"].items() if m["null_count"]
            },
        }
        if cov["missing_hours"] or cov["out_of_bounds_count"] or cov["unaligned_hours_count"]:
            raise ValueError(f"{location} {year}: incomplete hourly grid {cov}")
        parts[year] = part
    for year, part in parts.items():
        for month, month_df in part.groupby(part["timestamp_utc"].dt.month):
            key = silver_key(location, year, int(month))
            if not overwrite:
                try:
                    client.stat_object(BUCKET_SILVER, key)
                    result["skipped_existing"] += 1
                    continue
                except S3Error as err:
                    if not is_s3_not_found_error(err):
                        raise
            buf = io.BytesIO()
            month_df.reset_index(drop=True).to_parquet(buf, index=False, compression="snappy")
            body = buf.getvalue()
            client.put_object(
                BUCKET_SILVER, key, io.BytesIO(body), len(body), "application/octet-stream"
            )
            result["written"] += 1
    return result


def transform_era5(
    start_year: int,
    end_year: int,
    location: str | None = None,
    overwrite: bool = False,
    client: Minio | None = None,
) -> dict[str, Any]:
    if location and location not in NORTH_LOCATIONS:
        raise ValueError(f"Unknown location: {location}")
    client = client or get_minio_client()
    locations = [location] if location else sorted(NORTH_LOCATIONS)
    report: dict[str, Any] = {
        "source": SOURCE_NAME,
        "product": "reanalysis-era5-single-levels-timeseries",
        "window_utc_years": [start_year, end_year],
        "locations": [],
        "failures": [],
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    for loc in locations:
        try:
            res = transform_location(client, loc, start_year, end_year, overwrite)
            report["locations"].append(res)
            print(
                f"  ERA5 Silver {loc}: {res['rows']} rows, {res['written']} written, "
                f"{res['skipped_existing']} existing kept, saturation {res['humidity_set_to_saturation']}, "
                f"nulled {res['range_violations_nulled'] or 0}",
                flush=True,
            )
        except Exception as ex:
            print(f"  ERA5 Silver {loc}: FAILED: {ex}", flush=True)
            report["failures"].append({"location": loc, "error": str(ex)})
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"quality_reports/dq_report_era5_{stamp}.json"
    body = json.dumps(report, indent=2, default=str).encode("utf-8")
    client.put_object(BUCKET_SILVER, key, io.BytesIO(body), len(body), "application/json")
    report["report_key"] = f"{BUCKET_SILVER}/{key}"
    print(json.dumps({"report_key": report["report_key"], "failures": len(report["failures"])}))
    if report["failures"]:
        raise RuntimeError(f"ERA5 Silver completed with {len(report['failures'])} failure(s)")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Transform Bronze ERA5 time series to Silver.")
    parser.add_argument(
        "--start-year", type=int, required=True, help="First UTC year of the Bronze window"
    )
    parser.add_argument(
        "--end-year", type=int, required=True, help="Last UTC year of the Bronze window"
    )
    parser.add_argument("--location", default=None, help="One canonical location code")
    parser.add_argument("--overwrite", action="store_true", help="Replace existing Silver objects")
    args = parser.parse_args()
    if not 2016 <= args.start_year <= args.end_year <= 2025:
        parser.error("the project scope is 2016-2025")
    try:
        transform_era5(
            args.start_year, args.end_year, args.location and args.location.upper(), args.overwrite
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
