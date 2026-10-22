#!/usr/bin/env python3
"""scripts/compare_era5_open_meteo.py.

Cross-source comparison of ERA5 (nearest grid cell, scripts/transform_era5_to_silver.py) with
Open-Meteo (research point) in Silver, per location x UTC year (workbook ID 15; business
requirements section 5a: ERA5 is for cross-source comparison). Only hours present in both with
non-null values are paired; bias is ERA5 minus Open-Meteo.

Open-Meteo's historical API is built largely from ERA5-family reanalysis, so agreement is not
independent confirmation. Surface pressure depends on terrain height: the ERA5 cell's model
terrain differs from the point, so pressure (and to a lesser degree temperature) bias reflects
height as well as data. Report:
silver/quality_reports/compare_era5_open_meteo_<start>_<end>_<timestamp>.json
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from ingest_ghcnh import get_minio_client
from ingest_open_meteo import NORTH_LOCATIONS
from load_gold_observations import open_meteo_silver_key
from minio import Minio
from transform_era5_to_silver import BUCKET_SILVER, silver_key

VARIABLES = [
    "temperature_c",
    "humidity_percent",
    "pressure_hpa",
    "precipitation_mm",
    "wind_speed_ms",
    "cloud_cover_percent",
]


def _read(client: Minio, key: str) -> pd.DataFrame | None:
    try:
        resp = client.get_object(BUCKET_SILVER, key)
    except Exception as err:
        if getattr(err, "code", None) in ("NoSuchKey", "NoSuchObject"):
            return None
        raise
    try:
        return pd.read_parquet(io.BytesIO(resp.read()))
    finally:
        resp.close()
        resp.release_conn()


def metrics(a: pd.Series, b: pd.Series) -> dict[str, Any]:
    ok = a.notna() & b.notna()
    a, b = a[ok].astype("float64"), b[ok].astype("float64")
    if len(a) < 2:
        return {"pairs": int(len(a))}  # no pairs is not agreement
    d = a - b
    return {
        "pairs": int(len(a)),
        "mean_era5": round(float(a.mean()), 3),
        "mean_open_meteo": round(float(b.mean()), 3),
        "bias": round(float(d.mean()), 3),
        "mae": round(float(d.abs().mean()), 3),
        "rmse": round(float(np.sqrt((d**2).mean())), 3),
        "corr": round(float(a.corr(b)), 3),
    }


def compare_location_year(client: Minio, location: str, year: int) -> dict[str, Any]:
    era5, om = [], []
    for month in range(1, 13):
        e = _read(client, silver_key(location, year, month))
        o = _read(client, open_meteo_silver_key(location, year, month))
        if e is not None:
            era5.append(e)
        if o is not None:
            om.append(o)
    if not era5 or not om:
        return {
            "location": location,
            "year": year,
            "pairs": 0,
            "note": "a source has no Silver data",
        }
    cols = ["timestamp_utc", *VARIABLES]
    m = pd.concat(era5)[cols].merge(pd.concat(om)[cols], on="timestamp_utc", suffixes=("_e", "_o"))
    out: dict[str, Any] = {"location": location, "year": year, "pairs": int(len(m))}
    out["variables"] = {v: metrics(m[f"{v}_e"], m[f"{v}_o"]) for v in VARIABLES}
    rain = m[["precipitation_mm_e", "precipitation_mm_o"]].dropna()
    out["rain_total_mm"] = {
        "era5": round(float(rain["precipitation_mm_e"].sum()), 1),
        "open_meteo": round(float(rain["precipitation_mm_o"].sum()), 1),
        "paired_hours": int(len(rain)),
    }
    grid = pd.concat(era5)[["grid_latitude", "grid_longitude"]].iloc[0]
    out["era5_grid"] = [float(grid["grid_latitude"]), float(grid["grid_longitude"])]
    out["point"] = [NORTH_LOCATIONS[location]["latitude"], NORTH_LOCATIONS[location]["longitude"]]
    return out


def compare(start_year: int, end_year: int, client: Minio | None = None) -> dict[str, Any]:
    client = client or get_minio_client()
    rows = [
        compare_location_year(client, loc, year)
        for loc in sorted(NORTH_LOCATIONS)
        for year in range(start_year, end_year + 1)
    ]
    report = {
        "check": "ERA5 (grid cell) versus Open-Meteo (point), Silver hourly, bias = ERA5 - Open-Meteo",
        "window_utc_years": [start_year, end_year],
        "groups": rows,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"quality_reports/compare_era5_open_meteo_{start_year}_{end_year}_{stamp}.json"
    body = json.dumps(report, indent=2).encode("utf-8")
    client.put_object(BUCKET_SILVER, key, io.BytesIO(body), len(body), "application/json")
    report["report_key"] = f"{BUCKET_SILVER}/{key}"
    for loc in sorted(NORTH_LOCATIONS):
        groups = [g for g in rows if g["location"] == loc and g.get("pairs")]
        if not groups:
            print(f"  {loc}: no pairs")
            continue
        temp = [g["variables"]["temperature_c"] for g in groups]
        pres = [g["variables"]["pressure_hpa"] for g in groups]
        e = sum(g["rain_total_mm"]["era5"] for g in groups) / len(groups)
        o = sum(g["rain_total_mm"]["open_meteo"] for g in groups) / len(groups)
        print(
            f"  {loc}: pairs {sum(g['pairs'] for g in groups)}, temperature bias "
            f"{np.mean([t['bias'] for t in temp]):+.2f} C corr {np.mean([t['corr'] for t in temp]):.3f}, "
            f"pressure bias {np.mean([p['bias'] for p in pres]):+.1f} hPa, "
            f"rain per year ERA5 {e:.0f} mm / Open-Meteo {o:.0f} mm"
        )
    print(json.dumps({"report_key": report["report_key"], "groups": len(rows)}))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare ERA5 and Open-Meteo Silver hourly data.")
    parser.add_argument("--start-year", type=int, required=True, help="First UTC year")
    parser.add_argument("--end-year", type=int, required=True, help="Last UTC year")
    args = parser.parse_args()
    if not 2016 <= args.start_year <= args.end_year <= 2025:
        parser.error("the project scope is 2016-2025")
    try:
        compare(args.start_year, args.end_year)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
