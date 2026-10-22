#!/usr/bin/env python3
"""scripts/reconcile_dw.py.

Reconcile fact_weather_observation with Silver for a window (workbook ID 17, final task of the
Bronze -> DW DAG). Counts rows per source x location x UTC year in Silver `weather_hourly/`
(the sources loaded by scripts/load_gold_observations.py) and in the DW, and fails when any
group differs or exists on one side only. A JSON report is written to
gold/quality_reports/reconcile_<start>_<end>[_<location>]_<timestamp>.json.

Silver rows outside the canonical locations are ignored, as the loader ignores them. GHCNh
daily rain is reconciled too: Silver station-year rows against fact_station_daily_rain per
station x UTC year of the rain day.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from load_gold_observations import (
    BUCKET_GOLD,
    BUCKET_SILVER,
    LOADED_SOURCES,
    get_db_connection,
    get_minio_client,
    requested_locations,
)
from minio import Minio
from minio.error import S3Error
from station_config import GHCNH_RAIN_STATIONS, ghcnh_stations_for
from transform_ghcnh_to_silver import silver_keys as ghcnh_silver_keys

Key = tuple[str, str, int]  # source, location, UTC year

DW_COUNT_SQL = """
    SELECT s.source_name, l.location_code,
           extract(year FROM f.timestamp_utc AT TIME ZONE 'UTC')::int AS year,
           count(*)
    FROM fact_weather_observation f
    JOIN dim_source s ON s.source_key = f.source_key
    JOIN dim_location l ON l.location_key = f.location_key
    WHERE f.timestamp_utc >= %s AND f.timestamp_utc < %s
      AND s.source_name = ANY(%s) AND l.location_code = ANY(%s)
    GROUP BY 1, 2, 3
"""


def silver_counts(
    client: Minio, start_year: int, end_year: int, locations: list[str]
) -> Counter[Key]:
    counts: Counter[Key] = Counter()
    for year in range(start_year, end_year + 1):
        prefix = f"weather_hourly/year={year}/"
        for obj in client.list_objects(BUCKET_SILVER, prefix=prefix, recursive=True):
            if not obj.object_name.endswith(".parquet"):
                continue
            resp = client.get_object(BUCKET_SILVER, obj.object_name)
            try:
                df = pd.read_parquet(
                    io.BytesIO(resp.read()),
                    columns=["source_name", "location_code", "timestamp_utc"],
                )
            finally:
                resp.close()
                resp.release_conn()
            df["location_code"] = df["location_code"].astype(str).str.strip().str.upper()
            df = df[df["location_code"].isin(locations) & df["source_name"].isin(LOADED_SOURCES)]
            years = pd.to_datetime(df["timestamp_utc"], utc=True).dt.year
            for (src, loc, yr), n in (
                df.groupby([df["source_name"], df["location_code"], years]).size().items()
            ):
                counts[(src, loc, int(yr))] += int(n)
    return counts


DAILY_RAIN_COUNT_SQL = """
    SELECT st.station_id, extract(year FROM f.rain_day_utc)::int AS year, count(*)
    FROM fact_station_daily_rain f
    JOIN dim_station st ON st.station_key = f.station_key
    WHERE f.rain_day_utc >= %s AND f.rain_day_utc < %s AND st.station_id = ANY(%s)
    GROUP BY 1, 2
"""


def silver_daily_rain_counts(
    client: Minio, start_year: int, end_year: int, stations: list[str]
) -> Counter[Key]:
    counts: Counter[Key] = Counter()
    for sid in stations:
        for year in range(start_year, end_year + 1):
            key, _ = ghcnh_silver_keys(sid, year)
            try:
                resp = client.get_object(BUCKET_SILVER, key)
            except S3Error as err:
                if getattr(err, "code", None) in ("NoSuchKey", "NoSuchObject"):
                    continue  # no usable day that year: compared as 0 rows
                raise
            try:
                df = pd.read_parquet(io.BytesIO(resp.read()), columns=["rain_day_utc"])
            finally:
                resp.close()
                resp.release_conn()
            for yr, n in pd.to_datetime(df["rain_day_utc"]).dt.year.value_counts().items():
                counts[("GHCNh", sid, int(yr))] += int(n)
    return counts


def dw_daily_rain_counts(conn, start_year: int, end_year: int, stations: list[str]) -> Counter[Key]:
    start, end = datetime(start_year, 1, 1).date(), datetime(end_year + 1, 1, 1).date()
    with conn.cursor() as cur:
        cur.execute(DAILY_RAIN_COUNT_SQL, (start, end, stations))
        return Counter({("GHCNh", sid, int(yr)): int(n) for sid, yr, n in cur.fetchall()})


def dw_counts(conn, start_year: int, end_year: int, locations: list[str]) -> Counter[Key]:
    start = datetime(start_year, 1, 1, tzinfo=timezone.utc)
    end = datetime(end_year + 1, 1, 1, tzinfo=timezone.utc)
    with conn.cursor() as cur:
        cur.execute(DW_COUNT_SQL, (start, end, list(LOADED_SOURCES), locations))
        return Counter({(src, loc, int(yr)): int(n) for src, loc, yr, n in cur.fetchall()})


def compare(silver: Counter[Key], dw: Counter[Key]) -> list[dict[str, Any]]:
    """Groups whose counts differ, including groups present on one side only."""
    return [
        {"source": k[0], "location": k[1], "year": k[2], "silver": silver[k], "dw": dw[k]}
        for k in sorted(set(silver) | set(dw))
        if silver[k] != dw[k]
    ]


def reconcile(
    start_year: int,
    end_year: int,
    location: str | None = None,
    client: Minio | None = None,
    conn=None,
) -> dict[str, Any]:
    locations = requested_locations(location)
    stations = ghcnh_stations_for(location) if location else list(GHCNH_RAIN_STATIONS)
    client = client or get_minio_client()
    own_conn = conn is None
    conn = conn or get_db_connection()
    try:
        silver = silver_counts(client, start_year, end_year, locations)
        dw = dw_counts(conn, start_year, end_year, locations)
        rain_silver = silver_daily_rain_counts(client, start_year, end_year, stations)
        rain_dw = (
            dw_daily_rain_counts(conn, start_year, end_year, stations) if stations else Counter()
        )
    finally:
        if own_conn:
            conn.close()
    mismatches = compare(silver, dw)
    rain_mismatches = compare(rain_silver, rain_dw)
    report = {
        "check": "reconcile Silver weather_hourly vs fact_weather_observation",
        "window_utc_years": [start_year, end_year],
        "locations": locations,
        "sources": list(LOADED_SOURCES),
        "groups": len(set(silver) | set(dw)),
        "silver_rows": sum(silver.values()),
        "dw_rows": sum(dw.values()),
        "mismatches": mismatches,
        "daily_rain_stations": stations,
        "daily_rain_groups": len(set(rain_silver) | set(rain_dw)),
        "daily_rain_silver_rows": sum(rain_silver.values()),
        "daily_rain_dw_rows": sum(rain_dw.values()),
        "daily_rain_mismatches": rain_mismatches,
        "passed": not mismatches and not rain_mismatches,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    tag = f"{start_year}_{end_year}" + (f"_{locations[0]}" if location else "")
    key = f"quality_reports/reconcile_{tag}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    body = json.dumps(report, indent=2).encode("utf-8")
    client.put_object(BUCKET_GOLD, key, io.BytesIO(body), len(body), "application/json")
    report["report_key"] = f"{BUCKET_GOLD}/{key}"
    print(
        json.dumps(
            {k: v for k, v in report.items() if k not in ("mismatches", "daily_rain_mismatches")},
            indent=2,
        )
    )
    if mismatches or rain_mismatches:
        bad = mismatches + rain_mismatches
        raise RuntimeError(f"{len(bad)} group(s) differ between Silver and DW: {bad[:5]}")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile DW observation counts with Silver.")
    parser.add_argument("--start-year", type=int, required=True, help="First UTC year")
    parser.add_argument("--end-year", type=int, required=True, help="Last UTC year")
    parser.add_argument("--location", default=None, help="One canonical location code")
    args = parser.parse_args()
    try:
        reconcile(args.start_year, args.end_year, args.location)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
