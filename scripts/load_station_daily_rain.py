#!/usr/bin/env python3
"""scripts/load_station_daily_rain.py.

Load Silver GHCNh daily station rainfall into fact_station_daily_rain (workbook ID 9).
Requires sql/03_station_daily_rain.sql and seeded dim_station (scripts/seed_dimensions.py).

Each station-year is one transaction: rows in that scope that are absent from Silver are
deleted, then incoming rows are upserted on (source_key, station_key, rain_day_utc).
A station-year without a Silver object is retracted only when its Bronze manifest exists
(the file was read and produced no usable day); otherwise it is skipped.
"""

import argparse
import io
import sys
from datetime import date
from typing import Any

import pandas as pd
from ingest_ghcnh import bronze_keys, get_minio_client, is_s3_not_found_error, read_manifest
from load_gold_observations import get_db_connection
from minio import Minio
from minio.error import S3Error
from psycopg2.extras import execute_values
from station_config import GHCNH_RAIN_STATIONS, resolve_ghcnh_stations
from transform_ghcnh_to_silver import BUCKET_SILVER, SOURCE_NAME, silver_keys

UPSERT_SQL = """
    INSERT INTO fact_station_daily_rain (
        source_key, station_key, location_key, rain_day_utc, window_start_utc,
        window_end_utc, rain_24h_mm, is_trace, station_role, quality_code,
        measurement_code, report_type, source_code
    ) VALUES %s
    ON CONFLICT (source_key, station_key, rain_day_utc) DO UPDATE SET
        location_key = EXCLUDED.location_key,
        window_start_utc = EXCLUDED.window_start_utc,
        window_end_utc = EXCLUDED.window_end_utc,
        rain_24h_mm = EXCLUDED.rain_24h_mm,
        is_trace = EXCLUDED.is_trace,
        station_role = EXCLUDED.station_role,
        quality_code = EXCLUDED.quality_code,
        measurement_code = EXCLUDED.measurement_code,
        report_type = EXCLUDED.report_type,
        source_code = EXCLUDED.source_code;
"""

RETRACT_SQL = """
    DELETE FROM fact_station_daily_rain
    WHERE source_key = %s AND station_key = %s
      AND rain_day_utc >= %s AND rain_day_utc < %s
      AND NOT (rain_day_utc = ANY(%s::date[]));
"""


def lookup_keys(conn) -> tuple[int, dict[str, int], dict[str, int]]:
    with conn.cursor() as cur:
        cur.execute("SELECT source_key FROM dim_source WHERE source_name = %s", (SOURCE_NAME,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("dim_source has no 'GHCNh' row; apply sql/03_station_daily_rain.sql")
        cur.execute("SELECT station_id, station_key FROM dim_station")
        stations = dict(cur.fetchall())
        cur.execute("SELECT location_code, location_key FROM dim_location")
        locations = dict(cur.fetchall())
    return row[0], stations, locations


def read_silver(client: Minio, key: str) -> pd.DataFrame | None:
    try:
        resp = client.get_object(BUCKET_SILVER, key)
        try:
            return pd.read_parquet(io.BytesIO(resp.read()))
        finally:
            resp.close()
            resp.release_conn()
    except S3Error as err:
        if is_s3_not_found_error(err):
            return None
        raise


def load_station_year(
    conn, source_key: int, station_key: int, location_key: int, year: int, df: pd.DataFrame
) -> int:
    """Replace one station-year scope transactionally; return the number of rows upserted."""
    records: list[tuple[Any, ...]] = [
        (
            source_key,
            station_key,
            location_key,
            r.rain_day_utc,
            r.window_start_utc.to_pydatetime(),
            r.window_end_utc.to_pydatetime(),
            round(float(r.rain_24h_mm), 1),
            bool(r.is_trace),
            r.station_role,
            r.quality_code,
            r.measurement_code if isinstance(r.measurement_code, str) else None,
            r.report_type,
            r.source_code,
        )
        for r in df.itertuples(index=False)
    ]
    days = [rec[3] for rec in records]
    try:
        with conn.cursor() as cur:
            cur.execute(
                RETRACT_SQL,
                (source_key, station_key, date(year, 1, 1), date(year + 1, 1, 1), days),
            )
            if records:
                execute_values(cur, UPSERT_SQL, records, page_size=1000)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(records)


def run_load(
    start_year: int = 2016,
    end_year: int = 2025,
    station_ids: list[str] | None = None,
    conn=None,
    client: Minio | None = None,
) -> dict[str, Any]:
    client = client or get_minio_client()
    own_conn = conn is None
    conn = conn or get_db_connection()
    stats: dict[str, Any] = {
        "rows_upserted": 0,
        "station_years": 0,
        "retracted_scopes": 0,
        "failures": [],
    }
    try:
        source_key, station_keys, location_keys = lookup_keys(conn)
        for wmo_id in station_ids or list(GHCNH_RAIN_STATIONS):
            mapping = GHCNH_RAIN_STATIONS[wmo_id]
            station_key = station_keys.get(wmo_id)
            location_key = location_keys.get(mapping["location_code"])
            if station_key is None or location_key is None:
                stats["failures"].append(
                    {"station_id": wmo_id, "error": "station or location missing in dimensions"}
                )
                continue
            for year in range(start_year, end_year + 1):
                try:
                    df = read_silver(client, silver_keys(wmo_id, year)[0])
                    if df is None:
                        if read_manifest(client, bronze_keys(wmo_id, year)[1]) is None:
                            continue
                        df = pd.DataFrame(columns=["rain_day_utc"])
                        stats["retracted_scopes"] += 1
                    n = load_station_year(conn, source_key, station_key, location_key, year, df)
                    stats["rows_upserted"] += n
                    stats["station_years"] += 1
                    print(f"  {wmo_id} {year}: {n} days loaded")
                except Exception as ex:
                    stats["failures"].append({"station_id": wmo_id, "year": year, "error": str(ex)})
                    print(f"  [!] {wmo_id} {year}: {ex}")
    finally:
        if own_conn:
            conn.close()
    print(
        f"Station daily rain load: {stats['station_years']} station-years, "
        f"{stats['rows_upserted']} rows, failures {len(stats['failures'])}"
    )
    if stats["failures"]:
        raise RuntimeError(f"Station daily rain load had {len(stats['failures'])} failure(s)")
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Load Silver GHCNh daily rain into the DW.")
    parser.add_argument("--start-year", type=int, required=True, help="First year (inclusive)")
    parser.add_argument("--end-year", type=int, required=True, help="Last year (inclusive)")
    parser.add_argument("--stations", type=str, default=None, help="Comma-separated WMO ids")
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
        run_load(args.start_year, args.end_year, stations)
    except RuntimeError as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
