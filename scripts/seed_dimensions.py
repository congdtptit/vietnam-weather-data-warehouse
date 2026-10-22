#!/usr/bin/env python3
"""scripts/seed_dimensions.py.

Populate and synchronize Dimension tables in PostgreSQL Data Warehouse (weather_dwh)
and export Dimension Parquets to MinIO Gold bucket according to docs/warehouse-schema.md
and docs/NAMING_CONVENTIONS.md Table 5.
"""

import io
import os
import sys
from datetime import date, timedelta
from typing import Any

import pandas as pd
import psycopg2
from minio import Minio
from psycopg2.extras import execute_values
from station_config import STATION_REGISTRY

# Configure UTF-8 for Windows console
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Load environment variables
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
BUCKET_GOLD = os.getenv("MINIO_BUCKET_GOLD", "gold")

# PostgreSQL configuration
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DWH_DB = os.getenv("DWH_DB", "weather_dwh")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5434"))


def get_minio_client() -> Minio:
    """Initialize MinIO client with fallback endpoint."""
    endpoints = [MINIO_HOST, os.getenv("MINIO_ENDPOINT", "minio:9000"), "localhost:9000"]
    for ep in endpoints:
        try:
            client = Minio(
                endpoint=ep,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE,
            )
            if not client.bucket_exists(BUCKET_GOLD):
                client.make_bucket(BUCKET_GOLD)
            return client
        except Exception:
            continue
    raise RuntimeError("Failed to connect to MinIO after trying available endpoints.")


def get_db_connection():
    """Initialize PostgreSQL DWH connection with host/container fallback."""
    connection_attempts = [
        {"host": POSTGRES_HOST, "port": POSTGRES_PORT},
        {"host": "localhost", "port": 5434},
        {"host": "postgres", "port": 5432},
        {"host": "localhost", "port": 5432},
    ]
    last_err = None
    for attempt in connection_attempts:
        try:
            conn = psycopg2.connect(
                host=attempt["host"],
                port=attempt["port"],
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=DWH_DB,
                connect_timeout=5,
            )
            return conn
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Failed to connect to PostgreSQL DWH ({DWH_DB}): {last_err}")


def seed_dim_date(conn, minio_client: Minio, start_year: int = 2016, end_year: int = 2026) -> int:
    """Generate and insert DimDate rows into DWH, then export to MinIO Gold."""
    start_dt = date(start_year, 1, 1)
    end_dt = date(end_year, 12, 31)

    day_names_vn = {
        1: "Thứ Hai",
        2: "Thứ Ba",
        3: "Thứ Tư",
        4: "Thứ Năm",
        5: "Thứ Sáu",
        6: "Thứ Bảy",
        7: "Chủ Nhật",
    }

    records: list[tuple[Any, ...]] = []
    curr = start_dt
    one_day = timedelta(days=1)

    while curr <= end_dt:
        date_key = int(curr.strftime("%Y%m%d"))
        day_of_week = curr.isoweekday()  # 1 (Monday) -> 7 (Sunday)
        day_name = day_names_vn[day_of_week]
        day_of_month = curr.day
        month = curr.month
        month_name = f"Tháng {month}"
        quarter = (month - 1) // 3 + 1
        year = curr.year
        is_weekend = day_of_week in (6, 7)
        # Rainy season in northern Vietnam: May through October
        is_rainy_season = 5 <= month <= 10

        records.append(
            (
                date_key,
                curr,
                day_of_week,
                day_name,
                day_of_month,
                month,
                month_name,
                quarter,
                year,
                is_weekend,
                is_rainy_season,
            )
        )
        curr += one_day

    # Insert into PostgreSQL
    insert_sql = """
        INSERT INTO dim_date (
            date_key, full_date, day_of_week, day_name, day_of_month,
            month, month_name, quarter, year, is_weekend, is_rainy_season
        ) VALUES %s
        ON CONFLICT (date_key) DO UPDATE SET
            day_of_week = EXCLUDED.day_of_week,
            day_name = EXCLUDED.day_name,
            day_of_month = EXCLUDED.day_of_month,
            month = EXCLUDED.month,
            month_name = EXCLUDED.month_name,
            quarter = EXCLUDED.quarter,
            year = EXCLUDED.year,
            is_weekend = EXCLUDED.is_weekend,
            is_rainy_season = EXCLUDED.is_rainy_season;
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, records, page_size=2000)
    conn.commit()

    # Export to MinIO Gold Parquet
    df_date = pd.DataFrame(
        records,
        columns=[
            "date_key",
            "full_date",
            "day_of_week",
            "day_name",
            "day_of_month",
            "month",
            "month_name",
            "quarter",
            "year",
            "is_weekend",
            "is_rainy_season",
        ],
    )
    df_date["full_date"] = pd.to_datetime(df_date["full_date"])
    buf = io.BytesIO()
    df_date.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    parquet_bytes = buf.getvalue()
    minio_client.put_object(
        bucket_name=BUCKET_GOLD,
        object_name="dim_date/dim_date.parquet",
        data=io.BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/octet-stream",
    )

    print(
        f"  [+] dim_date: Seeded {len(records):,} days ({start_year} - {end_year}) in DWH & Gold Parquet"
    )
    return len(records)


def seed_dim_station(conn, minio_client: Minio) -> int:
    """Seed DimStation from canonical station registry and export to MinIO Gold."""
    # Look up location keys from dim_location
    with conn.cursor() as cur:
        cur.execute("SELECT location_key, location_code FROM dim_location;")
        loc_map = {row[1]: row[0] for row in cur.fetchall()}

    records: list[tuple[Any, ...]] = []
    for sid, info in STATION_REGISTRY.items():
        loc_code = info.get("location_code")
        loc_key = loc_map.get(loc_code) if loc_code else None
        # Only seed canonical stations or stations with mapped location
        records.append(
            (
                sid,
                info.get("station_name"),
                sid,  # wmo_id
                info.get("latitude"),
                info.get("longitude"),
                info.get("elevation_m"),
                loc_key,
                info.get("role") if loc_key else None,
                info.get("distance_km") if loc_key else None,
            )
        )

    insert_sql = """
        INSERT INTO dim_station (
            station_id, station_name, wmo_id, latitude, longitude, elevation_m, location_key,
            station_role, distance_km
        ) VALUES %s
        ON CONFLICT (station_id) DO UPDATE SET
            station_name = EXCLUDED.station_name,
            wmo_id = EXCLUDED.wmo_id,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            elevation_m = EXCLUDED.elevation_m,
            location_key = EXCLUDED.location_key,
            station_role = EXCLUDED.station_role,
            distance_km = EXCLUDED.distance_km;
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, records)
    conn.commit()

    # Query back and export to Gold Parquet
    df_station = pd.read_sql("SELECT * FROM dim_station ORDER BY station_key;", conn)
    buf = io.BytesIO()
    df_station.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    parquet_bytes = buf.getvalue()
    minio_client.put_object(
        bucket_name=BUCKET_GOLD,
        object_name="dim_station/dim_station.parquet",
        data=io.BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/octet-stream",
    )

    print(f"  [+] dim_station: Seeded {len(records)} stations in DWH & Gold Parquet")
    return len(records)


def export_other_dimensions(conn, minio_client: Minio):
    """Export dim_location, dim_source, and dim_time from DWH to MinIO Gold Parquet."""
    dim_tables = ["dim_location", "dim_source", "dim_time"]
    for table in dim_tables:
        df = pd.read_sql(f"SELECT * FROM {table};", conn)
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
        parquet_bytes = buf.getvalue()
        gold_key = f"{table}/{table}.parquet"
        minio_client.put_object(
            bucket_name=BUCKET_GOLD,
            object_name=gold_key,
            data=io.BytesIO(parquet_bytes),
            length=len(parquet_bytes),
            content_type="application/octet-stream",
        )
        print(f"  [+] {table}: Exported {len(df)} records to gold/{gold_key}")


def main():
    print("=" * 80)
    print(" SEEDING & SYNCHRONIZING DIMENSIONS (DWH & GOLD MINIO)")
    print("=" * 80)
    conn = get_db_connection()
    minio_client = get_minio_client()

    try:
        seed_dim_date(conn, minio_client, start_year=2016, end_year=2026)
        seed_dim_station(conn, minio_client)
        export_other_dimensions(conn, minio_client)
        print("=" * 80)
        print(" ALL DIMENSIONS SYNCHRONIZED SUCCESSFULLY!")
        print("=" * 80)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
