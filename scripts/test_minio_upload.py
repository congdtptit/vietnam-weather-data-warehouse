#!/usr/bin/env python3
"""
scripts/test_minio_upload.py
Script kiểm thử tải dữ liệu mẫu lên các bucket MinIO theo đúng quy ước đặt tên (Naming Convention).
"""

import os
import sys
import io
import json

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
import pandas as pd
from minio import Minio
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Fallback nếu chưa cài python-dotenv
    env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if os.path.exists(env_file):
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip("'\""))

MINIO_HOST = os.getenv("MINIO_EXTERNAL_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


def get_minio_client() -> Minio:
    return Minio(
        endpoint=MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def test_upload_bronze(client: Minio):
    print("\n--- 1. Kiểm thử tải dữ liệu lên tầng BRONZE ---")
    
    # 1.1 Mẫu Open-Meteo JSON
    open_meteo_path = "open_meteo/hourly/year=2024/month=05/open_meteo_HAN_hourly_20240501_20240531.json"
    sample_json = json.dumps({
        "latitude": 21.0285,
        "longitude": 105.8542,
        "location": "Hà Nội",
        "hourly": {
            "time": ["2024-05-01T00:00", "2024-05-01T01:00"],
            "temperature_2m": [26.5, 26.0],
            "relative_humidity_2m": [85, 88],
            "precipitation": [0.0, 1.2],
            "surface_pressure": [1008.2, 1007.9]
        }
    }, indent=2).encode("utf-8")
    
    client.put_object("bronze", open_meteo_path, io.BytesIO(sample_json), len(sample_json), "application/json")
    print(f"[✓] Đã upload Open-Meteo sample: bronze/{open_meteo_path}")

    # 1.2 Mẫu Meteostat CSV
    meteostat_path = "meteostat/station_hourly/year=2024/meteostat_station_48820_hourly_2024.csv"
    sample_csv = (
        "time,temp,dwpt,rhum,prcp,wdir,wspd,pres\n"
        "2024-05-01 00:00:00,26.5,23.8,85,0.0,120,3.5,1008.2\n"
        "2024-05-01 01:00:00,26.0,23.9,88,1.2,130,4.0,1007.9\n"
    ).encode("utf-8")
    client.put_object("bronze", meteostat_path, io.BytesIO(sample_csv), len(sample_csv), "text/csv")
    print(f"[✓] Đã upload Meteostat sample: bronze/{meteostat_path}")

    # 1.3 Mẫu Spatial GeoJSON
    geojson_path = "spatial/boundaries/vietnam_northern_provinces.geojson"
    sample_geojson = json.dumps({
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"name": "Hà Nội", "code": "HAN"},
                "geometry": {"type": "Point", "coordinates": [105.8542, 21.0285]}
            }
        ]
    }).encode("utf-8")
    client.put_object("bronze", geojson_path, io.BytesIO(sample_geojson), len(sample_geojson), "application/geo+json")
    print(f"[✓] Đã upload GeoJSON sample: bronze/{geojson_path}")


def test_upload_silver(client: Minio):
    print("\n--- 2. Kiểm thử tải dữ liệu lên tầng SILVER ---")
    
    # Tạo dữ liệu Pandas chuẩn hóa và xuất ra Parquet
    df_silver = pd.DataFrame({
        "timestamp_utc": pd.to_datetime(["2024-05-01 00:00:00+00:00", "2024-05-01 01:00:00+00:00"]),
        "timestamp_local": pd.to_datetime(["2024-05-01 07:00:00", "2024-05-01 08:00:00"]),
        "location_code": ["HAN", "HAN"],
        "temperature_c": [26.5, 26.0],
        "humidity_percent": [85.0, 88.0],
        "precipitation_mm": [0.0, 1.2],
        "pressure_hpa": [1008.2, 1007.9],
        "source_name": ["Open-Meteo", "Open-Meteo"]
    })
    
    parquet_buffer = io.BytesIO()
    df_silver.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_bytes = parquet_buffer.getvalue()

    silver_path = "weather_hourly/year=2024/month=05/weather_hourly_HAN_202405.parquet"
    client.put_object("silver", silver_path, io.BytesIO(parquet_bytes), len(parquet_bytes), "application/octet-stream")
    print(f"[✓] Đã upload Silver Parquet: silver/{silver_path}")


def test_upload_gold(client: Minio):
    print("\n--- 3. Kiểm thử tải dữ liệu lên tầng GOLD ---")
    
    # Tạo FactWeatherObservation data
    df_gold = pd.DataFrame({
        "date_key": [20240501, 20240501],
        "time_key": [700, 800],
        "location_key": [1, 1],
        "source_key": [1, 1],
        "temperature_c": [26.5, 26.0],
        "humidity_percent": [85.0, 88.0],
        "precipitation_mm": [0.0, 1.2],
        "rain_next_24h_label": [1, 1]
    })
    
    parquet_buffer = io.BytesIO()
    df_gold.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_bytes = parquet_buffer.getvalue()

    gold_path = "fact_weather_observation/year=2024/month=05/fact_weather_observation_202405.parquet"
    client.put_object("gold", gold_path, io.BytesIO(parquet_bytes), len(parquet_bytes), "application/octet-stream")
    print(f"[✓] Đã upload Gold Fact Parquet: gold/{gold_path}")


if __name__ == "__main__":
    try:
        client = get_minio_client()
        test_upload_bronze(client)
        test_upload_silver(client)
        test_upload_gold(client)
        print("\n" + "=" * 60)
        print(" TẤT CẢ KIỂM THỬ UPLOAD LÊN DATA LAKE THÀNH CÔNG!")
        print("=" * 60)
    except Exception as err:
        print(f"[!] Lỗi kiểm thử upload MinIO: {err}")
        print("💡 Hãy đảm bảo MinIO đang hoạt động và bucket đã được tạo!")
        sys.exit(1)
