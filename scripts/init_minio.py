#!/usr/bin/env python3
"""
scripts/init_minio.py
Khởi tạo và cấu hình các bucket Bronze, Silver, Gold trên MinIO Data Lake.
Đề tài: Xây dựng kho dữ liệu khí tượng đa nguồn phục vụ phân tích xu hướng và dự báo khả năng mưa tại miền Bắc Việt Nam
"""

import os
import sys
import io

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
from minio import Minio
from minio.error import S3Error
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

# Cấu hình MinIO
MINIO_HOST = os.getenv("MINIO_EXTERNAL_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

BUCKETS = [
    os.getenv("MINIO_BUCKET_BRONZE", "bronze"),
    os.getenv("MINIO_BUCKET_SILVER", "silver"),
    os.getenv("MINIO_BUCKET_GOLD", "gold"),
]

# Định nghĩa cấu trúc thư mục tiêu chuẩn cho từng bucket theo kiến trúc đề tài
BUCKET_STRUCTURES = {
    "bronze": [
        "open_meteo/hourly/.keep",
        "era5/reanalysis/.keep",
        "meteostat/station_hourly/.keep",
        "noaa/station_hourly/.keep",
        "spatial/boundaries/.keep",
    ],
    "silver": [
        "weather_hourly/.keep",
        "stations/.keep",
        "quality_reports/.keep",
    ],
    "gold": [
        "fact_weather_observation/.keep",
        "dim_date/.keep",
        "dim_time/.keep",
        "dim_location/.keep",
        "dim_source/.keep",
        "dim_station/.keep",
        "ml_features/.keep",
        "predictions/.keep",
    ],
}


def get_minio_client() -> Minio:
    """Tạo client kết nối MinIO"""
    # Nếu chạy bên trong Docker container, thử endpoint minio:9000 nếu localhost không kết nối được
    endpoint = MINIO_HOST
    print(f"[*] Đang kết nối tới MinIO tại: {endpoint} ...")
    try:
        client = Minio(
            endpoint=endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        # Kiểm tra kết nối thử
        client.list_buckets()
        return client
    except Exception as e:
        alt_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        if alt_endpoint != endpoint:
            print(f"[!] Thử kết nối với internal endpoint: {alt_endpoint} ...")
            try:
                client = Minio(
                    endpoint=alt_endpoint,
                    access_key=MINIO_ACCESS_KEY,
                    secret_key=MINIO_SECRET_KEY,
                    secure=MINIO_SECURE,
                )
                client.list_buckets()
                return client
            except Exception:
                pass
        raise e


def init_buckets(client: Minio):
    """Kiểm tra và tạo các bucket nếu chưa tồn tại"""
    print("\n" + "=" * 60)
    print(" BẮT ĐẦU KHỞI TẠO CÁC BUCKET DATA LAKE TRÊN MINIO")
    print("=" * 60)

    existing_buckets = [b.name for b in client.list_buckets()]
    print(f"[*] Danh sách bucket hiện có: {existing_buckets}")

    for bucket_name in BUCKETS:
        if client.bucket_exists(bucket_name):
            print(f"[+] Bucket '{bucket_name}': ĐÃ TỒN TẠI (Bỏ qua tạo mới)")
        else:
            client.make_bucket(bucket_name)
            print(f"[✓] Bucket '{bucket_name}': TẠO THÀNH CÔNG!")

        # Tạo cấu trúc thư mục phân vùng mẫu bằng các file .keep
        sub_folders = BUCKET_STRUCTURES.get(bucket_name, [])
        for folder_path in sub_folders:
            try:
                # Kiểm tra xem object đã tồn tại chưa
                client.stat_object(bucket_name, folder_path)
            except S3Error:
                # Tạo object rỗng .keep để giữ cây thư mục
                dummy_data = io.BytesIO(b"# Data Lake Directory Marker\n")
                client.put_object(
                    bucket_name=bucket_name,
                    object_name=folder_path,
                    data=dummy_data,
                    length=len(dummy_data.getvalue()),
                    content_type="text/plain",
                )
                print(f"    -> Đã tạo thư mục phân vùng: {bucket_name}/{folder_path}")

    print("\n" + "=" * 60)
    print(" HOÀN THÀNH TẠO 3 TẦNG BUCKET: BRONZE - SILVER - GOLD")
    print("=" * 60)


if __name__ == "__main__":
    try:
        client = get_minio_client()
        init_buckets(client)
    except Exception as err:
        print(f"\n[!] Lỗi khi kết nối hoặc khởi tạo MinIO: {err}")
        print("💡 Hãy đảm bảo MinIO container đã chạy (lệnh: docker compose up -d minio)")
        sys.exit(1)
