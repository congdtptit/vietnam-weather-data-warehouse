#!/usr/bin/env python3
"""
scripts/ingest_open_meteo.py
Thu thập dữ liệu khí tượng lịch sử từ Open-Meteo Archive API và lưu vào tầng Bronze (MinIO).

Phân chia dữ liệu theo yêu cầu đề tài:
- 2016 - 2023 : Dữ liệu huấn luyện (Training)
- 2024        : Dữ liệu kiểm định (Validation)
- 2025        : Dữ liệu kiểm thử (Testing)

8 địa điểm nghiên cứu miền Bắc:
HAN (Hà Nội), PTO (Phú Thọ), TNG (Thái Nguyên), TUQ (Tuyên Quang),
LCA (Lào Cai), HBI (Hòa Bình), HPH (Hải Phòng), NBI (Ninh Bình).
"""

import os
import sys
import io
import time
import json
import argparse
from datetime import datetime, timezone
import requests
from minio import Minio
from minio.error import S3Error

# Cấu hình encoding chuẩn utf-8 trên Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Fallback nạp biến môi trường từ .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
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
BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# 8 Địa điểm đại diện miền Bắc trong phạm vi nghiên cứu của đề tài
NORTH_LOCATIONS = {
    "HAN": {"name": "Hà Nội", "latitude": 21.0285, "longitude": 105.8542},
    "PTO": {"name": "Phú Thọ", "latitude": 21.3986, "longitude": 105.2280},
    "TNG": {"name": "Thái Nguyên", "latitude": 21.5942, "longitude": 105.8482},
    "TUQ": {"name": "Tuyên Quang", "latitude": 21.8236, "longitude": 105.2153},
    "LCA": {"name": "Lào Cai", "latitude": 22.4856, "longitude": 103.9707},
    "HBI": {"name": "Hòa Bình", "latitude": 20.8172, "longitude": 105.3376},
    "HPH": {"name": "Hải Phòng", "latitude": 20.8449, "longitude": 106.6881},
    "NBI": {"name": "Ninh Bình", "latitude": 20.2506, "longitude": 105.9745},
}

# Phân chia tập dữ liệu
YEAR_SPLITS = {
    2016: "training",
    2017: "training",
    2018: "training",
    2019: "training",
    2020: "training",
    2021: "training",
    2022: "training",
    2023: "training",
    2024: "validation",
    2025: "testing",
}

# Danh sách các trường dữ liệu khí tượng thu thập
HOURLY_VARIABLES = [
    "temperature_2m",          # Nhiệt độ không khí tại 2m (°C)
    "relative_humidity_2m",    # Độ ẩm tương đối (%)
    "dew_point_2m",            # Điểm sương (°C)
    "apparent_temperature",    # Nhiệt độ cảm nhận (°C)
    "precipitation",           # Tổng lượng mưa (mm) -> Target RainNext24h
    "rain",                    # Lượng mưa dạng lỏng (mm)
    "weather_code",            # Mã thời tiết WMO
    "surface_pressure",        # Áp suất bề mặt khí quyển (hPa)
    "cloud_cover",             # Độ che phủ mây tổng (%)
    "wind_speed_10m",          # Tốc độ gió tại 10m (m/s)
    "wind_direction_10m",      # Hướng gió tại 10m (độ)
    "wind_gusts_10m",          # Tốc độ gió giật (m/s)
    "direct_normal_irradiance",# Bức xạ mặt trời trực tiếp (W/m²)
    "shortwave_radiation",     # Bức xạ sóng ngắn (W/m²)
]


def get_minio_client() -> Minio:
    """Khởi tạo client kết nối MinIO"""
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
        # Nếu chạy trong mạng nội bộ docker
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


def fetch_open_meteo_year(lat: float, lon: float, year: int, max_retries: int = 5) -> dict:
    """Gửi request tới Open-Meteo Archive API để lấy dữ liệu 1 năm của 1 tọa độ"""
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": HOURLY_VARIABLES,
        "wind_speed_unit": "ms",
        "precipitation_unit": "mm",
        "timezone": "UTC",
    }

    delay = 2
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                print(f"      [!] Bị giới hạn tần suất (Rate limit 429), chờ {delay}s trước khi thử lại...")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"      [!] Lỗi HTTP {resp.status_code}: {resp.text[:100]}, chờ {delay}s...")
                time.sleep(delay)
                delay *= 2
        except requests.RequestException as ex:
            print(f"      [!] Ngoại lệ kết nối: {ex}, chờ {delay}s...")
            time.sleep(delay)
            delay *= 2

    raise RuntimeError(f"Không thể tải dữ liệu từ Open-Meteo sau {max_retries} lần thử cho năm {year}")


def ingest_data(start_year: int = 2016, end_year: int = 2025, target_location: str = None, target_split: str = None, overwrite: bool = False):
    """Quy trình thu thập và tải lên MinIO Bronze"""
    client = get_minio_client()

    locations = NORTH_LOCATIONS
    if target_location:
        loc_code = target_location.upper()
        if loc_code not in NORTH_LOCATIONS:
            raise ValueError(f"Mã địa điểm không hợp lệ: {target_location}. Chọn trong: {list(NORTH_LOCATIONS.keys())}")
        locations = {loc_code: NORTH_LOCATIONS[loc_code]}

    years = [y for y in range(start_year, end_year + 1) if y in YEAR_SPLITS]
    if target_split:
        target_split = target_split.lower()
        years = [y for y in years if YEAR_SPLITS.get(y) == target_split]

    total_tasks = len(locations) * len(years)
    current_idx = 0
    success_count = 0
    skipped_count = 0

    print("=" * 80)
    print(" BẮT ĐẦU THU THẬP DỮ LIỆU KHÍ TƯỢNG OPEN-METEO -> MINIO BRONZE")
    print("=" * 80)
    print(f"[*] Phạm vi năm: {years}")
    print(f"[*] Số địa điểm: {len(locations)} ({', '.join(locations.keys())})")
    print(f"[*] Tổng số file dự kiến: {total_tasks}")
    print(f"[*] Các trường dữ liệu: {', '.join(HOURLY_VARIABLES)}")
    print("=" * 80)

    for loc_code, loc_info in locations.items():
        loc_name = loc_info["name"]
        lat = loc_info["latitude"]
        lon = loc_info["longitude"]

        print(f"\n>>> Đang xử lý địa điểm [{loc_code}] - {loc_name} (Tọa độ: {lat}, {lon})")

        for year in years:
            current_idx += 1
            split_type = YEAR_SPLITS[year]
            object_path = f"open_meteo/hourly/year={year}/open_meteo_{loc_code}_hourly_{year}0101_{year}1231.json"

            # Kiểm tra xem file đã tồn tại trên MinIO chưa
            if not overwrite:
                try:
                    stat = client.stat_object(BUCKET_BRONZE, object_path)
                    if stat.size > 2000:
                        print(f"  [{current_idx}/{total_tasks}] (Split: {split_type.upper():<10}) {object_path} -> ĐÃ CÓ ({stat.size} bytes), bỏ qua.")
                        skipped_count += 1
                        continue
                except S3Error:
                    pass

            print(f"  [{current_idx}/{total_tasks}] (Split: {split_type.upper():<10}) Đang tải {loc_code} năm {year} ...", end="", flush=True)

            try:
                # 1. Gọi API Open-Meteo
                data = fetch_open_meteo_year(lat=lat, lon=lon, year=year)

                # 2. Bổ sung metadata quản lý dữ liệu
                data["_pipeline_metadata"] = {
                    "source": "Open-Meteo Archive API",
                    "location_code": loc_code,
                    "location_name": loc_name,
                    "dataset_split": split_type,
                    "year": year,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "hourly_record_count": len(data.get("hourly", {}).get("time", [])),
                }

                # 3. Chuẩn bị buffer JSON và tải lên MinIO
                json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
                client.put_object(
                    bucket_name=BUCKET_BRONZE,
                    object_name=object_path,
                    data=io.BytesIO(json_bytes),
                    length=len(json_bytes),
                    content_type="application/json",
                )

                records = data["_pipeline_metadata"]["hourly_record_count"]
                print(f" XONG ({records} bản ghi theo giờ, {len(json_bytes)/1024:.1f} KB)")
                success_count += 1

                # Tạm dừng nhẹ 1.2s để tuân thủ fair-use policy của Open-Meteo API
                time.sleep(1.2)

            except Exception as err:
                print(f" THẤT BẠI: {err}")

    print("\n" + "=" * 80)
    print(" KẾT QUẢ THU THẬP DỮ LIỆU:")
    print(f"  + Hoàn thành thành công : {success_count} file")
    print(f"  + Đã có sẵn (bỏ qua)    : {skipped_count} file")
    print(f"  + Tổng số file Bronze   : {success_count + skipped_count} / {total_tasks}")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Thu thập dữ liệu thời tiết Open-Meteo cho Data Lake Bronze")
    parser.add_argument("--start-year", type=int, default=2016, help="Năm bắt đầu (mặc định: 2016)")
    parser.add_argument("--end-year", type=int, default=2025, help="Năm kết thúc (mặc định: 2025)")
    parser.add_argument("--location", type=str, default=None, help="Mã địa điểm cụ thể (ví dụ: HAN, HPH) hoặc để trống cho tất cả 8 địa điểm")
    parser.add_argument("--split", type=str, default=None, choices=["training", "validation", "testing"], help="Lọc theo tập dữ liệu")
    parser.add_argument("--overwrite", action="store_true", help="Ghi đè file nếu đã tồn tại")

    args = parser.parse_args()

    try:
        ingest_data(
            start_year=args.start_year,
            end_year=args.end_year,
            target_location=args.location,
            target_split=args.split,
            overwrite=args.overwrite
        )
    except KeyboardInterrupt:
        print("\n[!] Tiến trình bị dừng bởi người dùng.")
        sys.exit(0)
    except Exception as e:
        print(f"\n[!] Lỗi: {e}")
        sys.exit(1)
