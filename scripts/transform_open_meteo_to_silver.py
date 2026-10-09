#!/usr/bin/env python3
"""
scripts/transform_open_meteo_to_silver.py
Làm sạch và chuẩn hóa dữ liệu khí tượng Open-Meteo từ tầng Bronze (JSON)
sang tầng Silver (Parquet) theo Hợp đồng dữ liệu (docs/data-contract.md)
và Quy ước đặt tên (docs/NAMING_CONVENTIONS.md).

Cấu trúc lưu trữ Silver:
  silver/weather_hourly/year=YYYY/month=MM/weather_hourly_<location>_<YYYYMM>.parquet
Báo cáo chất lượng:
  silver/quality_reports/dq_report_open_meteo_<timestamp>.json
"""

import os
import sys
import io
import json
import argparse
from datetime import datetime, timezone
import pandas as pd
from minio import Minio
from minio.error import S3Error

# Cấu hình UTF-8 cho Windows console
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Nạp biến môi trường từ .env
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
BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")

# 8 địa điểm nghiên cứu miền Bắc
NORTH_LOCATIONS = ["HAN", "PTO", "TNG", "TUQ", "LCA", "HBI", "HPH", "NBI"]

# Cột chuẩn hóa Silver theo Hợp đồng dữ liệu (docs/data-contract.md)
SILVER_STANDARD_COLUMNS = [
    "source_name",
    "location_code",
    "timestamp_utc",
    "timestamp_local",
    "temperature_c",
    "humidity_percent",
    "pressure_hpa",
    "precipitation_mm",
    "wind_speed_ms",
    "wind_direction_deg",
    "cloud_cover_percent",
    "solar_radiation_wm2",
    # Các trường mở rộng hữu ích từ Open-Meteo
    "dew_point_c",
    "apparent_temperature_c",
    "weather_code",
    "wind_gusts_ms",
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
        if not client.bucket_exists(BUCKET_SILVER):
            client.make_bucket(BUCKET_SILVER)
        return client
    except Exception:
        alt_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        client = Minio(
            endpoint=alt_endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        if not client.bucket_exists(BUCKET_SILVER):
            client.make_bucket(BUCKET_SILVER)
        return client


def clean_and_standardize_open_meteo(raw_json: dict, location_code: str) -> tuple[pd.DataFrame, dict]:
    """
    Quy trình làm sạch dữ liệu:
    1. Parse timestamps UTC chuẩn và suy ra timestamp_local (Asia/Ho_Chi_Minh).
    2. Đổi tên trường theo chuẩn snake_case và quy ước đơn vị (docs/NAMING_CONVENTIONS.md).
    3. Kiểm tra tính duy nhất (source, location, timestamp_utc) và khử trùng lặp.
    4. Kiểm tra lưới giờ kỳ vọng (expected chronological hourly grid) để phát hiện giờ thiếu.
    5. Kiểm tra khoảng giá trị hợp lệ (range constraints) và làm sạch.
    """
    hourly = raw_json.get("hourly", {})
    if not hourly or "time" not in hourly:
        raise ValueError("File JSON không chứa dữ liệu hourly hợp lệ")

    df = pd.DataFrame(hourly)
    initial_rows = len(df)

    # 1. Chuẩn hóa thời gian
    # Open-Meteo time format: 'YYYY-MM-DDTHH:MM' (được ingest với tham số timezone='UTC')
    df["timestamp_utc"] = pd.to_datetime(df["time"], utc=True)
    df["timestamp_local"] = df["timestamp_utc"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)

    # 2. Bổ sung các định danh grain chuẩn
    df["source_name"] = "Open-Meteo"
    df["location_code"] = location_code

    # 3. Ánh xạ tên cột theo Hợp đồng dữ liệu
    column_mapping = {
        "temperature_2m": "temperature_c",
        "relative_humidity_2m": "humidity_percent",
        "dew_point_2m": "dew_point_c",
        "apparent_temperature": "apparent_temperature_c",
        "surface_pressure": "pressure_hpa",
        "precipitation": "precipitation_mm",
        "wind_speed_10m": "wind_speed_ms",
        "wind_direction_10m": "wind_direction_deg",
        "cloud_cover": "cloud_cover_percent",
        "shortwave_radiation": "solar_radiation_wm2",
        "wind_gusts_10m": "wind_gusts_ms",
    }
    df = df.rename(columns=column_mapping)

    # 4. Kiểm tra trùng lặp trên Business Key: (source_name, location_code, timestamp_utc)
    dup_mask = df.duplicated(subset=["source_name", "location_code", "timestamp_utc"], keep="first")
    dup_count = int(dup_mask.sum())
    if dup_count > 0:
        df = df[~dup_mask].copy()

    # Sắp xếp tăng dần theo thời gian UTC
    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    # 5. Kiểm tra lưới giờ kỳ vọng (Expected Hourly Grid)
    start_ts = df["timestamp_utc"].min()
    end_ts = df["timestamp_utc"].max()
    expected_hours = len(pd.date_range(start=start_ts, end=end_ts, freq="h"))
    actual_hours = len(df)
    missing_hours_count = max(0, expected_hours - actual_hours)

    # 6. Kiểm tra và chuẩn hóa khoảng giá trị (Range & Quality Cleaning)
    # Độ ẩm và Độ mây phải nằm trong [0, 100]%
    out_of_range_humidity = int(((df["humidity_percent"] < 0) | (df["humidity_percent"] > 100)).sum())
    df["humidity_percent"] = df["humidity_percent"].clip(lower=0.0, upper=100.0)

    out_of_range_cloud = int(((df["cloud_cover_percent"] < 0) | (df["cloud_cover_percent"] > 100)).sum())
    df["cloud_cover_percent"] = df["cloud_cover_percent"].clip(lower=0.0, upper=100.0)

    # Mưa, gió, bức xạ không được âm
    neg_precip = int((df["precipitation_mm"] < 0).sum())
    df["precipitation_mm"] = df["precipitation_mm"].clip(lower=0.0)

    neg_wind = int((df["wind_speed_ms"] < 0).sum())
    df["wind_speed_ms"] = df["wind_speed_ms"].clip(lower=0.0)

    neg_solar = int((df["solar_radiation_wm2"] < 0).sum())
    df["solar_radiation_wm2"] = df["solar_radiation_wm2"].clip(lower=0.0)

    # Hướng gió trong [0, 360]
    invalid_wind_dir = int(((df["wind_direction_deg"] < 0) | (df["wind_direction_deg"] > 360)).sum())
    df["wind_direction_deg"] = df["wind_direction_deg"].clip(lower=0.0, upper=360.0)

    # Lọc giữ đúng các cột chuẩn
    available_cols = [c for c in SILVER_STANDARD_COLUMNS if c in df.columns]
    clean_df = df[available_cols].copy()

    # Thu thập chỉ số chất lượng (DQ metrics)
    dq_stats = {
        "initial_rows": initial_rows,
        "clean_rows": len(clean_df),
        "duplicates_removed": dup_count,
        "expected_hours": expected_hours,
        "missing_hours": missing_hours_count,
        "out_of_range_corrected": {
            "humidity": out_of_range_humidity,
            "cloud_cover": out_of_range_cloud,
            "precipitation_negative": neg_precip,
            "wind_speed_negative": neg_wind,
            "solar_radiation_negative": neg_solar,
            "wind_direction_invalid": invalid_wind_dir,
        },
        "null_counts": clean_df.isnull().sum().to_dict(),
    }

    return clean_df, dq_stats


def process_open_meteo_to_silver(
    start_year: int = 2016,
    end_year: int = 2025,
    target_location: str = None,
    overwrite: bool = False,
):
    """Xử lý chuyển đổi toàn bộ hoặc một phần Open-Meteo Bronze -> Silver"""
    client = get_minio_client()

    locations = [target_location.upper()] if target_location else NORTH_LOCATIONS
    years = list(range(start_year, end_year + 1))

    total_tasks = len(locations) * len(years)
    print("=" * 80)
    print(" BẮT ĐẦU CHUYỂN ĐỔI VÀ LÀM SẠCH: OPEN-METEO BRONZE -> SILVER PARQUET")
    print("=" * 80)
    print(f"[*] Phạm vi năm: {start_year} - {end_year} ({len(years)} năm)")
    print(f"[*] Địa điểm: {', '.join(locations)}")
    print(f"[*] Quy ước phân vùng: silver/weather_hourly/year=YYYY/month=MM/weather_hourly_<loc>_<YYYYMM>.parquet")
    print("=" * 80)

    overall_dq = {
        "pipeline": "Open-Meteo Bronze to Silver",
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "years": years,
        "locations": locations,
        "total_files_transformed": 0,
        "total_monthly_parquet_created": 0,
        "total_records_processed": 0,
        "total_duplicates_removed": 0,
        "total_missing_hours": 0,
        "details_by_location_year": {},
    }

    current_task = 0

    for loc in locations:
        print(f"\n>>> Đang xử lý địa điểm: [{loc}]")

        for year in years:
            current_task += 1
            bronze_key = f"open_meteo/hourly/year={year}/open_meteo_{loc}_hourly_{year}0101_{year}1231.json"

            # 1. Đọc dữ liệu từ Bronze
            try:
                obj = client.get_object(BUCKET_BRONZE, bronze_key)
                raw_bytes = obj.read()
                raw_json = json.loads(raw_bytes.decode("utf-8"))
            except S3Error as e:
                print(f"  [{current_task}/{total_tasks}] Năm {year}: Không tìm thấy Bronze {bronze_key} ({e})")
                continue
            except Exception as e:
                print(f"  [{current_task}/{total_tasks}] Năm {year}: Lỗi đọc Bronze: {e}")
                continue

            # 2. Làm sạch & Chuẩn hóa
            clean_df, dq_stats = clean_and_standardize_open_meteo(raw_json=raw_json, location_code=loc)

            task_key = f"{loc}_{year}"
            overall_dq["details_by_location_year"][task_key] = dq_stats
            overall_dq["total_files_transformed"] += 1
            overall_dq["total_records_processed"] += len(clean_df)
            overall_dq["total_duplicates_removed"] += dq_stats["duplicates_removed"]
            overall_dq["total_missing_hours"] += dq_stats["missing_hours"]

            # 3. Phân vùng theo tháng và ghi ra Parquet (theo docs/NAMING_CONVENTIONS.md)
            clean_df["year_month"] = clean_df["timestamp_utc"].dt.strftime("%Y%m")
            months_grouped = clean_df.groupby("year_month")

            created_months = 0
            for ym_str, group in months_grouped:
                y_str = ym_str[:4]
                m_str = ym_str[4:]
                silver_key = f"weather_hourly/year={y_str}/month={m_str}/weather_hourly_{loc}_{ym_str}.parquet"

                # Kiểm tra ghi đè
                if not overwrite:
                    try:
                        stat = client.stat_object(BUCKET_SILVER, silver_key)
                        if stat.size > 500:
                            created_months += 1
                            continue
                    except S3Error:
                        pass

                # Xuất ra Parquet bộ nhớ đệm
                parquet_buf = io.BytesIO()
                # Loại bỏ cột phụ year_month trước khi lưu
                export_df = group.drop(columns=["year_month"]).reset_index(drop=True)
                export_df.to_parquet(parquet_buf, index=False, engine="pyarrow", compression="snappy")
                parquet_bytes = parquet_buf.getvalue()

                # Upload lên MinIO Silver
                client.put_object(
                    bucket_name=BUCKET_SILVER,
                    object_name=silver_key,
                    data=io.BytesIO(parquet_bytes),
                    length=len(parquet_bytes),
                    content_type="application/octet-stream",
                )
                created_months += 1
                overall_dq["total_monthly_parquet_created"] += 1

            missing_flag = f", thiếu {dq_stats['missing_hours']}h" if dq_stats["missing_hours"] > 0 else ", 100% đủ giờ"
            print(f"  [{current_task}/{total_tasks}] Năm {year}: Xong ({len(clean_df):,} bản ghi -> {created_months} file tháng Parquet{missing_flag})")

    # 4. Ghi báo cáo chất lượng dữ liệu vào Silver: silver/quality_reports/
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    dq_report_key = f"quality_reports/dq_report_open_meteo_{now_str}.json"
    dq_json_bytes = json.dumps(overall_dq, indent=2, ensure_ascii=False).encode("utf-8")
    client.put_object(
        bucket_name=BUCKET_SILVER,
        object_name=dq_report_key,
        data=io.BytesIO(dq_json_bytes),
        length=len(dq_json_bytes),
        content_type="application/json",
    )

    print("\n" + "=" * 80)
    print(" KẾT QUẢ CHUYỂN ĐỔI OPEN-METEO TẦNG SILVER:")
    print(f"  + Số file Bronze đã xử lý       : {overall_dq['total_files_transformed']}")
    print(f"  + Tổng số file Parquet tạo ra   : {overall_dq['total_monthly_parquet_created']}")
    print(f"  + Tổng số bản ghi quan trắc giờ : {overall_dq['total_records_processed']:,}")
    print(f"  + Số bản ghi trùng lặp đã khử   : {overall_dq['total_duplicates_removed']}")
    print(f"  + Báo cáo chất lượng dữ liệu    : silver/{dq_report_key}")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Làm sạch và chuyển đổi Open-Meteo Bronze sang Silver Parquet")
    parser.add_argument("--start-year", type=int, default=2016, help="Năm bắt đầu (mặc định: 2016)")
    parser.add_argument("--end-year", type=int, default=2025, help="Năm kết thúc (mặc định: 2025)")
    parser.add_argument("--location", type=str, default=None, help="Mã địa điểm cụ thể (ví dụ: HAN) hoặc để trống cho cả 8 địa điểm")
    parser.add_argument("--overwrite", action="store_true", help="Ghi đè file nếu đã tồn tại trên Silver")

    args = parser.parse_args()

    try:
        process_open_meteo_to_silver(
            start_year=args.start_year,
            end_year=args.end_year,
            target_location=args.location,
            overwrite=args.overwrite,
        )
    except KeyboardInterrupt:
        print("\n[!] Tiến trình bị dừng bởi người dùng.")
        sys.exit(0)
    except Exception as e:
        print(f"\n[!] Lỗi: {e}")
        sys.exit(1)
