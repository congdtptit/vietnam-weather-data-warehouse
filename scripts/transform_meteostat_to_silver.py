#!/usr/bin/env python3
"""
scripts/transform_meteostat_to_silver.py
Làm sạch và chuẩn hóa dữ liệu quan trắc trạm khí tượng Meteostat từ tầng Bronze (CSV)
sang tầng Silver (Parquet) theo Hợp đồng dữ liệu (docs/data-contract.md)
và Quy ước đặt tên (docs/NAMING_CONVENTIONS.md).

Cấu trúc lưu trữ Silver:
  silver/weather_hourly/year=YYYY/month=MM/weather_hourly_meteostat_<station_id>_<YYYYMM>.parquet
  silver/stations/stations_metadata_cleaned.parquet
Báo cáo chất lượng:
  silver/quality_reports/dq_report_meteostat_<timestamp>.json
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

# Bảng ánh xạ trạm khí tượng và vị trí đại diện (docs/NAMING_CONVENTIONS.md Table 6)
STATION_METADATA = {
    "48820": {"station_name": "Hà Nội (Láng)", "location_code": "HAN", "latitude": 21.0167, "longitude": 105.8000, "elevation_m": 6.0},
    "48825": {"station_name": "Hải Phòng (Cát Bi)", "location_code": "HPH", "latitude": 20.9667, "longitude": 105.7667, "elevation_m": 6.0},
    "48826": {"station_name": "Phù Liễn (Hải Phòng)", "location_code": "HPH", "latitude": 20.8000, "longitude": 106.6333, "elevation_m": 119.0},
    "48830": {"station_name": "Lạng Sơn", "location_code": "HBI", "latitude": 21.8333, "longitude": 106.7667, "elevation_m": 258.0},
    "48840": {"station_name": "Thanh Hóa", "location_code": "NBI", "latitude": 19.8000, "longitude": 105.7833, "elevation_m": 9.0},
    "48806": {"station_name": "Sơn La", "location_code": "NW", "latitude": 21.3333, "longitude": 103.9000, "elevation_m": 600.0},
    "48808": {"station_name": "Cao Bằng", "location_code": "NE", "latitude": 22.6667, "longitude": 106.2500, "elevation_m": 260.0},
}

SILVER_STANDARD_COLUMNS = [
    "source_name",
    "station_id",
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
    # Trường mở rộng từ Meteostat
    "wind_gusts_ms",
    "weather_condition_code",
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


def clean_and_standardize_meteostat(raw_csv_bytes: bytes, station_id: str) -> tuple[pd.DataFrame, dict]:
    """
    Quy trình làm sạch dữ liệu quan trắc Meteostat:
    1. Parse cột year, month, day, hour thành timestamp_utc chuẩn và timestamp_local (Asia/Ho_Chi_Minh).
    2. Đổi đơn vị:
       - Tốc độ gió: km/h -> m/s (wspd / 3.6).
       - Gió giật: km/h -> m/s (wpgt / 3.6).
       - Độ che phủ mây: okta (0-8) -> % (cldc / 8 * 100).
    3. Giữ nguyên giá trị khuyết (null khác 0; không điền mưa thiếu bằng 0 theo data-contract).
    4. Kiểm tra khử trùng lặp trên Business Key (source, station_id, timestamp_utc).
    5. Kiểm tra khoảng giá trị hợp lệ (range checks).
    """
    df = pd.read_csv(io.BytesIO(raw_csv_bytes))
    initial_rows = len(df)

    # 1. Chuẩn hóa thời gian (UTC)
    df["timestamp_utc"] = pd.to_datetime(df[["year", "month", "day", "hour"]], utc=True)
    df["timestamp_local"] = df["timestamp_utc"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)

    # 2. Định danh và ánh xạ trạm
    st_info = STATION_METADATA.get(station_id, {})
    loc_code = st_info.get("location_code", "UNKNOWN")

    df["source_name"] = "Meteostat"
    df["station_id"] = str(station_id)
    df["location_code"] = loc_code

    # 3. Chuẩn hóa và đổi đơn vị đo lường
    # Nhiệt độ (°C)
    df["temperature_c"] = pd.to_numeric(df.get("temp"), errors="coerce").astype("float64")
    # Độ ẩm tương đối (%)
    df["humidity_percent"] = pd.to_numeric(df.get("rhum"), errors="coerce").astype("float64")
    # Áp suất mặt biển (hPa)
    df["pressure_hpa"] = pd.to_numeric(df.get("pres"), errors="coerce").astype("float64")
    # Lượng mưa giờ (mm)
    df["precipitation_mm"] = pd.to_numeric(df.get("prcp"), errors="coerce").astype("float64")
    # Tốc độ gió: km/h -> m/s
    df["wind_speed_ms"] = (pd.to_numeric(df.get("wspd"), errors="coerce") / 3.6).round(2).astype("float64")
    # Hướng gió (độ)
    df["wind_direction_deg"] = pd.to_numeric(df.get("wdir"), errors="coerce").astype("float64")
    # Độ phủ mây: okta (0-8) -> phần trăm [0, 100]%
    cldc_numeric = pd.to_numeric(df.get("cldc"), errors="coerce")
    df["cloud_cover_percent"] = (cldc_numeric / 8.0 * 100.0).round(1).astype("float64")
    # Bức xạ mặt trời: trạm mặt đất Meteostat không đo trực tiếp trường này
    df["solar_radiation_wm2"] = pd.Series([None] * len(df), dtype="float64")
    # Tốc độ gió giật: km/h -> m/s
    wpgt_numeric = pd.to_numeric(df.get("wpgt"), errors="coerce")
    df["wind_gusts_ms"] = (wpgt_numeric / 3.6).round(2).astype("float64")
    # Mã điều kiện thời tiết (WMO/METAR coco 1-27)
    df["weather_condition_code"] = pd.to_numeric(df.get("coco"), errors="coerce").astype("float64")

    # 4. Khử trùng lặp trên Business Key
    dup_mask = df.duplicated(subset=["source_name", "station_id", "timestamp_utc"], keep="first")
    dup_count = int(dup_mask.sum())
    if dup_count > 0:
        df = df[~dup_mask].copy()

    # Sắp xếp tăng dần theo thời gian UTC
    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    # 5. Kiểm tra tính liên tục lưới giờ kỳ vọng
    start_ts = df["timestamp_utc"].min()
    end_ts = df["timestamp_utc"].max()
    expected_hours = len(pd.date_range(start=start_ts, end=end_ts, freq="h"))
    actual_hours = len(df)
    missing_hours_count = max(0, expected_hours - actual_hours)

    # 6. Kiểm tra và chuẩn hóa khoảng giá trị (Range Validation)
    out_of_range_humidity = int(((df["humidity_percent"] < 0) | (df["humidity_percent"] > 100)).sum())
    df["humidity_percent"] = df["humidity_percent"].clip(lower=0.0, upper=100.0)

    out_of_range_cloud = int(((df["cloud_cover_percent"] < 0) | (df["cloud_cover_percent"] > 100)).sum())
    df["cloud_cover_percent"] = df["cloud_cover_percent"].clip(lower=0.0, upper=100.0)

    # Mưa, gió không âm
    neg_precip = int((df["precipitation_mm"] < 0).sum())
    df["precipitation_mm"] = df["precipitation_mm"].clip(lower=0.0)

    neg_wind = int((df["wind_speed_ms"] < 0).sum())
    df["wind_speed_ms"] = df["wind_speed_ms"].clip(lower=0.0)

    invalid_wind_dir = int(((df["wind_direction_deg"] < 0) | (df["wind_direction_deg"] > 360)).sum())
    df["wind_direction_deg"] = df["wind_direction_deg"].clip(lower=0.0, upper=360.0)

    clean_df = df[SILVER_STANDARD_COLUMNS].copy()

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
            "wind_direction_invalid": invalid_wind_dir,
        },
        "null_counts": {k: int(v) for k, v in clean_df.isnull().sum().items()},
    }

    return clean_df, dq_stats


def sync_stations_metadata(client: Minio):
    """Lưu bảng metadata trạm đo đã chuẩn hóa vào silver/stations/stations_metadata_cleaned.parquet"""
    rows = []
    for sid, info in STATION_METADATA.items():
        rows.append({
            "station_id": sid,
            "station_name": info["station_name"],
            "location_code": info["location_code"],
            "latitude": info["latitude"],
            "longitude": info["longitude"],
            "elevation_m": info["elevation_m"],
            "source_name": "Meteostat",
            "updated_at": datetime.now(timezone.utc),
        })
    df_st = pd.DataFrame(rows)
    buf = io.BytesIO()
    df_st.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    pbytes = buf.getvalue()

    target_key = "stations/stations_metadata_cleaned.parquet"
    client.put_object(
        bucket_name=BUCKET_SILVER,
        object_name=target_key,
        data=io.BytesIO(pbytes),
        length=len(pbytes),
        content_type="application/octet-stream",
    )
    print(f"[✓] Đã tạo bảng metadata trạm chuẩn hóa: silver/{target_key} ({len(df_st)} trạm)")


def process_meteostat_to_silver(
    start_year: int = 2016,
    end_year: int = 2025,
    station_ids: list[str] = None,
    overwrite: bool = False,
):
    """Quy trình chuyển đổi toàn bộ file Meteostat từ Bronze sang Silver Parquet"""
    client = get_minio_client()

    selected_stations = [s.strip() for s in station_ids] if station_ids else list(STATION_METADATA.keys())
    years = list(range(start_year, end_year + 1))

    print("=" * 80)
    print(" BẮT ĐẦU CHUYỂN ĐỔI VÀ LÀM SẠCH: METEOSTAT BRONZE -> SILVER PARQUET")
    print("=" * 80)
    print(f"[*] Phạm vi năm: {start_year} - {end_year} ({len(years)} năm)")
    print(f"[*] Danh sách trạm: {', '.join(selected_stations)}")
    print(f"[*] Quy ước phân vùng: silver/weather_hourly/year=YYYY/month=MM/weather_hourly_meteostat_<id>_<YYYYMM>.parquet")
    print("=" * 80)

    # 1. Cập nhật bảng metadata trạm vào silver/stations/
    sync_stations_metadata(client)

    overall_dq = {
        "pipeline": "Meteostat Bronze to Silver",
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "years": years,
        "stations": selected_stations,
        "total_files_transformed": 0,
        "total_monthly_parquet_created": 0,
        "total_records_processed": 0,
        "total_duplicates_removed": 0,
        "total_missing_hours": 0,
        "details_by_station_year": {},
    }

    total_tasks = len(selected_stations) * len(years)
    current_task = 0

    for sid in selected_stations:
        st_name = STATION_METADATA.get(sid, {}).get("station_name", f"Station {sid}")
        print(f"\n>>> Đang xử lý trạm [{sid}] - {st_name}")

        for year in years:
            current_task += 1
            bronze_key = f"meteostat/station_hourly/year={year}/meteostat_station_{sid}_hourly_{year}.csv"

            # Đọc CSV từ Bronze
            try:
                obj = client.get_object(BUCKET_BRONZE, bronze_key)
                raw_bytes = obj.read()
            except S3Error:
                # Trạm không có dữ liệu cho năm này (404 ở bước bronze)
                continue
            except Exception as e:
                print(f"  [{current_task}/{total_tasks}] Năm {year}: Lỗi đọc Bronze {bronze_key}: {e}")
                continue

            # Làm sạch & Chuẩn hóa
            clean_df, dq_stats = clean_and_standardize_meteostat(raw_csv_bytes=raw_bytes, station_id=sid)

            task_key = f"{sid}_{year}"
            overall_dq["details_by_station_year"][task_key] = dq_stats
            overall_dq["total_files_transformed"] += 1
            overall_dq["total_records_processed"] += len(clean_df)
            overall_dq["total_duplicates_removed"] += dq_stats["duplicates_removed"]
            overall_dq["total_missing_hours"] += dq_stats["missing_hours"]

            # Phân vùng theo tháng và ghi ra Parquet
            clean_df["year_month"] = clean_df["timestamp_utc"].dt.strftime("%Y%m")
            months_grouped = clean_df.groupby("year_month")

            created_months = 0
            for ym_str, group in months_grouped:
                y_str = ym_str[:4]
                m_str = ym_str[4:]
                silver_key = f"weather_hourly/year={y_str}/month={m_str}/weather_hourly_meteostat_{sid}_{ym_str}.parquet"

                if not overwrite:
                    try:
                        stat = client.stat_object(BUCKET_SILVER, silver_key)
                        if stat.size > 500:
                            created_months += 1
                            continue
                    except S3Error:
                        pass

                parquet_buf = io.BytesIO()
                export_df = group.drop(columns=["year_month"]).reset_index(drop=True)
                export_df.to_parquet(parquet_buf, index=False, engine="pyarrow", compression="snappy")
                pbytes = parquet_buf.getvalue()

                client.put_object(
                    bucket_name=BUCKET_SILVER,
                    object_name=silver_key,
                    data=io.BytesIO(pbytes),
                    length=len(pbytes),
                    content_type="application/octet-stream",
                )
                created_months += 1
                overall_dq["total_monthly_parquet_created"] += 1

            missing_flag = f", thiếu {dq_stats['missing_hours']}h" if dq_stats["missing_hours"] > 0 else ", 100% đủ giờ"
            print(f"  [{current_task}/{total_tasks}] Năm {year}: Xong ({len(clean_df):,} bản ghi -> {created_months} file tháng Parquet{missing_flag})")

    # Ghi báo cáo chất lượng dữ liệu vào silver/quality_reports/
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    dq_report_key = f"quality_reports/dq_report_meteostat_{now_str}.json"
    dq_json_bytes = json.dumps(overall_dq, indent=2, ensure_ascii=False).encode("utf-8")
    client.put_object(
        bucket_name=BUCKET_SILVER,
        object_name=dq_report_key,
        data=io.BytesIO(dq_json_bytes),
        length=len(dq_json_bytes),
        content_type="application/json",
    )

    print("\n" + "=" * 80)
    print(" KẾT QUẢ CHUYỂN ĐỔI METEOSTAT TẦNG SILVER:")
    print(f"  + Số file Bronze CSV đã xử lý   : {overall_dq['total_files_transformed']}")
    print(f"  + Tổng số file Parquet tạo ra   : {overall_dq['total_monthly_parquet_created']}")
    print(f"  + Tổng số bản ghi quan trắc giờ : {overall_dq['total_records_processed']:,}")
    print(f"  + Số bản ghi trùng lặp đã khử   : {overall_dq['total_duplicates_removed']}")
    print(f"  + Báo cáo chất lượng dữ liệu    : silver/{dq_report_key}")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Làm sạch và chuyển đổi Meteostat Bronze sang Silver Parquet")
    parser.add_argument("--start-year", type=int, default=2016, help="Năm bắt đầu (mặc định: 2016)")
    parser.add_argument("--end-year", type=int, default=2025, help="Năm kết thúc (mặc định: 2025)")
    parser.add_argument("--stations", type=str, default=None, help="Danh sách mã trạm phân tách bằng dấu phẩy")
    parser.add_argument("--overwrite", action="store_true", help="Ghi đè file nếu đã tồn tại trên Silver")

    args = parser.parse_args()

    st_list = [s.strip() for s in args.stations.split(",")] if args.stations else None

    try:
        process_meteostat_to_silver(
            start_year=args.start_year,
            end_year=args.end_year,
            station_ids=st_list,
            overwrite=args.overwrite,
        )
    except KeyboardInterrupt:
        print("\n[!] Tiến trình bị dừng bởi người dùng.")
        sys.exit(0)
    except Exception as e:
        print(f"\n[!] Lỗi: {e}")
        sys.exit(1)
