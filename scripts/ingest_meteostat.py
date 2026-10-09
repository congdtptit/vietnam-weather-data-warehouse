#!/usr/bin/env python3
"""
scripts/ingest_meteostat.py
Thu thập dữ liệu quan trắc trạm khí tượng lịch sử từ Meteostat Bulk Data Archive
và lưu trữ nguyên bản (Raw CSV) vào tầng Bronze trên MinIO Data Lake.

Cấu trúc đường dẫn Bronze (theo docs/NAMING_CONVENTIONS.md):
  meteostat/station_hourly/year=YYYY/meteostat_station_<station_id>_hourly_<YYYY>.csv
"""

import os
import sys
import io
import gzip
import time
import argparse
from datetime import datetime, timezone
import requests
from minio import Minio
from minio.error import S3Error

# Cấu hình encoding chuẩn UTF-8 trên Windows console
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

# Endpoint dữ liệu thô hàng giờ của Meteostat
METEOSTAT_BULK_HOURLY_URL = "https://data.meteostat.net/hourly/{year}/{station}.csv.gz"

# Danh mục các trạm khí tượng tham chiếu miền Bắc (docs/NAMING_CONVENTIONS.md Table 6)
METEOSTAT_STATIONS = {
    "48820": {"name": "Hà Nội (Láng)", "location_ref": "HAN", "latitude": 21.0167, "longitude": 105.8000},
    "48825": {"name": "Hải Phòng (Cát Bi)", "location_ref": "HPH/PTO", "latitude": 20.9667, "longitude": 105.7667},
    "48826": {"name": "Phù Liễn (Hải Phòng)", "location_ref": "TNG/HPH", "latitude": 20.8000, "longitude": 106.6333},
    "48830": {"name": "Lạng Sơn", "location_ref": "HBI/NE", "latitude": 21.8333, "longitude": 106.7667},
    "48840": {"name": "Thanh Hóa", "location_ref": "NBI", "latitude": 19.8000, "longitude": 105.7833},
    "48806": {"name": "Sơn La", "location_ref": "NW", "latitude": 21.3333, "longitude": 103.9000},
    "48808": {"name": "Cao Bằng", "location_ref": "NE", "latitude": 22.6667, "longitude": 106.2500},
    "48803": {"name": "Lào Cai", "location_ref": "LCA", "latitude": 22.5000, "longitude": 103.9667},
    "48816": {"name": "Tuyên Quang", "location_ref": "TUQ", "latitude": 21.8236, "longitude": 105.2153},
    "48838": {"name": "Móng Cái", "location_ref": "HPH/NE", "latitude": 21.5167, "longitude": 107.9667},
}


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


def fetch_meteostat_station_year(station_id: str, year: int, max_retries: int = 3) -> bytes | None:
    """
    Tải file nén .csv.gz từ kho lưu trữ Meteostat bulk và giải nén thành CSV thô.
    Trả về None nếu kho dữ liệu không có bản ghi (HTTP 404).
    """
    url = METEOSTAT_BULK_HOURLY_URL.format(year=year, station=station_id)
    delay = 2

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=20)
            if resp.status_code == 200:
                # Giải nén gzip thành bytes thô của file CSV nguyên bản
                csv_bytes = gzip.decompress(resp.content)
                return csv_bytes
            elif resp.status_code == 404:
                # Trạm không có dữ liệu đo đạc theo giờ trong năm yêu cầu
                return None
            elif resp.status_code == 429:
                print(f"      [!] Bị giới hạn tần suất (Rate limit 429), chờ {delay}s...")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"      [!] Lỗi HTTP {resp.status_code}, chờ {delay}s...")
                time.sleep(delay)
                delay *= 2
        except requests.RequestException as ex:
            print(f"      [!] Ngoại lệ kết nối ({ex}), thử lại lần {attempt}/{max_retries} sau {delay}s...")
            time.sleep(delay)
            delay *= 2

    raise RuntimeError(f"Không thể tải dữ liệu Meteostat sau {max_retries} lần thử cho trạm {station_id} năm {year}")


def ingest_meteostat_data(
    start_year: int = 2016,
    end_year: int = 2025,
    station_ids: list[str] = None,
    overwrite: bool = False,
):
    """Quy trình thu thập dữ liệu Meteostat và tải lên tầng Bronze của MinIO"""
    client = get_minio_client()

    selected_stations = {}
    if station_ids:
        for sid in station_ids:
            sid_str = sid.strip()
            if sid_str in METEOSTAT_STATIONS:
                selected_stations[sid_str] = METEOSTAT_STATIONS[sid_str]
            else:
                # Hỗ trợ trạm WMO/ICAO ngoài danh mục mặc định
                selected_stations[sid_str] = {"name": f"Station {sid_str}", "location_ref": "CUSTOM"}
    else:
        selected_stations = METEOSTAT_STATIONS

    years = list(range(start_year, end_year + 1))
    total_tasks = len(selected_stations) * len(years)

    print("=" * 80)
    print(" BẮT ĐẦU THU THẬP DỮ LIỆU TRẠM METEOSTAT -> MINIO BRONZE")
    print("=" * 80)
    print(f"[*] Phạm vi năm: {start_year} - {end_year} ({len(years)} năm)")
    print(f"[*] Số trạm khí tượng: {len(selected_stations)}")
    for sid, sinfo in selected_stations.items():
        print(f"    - Trạm {sid}: {sinfo['name']} (Tham chiếu: {sinfo.get('location_ref', 'N/A')})")
    print(f"[*] Tổng số tác vụ kiểm tra/tải: {total_tasks}")
    print("=" * 80)

    current_idx = 0
    success_count = 0
    skipped_count = 0
    not_found_count = 0

    for sid, sinfo in selected_stations.items():
        st_name = sinfo["name"]
        print(f"\n>>> Đang xử lý trạm [{sid}] - {st_name}")

        for year in years:
            current_idx += 1
            # Đường dẫn chuẩn hóa theo docs/NAMING_CONVENTIONS.md
            object_path = f"meteostat/station_hourly/year={year}/meteostat_station_{sid}_hourly_{year}.csv"

            # Kiểm tra xem file đã tồn tại trên MinIO chưa
            if not overwrite:
                try:
                    stat = client.stat_object(BUCKET_BRONZE, object_path)
                    if stat.size > 200:
                        print(f"  [{current_idx}/{total_tasks}] Năm {year}: {object_path} -> ĐÃ CÓ ({stat.size} bytes), bỏ qua.")
                        skipped_count += 1
                        continue
                except S3Error:
                    pass

            print(f"  [{current_idx}/{total_tasks}] Năm {year}: Đang kiểm tra/tải trạm {sid} ...", end="", flush=True)

            try:
                csv_bytes = fetch_meteostat_station_year(station_id=sid, year=year)

                if csv_bytes is None:
                    print(" KHÔNG CÓ DỮ LIỆU (Trạm không có báo cáo giờ năm này - 404)")
                    not_found_count += 1
                    continue

                # Lưu nguyên bản raw CSV và đính kèm metadata provenance vào object S3 (chỉ dùng US-ASCII cho HTTP headers)
                metadata = {
                    "source": "Meteostat Bulk Archive",
                    "station_id": sid,
                    "year": str(year),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }

                client.put_object(
                    bucket_name=BUCKET_BRONZE,
                    object_name=object_path,
                    data=io.BytesIO(csv_bytes),
                    length=len(csv_bytes),
                    content_type="text/csv",
                    metadata=metadata,
                )

                line_count = csv_bytes.count(b"\n")
                print(f" XONG ({line_count:,} dòng CSV, {len(csv_bytes)/1024:.1f} KB)")
                success_count += 1

                # Nghỉ ngắn giữa các request để tuân thủ fair-use policy
                time.sleep(0.5)

            except Exception as err:
                print(f" THẤT BẠI: {err}")

    print("\n" + "=" * 80)
    print(" KẾT QUẢ THU THẬP DỮ LIỆU METEOSTAT:")
    print(f"  + Tải thành công mới    : {success_count} file")
    print(f"  + Đã có sẵn (bỏ qua)    : {skipped_count} file")
    print(f"  + Không có dữ liệu (404): {not_found_count} tác vụ")
    print(f"  + Tổng số file Bronze   : {success_count + skipped_count}")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Thu thập dữ liệu trạm khí tượng Meteostat vào MinIO Bronze")
    parser.add_argument("--start-year", type=int, default=2016, help="Năm bắt đầu (mặc định: 2016)")
    parser.add_argument("--end-year", type=int, default=2025, help="Năm kết thúc (mặc định: 2025)")
    parser.add_argument(
        "--stations",
        type=str,
        default=None,
        help="Danh sách mã trạm phân tách bằng dấu phẩy (ví dụ: '48820,48825,48826'), hoặc để trống cho tất cả các trạm tham chiếu",
    )
    parser.add_argument("--overwrite", action="store_true", help="Ghi đè file nếu đã tồn tại trên MinIO")

    args = parser.parse_args()

    station_list = [s.strip() for s in args.stations.split(",")] if args.stations else None

    try:
        ingest_meteostat_data(
            start_year=args.start_year,
            end_year=args.end_year,
            station_ids=station_list,
            overwrite=args.overwrite,
        )
    except KeyboardInterrupt:
        print("\n[!] Tiến trình bị dừng bởi người dùng.")
        sys.exit(0)
    except Exception as e:
        print(f"\n[!] Lỗi: {e}")
        sys.exit(1)
