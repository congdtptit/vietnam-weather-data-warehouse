# HỆ THỐNG KHO DỮ LIỆU KHÍ TƯỢNG ĐA NGUỒN MIỀN BẮC VIỆT NAM

> **Học phần**: Kho dữ liệu và Kỹ thuật khai phá dữ liệu  
> **Đề tài**: *Xây dựng kho dữ liệu khí tượng đa nguồn phục vụ phân tích xu hướng và dự báo khả năng mưa tại miền Bắc Việt Nam*  
> **Công nghệ chủ đạo**: MinIO (Data Lake) | Apache Airflow (Orchestration) | PostgreSQL (Data Warehouse Star Schema) | Scikit-learn (Cây quyết định CART) | Power BI (OLAP/Dashboard)

---

## TÀI LIỆU VÀ TRẠNG THÁI TRIỂN KHAI

Repository hiện có cấu hình hạ tầng, script/DAG Open-Meteo → Bronze, DDL khởi tạo và script upload dữ liệu giả lập. Kiến trúc bên dưới là **mục tiêu**; chưa có bằng chứng trong lượt kiểm kê cho pipeline Silver/Gold, loader DW, OLAP, CART hoặc dashboard hoàn chỉnh. Xem [tiến độ và hạn chế](docs/progress.md).

| Tài liệu | Nội dung |
| --- | --- |
| [AGENTS.md](AGENTS.md) | Quy tắc cho AI và đọc tài liệu theo nhiệm vụ |
| [Kiến trúc](docs/architecture.md) | Luồng mục tiêu và runtime đang cấu hình |
| [Data contract](docs/data-contract.md) | UTC, cột, đơn vị, thiếu/trùng và quyết định còn mở |
| [Warehouse schema](docs/warehouse-schema.md) | DDL hiện có, grain, khóa, calendar và OLAP |
| [Rain label](docs/rain-label.md) | Nhãn 24h, chống leakage và validation/test |
| [Naming conventions](docs/NAMING_CONVENTIONS.md) | Bucket, object key và phân vùng |

Các skill trong `.agents/skills/` hướng dẫn quy trình chuyên môn; định nghĩa riêng của dự án nằm trong tài liệu domain ở trên. Hướng dẫn AI/skill viết bằng tiếng Anh; tài liệu nhóm giữ tiếng Việt.

## 1. TỔNG QUAN KIẾN TRÚC HỆ THỐNG

Dự án hướng tới pipeline hoàn chỉnh từ thu thập, lưu trữ, xử lý, nạp kho dữ liệu đến khai phá dữ liệu dự báo mưa:

```
[Nguồn Dữ Liệu]
 ├── Open-Meteo REST API (JSON)
 ├── ERA5 / ERA5-Land (NetCDF)
 ├── Meteostat / NOAA (CSV/Text)
 └── Dữ liệu địa giới Việt Nam (GeoJSON)
       │
       ▼
[DATA LAKE - MinIO]
 ├── Bronze  (Dữ liệu thô nguyên bản)
 ├── Silver  (Dữ liệu làm sạch, chuẩn UTC, có giờ Việt Nam suy ra, Parquet)
 └── Gold    (Dữ liệu tổng hợp, Fact/Dimensions, Feature sets cho ML)
       │
       ▼ [Apache Airflow ETL Pipeline]
[DATA WAREHOUSE - PostgreSQL]
 ├── FactWeatherObservation (Grain: 1 nguồn × 1 vị trí × 1 giờ)
 ├── FactWeatherPrediction (Kết quả dự báo mô hình ML)
 └── Dimensions (DimDate, DimTime, DimLocation, DimSource, DimStation)
       │
       ├──► [KHAI PHÁ DỮ LIỆU] Thuật toán CART Decision Tree (Dự báo RainNext24h: Yes/No)
       └──► [OLAP & BI] Power BI Dashboard (Roll-up, Drill-down, Slice, Dice, Bản đồ)
```

---

## 2. HƯỚNG DẪN KHỞI TẠO GIT REPOSITORY (DÀNH CHO NGƯỜI DÙNG)

Do bạn đã lưu ý sẽ **tự tạo repo Git**, cấu trúc mã nguồn đã được chuẩn bị đầy đủ sẵn sàng với file `.gitignore` và `.env` chuẩn. Bạn chỉ cần thực hiện các lệnh sau trên terminal tại thư mục dự án:

```powershell
# 1. Khởi tạo Git repo
git init

# 2. Thêm tất cả các file vào staging
git add .

# 3. Tạo commit đầu tiên
git commit -m "feat: setup Docker Compose stack (MinIO, Airflow, PostgreSQL), Star Schema DWH and MinIO Medallion buckets"

# 4. (Tùy chọn) Liên kết với remote repository của bạn trên GitHub / GitLab
# git remote add origin https://github.com/<your-username>/<your-repo-name>.git
# git branch -M main
# git push -u origin main
```

---

## 3. CÁC THÀNH PHẦN STACK (DOCKER COMPOSE)

| Dịch vụ | Container Name | Cổng ngoài (Host) | Cổng trong (Container) | Tài khoản / Thông tin mặc định |
|---|---|:---:|:---:|---|
| **MinIO Console** | `weather-minio` | `9001` | `9001` | User: `minioadmin` / Pass: `minioadmin` |
| **MinIO S3 API** | `weather-minio` | `9000` | `9000` | Endpoint: `http://localhost:9000` |
| **MinIO Init** | `weather-minio-init` | - | - | Tự động tạo 3 bucket: `bronze`, `silver`, `gold` |
| **Airflow Webserver** | `weather-airflow-webserver` | `8080` | `8080` | User: `admin` / Pass: `admin` |
| **Airflow Scheduler** | `weather-airflow-scheduler` | - | - | Chạy ngầm điều phối các DAGs |
| **Airflow Init** | `weather-airflow-init` | - | - | Khởi tạo schema và tài khoản admin |
| **PostgreSQL DWH** | `weather-postgres` | `5434`* | `5432` | User: `postgres` / Pass: `postgres`<br>DB: `weather_dwh` & `airflow` |

*\*Lưu ý cổng PostgreSQL*: Mặc định ánh xạ cổng host **5434** (thay vì 5432) để tránh xung đột nếu máy bạn đang có một container PostgreSQL khác đang chạy cổng 5432. Bạn có thể tùy chỉnh lại trong file `.env` nếu muốn.

---

## 4. HƯỚNG DẪN KHỞI CHẠY HỆ THỐNG

### Bước 1: Chuẩn bị cấu hình và khởi động stack

Nếu chưa có `.env`, sao chép `.env.example` thành `.env` và điều chỉnh cấu hình cục bộ. Giữ `.env` hiện có nếu đã thiết lập.

Compose hiện dùng image Airflow, không có `build:` trỏ tới `Dockerfile`; lệnh bên dưới không xây Dockerfile tùy chỉnh. Dependency được cấu hình qua `_PIP_ADDITIONAL_REQUIREMENTS`; xem [kiến trúc](docs/architecture.md) trước khi triển khai ERA5/GeoPandas.

```powershell
# Khởi động ở chế độ background (detached)
docker compose up -d
```

Container `weather-minio-init` được cấu hình tạo 3 bucket: `bronze`, `silver`, `gold`. Xác minh trạng thái sau khởi động; bucket tồn tại chưa chứng minh có dữ liệu thật.

### Bước 2: Kiểm tra trạng thái các container

```powershell
docker compose ps
```

### Bước 3: Truy cập các giao diện quản trị
1. **MinIO Web Console**: Truy cập [http://localhost:9001](http://localhost:9001)  
   - Đăng nhập: `minioadmin` / `minioadmin`  
   - Bạn sẽ thấy 3 bucket: `bronze`, `silver`, `gold`.
2. **Apache Airflow UI**: Truy cập [http://localhost:8080](http://localhost:8080)  
   - Đăng nhập: `admin` / `admin`  
   - Bật DAG mẫu `weather_system_healthcheck_dag` để kiểm tra kết nối tự động.
3. **PostgreSQL Data Warehouse**: Kết nối qua DBeaver / Navicat / pgAdmin / Power BI:
   - Host: `localhost`
   - Port: `5434`
   - Database: `weather_dwh`
   - User: `postgres`
   - Password: `postgres`

---

## 5. TẠO VÀ KIỂM THỬ BUCKET TRÊN MINIO (TASK 2)

### 5.1. Tự động hóa qua Script Python
Bên cạnh container `minio-init`, bạn có thể chạy trực tiếp script Python để khởi tạo bucket và các thư mục phân vùng mẫu:

```powershell
# Chạy script khởi tạo bucket và cây thư mục phân vùng
python scripts/init_minio.py
```

### 5.2. Chạy script kiểm thử tải dữ liệu mẫu theo đúng Quy ước
Script bên dưới **ghi dữ liệu giả lập** vào 3 tầng, không là kiểm tra read-only và không chứng minh ETL thực. GeoJSON mẫu chỉ là Point; Gold mẫu chưa đủ hợp đồng nạp Fact observation. Chỉ chạy khi muốn kiểm tra upload trên môi trường thử nghiệm:

```powershell
python scripts/test_minio_upload.py
```

### 5.3. Tóm tắt Quy ước đặt tên file (Naming Convention)
Chi tiết đầy đủ xem tại file tài liệu: **[docs/NAMING_CONVENTIONS.md](docs/NAMING_CONVENTIONS.md)**.

- **Tầng Bronze (Raw)**:
  * Open-Meteo: `bronze/open_meteo/hourly/year=YYYY/month=MM/open_meteo_<location>_hourly_<start>_<end>.json`
  * ERA5: `bronze/era5/reanalysis/year=YYYY/month=MM/era5_north_vietnam_hourly_<YYYYMM>.nc`
  * Meteostat: `bronze/meteostat/station_hourly/year=YYYY/meteostat_station_<id>_hourly_<YYYY>.csv`
  * Địa giới không gian: `bronze/spatial/boundaries/vietnam_northern_provinces_v1.geojson`
- **Tầng Silver (Cleaned - Parquet)**:
  * Thời tiết theo giờ: `silver/weather_hourly/year=YYYY/month=MM/weather_hourly_<location>_<YYYYMM>.parquet`
  * Danh mục trạm đo: `silver/stations/stations_metadata_cleaned.parquet`
- **Tầng Gold (Serving & ML - Parquet)**:
  * Bảng Fact: `gold/fact_weather_observation/year=YYYY/month=MM/fact_weather_observation_<YYYYMM>.parquet`
  * Dataset huấn luyện ML (CART): `gold/ml_features/cart_rain_prediction_<split>_<version>.parquet`
  * Kết quả dự báo: `gold/predictions/year=YYYY/month=MM/rain_predictions_cart_<YYYYMMDD>.parquet`

---

## 6. CẤU TRÚC THƯ MỤC DỰ ÁN

```
Test_DL_weather/
├── .env                          # File cấu hình biến môi trường cục bộ
├── .env.example                  # Template mẫu biến môi trường
├── .gitignore                    # Bỏ qua log, data lớn, cache, secret keys
├── Dockerfile                    # Dockerfile tùy chỉnh, chưa được Compose dùng để build
├── docker-compose.yml            # Khởi chạy MinIO, Airflow, PostgreSQL
├── requirements.txt              # Danh sách thư viện Python cần thiết
├── README.md                     # Tài liệu hướng dẫn dự án
├── AGENTS.md                     # Quy tắc AI và định tuyến tài liệu
├── .agents/skills/               # Weather skills và các skill đã cài khác
├── đề tài.pdf                     # Tài liệu mô tả đề tài gốc
├── dags/                         # Chứa các DAG của Airflow
│   ├── AGENTS.md                 # Quy tắc Airflow
│   ├── weather_pipeline_health_dag.py # DAG kiểm tra kết nối MinIO và Postgres DWH
│   └── dag_ingest_open_meteo.py   # DAG lịch sử thủ công theo split
├── docs/                         # Tài liệu quy chuẩn kỹ thuật
│   ├── NAMING_CONVENTIONS.md     # Quy ước bucket và object key
│   ├── architecture.md          # Kiến trúc và runtime
│   ├── data-contract.md         # Hợp đồng Silver/Gold
│   ├── warehouse-schema.md      # Grain, khóa và OLAP
│   ├── rain-label.md            # Nhãn và đánh giá
│   └── progress.md              # Bằng chứng tiến độ và hạn chế
├── scripts/                      # Các script tiện ích và ETL độc lập
│   ├── AGENTS.md                 # Quy tắc ETL
│   ├── init_minio.py             # Khởi tạo bucket và folder placeholder trên MinIO
│   ├── ingest_open_meteo.py      # Thu thập lịch sử theo năm vào Bronze
│   └── test_minio_upload.py      # Upload mẫu dữ liệu kiểm thử theo quy ước
├── sql/                          # Scripts khởi tạo cơ sở dữ liệu
│   ├── AGENTS.md                 # Quy tắc DW và migration
│   ├── 01_init_postgres.sh       # Script entrypoint cho PostgreSQL container
│   ├── 01_init_databases.sql     # DDL tạo database weather_dwh
│   └── init_dwh_schema.sql       # DDL tạo Star Schema và nạp dữ liệu Dim mẫu
├── plugins/                      # Custom Airflow plugins (nếu có)
├── config/                       # Airflow config bổ sung
└── data/                         # Thư mục dữ liệu cục bộ tạm thời (đã gitignore)
    ├── bronze/
    ├── silver/
    └── gold/
```

---

## 7. LỆNH TIỆN ÍCH QUẢN TRỊ DOCKER

- **Dừng stack**:
  ```powershell
  docker compose down
  ```
- **Dừng và xóa toàn bộ dữ liệu (volumes)**:
  ```powershell
  docker compose down -v
  ```
- **Xem log dịch vụ cụ thể**:
  ```powershell
  docker compose logs -f minio
  docker compose logs -f airflow-webserver
  docker compose logs -f postgres
  ```
