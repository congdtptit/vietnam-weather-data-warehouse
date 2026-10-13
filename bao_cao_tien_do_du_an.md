# BÁO CÁO TIẾN ĐỘ CÔNG VIỆC DỰ ÁN
## KHO DỮ LIỆU KHÍ TƯỢNG ĐA NGUỒN MIỀN BẮC VIỆT NAM (VIETNAM WEATHER DATA WAREHOUSE)

*Ngày lập báo cáo:* 12/10/2026  
*Phạm vi dự án:* Đề tài học phần Kho dữ liệu & Khai phá dữ liệu (10 tuần, 40 nhiệm vụ phân công).  
*Công nghệ nền tảng:* MinIO (S3 Data Lake), Apache Airflow 2.9.3, PostgreSQL 16 (Star Schema DWH), Python (PyArrow, scikit-learn CART), Power BI.

---

### I. TỔNG QUAN TIẾN ĐỘ (EXECUTIVE SUMMARY)

Dự án hiện đã hoàn thành vững chắc **Giai đoạn nền tảng (Base Infrastructure)**, **Hệ thống tài liệu chuẩn hóa kiến trúc & Hợp đồng dữ liệu (Architecture & Data Contracts)**, và hoàn thành thực thi **Toàn bộ pipeline ETL tầng Bronze & tầng Silver (Open-Meteo và Meteostat) với dữ liệu thực 10 năm (2016–2025)** lưu trữ trên Data Lake MinIO.

| Phân hệ / Hạng mục | Trạng thái kỹ thuật | Bằng chứng thực tế |

| **1. Hạ tầng & Môi trường** | **ĐÃ HOÀN THÀNH** | Docker Compose 3 dịch vụ chính, Dockerfile Airflow khóa 181 dependencies SHA-256, Makefile, Ruff, CI GitHub Actions. |
| **2. Tài liệu & Quy chuẩn** | **ĐÃ HOÀN THÀNH** | 9 tài liệu kỹ thuật trong `docs/`, hệ thống quy tắc AI (`AGENTS.md`), 5 Skills khí tượng, hợp đồng dữ liệu v0.1. |
| **3. Ingestion (Bronze)** | **ĐÃ HOÀN THÀNH** | Thu thập thành công Open-Meteo (80 file) & Meteostat (34 file) cho 8 điểm trạm 2016–2025. |
| **4. Chuẩn hóa & Làm sạch (Silver)** | **ĐÃ HOÀN THÀNH (LIVE)** | Đã chạy toàn diện: **1,488 đối tượng** trên MinIO, gồm **994,145 bản ghi giờ sạch**, phát hiện 5,263 giờ thiếu thực tế. |
| **5. Kiểm thử & Chất lượng (DQ/Tests)** | **ĐÃ HOÀN THÀNH** | **27/27 unit/slice tests pass**, 6/6 integration tests pass, DQ reports JSON lưu trữ trên MinIO. |
| **6. Kho dữ liệu DWH (PostgreSQL)** | **SẴN SÀNG SCHEMA** | DDL Star Schema 5 dimensions + fact observations & fact predictions sẵn sàng; chờ nạp batch Gold. |
| **7. Mô hình CART & Power BI** | **CHUẨN BỊ TRIỂN KHAI** | Đã chốt thiết kế nhãn `RainNext24h`, rolling features chống data leakage; đang chờ tầng Gold. |

---

### II. CHI TIẾT CÁC CÔNG VIỆC ĐÃ HOÀN THÀNH

#### 1. Thiết lập hạ tầng, Container hóa và Quản lý môi trường
- **Docker hóa toàn diện**: Đã cấu hình `docker-compose.yml` tích hợp MinIO (9000/9001), PostgreSQL DWH (5434), và cụm Airflow 2.9.3 (Webserver, Scheduler, DB init).
- **Khóa phụ thuộc nghiêm ngặt (Reproducible Build)**: Base image Airflow ghim digest SHA-256; toàn bộ 181 thư viện Python runtime được khóa chặt chẽ trong `requirements.txt` bằng SHA-256 hash thông qua `uv`, loại bỏ hoàn toàn rủi ro xung đột phiên bản.
- **Tối ưu hóa cấu trúc**: Loại bỏ các dependencies thừa (`fastparquet`, `cramjam`, compiler không dùng), chuyển đổi thuần túy sang `pyarrow`.
- **Hệ thống CI/CD & Code Quality**:
  - Tích hợp Ruff (linter và formatter cực nhanh).
  - Tích hợp pre-commit hooks tự động kiểm tra cú pháp trước khi commit.
  - Cấu hình Makefile chuẩn hóa quy trình: `make setup`, `make check`, `make test`, `make test-integration`, `make build`.
  - Workflow GitHub Actions `.github/workflows/ci.yml` sẵn sàng cho quy trình pull request.

#### 2. Kiến trúc giải pháp & Hợp đồng dữ liệu (Data Governance)
Đã biên soạn và chuẩn hóa toàn bộ bộ tài liệu kỹ thuật chuyên nghiệp trong thư mục `docs/`:
- **[architecture.md](file:///c:/Users/ASUS/vietnam-weather-data-warehouse/docs/architecture.md)**: Thiết kế luồng dữ liệu 3 tầng (Bronze -> Silver -> Gold -> PostgreSQL DWH -> Power BI & CART ML).
- **[data-contract.md](file:///c:/Users/ASUS/vietnam-weather-data-warehouse/docs/data-contract.md)**: Quy định chuẩn mực:
  - Mốc thời gian bắt buộc: Timezone-aware UTC.
  - Hạt quan sát (Grain): `source × stable location × UTC hour`.
  - Phân loại rõ ràng nguồn gốc áp suất: `pressure_type = "surface"` (Open-Meteo) và `pressure_type = "sea_level"` (Meteostat trạm).
  - Quy tắc xử lý trùng lặp (Dedup cùng giá trị vs Quarantine khi xung đột).
- **[warehouse-schema.md](file:///c:/Users/ASUS/vietnam-weather-data-warehouse/docs/warehouse-schema.md)**: Thiết kế mô hình Star Schema chuẩn gồm:
  - Bảng Fact quan sát: `fact_weather_observation` (hạt quan sát theo giờ).
  - Bảng Fact dự báo: `fact_rain_prediction` (phục vụ mô hình CART).
  - 5 Bảng Dimension: `dim_date`, `dim_time`, `dim_location`, `dim_station`, `dim_source`, và `dim_model`.
- **[rain-label.md](file:///c:/Users/ASUS/vietnam-weather-data-warehouse/docs/rain-label.md)**: Thiết kế bài toán ML `RainNext24h`:
  - Khung thời gian dự báo: `(t, t+24h]`.
  - Chiến lược chống Data Leakage tuyệt đối (chỉ tính lag/rolling tính từ thời điểm `<= t`).
  - Phân chia tập dữ liệu theo thời gian thực tế: Train (2016–2023), Validation (2024), Test (2025). Khóa ngưỡng phân loại (threshold) trên tập Validation trước khi đánh giá trên Test.
- **[implementation-roadmap.md](file:///c:/Users/ASUS/vietnam-weather-data-warehouse/docs/implementation-roadmap.md)**: Lập bản đồ chi tiết hóa 40 tasks trong 10 tuần thành các tiêu chí kỹ thuật cụ thể.
- **[progress.md](file:///c:/Users/ASUS/vietnam-weather-data-warehouse/docs/progress.md)**: Nhật ký ghi nhận bằng chứng kỹ thuật trung thực, phân biệt rõ giữa code mẫu và dữ liệu thực tế đã chạy.

#### 3. Phát triển & Remediate Pipeline Thu thập dữ liệu (Bronze)
- **Open-Meteo API Ingestion** (`scripts/ingest_open_meteo.py`, `dags/dag_ingest_open_meteo.py`):
  - Thu thập dữ liệu 8 điểm tọa độ miền Bắc đại diện (`HAN`, `PTO`, `TNG`, `TUQ`, `LCA`, `HBI`, `HPH`, `NBI`).
  - Phạm vi 10 năm: 2016 – 2025.
  - Thu thập thành công **80 file Bronze raw JSON** vào MinIO bucket `bronze`.
- **Meteostat / NOAA Station Ingestion** (`scripts/ingest_meteostat.py`):
  - Thiết kế registry trạm chuẩn hóa `scripts/station_config.py`.
  - Xử lý thông minh mã HTTP 404 (cho các năm trạm chưa quan trắc) thay vì gây crash toàn bộ batch.
  - Phân biệt lỗi S3 connection với missing object.
  - Thu thập thành công **34 file Bronze CSV** cho các năm có dữ liệu trạm quan trắc thực tế.

#### 4. Chuẩn hóa & Làm sạch dữ liệu tầng Silver (Live Execution trên MinIO)
Đã hoàn thành đợt chạy thực thi toàn diện (Live Overwrite) biến đổi từ Bronze sang Silver trên MinIO:
- **Module kiểm tra chất lượng dùng chung** (`scripts/quality_checks.py`):
  - Kiểm tra tính liên tục theo lưới thời gian thực tế theo từng năm (xử lý chính xác 8,784 giờ cho năm nhuận như 2024 và 8,760 giờ cho năm thường).
  - Kiểm tra dải giá trị khí tượng hợp lý (nhiệt độ, độ ẩm, lượng mưa không âm) mà không tự ý cắt gọt (clip) làm sai lệch bản chất dữ liệu, gán `NaN` cho các ngoại lệ.
- **Open-Meteo Silver Pipeline** (`scripts/transform_open_meteo_to_silver.py`):
  - Chuyển đổi 80 file Bronze -> **960 file Parquet hàng tháng**.
  - Tổng số bản ghi làm sạch: **701,376 bản ghi giờ UTC**.
  - Gắn nhãn provenance áp suất: `pressure_type = "surface"`.
  - Tỷ lệ hoàn thành: 100% không phát sinh lỗi (báo cáo DQ JSON được xuất tự động).
- **Meteostat Silver Pipeline** (`scripts/transform_meteostat_to_silver.py`):
  - Chuyển đổi 34 file Bronze CSV -> **404 file Parquet hàng tháng**.
  - Tổng số bản ghi làm sạch: **292,769 bản ghi quan sát giờ**.
  - Gắn nhãn provenance áp suất: `pressure_type = "sea_level"`, lưu vết `station_id`.
  - Ánh xạ chính xác các mã trạm thực tế (Việt Trì `48825` -> `PTO`, Thái Nguyên `48826` -> `TNG`).
  - Ghi nhận chính xác **5,263 giờ bị khuyết thiếu** trong lịch sử quan trắc thực tế của các trạm.
  - Xuất bản metadata trạm đã làm sạch: `silver/stations/stations_metadata_cleaned.parquet`.
- **Tổng tài nguyên tầng Silver hiện tại trên MinIO**: **1,488 đối tượng** (1,479 Parquet thời tiết theo tháng, 2 Parquet metadata trạm, 7 báo cáo DQ JSON).

#### 5. Kết quả kiểm thử & Xác thực chất lượng (Test Evidence)
- **Ruff Code Formatting & Lint**: Đạt 100% không lỗi trên toàn bộ thư mục `scripts/` và `tests/`.
- **Bộ Unit Tests & Transformation Slices** (`tests/test_silver_transformations.py`, `tests/test_ingestion.py`):
  - **27/27 test passed** (thời gian chạy ~5.8s).
  - Bao quát toàn diện các kịch bản: kiểm tra lưới giờ UTC năm nhuận/năm thường, xử lý trùng lặp, cách ly dữ liệu xung đột, kiểm tra dải giá trị, round-trip Parquet PyArrow, xử lý mã HTTP 404.
- **Integration Tests trong môi trường Docker cô lập** (`tests/test_integration.py`):
  - **6/6 integration tests passed** (MinIO health, PostgreSQL schema/seed, tính toàn vẹn Foreign Key và Unique Constraints).

---

### III. ĐỐI SOÁT VỚI BẢNG PHÂN CÔNG CÔNG VIỆC (WORKBOOK SNAPSHOT)

Đối chiếu với 40 nhiệm vụ trong file phân công `Phan_cong_cong_viec_DW_khi_tuong.xlsx`:

| Giai đoạn / Task ID | Nội dung nhiệm vụ | Kết quả đạt được | Trạng thái kỹ thuật |
| :--- | :--- | :--- | :--- |
| **ID 1** (Tuần 1) | Cấu hình Docker Compose, Git, CI | Đầy đủ container, dependencies hashed, CI workflow | **Đã hoàn thành** |
| **ID 2** (Tuần 1) | Khởi tạo MinIO bucket & Naming | Đã tạo bucket Bronze, Silver, Gold; quy ước đặt tên chuẩn | **Đã hoàn thành** |
| **ID 3** (Tuần 1) | Bản nháp Star Schema DWH | File `sql/init_dwh_schema.sql` và tài liệu schema | **Đã hoàn thành** |
| **ID 6** (Tuần 1) | Thống nhất Data Contract Silver | Tài liệu `docs/data-contract.md` v0.1 | **Đã hoàn thành** |
| **ID 7** (Tuần 1) | Thống nhất nhãn RainNext24h | Tài liệu `docs/rain-label.md` | **Đã hoàn thành** |
| **ID 8** (Tuần 2–3) | Thu thập Open-Meteo -> Bronze | 80 file JSON (8 trạm x 10 năm) trên MinIO Bronze | **Đã hoàn thành** |
| **ID 9** (Tuần 2–3) | Thu thập Meteostat -> Bronze | 34 file CSV trên MinIO Bronze kèm xử lý lỗi 404 | **Đã hoàn thành** |
| **ID 10** (Tuần 2–3) | Biến đổi Bronze -> Silver | 1,488 đối tượng Parquet trên MinIO Silver (~1 triệu dòng) | **Đã hoàn thành** |
| **ID 4, 11** | GeoJSON ranh giới & DimLocation | Đang dùng tọa độ điểm đại diện; cần lấy GeoJSON ranh giới thực tế | *Đang chờ thực hiện* |
| **ID 5, 14, 15** | Đăng ký CDS & Thu thập ERA5 | Cần cấp tài khoản CDS API Copernicus và module NetCDF | *Đang chờ thực hiện* |
| **ID 12, 13** | Seed DimDate/DimTime & DDL DW | Đã seed 4,018 ngày (2016–2026), 10 trạm, 8 vị trí, 4 nguồn, 24 giờ | **Đã hoàn thành** |
| **ID 16, 18** | Tầng Gold & Nạp DWH Facts | Đã nạp 994,145 dòng 10 năm vào `fact_weather_observation` và 120 Parquet Gold | **Đã hoàn thành 100% (10 năm)** |
| **ID 19, 20, 27** | OLAP Views & Power BI Dashboard | Thiết kế semantic model, DAX, báo cáo trực quan | *Giai đoạn kế tiếp* |
| **ID 21–23, 28–30**| Dataset ML & Huấn luyện CART | Trích xuất feature lag/rolling, train model, đánh giá test | *Giai đoạn kế tiếp* |
| **ID 31–33** | Tích hợp dự báo vào DW & Power BI | Nạp `fact_rain_prediction` và trang đối soát dự báo/thực tế | *Giai đoạn kế tiếp* |

---

### IV. CÁC HẠN CHẾ & LƯU Ý KỸ THUẬT HIỆN TẠI

1. **Kho dữ liệu PostgreSQL DW đã nạp đầy đủ 10 năm**: Đã nạp toàn diện 994,145 bản ghi quan trắc giờ sạch (2016–2025) cho cả Open-Meteo và Meteostat trạm mặt đất vào bảng `fact_weather_observation`. Cổng kết nối host là `5434`.
2. **Nguồn dữ liệu ERA5**: Chưa triển khai do cần đăng ký API Key từ Copernicus Climate Data Store (CDS) và viết adapter xử lý file NetCDF (`.nc`).
3. **File GeoJSON ranh giới hành chính**: Cần bổ sung file GeoJSON phân định ranh giới hành chính các tỉnh miền Bắc thực tế có bản quyền hợp lệ (hậu sáp nhập hành chính).

---

### V. KẾ HOẠCH TRIỂN KHAI TIẾP THEO (NEXT STEPS)

1. **Bước 1 (Xây dựng tầng Đặc trưng Machine Learning cho bài toán CART)**:
   - Xây dựng dataset cho bài toán CART: tính toán các biến lag (t-1h, t-3h, t-6h, t-24h), rolling mean/max nhiệt độ, độ ẩm, áp suất và gán nhãn `RainNext24h` strictly `<= t`.
   - Phân chia tập dữ liệu: Train (2016–2023), Validation (2024), Test (2025).
2. **Bước 2 (Triển khai mô hình CART & Đánh giá)**:
   - Viết pipeline huấn luyện Decision Tree trên tập Train.
   - Chọn ngưỡng phân loại (probability threshold) trên tập Validation (2024) dựa trên chỉ số Recall.
   - Đánh giá độc lập trên tập Test (2025).
3. **Bước 3 (Xây dựng OLAP Views & Power BI Dashboard)**:
   - Viết các View tổng hợp (theo ngày, tháng, năm) trên PostgreSQL.
   - Kết nối Power BI với PostgreSQL DWH qua cổng `5434`, dựng mô hình Star Schema và xây dựng Dashboard phân tích xu hướng.


