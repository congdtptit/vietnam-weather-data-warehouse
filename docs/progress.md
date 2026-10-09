# Tiến độ và bằng chứng

Kiểm kê ngày **08/10/2026** (giờ Việt Nam). Nguồn: đọc code/cấu hình/tài liệu trong repo. Lượt này không chạy Docker, API, backfill, SQL trên database hoặc huấn luyện mô hình; vì vậy trạng thái runtime chưa xác minh.

Workbook `Phan_cong_cong_viec_DW_khi_tuong.xlsx` là nơi quản lý phân công 40 việc và người phụ trách. Tài liệu này ghi bằng chứng kỹ thuật và hạn chế, không tạo bảng trạng thái phân công song song. Không đánh dấu hoàn thành trong workbook từ việc file code tồn tại.

## Kiểm kê

| Hạng mục | Bằng chứng | Trạng thái xác minh |
| --- | --- | --- |
| Hạ tầng | `docker-compose.yml`, `.env.example`, `Dockerfile` | Có cấu hình; chưa kiểm tra services/runtime |
| Bucket và naming | `scripts/init_minio.py`, MinIO init service, `docs/NAMING_CONVENTIONS.md` | Đã xác minh live: 3 bucket `bronze`, `silver`, `gold` hoạt động trên MinIO |
| Open-Meteo → Bronze | `scripts/ingest_open_meteo.py`, `dags/dag_ingest_open_meteo.py` | Đã chạy toàn bộ: 80/80 file (8 điểm × 10 năm 2016–2025, ~701.376 bản ghi theo giờ) nạp vào MinIO `bronze/` |
| Connectivity | `dags/weather_pipeline_health_dag.py` | Có DAG kiểm tra kết nối/schema cơ bản; không chứng minh data completeness |
| Open-Meteo → Silver | `scripts/transform_open_meteo_to_silver.py` | Đã triển khai và chạy: 960 file Parquet nạp vào MinIO `silver/weather_hourly/` (701.376 bản ghi giờ làm sạch, 100% đủ giờ, kèm báo cáo DQ) |
| Gold / DW Loader | `scripts/test_minio_upload.py` | Chỉ upload synthetic sample; chưa có ETL Gold và DW loader thực |
| DW | `sql/init_dwh_schema.sql`, init shell | Có schema/seed một số Dim; chưa xác minh live DB/loader |
| Meteostat → Bronze | `scripts/ingest_meteostat.py` | Đã triển khai và chạy: 44 file CSV thô nạp vào MinIO `bronze/meteostat/station_hourly/` (các năm trạm có phát báo giờ thực tế) |
| Meteostat → Silver | `scripts/transform_meteostat_to_silver.py` | Đã triển khai và chạy: 518 file Parquet nạp vào MinIO `silver/weather_hourly/` (375.188 bản ghi giờ làm sạch/đổi đơn vị, bảng trạm `silver/stations/`, kèm báo cáo DQ) |
| ERA5, NOAA, GeoJSON thật | Chưa tìm thấy implementation/artifact thực trong phần kiểm kê | Cần triển khai hoặc bổ sung bằng chứng |
| OLAP, Power BI, CART, forecast integration | Chưa tìm thấy view/model/PBIX thực | Cần triển khai hoặc bổ sung bằng chứng |
| Hướng dẫn AI | AGENTS gốc/con, 6 tài liệu domain, 5 weather skills | Đã viết; kết quả kiểm tra tài liệu ở bên dưới |

## Sai lệch và việc cần xử lý

| Vấn đề | Ảnh hưởng | Bước khắc phục tiếp theo |
| --- | --- | --- |
| Open-Meteo thêm `_pipeline_metadata` rồi serialize lại JSON Bronze | Không bảo toàn payload nguyên bản theo mục tiêu | Giữ raw response riêng và metadata/manifest riêng |
| Ingestion bắt lỗi từng file, chỉ in lỗi rồi tiếp tục | Subprocess có thể exit 0 dù thiếu file; DAG báo thành công không đầy đủ | Thu thập failures và trả lỗi sau batch; kiểm tra resume/rerun |
| Skip dựa trên object size >2000 | Không xác minh schema/checksum/coverage | Dùng manifest và kiểm tra nội dung/phạm vi |
| Retry mọi non-200, chưa xử lý `Retry-After` | Retry cả lỗi cấu hình, khó phản ánh lỗi đúng | Phân loại transient/permanent, retry hữu hạn |
| Bronze actual theo năm, ví dụ naming theo tháng | Người đọc có thể nhầm phạm vi key | Đã ghi hai trường hợp hỗ trợ; không đổi key code hiện tại |
| Gold sample thiếu timestamp, dùng giờ local, có label tương lai | Không phải dataset nạp Fact observation hợp lệ | Tách synthetic sample khỏi ETL thực; chốt calendar basis và Gold contract |
| DDL thiếu DimDate seed, DimModel, prediction unique/origin/window | Chưa đủ nạp/đối soát dự báo | Thiết kế migration/loader sau khi chốt hợp đồng |
| Compose không build Dockerfile; dependency runtime cài riêng | Dockerfile/requirements không chứng minh môi trường thực | Chọn cơ chế image/dependency và kiểm chứng riêng |
| Init dùng `weather_dwh` cố định dù có `DWH_DB` | Đổi env name có thể lệch database khởi tạo | Đồng bộ cấu hình/init trước hỗ trợ tên khác |
| Health check dùng bucket names cố định | Cấu hình bucket tùy chỉnh chưa được kiểm tra tương ứng | Đồng bộ checks với env khi sửa hạ tầng |
| Sample GeoJSON là Point giả lập | Không phải ranh giới hành chính thật | Thu thập và version hóa GeoJSON thực |
| Nhãn, fallback, coverage và chọn trạm chưa chốt | ETL/ML có thể tích hợp sai ngữ nghĩa | Chốt các mục mở trong data contract, schema và rain label |

## Kết quả lượt tổ chức tài liệu

- Giữ nguyên code, cấu hình runtime, DDL, workbook phân công và 5 tên weather skill.
- Thêm AGENTS gốc, hướng dẫn trong `scripts/`, `dags/`, `sql/`; tài liệu kiến trúc, hợp đồng, schema, nhãn và tiến độ.
- Cập nhật README/naming và skill để trỏ đến tài liệu chính thức, phân biệt thiết kế với implementation.
- Kiểm tra hoàn tất: 11 file Markdown dự án với 23 liên kết nội bộ hợp lệ và code fence đóng đủ; cấu trúc/frontmatter, UI metadata và document references của 5 weather skill hợp lệ. Chỉ kiểm chứng tài liệu; các kiểm thử runtime bên trên vẫn chưa thực hiện.

## Cách cập nhật

Sau công việc kỹ thuật, ghi phạm vi đã chạy, số file/dòng/khóa, window, lệnh hoặc artifact làm bằng chứng, kết quả và hạn chế. Phân biệt đã viết → đã kiểm tra sample → đã chạy toàn phạm vi. Người phụ trách và trạng thái giao việc cập nhật ở workbook khi có đủ bằng chứng.
