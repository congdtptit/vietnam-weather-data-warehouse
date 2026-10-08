# Schema Data Warehouse khí tượng

Kiểm kê ngày 08/10/2026 từ `sql/init_dwh_schema.sql`. Phân biệt DDL đã có với đề xuất cần migration; chưa xác minh database đang chạy.

## Mô hình hiện có

| Bảng | Khóa và quan hệ trong DDL |
| --- | --- |
| `dim_date` | PK `date_key`; ngày, tháng, quý, năm và cờ mùa/cuối tuần |
| `dim_time` | PK `time_key`; giờ, phút, buổi; seed đủ 24 giờ |
| `dim_location` | PK `location_key`; UNIQUE `location_code`; tên, tỉnh, tọa độ, độ cao; seed 8 điểm |
| `dim_source` | PK `source_key`; UNIQUE `source_name`; loại/định dạng/tần suất; seed 4 nguồn |
| `dim_station` | PK `station_key`; UNIQUE `station_id`; FK `location_key` |
| `fact_weather_observation` | PK `fact_id`; FK date/time/location/source/station; `station_key` nullable |
| `fact_weather_prediction` | PK `prediction_id`; FK prediction date/time, target date, location; model name/version |

DDL chưa seed `dim_date`/`dim_station`, chưa có `DimModel`, loader thực hoặc view OLAP. Fact có schema không có nghĩa là đã có dữ liệu.

## Grain và khóa

- Yêu cầu observation: một nguồn × một điểm nghiên cứu × một giờ UTC.
- Constraint hiện tại: UNIQUE `(date_key, time_key, location_key, source_key)`. Tương đương grain thời gian chỉ khi date/time keys cùng basis và ánh xạ nhất quán với `timestamp_utc`.
- Đề xuất business key trực tiếp: `(source_key, location_key, timestamp_utc)`. Đây chưa là constraint đang có; cần migration và kiểm tra consumer trước khi thay.
- Surrogate key dùng join Dim–Fact; business key dùng lookup/upsert. Không dùng `created_at` làm khóa quan trắc.
- Nhiều trạm cùng nguồn/địa điểm/giờ cần quy tắc chọn/gộp trước nạp, hoặc fact station-grain riêng nếu giữ mọi trạm. `station_key` không âm thầm mở rộng grain hiện có.

## Quyết định còn mở và thiếu hụt

| Nội dung | Hiện tại | Việc cần quyết định/triển khai |
| --- | --- | --- |
| Calendar basis | DDL chưa quy định; mẫu Gold dùng giờ local | Chọn UTC/local cho date/time keys, hỗ trợ role/view khác nếu cần |
| Date dimension | Có DDL, chưa seed | Ngày đủ phạm vi và weekday theo convention thống nhất |
| Địa giới | Một cột `province` | Mapping tỉnh cũ/mới, mã và version/hiệu lực; SCD2 nếu thực sự cần lịch sử |
| Trạm | `station_id` unique toàn bảng | Xác minh namespace ID theo nguồn, mapping khoảng cách và selection rule |
| Prediction grain | Chưa unique key, chưa origin/target UTC window | Location × origin × horizon × model version, thêm source/scenario khi cần |
| Model | Name/version ở fact | Thiết kế `dim_model` và metadata artifact phù hợp |
| Loader | Chưa có | Dim trước Fact, staging/transaction/upsert và kiểm chứng rerun |

`predicted_at` hiện là thời gian tạo dòng kiểu TIMESTAMP; không phải forecast origin. Một `target_date_key` không đủ biểu diễn mọi cửa sổ rolling 24h. Probability/threshold/label version và UTC target start/end cần thiết kế khi tích hợp ML.

## Quy tắc tổng hợp

| Chỉ số | Phép tổng hợp |
| --- | --- |
| Mưa theo giờ | SUM interval không chồng lấn, cùng nguồn/địa điểm; nêu coverage |
| Nhiệt độ, độ ẩm, áp suất, tốc độ gió | AVG/MIN/MAX phù hợp; roll-up mean từ sum/count hoặc trọng số |
| Hướng gió | Circular/vector aggregation, không AVG tuyến tính số độ |
| Ngày mưa/nóng | Tính cờ nguồn × địa điểm × ngày với ngưỡng và coverage rồi đếm |
| Xác suất dự báo | Thống kê tập forecast xác định; không SUM hay gọi mean là xác suất cả vùng |

Không cộng mưa giữa các nguồn tại cùng điểm/giờ. Không cộng mưa nhiều điểm rồi gọi là mưa trung bình vùng. Partial total không phải tổng ngày đầy đủ. View ngày/tháng/năm cần source/location/calendar basis và valid/expected counts; join không được nhân dòng.

## Migration và kiểm chứng

Init chỉ phục vụ database mới: `CREATE TABLE IF NOT EXISTS` không sửa cấu trúc cũ. Script `01_init_postgres.sh` dùng literal `weather_dwh`, chưa hỗ trợ đầy đủ `DWH_DB` tùy chỉnh. Không xóa volume để thay schema.

Khi thực hiện thay đổi: đọc PostgreSQL skill, đưa migration riêng, kiểm tra Dim uniqueness/FK, fact grain, calendar boundary, rerun và roll-up giờ → ngày → tháng. Tách SQL mới viết khỏi SQL thực thi và báo phạm vi dữ liệu đã kiểm tra.
