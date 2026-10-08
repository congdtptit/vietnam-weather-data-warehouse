# Hợp đồng dữ liệu khí tượng

Phiên bản tài liệu: draft v0.1, 08/10/2026. Đây là hợp đồng mục tiêu cho triển khai; chưa có pipeline Silver/Gold thực để chứng minh tuân thủ.

## Trạng thái quyết định

| Nội dung | Trạng thái và cơ sở |
| --- | --- |
| Thời gian chuẩn UTC, đơn vị chuẩn và Parquet | Quy ước đã có trong NAMING_CONVENTIONS và phân công |
| Observation: nguồn × địa điểm × giờ | Yêu cầu đề tài |
| Tên biến chính bên dưới | Quy ước hiện có, được DDL sử dụng |
| Dedup khi giá trị xung đột, chọn trạm, minimum coverage | Cần cả nhóm chốt trước publish dữ liệu thực |
| Interval semantics từng sản phẩm, mapping pressure/radiation | Cần xác minh theo metadata/tài liệu nguồn khi triển khai |
| Tên và kiểu cột lineage/DQ bổ sung | Đề xuất hợp đồng; phải chốt cùng producer/consumer |

## Grain, định danh và thời gian

- Silver observation đích duy nhất theo `(source_name, location_code, timestamp_utc)`; tên nguồn ánh xạ ổn định sang `DimSource`. Raw station data có thể có grain khác trước bước chọn/gộp trạm.
- Giữ mã hiện có `HAN`, `PTO`, `TNG`, `TUQ`, `LCA`, `HBI`, `HPH`, `NBI`. Đây là mã điểm nghiên cứu, không phải mã tỉnh hành chính hiện tại. Giữ tọa độ yêu cầu và tọa độ trạm/ô lưới thực tế để truy vết.
- `timestamp_utc`: timestamp có timezone UTC, mốc giờ sau bước resample được định nghĩa. Timestamp naive từ nguồn chỉ được gán timezone khi có metadata xác nhận.
- `timestamp_local`: giờ suy ra theo `Asia/Ho_Chi_Minh`, phục vụ hiển thị; không là khóa độc lập. PostgreSQL hiện lưu cột này dưới dạng timestamp không timezone.
- Cửa sổ xử lý nội bộ `[start, end)`. Timestamp của phép đo tích lũy có thể chỉ đầu/cuối interval; cần ghi semantics, không đồng nhất cửa sổ xử lý với nhãn tương lai.
- Quy tắc chọn calendar basis của `date_key`/`time_key` còn mở; xem [warehouse schema](warehouse-schema.md).

## Các cột quan trắc chuẩn

Kiểu dưới đây là đề xuất trao đổi Silver, không thay đổi kiểu NUMERIC hiện có của PostgreSQL.

| Cột | Kiểu Silver mục tiêu | Đơn vị / ý nghĩa |
| --- | --- | --- |
| `source_name` | string, không null | Định danh nguồn, giữ riêng từng nguồn |
| `location_code` | string, không null | Điểm nghiên cứu ổn định |
| `timestamp_utc` | timestamp UTC, không null | Thời điểm đại diện đã được xác minh |
| `timestamp_local` | timestamp, suy ra | Giờ Việt Nam |
| `temperature_c` | float nullable | °C |
| `humidity_percent` | float nullable | Độ ẩm tương đối, % |
| `pressure_hpa` | float nullable | hPa; phải ghi loại surface/sea-level trước mapping |
| `precipitation_mm` | float nullable | Tổng lượng giáng thủy của interval một giờ, không cộng dồn nhiều giờ |
| `wind_speed_ms` | float nullable | m/s, độ cao đo theo sản phẩm |
| `wind_direction_deg` | float nullable | Độ theo quy ước khí tượng nguồn |
| `cloud_cover_percent` | float nullable | % |
| `solar_radiation_wm2` | float nullable | W/m²; phải chốt loại bức xạ và interval trước mapping |

Không ép các biến khác nhau như direct-normal irradiance và shortwave radiation vào một cột chỉ vì cùng đơn vị. Áp suất surface và sea-level cũng không tương đương. Biến mở rộng được giữ ở Bronze hoặc bổ sung schema có phiên bản khi cần.

## Các tầng và provenance

- Bronze mục tiêu giữ nguyên bytes nguồn; metadata tải tách riêng hoặc ở object metadata/manifest: source/product, request không có secret, retrieval time, checksum, coverage và phiên bản. Script hiện tại thêm `_pipeline_metadata` vào JSON nên chưa đáp ứng bảo toàn nguyên bản.
- Silver giữ nguồn, đơn vị, thời gian chuẩn, mapping location/station, lineage và cờ chất lượng. Tên cột bổ sung cần chốt; không coi chúng đã được code hỗ trợ.
- Gold observation vẫn giữ grain đa nguồn. Dataset ML pivot/fusion có grain riêng, feature names và source policy rõ ràng. Nhãn tương lai thuộc dataset ML, không tự đưa vào fact observation.
- Bucket/key theo [naming conventions](NAMING_CONVENTIONS.md); thay key/schema phải cập nhật consumer cùng lúc.

## Thiếu, trùng và chất lượng

- Null khác 0. Giờ không có record khác record có biến null; báo cả hai. Không điền mưa thiếu bằng 0 hoặc tự tạo nhãn No.
- Duplicate giống nhau có thể dedup theo quy tắc xác định; duplicate giá trị xung đột cần quarantine/báo lỗi hoặc chính sách ưu tiên đã chốt. Không tùy tiện giữ dòng đầu.
- Không ép record dưới giờ về mốc giờ mà chưa có resampling rule theo biến. Không nội suy nhãn; imputation feature phải tuân thủ [rain label](rain-label.md).
- Kiểm tra khóa, schema, UTC, duplicate, missing hours và đơn vị trước publish. Độ ẩm/mây trong [0,100], mưa/gió không âm; ngưỡng ngoại lai và coverage cấu hình theo mục đích, chưa được chốt toàn dự án.
- ERA5 accumulation phải xét reset/cycle đúng sản phẩm trước đổi sang mưa theo giờ. Đối soát nguồn trên interval, vị trí và đại lượng tương đương; không coi nguồn tái phân tích liên quan là xác nhận độc lập.

## Thay đổi hợp đồng

Ghi phiên bản, phần thay đổi và quyết định còn mở; cập nhật adapter, DQ, Gold, loader và BI/ML bị ảnh hưởng. Kiểm tra sample và chạy lại không trùng trước khi công bố tuân thủ. Không tự coi bản draft là phê duyệt mọi đề xuất.
