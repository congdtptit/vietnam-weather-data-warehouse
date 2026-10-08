# RainNext24h và phương pháp đánh giá

Draft v0.1, 08/10/2026. Đề tài yêu cầu phân loại có/không mưa trong 24 giờ tiếp theo bằng CART. Tài liệu này ghi quyết định đã có và điểm cần cả nhóm chốt; chưa có code nhãn hoặc mô hình thực.

## Quyết định và trạng thái

| Nội dung | Trạng thái |
| --- | --- |
| CART, hai lớp RainNext24h Yes/No | Yêu cầu đề tài |
| Train 2016–2023, validation 2024, test 2025 | Yêu cầu đề tài |
| Chú trọng Recall, báo Confusion Matrix/Accuracy/Precision/Recall/F1 | Yêu cầu đề tài |
| Chọn probability threshold trên validation, khóa trước test | Quy tắc phương pháp để tránh dùng test chọn mô hình |
| Ngưỡng tổng mưa tạo nhãn: 0.1 hay 1 mm | Chưa chốt; là lựa chọn được gợi ý trong workbook |
| Ưu tiên trạm, fallback ERA5 | Đề xuất trong workbook; cần chốt phạm vi fallback |
| Chọn nguồn theo toàn cửa sổ hay từng giờ | Chưa chốt |
| Minimum coverage và cách xử lý nhãn thiếu | Chưa chốt; đề xuất yêu cầu đủ 24 giờ hợp lệ |
| Tradeoff Recall với Precision/false alarms | Chưa chốt mục tiêu định lượng |

Ngưỡng lượng mưa (mm) định nghĩa ground truth; ngưỡng xác suất định nghĩa lớp dự báo. Không dùng hai ngưỡng thay thế nhau.

## Thời điểm và nhãn

- Forecast origin `t` là thời điểm thông tin được dùng để dự báo, không nhất thiết là thời điểm job chạy.
- Với mưa hourly timestamp ở cuối interval, target là `(t, t+24h]`, gồm các giờ t+1h đến t+24h. Với timestamp ở đầu interval, chuyển/diễn giải interval theo nguồn trước khi tính; không áp dụng cùng `shift` một cách máy móc.
- Nhãn Rain khi tổng mưa target đạt ngưỡng mm đã chốt. Giữ target start/end, source/provenance, label version và valid hour count.
- Đề xuất: thiếu giờ cần thiết thì label unavailable, loại khỏi đánh giá; không biến thiếu thành No. Cuối chuỗi dưới 24h không là nhãn hợp lệ. Chính sách khác cần quyết định rõ trước triển khai.
- Tính riêng theo location trên grid giờ đã kiểm tra gap. `shift(-24)` không đảm bảo 24h nếu chuỗi thiếu giờ.

## Đặc trưng và chống rò rỉ

- Chỉ dùng dữ liệu có thể biết tại hoặc trước t: lag, trailing rolling, biến thiên áp suất 3h/24h và giờ/tháng. Xét cả publication latency; measurement timestamp không chứng minh dữ liệu đã sẵn có.
- Không centered rolling, future shift, bfill hoặc nội suy dùng tương lai. Imputer/preprocessing/feature selection fit trên train; validation/test chỉ transform.
- Lag/rolling không tràn giữa địa điểm. Pivot/fusion nguồn theo rule cố định, không dùng test chọn nguồn hay feature.
- Nếu chỉ có final reanalysis ERA5, báo thí nghiệm hồi cứu; chưa chứng minh forecast thời gian thực. Kiểm tra availability của feature trước khi vận hành.

## Chia tập và chọn ngưỡng

- Chia theo forecast origin UTC: train `[2016-01-01, 2024-01-01)`, validation `[2024-01-01, 2025-01-01)`, test `[2025-01-01, 2026-01-01)`; không random shuffle.
- Purge mẫu train/validation có target dùng bất kỳ interval mưa nào thuộc split tiếp theo. Với interval-end timestamp, giờ đúng mốc bắt đầu split tiếp theo cũng phải loại khỏi nhãn split trước theo quy ước split đã chọn. Lịch sử quá khứ từ split trước được dùng cho feature split sau; test chỉ giữ target có ground truth đầy đủ.
- Tinh chỉnh `max_depth`, `min_samples_leaf`, `ccp_alpha`, `class_weight` và probability threshold trên train/validation; pruning path chỉ dùng train. Không resample trước chia tập.
- Chú trọng Recall nhưng kèm Precision/F1, support và false alarms; all-Yes không mặc nhiên là mô hình tốt. Đối chiếu baseline luôn No và baseline phù hợp.
- Khóa artifact/features/threshold/label contract trước đánh giá test. Nếu xem test rồi sửa thiết kế, công bố test đã tham gia phát triển.
- Workbook có mục ID 29 nhắc chỉnh ngưỡng cùng bước test 2025; khi thực hiện phải chọn ngưỡng bằng validation 2024, giữ test để đánh giá. Không sửa workbook trong lượt tổ chức tài liệu này.

## Artifact và nghiệm thu

Lưu model + preprocessing, ordered features/units, model/label version, source policy, train window, threshold, seed và dependency versions. Prediction giữ origin UTC, target window, location, probability và class; thiết kế khóa nạp theo [warehouse schema](warehouse-schema.md).

Báo TP/FP/TN/FN với Rain là positive class; Accuracy/Precision/Recall/F1, support, coverage và threshold. Undefined metric phải được giải thích. Kiểm tra nhãn trên mẫu t, t+1h, t+24h, t+25h, gap, biên năm; thay dữ liệu sau t không được làm đổi feature tại t.
