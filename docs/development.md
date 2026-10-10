# Môi trường phát triển

## Runtime thống nhất

- Runtime chính: Linux container, Airflow **2.9.3**, Python **3.10**.
- `Dockerfile` dùng `apache/airflow:2.9.3-python3.10` khóa bằng digest SHA-256; Compose build và dùng image `weather-airflow:2.9.3-python3.10` cho init, webserver và scheduler.
- Thư viện được cài khi build, không cài lại lúc mỗi container khởi động. Build chạy `pip check` để phát hiện dependency không tương thích.
- `.dockerignore` chỉ cho Dockerfile, lockfile, cấu hình test và code/fixture cần cho target test vào build context, tránh gửi `.env`, dữ liệu và log tới builder. Target `runtime` không copy code/fixture test hoặc cài công cụ dev.
- Đây là môi trường phát triển của dự án hiện tại, không phải chứng nhận production hay kế hoạch nâng cấp Airflow.

## Setup và kiểm tra image

Cài Docker Desktop, mở ứng dụng và chờ Docker Engine sẵn sàng. Nếu chưa có `.env`, sao chép `.env.example` thành `.env`; giữ file đã cấu hình nếu có. Không cần cài Python/thư viện thời tiết trên host để chạy container.

```bash
docker compose config --quiet
docker compose build airflow-webserver
docker run --rm --network none --entrypoint python weather-airflow:2.9.3-python3.10 --version
docker run --rm --network none --entrypoint pip weather-airflow:2.9.3-python3.10 check
```

Các lệnh trên không chạy ingestion hay khởi tạo database. Sau khi build thành công, khởi động hệ thống khi cần:

```bash
docker compose up -d --no-build
docker compose ps
```

Lệnh `up` chạy các service init và có thể khởi tạo database/bucket theo cấu hình. Build mới không thay image của container đang chạy; dùng `up` để áp dụng khi sẵn sàng. Không xóa volume để cập nhật môi trường.

`AIRFLOW_IMAGE_NAME` trong `.env` cũ không còn được sử dụng: tên image do Compose quy định để tránh bỏ qua Dockerfile hoặc ghi đè tag image chính thức. Khi đổi Airflow/Python, cập nhật Dockerfile, tag Compose, `.python-version`, constraints và lockfile cùng nhau.

## Dependency và cách cập nhật

- `requirements.in`: dependency trực tiếp và phiên bản lựa chọn của dự án.
- `constraints/airflow-2.9.3-python3.10.txt`: bản sao nguyên vẹn [constraints chính thức](https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt), dùng làm đầu vào resolver.
- `requirements.txt`: dependency trực tiếp và gián tiếp đã khóa, kèm SHA-256; Docker cài với `--require-hashes`.
- Lock được tạo bằng **uv 0.8.22**, target **Python 3.10/Linux x86_64**. Kiểm tra build riêng trước khi coi kiến trúc khác đã được hỗ trợ.

Không chỉnh lockfile bằng tay. Sau khi sửa `requirements.in`, dùng uv 0.8.22 để tạo lại:

```bash
uv pip compile requirements.in --python-version 3.10 --python-platform linux --generate-hashes --output-file requirements.txt --no-header --quiet
docker compose build airflow-webserver
```

Airflow 2.9.3 cần SQLAlchemy dòng 1.4; không nâng lên 2.x độc lập. `s3fs` được giữ ở 2023.12.2 theo constraints để đồng bộ `fsspec`/`aiobotocore`. Constraints cũ phục vụ tương thích phiên bản hiện tại; nâng cấp bảo mật cần một lượt nâng cấp và kiểm thử riêng.

Dependency Python và base image Airflow đã khóa. Dockerfile cài Python wheels (`--only-binary=:all:`) và không cài thêm compiler/gói apt; `curl` phục vụ healthcheck đã có trong base image. GeoJSON/NetCDF dùng thư viện native đi cùng wheels, không cần GDAL CLI trong workflow hiện tại. Nếu thêm dependency không có wheel cho kiến trúc đích, build sẽ báo lỗi để nhóm quyết định rõ cách hỗ trợ. MinIO/PostgreSQL vẫn dùng cấu hình image hiện có; chưa chứng nhận rebuild giống từng byte.

## Python local tùy chọn

`.python-version` ghi **3.10** cho công cụ quản lý Python/IDE; file này không tự cài Python. Nếu cần chạy script ngoài Docker, dùng Python 3.10 và virtualenv riêng; không dùng Python 3.12 hệ thống để xác nhận runtime container. Lock hiện được kiểm chứng theo target Linux; container là runtime tham chiếu, môi trường native macOS/Windows cần kiểm tra riêng.

## Thành viên mới và kiểm tra code

Cần Git, Python >=3.10 cho công cụ local, GNU Make/bash và Docker Desktop/Engine với Compose v2. Windows dùng WSL2 có tích hợp Docker. Python của runtime vẫn là 3.10 trong container, kể cả khi Python công cụ local là 3.12.

Sau khi clone repository và vào thư mục dự án:

```bash
make setup          # Tạo .venv, cài requirements-dev.txt có hash
make hooks          # Cài pre-commit cho checkout này
make lint           # Ruff lint và kiểm tra format
make test           # Build target test, chạy offline unit/runtime/DAG tests
make test-integration
```

`make help` liệt kê lệnh; `make check` chạy lint, kiểm tra Compose và cả hai nhóm test. Các lệnh test không cần `.env` của dự án. Lần setup/build đầu cần mạng để tải package/image; test offline chạy với `--network none`.

Trước khi commit:

```bash
make format         # Sửa lint an toàn và format scripts/dags/tests
make lint
make precommit      # Chạy hook trên tất cả file đã được Git theo dõi
```

Pre-commit cũng chạy khi `git commit`; nếu hook sửa file, xem lại diff và stage lại file trước khi commit. Hook dùng Ruff trong `.venv` nên cần `make setup` cho mỗi checkout. Cache pre-commit từ Makefile nằm trong `.venv/.cache/`. File mới chưa được Git theo dõi được kiểm tra bởi `make lint`; hook sẽ kiểm tra khi được stage.

Muốn mở hệ thống phát triển, tạo `.env` từ `.env.example` nếu chưa có rồi chạy `make up`. `make down` dừng service và giữ volume. Không dùng hệ thống phát triển để chạy integration test.

## Bộ test và môi trường riêng

- `pyproject.toml` chỉ cấu hình Ruff/pytest, không thay đổi cách đóng gói ứng dụng. Pytest chỉ collect `tests/`; script upload demo cũ đã bỏ. Parquet thống nhất dùng `pyarrow`.
- `tests/fixtures/open_meteo_hourly.json` gồm **3 giờ giả lập**, đi qua biên ngày nhuận/tháng, có mưa 0 và null. Fixture này không đại diện dữ liệu đủ một năm.
- Unit tests mock API/MinIO/subprocess và chặn socket; kiểm tra request UTC/đơn vị, retry, phạm vi ingest và trạng thái subprocess. Runtime tests kiểm tra Parquet/NetCDF/GeoJSON/CART. DAG tests import và kiểm tra dependency, không chạy DAG lịch sử.
- `docker-compose.test.yml` là Compose **độc lập**: project `weather-tests`, DB `weather_test`, tài khoản thử nghiệm, tmpfs cho PostgreSQL/MinIO, network nội bộ, không cổng host, không bind mount hay volume dữ liệu development. Không ghép file này với Compose chính.
- `make test-integration` bỏ qua `.env`, chạy schema hiện có và ingestion với API mock trên dịch vụ test; tự dọn stack khi đạt hoặc lỗi. Khi lỗi, in log trước khi dọn. Có thể dùng `make test-integration TEST_PROJECT=weather-tests-alice` cho một project test riêng.
- Fixture integration kiểm tra host/database/user test trước khi kết nối. Fixture setup schema và kiểm tra seed rerun, uniqueness/FK, task health và rerun một object Bronze; chưa kiểm tra loader DW thực vì chưa có loader.
- `WEATHER_TESTING=1` là guard bắt buộc của pytest, đã đặt trong image/Compose test. Dùng `make test` thay vì chạy pytest native với thư viện chưa cài.

Test không xác nhận chất lượng dữ liệu lịch sử hay độ chính xác CART. Lỗi ingestion che giấu thất bại từng file, skip theo kích thước và sửa payload Bronze vẫn là các hạn chế trong [tiến độ](progress.md), chưa được sửa trong lượt setup test.

## Dependency của công cụ

`requirements-dev.in` khai báo pytest/Ruff/pre-commit; `requirements-dev.txt` khóa các dependency gián tiếp cùng hash, có marker theo Python/OS. Dependency dùng chung được constrain bởi lock runtime. Tạo lại bằng uv 0.8.22:

```bash
uv pip compile requirements-dev.in --python-version 3.10 --universal --generate-hashes --output-file requirements-dev.txt --no-header --quiet
make setup
make check
```

Khi thay runtime lock, tạo lại dev lock và kiểm tra cả hai target. Compose chính build `runtime`; pytest build `test`, có thêm công cụ và fixture. `docker build .` mặc định chọn stage cuối (`test`); dùng `docker build --target runtime` nếu build runtime trực tiếp.

## GitHub Actions

`.github/workflows/ci.yml` chạy khi mở/cập nhật pull request, push vào `main`, hoặc chạy thủ công. Workflow dùng Python 3.10 cho tooling và cùng Makefile để kiểm tra Ruff, cấu hình Compose, test offline và integration. CI chỉ validate cấu hình pre-commit, không chạy lại hai Ruff hook đã được `make lint` kiểm tra; hook vẫn chạy ở máy thành viên trước commit. Không dùng secrets production, không deploy hoặc tải lịch sử.

Sau khi push cấu hình lên GitHub, xem tab Actions để xác nhận run trên runner thật. Nếu muốn chặn merge khi CI lỗi, cấu hình branch protection/ruleset với status check **CI / Lint and tests** theo tên được GitHub hiển thị; file workflow không tự bật quy tắc bảo vệ nhánh. Xem [tiến độ](progress.md) để phân biệt kiểm tra local với run GitHub.
