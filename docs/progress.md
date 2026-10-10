# Tiến độ và bằng chứng

Kiểm kê ngày **08/10/2026** (giờ Việt Nam). Nguồn: đọc code/cấu hình/tài liệu trong repo. Lượt này không chạy Docker, API, backfill, SQL trên database hoặc huấn luyện mô hình; vì vậy trạng thái runtime chưa xác minh.

Workbook `Phan_cong_cong_viec_DW_khi_tuong.xlsx` là nơi quản lý phân công 40 việc và người phụ trách. Tài liệu này ghi bằng chứng kỹ thuật và hạn chế, không tạo bảng trạng thái phân công song song. Không đánh dấu hoàn thành trong workbook từ việc file code tồn tại.

## Kiểm kê

| Hạng mục | Bằng chứng | Trạng thái xác minh |
| --- | --- | --- |
| Hạ tầng | `docker-compose.yml`, `.env.example`, `Dockerfile` | Có cấu hình; chưa kiểm tra services/runtime |
| Bucket và naming | `scripts/init_minio.py`, MinIO init service, `docs/NAMING_CONVENTIONS.md` | Có script/quy ước; chưa xác minh bucket live |
| Open-Meteo → Bronze | `scripts/ingest_open_meteo.py`, `dags/dag_ingest_open_meteo.py` | Có code 2016–2025 × 8 điểm; chưa kiểm tra data coverage |
| Connectivity | `dags/weather_pipeline_health_dag.py` | Có DAG kiểm tra kết nối/schema cơ bản; không chứng minh data completeness |
| Silver/Gold | Hợp đồng trong docs; pytest dùng fixture giả lập | Chưa có ETL thực; script upload demo cũ đã bỏ ngày 09/10/2026 |
| DW | `sql/init_dwh_schema.sql`, init shell | Có schema/seed một số Dim; chưa xác minh live DB/loader |
| ERA5, Meteostat/NOAA, GeoJSON thật | Chưa tìm thấy implementation/artifact thực trong phần kiểm kê | Cần triển khai hoặc bổ sung bằng chứng |
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
| Gold sample cũ thiếu timestamp, dùng giờ local, có label tương lai | Script demo đã bỏ; Gold loader thực vẫn chưa có | Chốt calendar basis và Gold contract khi triển khai ETL thực |
| DDL thiếu DimDate seed, DimModel, prediction unique/origin/window | Chưa đủ nạp/đối soát dự báo | Thiết kế migration/loader sau khi chốt hợp đồng |
| Compose trước đây không build Dockerfile; dependency runtime cài riêng | Đã xử lý cho image Airflow ngày 09/10/2026 | Build, `pip check` và smoke test đã đạt; xem bằng chứng bên dưới |
| Init dùng `weather_dwh` cố định dù có `DWH_DB` | Đổi env name có thể lệch database khởi tạo | Đồng bộ cấu hình/init trước hỗ trợ tên khác |
| Health check dùng bucket names cố định | Cấu hình bucket tùy chỉnh chưa được kiểm tra tương ứng | Đồng bộ checks với env khi sửa hạ tầng |
| Fixture GeoJSON là Point giả lập | Không phải ranh giới hành chính thật | Thu thập và version hóa GeoJSON thực |
| Nhãn, fallback, coverage và chọn trạm chưa chốt | ETL/ML có thể tích hợp sai ngữ nghĩa | Chốt các mục mở trong data contract, schema và rain label |

## Kết quả lượt tổ chức tài liệu

- Giữ nguyên code, cấu hình runtime, DDL, workbook phân công và 5 tên weather skill.
- Thêm AGENTS gốc, hướng dẫn trong `scripts/`, `dags/`, `sql/`; tài liệu kiến trúc, hợp đồng, schema, nhãn và tiến độ.
- Cập nhật README/naming và skill để trỏ đến tài liệu chính thức, phân biệt thiết kế với implementation.
- Kiểm tra hoàn tất: 11 file Markdown dự án với 23 liên kết nội bộ hợp lệ và code fence đóng đủ; cấu trúc/frontmatter, UI metadata và document references của 5 weather skill hợp lệ. Chỉ kiểm chứng tài liệu; các kiểm thử runtime bên trên vẫn chưa thực hiện.

## Cách cập nhật

Sau công việc kỹ thuật, ghi phạm vi đã chạy, số file/dòng/khóa, window, lệnh hoặc artifact làm bằng chứng, kết quả và hạn chế. Phân biệt đã viết → đã kiểm tra sample → đã chạy toàn phạm vi. Người phụ trách và trạng thái giao việc cập nhật ở workbook khi có đủ bằng chứng.

## Setup môi trường Airflow — 09/10/2026

- Giữ Airflow 2.9.3 và Python 3.10; khóa base image Dockerfile bằng SHA-256. Compose build image `weather-airflow:2.9.3-python3.10` dùng chung cho init/webserver/scheduler, bỏ cài thư viện lúc khởi động. `AIRFLOW_IMAGE_NAME` trong env cũ không còn được dùng.
- Thêm `requirements.in`, constraints chính thức của Airflow 2.9.3/Python 3.10 và lock `requirements.txt` gồm **183 package pins**, có SHA-256 cho mọi package. Resolver: uv 0.8.22, target Python 3.10/Linux x86_64. Đồng bộ SQLAlchemy 1.4.52 và s3fs 2023.12.2 theo dependency của Airflow. Build đầu phát hiện setuptools mới không tương thích `gcloud-aio-auth` trong base image; đã khóa setuptools 66.1.1 và build lại thành công.
- Thêm `.python-version`, `.dockerignore`; cập nhật `.env.example`, README, architecture và [hướng dẫn môi trường](development.md). Không chỉnh workbook, SQL hoặc pipeline.

### Kiểm tra thực sự đã chạy

| Kiểm tra | Kết quả và phạm vi |
| --- | --- |
| `docker compose --env-file .env.example config --quiet` và `docker compose config --quiet` | Đạt với env mẫu và env cục bộ hiện có; không xuất giá trị secrets |
| Kiểm tra Compose JSON / lockfile | 3 service dùng chung image/build, không còn `_PIP_ADDITIONAL_REQUIREMENTS`; 183 pins có hash và khớp dependency trực tiếp |
| `docker compose --env-file .env.example build airflow-webserver` | Đạt trên Docker Desktop Linux/amd64; `pip install --require-hashes` và `pip check` hoàn tất, output `No broken requirements found.` |
| Container tạm `docker run --rm -i --network none ... --entrypoint python` | Đạt: Python 3.10, Airflow 2.9.3, SQLAlchemy 1.4.52; import các thư viện và provider sử dụng |
| Smoke dữ liệu giả lập trong `/tmp` của container | 3 dòng Parquet UTC round-trip bằng pyarrow và fastparquet; 3 giá trị NetCDF round-trip; 1 GeoJSON Point có CRS; CART fit 4 mẫu và kiểm tra 2 dự đoán; đều đạt |
| Airflow `DagBag` trên mount DAG read-only | 2 DAG hiện có import được, không có import error; không chạy task |
| Markdown và diff | Kiểm tra liên kết nội bộ/code fences của README, architecture, development, progress; `git diff --check` đạt |

Docker Desktop đã được mở để build. Không chạy `compose up`, không khởi tạo database/bucket, không gọi API thời tiết, không tải lịch sử, không nạp DW hoặc đánh giá mô hình thực. Smoke test chỉ chứng minh thư viện hoạt động trên mẫu giả lập; không chứng minh pipeline/coverage/runtime của các service. Log kiểm tra lưu tạm tại `/private/tmp/weather-airflow-build.log` và `/private/tmp/weather-runtime-smoke.log`, không là artifact dữ liệu chính thức.

Giới hạn còn lại: gói apt chưa khóa bằng snapshot; image MinIO/PostgreSQL giữ cấu hình trước; chưa kiểm chứng Linux/arm64 hoặc Python native trên macOS/Windows. CI, pytest, Ruff và pre-commit là các bước tiếp theo, chưa được triển khai trong lượt này.

## Dọn cấu trúc — 09/10/2026

- Xóa `sql/01_init_databases.sql`: file không được Compose mount hoặc code gọi, trùng bước tạo database đã có trong `sql/01_init_postgres.sh`. Cập nhật cây thư mục README; giữ nguyên initializer đang được sử dụng và schema.
- Xóa `.DS_Store` tại root và 2 file bytecode trong `dags/__pycache__/`; đây là cache có thể tạo lại.
- Thêm `~$*` vào `.gitignore` để loại file lock Office khỏi Git. File lock workbook đang được tiến trình Excel mở theo `lsof`, nên không xóa file đang sử dụng.
- Giữ `config/`, `plugins/`, `data/` vì đang có mount trong Compose; giữ log phục vụ truy vết, tài liệu đề tài/phân công và các file dependency/constraints cần cho build hoặc tạo lại lock.
- Kiểm tra: tìm tham chiếu tới SQL đã xóa, kiểm tra các bind mount nguồn còn tồn tại, `docker compose config --quiet`, liên kết Markdown/code fences và `git diff --check`. Không chạy SQL, khởi động services hoặc tác động dữ liệu thời tiết.

## Ruff, pytest, CI và onboarding — 09/10/2026

- Thêm `pyproject.toml` cho Ruff/pytest, `.pre-commit-config.yaml`, `requirements-dev.in`/lock có hash, Makefile, `.github/workflows/ci.yml`, Compose test và 5 file Python trong `tests/` cùng fixture JSON.
- Dockerfile có target `runtime`/`test`; Compose chính chỉ build `runtime`, target `test` cài dev tools và copy code/fixture. Dev lock constrain các dependency dùng chung bởi runtime lock. `.dockerignore` giữ allowlist, không gửi `.env`, log hoặc dữ liệu vào builder.
- Ruff: E4/E7/E9/F/I và format; giới hạn `scripts/`, `dags/`, `tests/`. Chỉ format/sắp xếp import và bỏ import không dùng trong code hiện có, không sửa hợp đồng hay logic ETL. Hook đã cài vào `.git/hooks/pre-commit` của checkout này; thành viên khác chạy `make setup`/`make hooks`.
- Pytest chỉ collect `tests/`, yêu cầu `WEATHER_TESTING=1`. Unit test mock API/MinIO/subprocess và chặn socket. Test offline dùng `--network none`; integration dùng project `weather-tests`, DB `weather_test`, MinIO riêng, tmpfs, không host port/bind mount/production volume và network `internal`. Makefile tự dọn stack, giữ mã exit của test và in log khi lỗi.
- Workflow pull request/push main/manual chạy setup, Ruff, pre-commit, Compose config, test offline và integration; quyền `contents: read`, không secrets production hoặc deploy. Đã kiểm tra cấu hình local, **chưa push hoặc chạy workflow trên GitHub**; branch protection chưa được bật.
- Cập nhật README, architecture, scoped DAG instructions và [hướng dẫn thành viên mới](development.md).

### Bằng chứng kiểm tra

| Lệnh / kiểm tra | Kết quả |
| --- | --- |
| `make setup` | Đạt; tooling venv trên host Python 3.12.4; pytest 8.3.5, Ruff 0.11.13, pre-commit 4.2.0 cùng dependency khóa/hash |
| `make lint`, pre-commit validate-config, hooks trên file code/test và `make precommit` | Đạt, 10 file Python đã format; Ruff lint/format hooks đạt |
| `make config-check` | Compose development và Compose test parse hợp lệ |
| `make test` | **15 passed**, 6 integration deselected; container Linux/amd64, Python **3.10.14**; build test image và `pip check` đạt |
| `make test-integration` | **6 passed**, 15 offline deselected; object Bronze rerun cùng key, MinIO health, schema/seed rerun, location uniqueness, station FK, PostgreSQL health đều đạt |
| Cleanup stack test | Make tự xóa 3 container và network test; PostgreSQL/MinIO dùng tmpfs, không tạo volume dữ liệu lâu dài |
| `make build` và kiểm tra metadata/config Docker | Target runtime build đạt, giữ entrypoint Airflow và không có `WEATHER_TESTING`; xác nhận không còn container test, cấu hình test không có port/volume và network nội bộ |
| Python AST, YAML workflow/hook, fixture JSON, Markdown links/fences, `git diff --check` | Đạt; không chạy code qua syntax check |

Lượt integration đầu có 5 passed/1 failed do fixture duplicate location thiếu `province` NOT NULL nên chưa chạm unique constraint. Đã bổ sung cột bắt buộc trong fixture và chạy lại: 6 passed. Schema production không thay đổi.

Phạm vi dữ liệu: 3 giờ **giả lập** quanh biên 29/02–01/03/2024, 1 GeoJSON Point, 4 mẫu CART thư viện; integration gọi ingestion cho HAN/2024 nhưng API được mock chỉ trả 3 giờ, **không tải dữ liệu cả năm**. Nạp schema/seed chỉ vào DB test; không nạp fact thời tiết hoặc mô hình thật. Rerun Bronze kiểm tra số object, không chứng minh coverage hoặc upsert DW.

Giới hạn: test offline có 5 warnings — 4 deprecation warnings từ `schedule_interval` trên Airflow 2.9.3 và 1 native warning `numpy.ndarray size changed` khi mở NetCDF. Round-trip NetCDF nhỏ đạt nhưng warning này chưa được chẩn đoán đầy đủ; không ẩn warning trong pytest. Các lỗi ingestion/provenance/size-skip đã ghi ở trên vẫn còn; GitHub runner, Windows/arm64 và dữ liệu thật chưa được xác minh. Kết quả local không chứng minh hoàn tất ETL, backfill, DW hoặc độ chính xác dự báo.

## Tinh gọn theo phạm vi dự án — 09/10/2026

- CI giữ `make lint` cho Ruff lint/format, thay bước chạy lại `make precommit` bằng validate-config. Hook pre-commit local và lệnh Makefile vẫn được giữ.
- Bỏ `fastparquet` khỏi dependency trực tiếp; tạo lại runtime/dev lock bằng uv 0.8.22. Runtime lock giảm **183 → 181 package pins**, bỏ cả `cramjam`. Test Parquet giữ kiểm tra round-trip UTC/null bằng `pyarrow`.
- Bỏ bước apt bổ sung trong Dockerfile, gồm compiler, các gói `*-dev` và GDAL CLI. Base image khóa digest đã có `curl`; dependency Python cài wheels với hash và `--only-binary=:all:`. Target runtime/test vẫn giữ nguyên tách biệt.
- Xóa script demo `scripts/test_minio_upload.py`; thay hướng dẫn upload bằng `make test-integration`, cập nhật scoped instructions, README, architecture và development. Không xóa object/dữ liệu từng được script demo tạo trong các bucket cũ.
- Xóa skill React/Next.js trong **project** và 3 thư mục skill trống Prisma/tuyển dụng. Giữ `ui-ux-pro-max`, Power BI, PostgreSQL và 4 weather skill còn lại; không thay đổi skill toàn cục.

### Kiểm chứng sau tinh gọn

- `make check` hoàn tất: Ruff lint/format đạt (9 file Python), hai Compose config hợp lệ; **15 offline tests + 6 integration tests đạt**. Runtime và test stage build không cần apt/compiler bổ sung; cả hai `pip check` đạt.
- NetCDF, GeoJSON và Parquet round-trip nhỏ vẫn đạt; unit API/DAG và integration Bronze/schema/seed/FK/uniqueness/health vẫn đạt. 5 warnings đã ghi ở lượt trước vẫn còn, không được ẩn hoặc coi là đã xử lý.
- `make build` tạo lại tag runtime; container không mạng xác nhận `curl` sẵn có và `fastparquet`/`cramjam` không còn trong runtime. Stack test đã tự dọn, không còn container test.
- Validate pre-commit config, Python AST, hash trong hai lockfile, liên kết Markdown/code fences và `git diff --check` đạt. Không còn tham chiếu đang dùng tới script demo hoặc các skill đã xóa; tên cũ chỉ xuất hiện trong lịch sử/bằng chứng này.

Phạm vi dữ liệu vẫn là fixture giả lập 3 giờ quanh 29/02–01/03/2024, 1 GeoJSON Point và 4 mẫu CART; DB/MinIO integration tạm, không tải lịch sử hoặc nạp DW phát triển. Chưa chạy workflow trên GitHub và chưa kiểm chứng arm64/Windows. Image runtime cũ không còn trong Docker cache ở đầu lượt này nên không có số đo dung lượng trước/sau để công bố phần trăm giảm; xác minh việc bỏ apt dựa trên Dockerfile/build/tests thực tế.

## Kiểm tra lại — 10/10/2026

Kiểm tra checkout tại commit `2f57bc1`, không sửa code hoặc schema. Docker Desktop ban đầu chưa chạy; đã mở ứng dụng và chạy lại bộ kiểm tra qua Docker.

| Kiểm tra thực tế | Kết quả |
| --- | --- |
| `make check` | Exit 0: Ruff lint/format đạt cho 9 file Python; Compose development/test hợp lệ; build target test đạt |
| Test offline, container `--network none` | **15 passed**, 6 integration deselected, 9,37 giây; Python 3.10.14, pytest 8.3.5 |
| Test integration trên stack tạm | **6 passed**, 15 offline deselected, 5,35 giây; upload Bronze/rerun, health tasks, schema/seed rerun, uniqueness và FK đạt |
| Cleanup | Make dọn 3 container và network test; kiểm tra theo label project `weather-tests` không còn container/network |
| `make build` và container runtime không mạng | Build đạt, sử dụng cache; Python 3.10.14, Airflow 2.9.3, `pip check`: `No broken requirements found.`, có `/usr/bin/curl` |
| Kiểm tra tĩnh | Pre-commit validate-config, Python AST, YAML workflow/hook, JSON fixture, `bash -n` initializer PostgreSQL, liên kết Markdown/code fences và `git diff --check` đạt |

Test offline vẫn có **5 warnings**: 4 deprecation warnings từ `schedule_interval` và 1 warning `numpy.ndarray size changed` khi mở NetCDF; chưa xử lý hoặc chẩn đoán thêm. Các test đạt không xóa bỏ những hạn chế ingestion đã ghi ở trên.

Phạm vi: fixture 3 giờ giả lập quanh 29/02–01/03/2024, 1 GeoJSON Point và 4 mẫu CART; API Open-Meteo được mock, PostgreSQL/MinIO integration dùng tmpfs riêng. Không khởi động stack phát triển, không tải dữ liệu thật, không nạp DW phát triển hoặc chạy DAG lịch sử. Chưa xác nhận toàn bộ stack phát triển chạy cùng nhau, pipeline đầu cuối, GitHub Actions, arm64/Windows hoặc độ chính xác dự báo; Silver/Gold, loader DW và artifact ML/BI còn thiếu. Log tạm: `/private/tmp/weather-recheck-20261010.log` và `/private/tmp/weather-runtime-recheck-20261010.log`.

## Rà soát bất thường và thiếu hụt — 10/10/2026

Đối chiếu code ingestion, Compose, DDL, test và hợp đồng domain; không thay đổi code/schema hoặc chạy dữ liệu thật.

- **Ưu tiên cao — init Airflow có thể báo thành công khi migration lỗi:** chuỗi `airflow db migrate && ... && airflow users create ... || true && ...` trong Compose áp dụng `|| true` cho cả chuỗi trước nó. Mô phỏng cùng cấu trúc bằng `bash -c` với bước đầu `false` vẫn in thông báo thành công và exit 0. Đây là kiểm chứng ngữ nghĩa shell, chưa cố tình làm migration Airflow thật thất bại. Test hiện tại không chạy entrypoint này.
- **Init MinIO chưa bảo đảm báo lỗi:** vòng `until mc alias set` không có timeout; các lệnh tạo bucket không có fail-fast và kết thúc bằng `exit 0`. Lỗi tạo bucket có thể bị che. Nhận định từ code, chưa gây lỗi runtime để kiểm chứng.
- Các lỗi ingestion vẫn tồn tại: nuốt lỗi từng file, skip bằng kích thước, thiếu kiểm tra độ dài/schema/coverage của response, thay đổi raw payload Bronze. Bộ test hiện tại chưa bảo đảm batch thất bại được DAG ghi nhận; test skip đang xác nhận chính hành vi size-rule cũ.
- Cấu hình tùy chỉnh chưa nhất quán: initializer cố định `weather_dwh`, health task cố định tên 3 bucket; MinIO runtime dùng tag `latest` trong khi integration dùng digest. Integration chưa xác minh toàn bộ entrypoint và image của Compose phát triển.
- DW chưa seed DimDate hoặc có loader/OLAP; unique observation dùng date/time keys với calendar basis chưa chốt; prediction chưa có khóa rerun và origin/target window UTC. Hợp đồng nhãn mưa, coverage, nguồn fallback và chọn trạm vẫn là các quyết định mở.
- Chưa có collector ERA5/trạm/địa giới thật, ETL Silver/Gold, feature/label pipeline, artifact CART hoặc Power BI. 21 test đạt và 5 warnings đã ghi ở trên chỉ bao phủ phần triển khai hiện có.

Thứ tự đề xuất: sửa failure status của init/ingestion và bổ sung test lỗi → chốt hợp đồng còn mở → hoàn thiện một lát cắt Open-Meteo/Bronze/Silver/Gold/DW trên cửa sổ nhỏ, kiểm tra rerun → mở rộng đa nguồn, OLAP/BI và CART. Đây là đề xuất ưu tiên kỹ thuật, không thay đổi phân công trong workbook.
