# Progress and Evidence

Inventory dated **2026-10-08** (Vietnam time), based on repository code/configuration/documents. That inventory did not run Docker, APIs, backfills, live SQL, or model training, so runtime was unverified at that point. Later dated entries record subsequent verification.

Workbook `Phan_cong_cong_viec_DW_khi_tuong.xlsx` owns the 40 task assignments and responsible members. This document records technical evidence and limitations, not a parallel assignment-status tracker. Code file existence alone must not mark a workbook task complete.

## Inventory

| Item | Evidence | Verification status |
| --- | --- | --- |
| Infrastructure | `docker-compose.yml`, `.env.example`, `Dockerfile` | Configuration exists; services/runtime were not checked |
| Buckets and naming | `scripts/init_minio.py`, MinIO init service, `docs/NAMING_CONVENTIONS.md` | Scripts/conventions exist; live buckets unverified |
| Open-Meteo → Bronze | `scripts/ingest_open_meteo.py`, `dags/dag_ingest_open_meteo.py` | Code covers 2016–2025 × eight points; data coverage unverified |
| Connectivity | `dags/weather_pipeline_health_dag.py` | Basic connection/schema DAG; not evidence of data completeness |
| Silver/Gold | Documented contracts and synthetic pytest fixtures | No real ETL; old upload demo removed on 2026-10-09 |
| DW | `sql/init_dwh_schema.sql`, init shell | Schema and some dimension seeds exist; live DB/loader unverified |
| ERA5, Meteostat/NOAA, real GeoJSON | No real implementation/artifacts found during inventory | Implementation or additional evidence needed |
| OLAP, Power BI, CART, forecast integration | No real views/models/PBIX found | Implementation or additional evidence needed |
| AI instructions | Root/scoped AGENTS, six domain documents, five weather skills | Authored; document checks below |

## Discrepancies and follow-up work

| Issue | Impact | Next corrective step |
| --- | --- | --- |
| Open-Meteo adds `_pipeline_metadata` and reserializes Bronze JSON | Original-payload preservation target not met | Separate raw response and metadata/manifests |
| Ingestion catches per-file errors, prints them, and continues | Subprocess may exit 0 despite missing files; DAG can report incomplete success | Collect failures and raise after the batch; verify resume/reruns |
| Skipping based on object size >2000 | Schema/checksum/coverage unverified | Use manifests and content/scope checks |
| Retries every non-200 response without `Retry-After` | Retries configuration errors and obscures error meaning | Classify transient/permanent failures and bound retries |
| Actual Bronze is annual; naming examples are monthly | Readers may confuse key scope | Both supported cases documented; current code keys preserved |
| Old Gold sample lacked timestamps, used local time, and included future labels | Demo removed; real Gold loader still missing | Agree calendar basis and Gold contract when implementing real ETL |
| DDL lacks DimDate seeds, DimModel, prediction uniqueness/origin/window | Incomplete prediction loading/reconciliation support | Design migrations/loaders after contract decisions |
| Compose formerly bypassed Dockerfile; runtime dependencies installed separately | Addressed for Airflow image on 2026-10-09 | Build, `pip check`, and smoke tests passed; see below |
| Init fixes `weather_dwh` despite configurable `DWH_DB` | Environment changes may target a different initialized database | Align configuration/init before supporting alternative names |
| Healthcheck fixes bucket names | Custom bucket configuration not checked consistently | Align checks with environment when updating infrastructure |
| GeoJSON fixture is a synthetic Point | Not real administrative boundaries | Acquire and version real GeoJSON |
| Labels, fallback, coverage, and station selection unresolved | ETL/ML integration may use inconsistent semantics | Resolve open data-contract/schema/label decisions |

## Documentation organization results

- Preserved code, runtime configuration, DDL, allocation workbook, and five weather skill names.
- Added root AGENTS and scoped instructions in `scripts/`, `dags/`, `sql/`; architecture, contract, schema, label, and progress documents.
- Updated README/naming and skills to reference authoritative documents and distinguish design from implementation.
- Verified 11 project Markdown files with 23 valid internal links and balanced fences; structure/frontmatter, UI metadata, and references for five weather skills passed. This verified documents only; runtime checks had not yet been performed.

## Updating this log

After technical work, record executed scope, file/row/key counts, windows, commands/artifacts, results, and limitations. Distinguish authored → sample-verified → full-scope-executed. Update workbook assignment/status fields only with sufficient evidence and within the authorized workbook scope.

## Airflow environment setup — 2026-10-09

- Retained Airflow 2.9.3 and Python 3.10; pinned the Dockerfile base image by SHA-256. Compose builds `weather-airflow:2.9.3-python3.10` for init/webserver/scheduler, removing startup dependency installation. Older environment variable `AIRFLOW_IMAGE_NAME` is no longer used.
- Added `requirements.in`, official Airflow 2.9.3/Python 3.10 constraints, and a `requirements.txt` lock with **183 package pins**, each with SHA-256 hashes. Resolver: uv 0.8.22, Python 3.10/Linux x86_64. Aligned SQLAlchemy 1.4.52 and s3fs 2023.12.2 with Airflow. Initial build found newer setuptools incompatible with bundled `gcloud-aio-auth`; pinned setuptools 66.1.1 and rebuilt successfully.
- Added `.python-version`, `.dockerignore`; updated `.env.example`, README, architecture, and [environment guidance](development.md). No workbook, SQL, or pipeline edits.

### Checks actually executed

| Check | Result and scope |
| --- | --- |
| `docker compose --env-file .env.example config --quiet` and `docker compose config --quiet` | Passed using sample and existing local environments; no secret values printed |
| Compose JSON / lockfile inspection | Three services share image/build; `_PIP_ADDITIONAL_REQUIREMENTS` removed; 183 hashed pins match direct dependencies |
| `docker compose --env-file .env.example build airflow-webserver` | Passed on Docker Desktop Linux/amd64; `pip install --require-hashes` and `pip check` completed with `No broken requirements found.` |
| Temporary `docker run --rm -i --network none ... --entrypoint python` container | Passed: Python 3.10, Airflow 2.9.3, SQLAlchemy 1.4.52; required libraries/providers import |
| Synthetic smoke checks in container `/tmp` | Three UTC Parquet rows round-trip through pyarrow and fastparquet; three NetCDF values; one GeoJSON Point with CRS; CART fit on four samples and two predictions; all passed |
| Airflow `DagBag` on read-only DAG mount | Both existing DAGs imported without errors; tasks not executed |
| Markdown/diff | README/architecture/development/progress links and fences validated; `git diff --check` passed |

Docker Desktop was opened for builds. No `compose up`, database/bucket initialization, weather API calls, historical downloads, DW loading, or real-model evaluation. Smoke tests establish library behavior on synthetic samples, not pipeline/coverage/service runtime. Temporary logs: `/private/tmp/weather-airflow-build.log` and `/private/tmp/weather-runtime-smoke.log`; not official data artifacts.

Limits at this stage: apt packages not snapshot-pinned; MinIO/PostgreSQL image configuration preserved; Linux/arm64 and native macOS/Windows Python unverified. CI, pytest, Ruff, and pre-commit remained future work at this entry's date.

## Structure cleanup — 2026-10-09

- Removed `sql/01_init_databases.sql`: not mounted by Compose or invoked by code, duplicating database creation in `sql/01_init_postgres.sh`. Updated README tree; retained active initializer and schema.
- Removed root `.DS_Store` and two bytecode files in `dags/__pycache__/`; these are reproducible caches.
- Added `~$*` to `.gitignore` to exclude Office lock files. `lsof` showed the workbook lock held by Excel, so the active lock file was not deleted.
- Kept `config/`, `plugins/`, `data/` because Compose mounts them; kept trace logs, proposal/allocation sources, and dependencies/constraints needed for builds and lock regeneration.
- Checked references to removed SQL, existence of bind-mount sources, `docker compose config --quiet`, Markdown links/fences, and `git diff --check`. No SQL execution, service startup, or weather-data changes.

## Ruff, pytest, CI, and onboarding — 2026-10-09

- Added Ruff/pytest `pyproject.toml`, `.pre-commit-config.yaml`, hashed `requirements-dev.in`/lock, Makefile, `.github/workflows/ci.yml`, test Compose, five Python files in `tests/`, and a JSON fixture.
- Dockerfile has `runtime`/`test` targets. Main Compose builds only `runtime`; `test` adds tooling and code/fixtures. Shared development dependencies are constrained by the runtime lock. `.dockerignore` is an allowlist excluding `.env`, logs, and data from builders.
- Ruff uses E4/E7/E9/F/I and formatting for `scripts/`, `dags/`, `tests/`. Only formatting/import ordering/unused-import removal affected existing code; no ETL logic or contract changes. Hooks installed in this checkout's `.git/hooks/pre-commit`; others run `make setup`/`make hooks`.
- Pytest collects only `tests/` and requires `WEATHER_TESTING=1`. Unit tests mock API/MinIO/subprocess and block sockets. Offline tests use `--network none`. Integration uses isolated project `weather-tests`, DB `weather_test`, separate MinIO, tmpfs, no host ports/bind mounts/production volumes, and an `internal` network. Makefile cleans up, preserves test exit status, and prints logs on failure.
- Pull-request/main-push/manual workflow runs setup, Ruff, pre-commit, Compose configuration, offline tests, and integration with `contents: read`; no production secrets or deployment. Local configuration verified; **not yet pushed or executed on GitHub** at this stage; branch protection was recorded as not enabled at this stage.
- Updated README, architecture, scoped DAG instructions, and [onboarding guidance](development.md).

### Verification evidence

| Command/check | Result |
| --- | --- |
| `make setup` | Passed; host tooling Python 3.12.4; pytest 8.3.5, Ruff 0.11.13, pre-commit 4.2.0 and hashed/locked dependencies |
| `make lint`, pre-commit validate-config, code/test hooks, and `make precommit` | Passed; ten Python files formatted; Ruff lint/format hooks passed |
| `make config-check` | Development and test Compose parsed successfully |
| `make test` | **15 passed**, six integration tests deselected; Linux/amd64 container, Python **3.10.14**; test image build and `pip check` passed |
| `make test-integration` | **6 passed**, 15 offline tests deselected; same-key Bronze rerun, MinIO health, schema/seed rerun, location uniqueness, station FK, and PostgreSQL health passed |
| Test cleanup | Make removed three containers and the test network; PostgreSQL/MinIO tmpfs created no persistent data volumes |
| `make build` and Docker metadata/config inspection | Runtime build passed, Airflow entrypoint preserved, no `WEATHER_TESTING`; no remaining test containers, test ports/volumes, or external network |
| Python AST, workflow/hook YAML, fixture JSON, Markdown links/fences, `git diff --check` | Passed; syntax checks did not execute application code |

The first integration run had 5 passed/1 failed: the duplicate-location fixture lacked NOT NULL `province`, so it did not reach the unique constraint. Added required fixture data and reran: 6 passed. Production schema unchanged.

Data scope: **three synthetic hours** across 2024-02-29/2024-03-01, one GeoJSON Point, and four CART library samples. Integration invoked HAN/2024 ingestion, but the mocked API returned only three hours; **no full-year download**. Schema/seeds loaded only into test DB; no weather facts or real model. Bronze rerun checked object counts, not coverage or DW upserts.

Limits: five offline warnings — four `schedule_interval` deprecations on Airflow 2.9.3 and one native `numpy.ndarray size changed` warning opening NetCDF. Small NetCDF round-trip passed; the warning was not fully diagnosed or hidden. Ingestion/provenance/size-skip limitations remain. GitHub runner, Windows/arm64, and real data unverified. Local results do not establish complete ETL/backfill/DW or forecast accuracy.

## Simplification within project scope — 2026-10-09

- CI retains `make lint` for Ruff lint/format and replaces duplicate `make precommit` execution with validate-config. Local hooks and Makefile commands remain.
- Removed direct `fastparquet` dependency; regenerated runtime/development locks using uv 0.8.22. Runtime pins decreased **183 → 181**, removing `cramjam` as well. Parquet tests retain UTC/null round-trip checks using `pyarrow`.
- Removed additional apt installation, including compilers, `*-dev` packages, and GDAL CLI. The digest-pinned base already contains `curl`; Python dependencies install hashed wheels with `--only-binary=:all:`. Runtime/test targets remain separate.
- Removed demo `scripts/test_minio_upload.py`; replaced upload instructions with `make test-integration` and updated scoped instructions, README, architecture, and development. Existing bucket objects created by that demo were not deleted.
- Removed the **project-local** React/Next.js skill and three empty Prisma/recruitment skill directories. Kept `ui-ux-pro-max`, Power BI, PostgreSQL, and four other weather skills; global skills unchanged.

### Verification after simplification

- `make check` completed: Ruff lint/format passed for nine Python files; both Compose configurations valid; **15 offline + 6 integration tests passed**. Runtime/test stages built without additional apt/compiler installation; both `pip check` checks passed.
- Small NetCDF/GeoJSON/Parquet round-trips, API/DAG units, and Bronze/schema/seed/FK/uniqueness/health integration passed. The five earlier warnings remain; they were not suppressed or considered resolved.
- `make build` recreated runtime tag. A networkless container confirmed `curl` exists and `fastparquet`/`cramjam` are absent. Test stack cleaned itself up; no test containers remained.
- Pre-commit config, Python AST, hashes in both lockfiles, Markdown links/fences, and `git diff --check` passed. No active references to removed demos/skills remain; old names appear only in history/evidence.

Data scope remains the three-hour synthetic leap-day/month-boundary fixture, one GeoJSON Point, and four CART samples; temporary integration DB/MinIO, no historical download or development DW loading. GitHub workflow and arm64/Windows unverified. The older runtime image was absent from Docker cache at the start, so no before/after image-size reduction percentage is reported. Apt removal was verified through actual Dockerfile/build/tests.

## Recheck — 2026-10-10

Verified checkout at commit `2f57bc1` without code/schema changes. Docker Desktop was initially stopped; opened it and reran checks through Docker.

| Actual check | Result |
| --- | --- |
| `make check` | Exit 0: Ruff lint/format passed for nine Python files; development/test Compose valid; test target built |
| Offline tests in `--network none` container | **15 passed**, six integration tests deselected, 9.37 seconds; Python 3.10.14, pytest 8.3.5 |
| Integration in temporary stack | **6 passed**, 15 offline tests deselected, 5.35 seconds; Bronze upload/rerun, health tasks, schema/seed rerun, uniqueness/FK passed |
| Cleanup | Make removed three test containers/network; filtering project `weather-tests` found no remaining containers/networks |
| `make build` and networkless runtime container | Cached build passed; Python 3.10.14, Airflow 2.9.3, `pip check`: `No broken requirements found.`, `/usr/bin/curl` present |
| Static checks | Pre-commit config, Python AST, workflow/hook YAML, fixture JSON, PostgreSQL initializer `bash -n`, Markdown links/fences, and `git diff --check` passed |

Offline tests still report **five warnings**: four `schedule_interval` deprecations and one `numpy.ndarray size changed` when opening NetCDF. No additional fix/diagnosis. Passing tests do not eliminate the ingestion limitations above.

Scope: three synthetic hours across 2024-02-29/2024-03-01, one GeoJSON Point, four CART samples; mocked Open-Meteo API and separate integration tmpfs PostgreSQL/MinIO. No development stack startup, real download, development DW loading, or historical DAG runs. Full development-stack operation, end-to-end pipeline, GitHub Actions, arm64/Windows, and forecast accuracy remain unverified; Silver/Gold, DW loader, and ML/BI artifacts remain missing. Temporary logs: `/private/tmp/weather-recheck-20261010.log` and `/private/tmp/weather-runtime-recheck-20261010.log`.

## Anomaly and gap review — 2026-10-10

Compared ingestion, Compose, DDL, tests, and domain contracts without code/schema changes or real-data processing.

- **High priority: Airflow init may report success after migration failure.** Compose chain `airflow db migrate && ... && airflow users create ... || true && ...` applies `|| true` to the preceding chain. A matching `bash -c` simulation beginning with `false` still printed success and exited 0. This verifies shell semantics, not a deliberately failed real Airflow migration. Tests do not execute that entrypoint.
- **MinIO init does not guarantee failure reporting.** `until mc alias set` has no timeout; bucket-creation commands lack fail-fast and end with `exit 0`. Bucket errors may be masked. Static finding; no induced runtime failure test.
- Ingestion still masks per-file failures, skips by size, lacks response length/schema/coverage validation, and modifies raw Bronze payloads. Existing tests do not establish that batch failures reach DAG status; the skip test confirms the current size rule.
- Custom configuration is inconsistent: initializer fixes `weather_dwh`, health tasks fix three bucket names; runtime MinIO uses `latest` while integration uses a digest. Integration does not verify all development Compose entrypoints/images.
- DW lacks DimDate seeds/loaders/OLAP. Observation uniqueness uses date/time keys with unresolved calendar basis; predictions lack rerun keys and UTC origin/target windows. Label/coverage/fallback/station decisions remain open.
- Real ERA5/station/boundary collectors, Silver/Gold ETL, label/features, CART artifacts, and Power BI are missing. The 21 passing tests and five warnings cover only existing implementation.

Proposed order: fix init/ingestion failure status with failure tests → resolve contracts → implement a small-window Open-Meteo/Bronze/Silver/Gold/DW slice with reruns → expand sources, OLAP/BI, and CART. This is a technical-priority proposal, not an assignment change in the workbook.

## Business documentation and implementation guidance — 2026-10-10

- Assigned scope: study the proposal/task allocation, write business documentation, and establish progress updates after coding. No ETL/DW/ML/BI implementation was performed in this task.
- Sources reviewed: all seven PDF pages, extracted and rendered, including the architecture diagram on page 4; four workbook sheets (`Tổng quan`, `Theo dõi công việc`, `Vai trò`, `Rủi ro`), including 40 task IDs in `Theo dõi công việc!A5:O44`. Read source values/formulas without recalculating the workbook or inferring progress from cached summaries.
- Added [business-requirements.md](business-requirements.md): source references, objectives, scope, exchange products, acceptance criteria, open decisions, and proposal/workbook/code discrepancies.
- Added [implementation-roadmap.md](implementation-roadmap.md): stages across 10 weeks, dependencies, all 40 IDs with acceptance evidence, bounded task prompts, and a progress-entry template.
- Updated `AGENTS.md` and README to route AI to the new documents and require progress updates in the same turn after coding/testing, including partial or failed results.
- Language correction: the initial business documents were written in Vietnamese despite the requested English convention. Both new documents, their templates, this progress entry, and the new README/rain-label additions now use English. `AGENTS.md` requires English for new or changed code and documentation. Preserve proper names, source payloads, workbook sheet names, stable IDs, and existing business values; historical source documents remain unchanged.
- Updated `rain-label.md` for ID 36: week 9 threshold analysis must not turn test into model selection. ID 29 still requires selecting the threshold on validation 2024 before test 2025. No workbook edits or label decisions were approved.
- Acceptance clarification: a verified technical base does not establish completion of every week 1 task. ID 1 requires team-machine setup; ID 4 requires real GeoJSON, ID 5 a CDS request, and IDs 6/7 agreed contracts. Full acceptance evidence is still missing. All 40 source workbook tasks currently say `Chưa bắt đầu`; their management status was preserved.
- Checks: local Markdown links and code fences validated; all 40 roadmap IDs/week ranges matched the workbook; source PDF/workbook SHA-256 values were unchanged; `git diff --check` passed. No new frontmatter was introduced. The English revision retains the same ID mapping and source references.
- Data scope: document/task metadata only. No services, API calls, historical downloads, SQL execution, training, or reruns of the 21 tests. Earlier test results do not verify the proposed business workflow. Acceptance criteria remain unexecuted plans. Week 1 starts on an assumed 2026-10-12 in `Tổng quan!E11`, not a newly confirmed team schedule.
- Next step: the user selects a task ID/increment and scope; resolve dependent week 1 decisions before implementation. These documentation changes are local and have not been committed or pushed in this task.

## Documentation language normalization — 2026-10-10

- Translated the seven existing architecture, data contract, development, naming, progress, rain-label, and warehouse documents into English. All nine Markdown documents in `docs/`, including business requirements and the implementation roadmap, now use English prose.
- Preserved proper location/station names, exact source filenames and workbook sheet/status references, historical evidence, pending decisions, identifiers, commands, and contract semantics. The original proposal PDF and allocation workbook were not changed.
- Checks: English prose scan passed with explicit exceptions for source references and proper names; local Markdown links and code fences passed; original inline references, shell commands, and diagram topology were retained; all 40 roadmap task IDs/week ranges matched the workbook; source PDF/workbook SHA-256 values remained unchanged; `git diff --check` passed. No frontmatter was introduced.
- Scope: documentation only. No executable code, schema, task ownership, source data, services, downloads, or test runs were changed. Existing runtime evidence and implementation limits remain as recorded above. Changes are local and have not been committed or pushed in this task.

## Vietnamese README rewrite — 2026-10-10

- Rewrote `README.md` in Vietnamese at the user's explicit request for this file. Code and the nine domain documents in `docs/` retain the English convention.
- Recorded the README language exception in `AGENTS.md` so future AI tasks preserve this choice.
- Consolidated the verified base status, missing business components, document/source links, setup requirements, Make commands, development endpoints, repository layout, and task-based AI workflow. Removed the outdated first-repository Git initialization instructions and added both new business documents to the directory overview.
- Checks: a Python static check validated 18 local links, balanced code fences, all referenced Make targets, Compose services, DAG identifiers, and inclusion of all nine domain documents. No frontmatter was introduced. `git diff --check` passed.
- Scope: documentation only; no services, test suites, data processing, schema changes, source artifact edits, or workbook status changes. Previously recorded runtime test results were cited as historical evidence, not rerun. Changes remain local and uncommitted.

## Meteostat and Silver ETL Review Remediations — 2026-10-11

- Related work: workbook IDs 9 (`Meteostat/NOAA → Bronze`) and 10 (`API/stations → Silver`) in `Theo dõi công việc!A13:O14`, `docs/data-contract.md` draft v0.1, and `docs/NAMING_CONVENTIONS.md` Table 6.
- Objective/scope: Resolve code review items on Meteostat ingestion, Open-Meteo & Meteostat Silver transformations, deduplication, grid coverage, range validation, station synchronization, pressure type provenance, failure status reporting, and Ruff formatting.
- Disentangling unverified claims: Earlier unverified notes claiming "960 files / 701,376 records / 100% đủ giờ" and "518 files / 375,188 records" are separated from verified evidence. That coverage metric was flawed (inferred from data min/max timestamps rather than research scope), and no ten-year production download was authorized or executed in this turn.
- Changes:
  - Authored `scripts/station_config.py`: Shared station registry across ingest and transform, validating the 8 canonical research locations from Table 6 (`HAN`, `PTO`, `TNG`, `TUQ`, `LCA`, `HBI`, `HPH`, `NBI`). Unmapped regional stations (`48806` Sơn La, `48808` Cao Bằng) are marked unmapped pending team decision, blocking invalid publication or `UNKNOWN` output.
  - Authored `scripts/quality_checks.py`: Chronological UTC hourly grid coverage calculation over explicit `[start, end)` windows, handling leap years (8,784h in 2024 vs 8,760h in 2023) and empty DataFrames; meteorological range validation setting negative precipitation and out-of-range humidity/cloud/wind to `NaN` (no clipping to valid values); order-invariant deduplication separating identical duplicates (deterministic collapse) from conflicting duplicates (quarantine).
  - Updated `scripts/ingest_meteostat.py`: Unified station configuration, distinct HTTP 404 (allowed skip: no reporting) vs S3/network/timeout failures, proper `is_s3_not_found_error()` (ignoring `NoSuchKey` but failing on `AccessDenied`), batch failure aggregation, and exit code 1 on failure for Airflow detection.
  - Updated `scripts/transform_meteostat_to_silver.py`: Unified station config, grain dedup with conflict quarantine, range validation without clipping, explicit sea-level pressure type (`pressure_type = "sea_level"`), provenance retention (`station_id`, `observation_type`), grid coverage over `[start, end)`, error reporting and exit code 1 on failures.
  - Updated `scripts/transform_open_meteo_to_silver.py`: Range validation without clipping, explicit surface pressure type (`pressure_type = "surface"`), conflict quarantine on grain, grid coverage over `[start, end)`, error reporting and exit code 1 on failures.
  - Updated `docs/data-contract.md`: Standard observation columns table updated to record `pressure_type`, `station_id`, and `observation_type`.
  - Authored `tests/test_silver_transformations.py`: 20 unit and small slice tests covering all 7 review scenarios.
- Checks:
  - `python -m ruff check scripts/ tests/`: Passed with 0 errors.
  - `python -m ruff format --check scripts/ tests/`: Passed with 14 files formatted.
  - `WEATHER_TESTING=1 pytest tests/test_ingestion.py tests/test_silver_transformations.py`: **27 passed in 1.20s** (7 ingestion + 20 silver transformation / quality / slice tests).
  - Small-scope mock execution for both Open-Meteo and Meteostat transformers verified Parquet serialization and PyArrow deserialization round-trip.
- Data scope: Small synthetic fixtures and mocks (3 synthetic hours across 2024-02-29/2024-03-01 boundary, mock CSV/JSON payloads, PyArrow table round-trip). No live external weather API calls, no MinIO production writes, and no full 10-year historical backfill.
- Limits: Full historical backfill, DW loading, and real MinIO cluster operations remain unexecuted and unverified.

## Silver Layer Live Overwrite Execution — 2026-10-11

- Related work: workbook ID 10 (`API/stations → Silver`) in `Theo dõi công việc!A14:O14`, `docs/data-contract.md`.
- Objective/scope: Execute full Silver layer overwrite from existing Bronze data in MinIO to apply reviewed quality checks, pressure type distinctions, range validation without clipping, and updated station mappings.
- Execution commands and results:
  - `python scripts/transform_open_meteo_to_silver.py --start-year 2016 --end-year 2025 --overwrite`:
    - 80 Bronze files processed (8 locations x 10 years).
    - 960 monthly Parquet files created under `silver/weather_hourly/`.
    - 701,376 clean hourly records with explicit `pressure_type = "surface"`.
    - DQ report: `silver/quality_reports/dq_report_open_meteo_20261011_010719.json`.
    - 0 failures encountered.
  - `python scripts/transform_meteostat_to_silver.py --start-year 2016 --end-year 2025 --overwrite`:
    - 34 Bronze CSV files processed (only station-years with actual hourly reports; 404 years cleanly skipped).
    - 404 monthly Parquet files created under `silver/weather_hourly/` with explicit `pressure_type = "sea_level"`, `station_id` provenance, and corrected station mappings (Việt Trì `48825` -> `PTO`, Thái Nguyên `48826` -> `TNG`).
    - 292,769 clean hourly observation records.
    - 5,263 missing hours detected across years via chronological grid comparison (e.g. Việt Trì 2017: 5,072 records, 57.9% coverage of 8,760h, 3,688h missing).
    - Cleaned station metadata published: `silver/stations/stations_metadata_cleaned.parquet` (10 stations with status annotations).
    - DQ report: `silver/quality_reports/dq_report_meteostat_20261011_010757.json`.
    - 0 failures encountered.
  - Total Silver objects in MinIO: 1,488 objects (1,479 hourly Parquets, 2 station metadata Parquets, 7 DQ JSON reports).
- Verification: PyArrow sample inspection confirmed schema, UTC timezone, and `pressure_type` integrity across both sources.
- Limits: Fact observation loading into PostgreSQL DWH, Gold aggregation, CART ML datasets, and Power BI dashboards remain unexecuted.

## Gold Observations Curation & Data Warehouse Loading — 2026-10-13

- Related work: workbook IDs 12 (`DimDate/Time/Source`), 13 (`Star Schema DDL`), 16 (`Silver → Gold`), and 18 (`Observation fact loading`) in `Theo dõi công việc!A16:O22`, `docs/warehouse-schema.md`, `docs/data-contract.md`, and `docs/NAMING_CONVENTIONS.md` Table 5.
- Objective/scope: Populate and synchronize Dimension tables in PostgreSQL DWH (`weather_dwh`), export Dimension Parquets to MinIO Gold bucket, curate Silver observations into monthly Star Schema Parquet files under `gold/fact_weather_observation/`, and load into PostgreSQL `fact_weather_observation`. Calendar basis selected per confirmed user decision: Vietnam Local Time (`Asia/Ho_Chi_Minh` UTC+7 / `timestamp_local`) for `date_key` (YYYYMMDD) and `time_key` (HH00). Verified on 2024 scope (leap year, 8 canonical locations) with full idempotent rerun.
- Changes:
  - Authored `scripts/seed_dimensions.py`: Generated 4,018 days (2016–2026) for `dim_date`, seeded 10 stations for `dim_station`, and exported all 5 dimension tables (`dim_date`, `dim_location`, `dim_source`, `dim_station`, `dim_time`) to MinIO `gold/` as snappy Parquet.
  - Authored `scripts/load_gold_observations.py`: Curated Silver hourly data, resolved dimension surrogate keys, mapped Vietnam local time keys (`date_key`, `time_key`), exported consolidated monthly Parquet to `gold/fact_weather_observation/`, and loaded into PostgreSQL `fact_weather_observation` using transactional upserts (`ON CONFLICT (date_key, time_key, location_key, source_key) DO UPDATE`).
  - Authored `tests/test_gold_observations.py`: 4 unit tests verifying calendar boundaries (17:00 UTC -> 00:00 next day local), unmapped location filtering, Parquet round-trip serialization, and SQL type conversion.
  - Normalized line endings to LF in `sql/01_init_postgres.sh`.
- Checks:
  - `python scripts/seed_dimensions.py`: Seeded 4,018 dates, 10 stations, exported 5 dimension Parquets to MinIO.
  - `python scripts/load_gold_observations.py --start-year 2024 --end-year 2024`: 12 months processed in 30.73s; 113,545 records curated to MinIO Gold Parquet and 113,545 rows upserted into PostgreSQL DWH (`fact_weather_observation`). 0 failures.
  - SQL verification:
    - Open-Meteo: 70,272 rows (8 locations x 8,784h leap year), avg temp 24.42 °C.
    - Meteostat: 43,273 rows across 5 active canonical stations, avg temp 24.28 °C.
  - Idempotent rerun verification: Reran `python scripts/load_gold_observations.py --start-year 2024 --end-year 2024 --location HAN`; confirmed total row count in PostgreSQL remained exactly 113,545 with 0 duplicates.
  - `python -m ruff check scripts/ tests/test_gold_observations.py`: Passed with 0 errors.
  - `python -m ruff format --check scripts/ tests/test_gold_observations.py`: Passed.
  - `WEATHER_TESTING=1 pytest tests/test_ingestion.py tests/test_silver_transformations.py tests/test_gold_observations.py`: **31 passed in 1.19s**.
- Data: Real Silver Parquet data from MinIO for 2024 across 8 northern Vietnam canonical locations (HAN, PTO, TNG, TUQ, LCA, HBI, HPH, NBI). 113,545 observation rows loaded into PostgreSQL DWH. 12 monthly Gold Parquet files + 5 dimension Parquet files created in MinIO `gold` bucket.
- Limits: Years 2016–2023 and 2025 remain in Silver Parquet on MinIO and have not yet been backfilled into PostgreSQL DWH. CART ML dataset generation (ID 21–23) and Power BI connection (ID 20) remain future work.

## Full 10-Year Gold Observations Backfill & DWH Population — 2026-10-13

- Related work: workbook IDs 16 (`Silver → Gold`) and 18 (`Observation fact loading`) in `Theo dõi công việc!A20:O22`, `docs/warehouse-schema.md`, `docs/data-contract.md`, and `docs/NAMING_CONVENTIONS.md` Table 5.
- Objective/scope: Execute complete 10-year backfill (2016–2025) of hourly weather observations from Silver Parquet into MinIO Gold Parquet and PostgreSQL Data Warehouse (`fact_weather_observation`), covering 8 canonical northern Vietnam locations across all 120 months.
- Execution commands and results:
  - `python scripts/load_gold_observations.py --start-year 2016 --end-year 2025`:
    - 120 months processed (10 years x 12 months) in 289.6 seconds.
    - 120 monthly Star Schema Parquet files created under `gold/fact_weather_observation/`.
    - **994,145 curated hourly observation records** loaded into PostgreSQL DWH `fact_weather_observation` (100% exact reconciliation between Silver clean, Gold Parquet, and PostgreSQL DWH).
    - Source breakdown:
      - Open-Meteo: **701,376 records** (8 locations x 10 years x 8,760h/8,784h; average temperature 23.92 °C, total rainfall 180,520.90 mm).
      - Meteostat: **292,769 records** (all available station observation hours across 5 active canonical stations; average temperature 24.12 °C, total rainfall 46,508.10 mm).
    - Total objects in MinIO `gold` bucket: **136 objects** (120 monthly fact Parquets, 5 dimension Parquets, 11 DQ JSON reports).
    - DQ & Reconciliation report: `gold/quality_reports/dq_report_gold_load_20261013_224341.json`.
    - 0 failures encountered.
- Verification:
  - Direct SQL aggregate queries on `fact_weather_observation` confirmed full temporal range (`2016-01-01 07:00:00` to `2026-01-01 06:00:00` Vietnam local time) and distinct location/source coverage.
  - Idempotent upsert verified with 0 duplicate rows.
- Limits: CART ML dataset feature extraction (ID 21–23) and Power BI dashboards (ID 19–20) remain the next tasks.

## S3Error NoSuchKey Detection Robustness Fix — 2026-10-14

- Related work: workbook IDs 9 (`Meteostat/NOAA → Bronze`) and 10 (`API/stations → Silver`) in `Theo dõi công việc!A13:O14`, `tests/test_silver_transformations.py`.
- Objective/scope: Fix failing unit tests `test_is_s3_not_found_error_distinction` and `test_ingest_handles_404_as_allowed_skip` by making S3 missing-object detection (`is_s3_not_found_error`) robust to diverse `minio.error.S3Error` representations where `code` is `None` or located in `message`, `args`, or string representations across different library versions and test mock shapes.
- Changes:
  - `scripts/transform_meteostat_to_silver.py`: Enhanced `is_s3_not_found_error(err)` to inspect `code`, `message`, `args`, and string representation for `NoSuchKey`, `ResourceNotFound`, and HTTP 404 markers, while strictly asserting that `AccessDenied` / auth errors evaluate to `False`.
  - `scripts/ingest_meteostat.py`: Updated `is_s3_not_found_error(err)` identically to ensure missing objects during `client.stat_object()` proceed to fetch without triggering unhandled ingestion failures.
  - `scripts/transform_open_meteo_to_silver.py`: Updated `is_s3_not_found_error(err)` symmetrically for unified error handling across pipelines.
  - `tests/test_silver_transformations.py`: Expanded `test_is_s3_not_found_error_distinction` to verify `S3Error(None, "NoSuchKey", ...)`, mock `code=None` with message, string-only `NoSuchKey`, HTTP 404, `AccessDenied`, and `None`.
- Checks:
  - `python -m ruff check scripts/ tests/test_silver_transformations.py`: Passed (0 errors).
  - `python -m ruff format --check scripts/ tests/test_silver_transformations.py`: Passed (11 files formatted).
  - `WEATHER_TESTING=1 pytest tests/test_silver_transformations.py::test_is_s3_not_found_error_distinction tests/test_silver_transformations.py::test_ingest_handles_404_as_allowed_skip -v`: **2 passed in 0.99s**.
  - `WEATHER_TESTING=1 pytest tests/test_silver_transformations.py -v`: **20 passed in 1.21s**.
  - `WEATHER_TESTING=1 pytest tests/test_gold_observations.py tests/test_ingestion.py tests/test_silver_transformations.py -v`: **31 passed in 1.25s**.
- Data: Synthetic mock error fixtures and isolated test suite. No production S3/MinIO writes or network requests made.
- Limits: `tests/test_runtime.py` requires optional dependency `geopandas` not installed in local environment (unrelated to S3 error handling). Full live pipeline execution unchanged.
- Next step: Ready to commit and push to branch or continue with CART ML dataset preparation (ID 21–23).

## PR #5 Merge Review Remediation (Gold ETL & DWH Loading) — 2026-10-16

- Related work: workbook IDs 8, 9, 10, 16, 18 (`Open-Meteo/Meteostat Ingestion, Silver ETL, Gold Curation & DWH Loading`), PR #5 review findings.
- Objective/scope: Resolve all 6 technical defects (4 P1s, 2 P2s), add regression test suite, reconcile calendar key documentation across domain contracts, translate Vietnamese progress report to English, and eliminate whitespace/formatting defects.
- Changes:
  - `scripts/transform_meteostat_to_silver.py`, `scripts/transform_open_meteo_to_silver.py`, `scripts/ingest_meteostat.py`, `scripts/ingest_open_meteo.py`: Fixed `is_s3_not_found_error` to directly inspect authoritative error code attribute (`NoSuchKey`, `NoSuchBucket`, `NoSuchObject`, `ResourceNotFound`) and HTTP 404 response status, removing substring matching on arbitrary message/request ID strings. Re-raise retryable server errors.
  - `scripts/load_gold_observations.py`:
    - Added `SilverReadError` and proper S3 `NoSuchKey` handling to prevent swallowing Silver read/Parquet parse errors, blocking publication and recording failed keys in `stats['failures']`.
    - Added location merge in `save_gold_parquet` to preserve unaffected locations during location-filtered reruns.
    - Added strict validation for identical vs conflicting duplicates; identical duplicates collapse cleanly, conflicting duplicates on observation grain reject publication with descriptive error and record failure.
    - Added transactional rollback (`conn.rollback()`) on SQL error between months, preventing connection abortion from cascading.
    - Added UTC timestamp timezone-awareness and exact hourly grid validation (`minute == second == microsecond == nanosecond == 0`), and deterministic local timestamp derivation from canonical UTC using `Asia/Ho_Chi_Minh`.
  - `tests/test_silver_transformations.py`: Updated `S3Error` constructors to match pinned `minio==7.2.7` library signature `S3Error(code, message, resource, request_id, host_id, response, ...)`, and added test for S3Error with '404' in request ID.
  - `tests/test_review_regressions.py`: Authored comprehensive regression test suite covering all 6 review defects (unit and integration tests).
  - `docs/data-contract.md` & `docs/warehouse-schema.md`: Reconciled calendar key basis, confirming Vietnam Local Time (`Asia/Ho_Chi_Minh`, UTC+7) for `date_key` (YYYYMMDD) and `time_key` (HH00) derived deterministically from canonical `timestamp_utc`.
  - `bao_cao_tien_do_du_an.md`: Translated complete report into English to comply with repository documentation language rule, and removed trailing whitespace.
- Checks:
  - `ruff check scripts dags tests`: PASS (all checks passed).
  - `ruff format --check scripts dags tests`: PASS (all files formatted).
  - `docker run --rm --network none weather-tests:2.9.3-python3.10 -m 'not integration'`: PASS (43 passed, 8 deselected).
  - `docker compose -p weather-tests -f docker-compose.test.yml up --abort-on-container-exit --exit-code-from tests tests`: PASS (8 passed, 43 deselected).
- Data: Synthetic test fixtures and Docker test services (PostgreSQL 16, MinIO); existing MinIO Lakehouse and PostgreSQL DW data preserved.
- Evidence: Regression test suite in `tests/test_review_regressions.py`.
- Limits: CDS/ERA5 API ingestion and regional boundary GeoJSON remain open pending external access.
- Next step: Ready for PR #5 re-review and merge into `main`.

## PR #5 Re-Review Remediations (Gold ETL & DWH Loading) — 2026-10-17

- Related work: workbook IDs 16 (`Silver → Gold`) and 18 (`Observation fact loading`), PR #5 re-review findings on branch `feat/gold-etl-and-dwh-loading`.
- Objective/scope: Resolve 4 specific re-review findings:
  1. [P1] Treat missing Silver object (`S3Error` / `NoSuchKey`) on already-listed keys during read as fatal error, raising `SilverReadError` with key details and failing batch curation/publication instead of skipping (`continue`).
  2. [P2] In DWH loading, enforce complete calendar-month scope deletion (`[month_start_utc, next_month_start_utc)`) scoped to rerun locations, ensuring retracted Silver rows are cleanly removed from both Gold Parquet and PostgreSQL DWH without leaking into other locations or leaving orphaned records.
  3. [P2] Validate timestamp timezone awareness on incoming Silver data before UTC conversion, rejecting naive timestamps with explicit `ValueError`.
  4. [P2] Strictly validate month boundaries during monthly curation, rejecting any aware timestamps outside `[month_start_utc, next_month_start_utc)` with explicit `ValueError`.
- Changes:
  - `scripts/load_gold_observations.py`:
    - Replaced silent `continue` with `raise SilverReadError(...)` when reading listed Silver objects.
    - Added timezone-naive check via `pd.to_datetime(ts_series, utc=False)` inspecting `tzinfo` before UTC conversion.
    - Added out-of-month check rejecting timestamps before `month_start_utc` or on/after `next_month_start_utc`.
    - Corrected month-boundary range in DWH loading deletion to cover the full calendar month `[month_start_utc, next_month_start_utc)`.
  - `tests/test_review_regressions.py`:
    - Added 4 dedicated regression probe tests:
      - `test_s3_error_no_such_key_on_listed_silver_object_blocks_publication`
      - `test_rereview_naive_timestamp_rejected`
      - `test_rereview_out_of_month_aware_timestamp_rejected`
      - `test_rereview_retracted_silver_row_removed_from_gold_and_dw` (integration).
- Checks:
  - `python -m ruff check scripts dags tests`: PASS (all checks passed).
  - `python -m ruff format --check scripts dags tests`: PASS (21 files formatted).
  - `python -m pytest tests/test_review_regressions.py -m "not integration"`: PASS (7 passed, 3 deselected).
- Data scope: Isolated mock objects and synthetic test DataFrames. No production MinIO data or PostgreSQL DW facts modified.
- Limits: Integration probe `test_rereview_retracted_silver_row_removed_from_gold_and_dw` requires active test docker containers (`pg-test` and `minio-test`).
- Next step: All code and unit regression tests passed; ready for final review and PR merge.

## Bounded MinIO demonstration — 2026-10-13

- Related work: workbook IDs 2 (buckets), 8 (Open-Meteo Bronze), and 10 (Silver cleaning); draft data contract v0.1. This is sample verification, not full task acceptance. Workbook assignments/statuses were not changed.
- Objective/scope: Start MinIO for a teacher demonstration, inspect existing data, and prepare a small real-data cleaning example plus an explicitly synthetic error example. The Compose project's MinIO volume was newly created; all three buckets initially contained zero objects. No pre-existing data was removed.
- Changes/artifacts: Started `minio` and `minio-init`; created objects only under `demo/20261013T064756Z/real/` and `demo/20261013T064756Z/synthetic/` in Bronze/Silver. Local copies are under `data/bronze/demo/20261013T064756Z/` and `data/silver/demo/20261013T064756Z/` (ignored data directories). A temporary helper `/private/tmp/weather_minio_demo.py` called the existing `clean_and_standardize_open_meteo` function; no production pipeline, SQL, model, or DAG code changed.
- Commands/checks actually executed:
  - Initial `docker info` failed because Docker Desktop was stopped; sandboxed application lookup/socket access also failed. Opened `/Applications/Docker.app` and used approved Docker access.
  - `docker compose up -d minio minio-init`: PASS; three buckets created and MinIO remained running. Image version reported `RELEASE.2026-09-22T19-25-18Z` on linux/amd64.
  - `docker compose run --rm --no-deps -T --entrypoint python -v /private/tmp/weather_minio_demo.py:/tmp/weather_minio_demo.py:ro airflow-webserver /tmp/weather_minio_demo.py`: exit 0. Temporary Python container only; no Airflow webserver/scheduler or PostgreSQL process started. Compose also created an unused PostgreSQL volume while resolving the service configuration.
  - Helper assertions: API UTC offset and wind/rain units; 48 equal-length hourly arrays; unique clean business keys; Parquet/PyArrow round-trip equality; downloaded SHA-256 equals uploaded content for every demo object; expected synthetic cleaning counts. All passed.
  - Same-key upload rerun: object counts unchanged (Bronze 4, Silver 7, Gold 0). This checks replacement/object counts, not the full ingestion/ETL rerun or coverage outside the sample.
  - `docker run --rm --network none --entrypoint python weather-airflow:2.9.3-python3.10 ...`: Python 3.10.14, Airflow 2.9.3, pandas 2.1.4, PyArrow 16.1.0. Demo run started at 06:47:56 UTC; elapsed execution duration was not separately measured.
  - `curl` checks: API `/minio/health/live` HTTP 200; console HTTP 200. `open http://localhost:9001` succeeded. Browser automation was unavailable due to a tool sandbox initialization error; console login was not automated or verified.
- Real data: One live Open-Meteo Archive API request for HAN coordinates (21.0285, 105.8542), eight variables, `[2024-02-29T00:00:00Z, 2024-03-02T00:00:00Z)`. Preserved original response bytes in the demo Bronze object. 48 input rows → 48 Silver rows; 48/48 expected hours present, zero missing hours, nulls, conflicts, or range violations for these variables. This is historical API/reanalysis data, not verified station observations or a forecast.
- Synthetic data: Seven deliberately constructed rows over six expected hours on 2024-03-01, clearly labeled in paths, payload, reports, and object metadata. One identical duplicate removed; two conflicting rows excluded and saved separately; four clean rows remain. One humidity value of 150% and one precipitation value of -2 mm became null. Two hours are missing from the clean output, one absent originally and one removed because of conflicting measurements. No imputation was performed.
- Limits: Gold stayed empty; no DW loading, ML, Power BI, full historical download, Meteostat real-data run, main pipeline integration, or full test-suite rerun. Demonstration prefixes are intentionally separate from production object paths and are not evidence of full-scope ETL acceptance.
- Next step: Demonstrate raw JSON/CSV, cleaned Parquet/CSV, quality reports, and conflicting-record CSV; then implement and verify a bounded Gold/DW slice under agreed contracts.

## DFD explanation with DW-sourced analytics and ML — 2026-10-13

- Related work: workbook IDs 3 (schema/design), 17 (integrated flow), and 31 (prediction integration), plus `docs/business-requirements.md` section 3, which permits an ML dataset exported from the DW. Design explanation only; no full-task acceptance or assignment/status change.
- Objective/scope: Explain the user's requested target flow in two images: Gold observation data loads into DW; both historical analytics and ML read DW; scoring writes predictions back to DW. The earlier direct Gold-to-ML diagram is an alternative design, not an implemented requirement. Domain contracts, label/source decisions, and production architecture documents were not changed or approved by these illustrations.
- Changes/artifacts: `outputs/dfd-weather-20261013/build_diagrams.py`, `dfd-level-0.png/.svg`, and `dfd-level-1.png/.svg`. Vietnamese diagram labels match the user's explanatory conversation; generator identifiers, comments, and logs use English. Level 0 is explicitly the context diagram (one system process); level 1 decomposes it into seven processes. Images distinguish code/sample verification from missing Gold, loaders, ML, and BI.
- Checks: Bundled Python 3.12/Pillow executed the generator successfully (final tool run 0.46 s). Level 0 has three nodes/four labeled flows; level 1 has fourteen nodes/seventeen labeled flows. Assertions validated flow endpoints. Both PNGs were visually inspected, then regenerated to resolve process-number/title overlap and reposition labels; final visual inspection passed. PNG dimensions are 3200×1700 and 3560×2780. SVG XML and DW-routing checks passed; `git diff --check` passed.
- Data/runtime scope: Diagram authoring only. No new API calls, data processing, SQL/schema changes, historical downloads, services, training, or test-suite runs. Previous 48-hour real-data demo appears only as a bounded status annotation.
- Limits/next step: These are explanatory target diagrams, not evidence that the DW-first pipeline or any model exists. Confirm DFD level naming with the course's convention if its context diagram is named separately; agree ML export/preprocessing and prediction contracts before implementation.

## Full workflow and detailed warehouse diagrams — 2026-10-13

- Related work: workbook IDs 3/13 (warehouse structure), 17 (full ETL), 21–23/28–30 (ML preparation/evaluation), and 31–33 (prediction integration). User requested more detailed explanatory images; no implementation or workbook acceptance update.
- Scope/changes: Added `outputs/dfd-weather-20261013/build_full_diagrams.py`, `weather-full-flow.png/.svg`, and `star-schema-detail.png/.svg`. Reused the earlier renderer; previous context/level-1 images retain their content. The full workflow includes sources, Bronze/Silver/Gold, dimension-first DW loading, analytics/Power BI, EDA/labels/features/CART, chronological train/validation/test, model storage, scoring, prediction writeback, user requests, and Airflow's orchestration role. The relationship diagram is separate from the workflow and includes every column extracted from the seven existing DDL tables, plus explicitly proposed DimModel fields.
- Checks: Bundled Python 3.12/Pillow generator exited 0; render/validation command completed in 1.81 s before the final small label/flow adjustment. Assertions found seven DDL tables and seventeen observation-fact columns. Diagram endpoint checks passed (workflow fourteen nodes/sixteen labeled flows; schema eight nodes/ten relationships). Python AST, PNG integrity, and SVG XML passed. Both images were visually inspected; overlapping arrow labels were repositioned, then final affected image inspected. `git diff --check` passed.
- Artifacts: Workflow PNG 4720×3880; schema PNG 4800×4620; vector SVG siblings allow zoom without raster text loss. PK/FK/UQ and nullable observation station keys are identified. Notes retain unresolved date/time calendar basis, multiple-station handling, incomplete prediction origin/window/uniqueness, and Silver pressure/provenance columns missing from current DW.
- Data/runtime: Diagram authoring and read-only DDL parsing only. No API calls, ETL, SQL execution, migrations, service changes, training, or model evaluation. Prior 48-hour Open-Meteo demo is a status annotation, not expanded data scope.
- Limits/next step: DW-first ML remains an explanatory target, and DimModel metadata fields are proposals. Gold, loader, model, scoring, and BI are marked pending; no schema/database was changed. Validate the proposed contracts before implementing the next bounded Gold/DW slice.

## High-level architecture image — 2026-10-13

- Related work: workbook IDs 1 (Docker environment), 17 (orchestration), and 31–33 (ML/BI integration). User requested an HLD illustration; no implementation acceptance or assignment/status change.
- Scope/changes: Added `outputs/dfd-weather-20261013/build_hld.py` and `hld-weather-platform.png/.svg`. Compared `docker-compose.yml`, `Dockerfile`, and architecture documentation. Diagram shows external sources/operator browser, Docker Compose/weather-network boundary, Python ETL/DQ, Airflow 2.9.3/Python 3.10/LocalExecutor, MinIO buckets/persistent volume, PostgreSQL 16 with distinct Airflow metadata and weather DW databases, a proposed Python CART module, external Power BI, and users. Data connections are solid; orchestration/UI/metadata connections are dashed. UI/API/DB ports are labeled with configured defaults where applicable; no credentials are included.
- Checks: Bundled Python 3.12/Pillow generator exited 0 (initial run 1.23 s); endpoint validation covered eleven nodes/fourteen labeled connections. Python AST, SVG XML, and PNG integrity passed. Image was visually inspected, two intersecting labels were moved and a caption typo corrected, then regenerated and visually rechecked. PNG is 5640×3580. `git diff --check` passed.
- Data/runtime: Documentation image only. No Compose/service changes, APIs, ingestion, SQL execution, database alterations, training, or runtime tests. The existing MinIO/48-hour demo is referenced as prior bounded evidence.
- Limits/next step: HLD represents the user's DW-first target, not a fully running stack. Gold/loader/ML/BI remain pending, model artifact storage is unresolved, and the Python ML box is a proposed module rather than an existing separate service. Current Compose uses one PostgreSQL server for both database roles. Confirm contracts before implementing the next bounded pipeline increment.

## Review of bacnx Gold/DW pull request #5 — 2026-10-16

- Related work: workbook IDs 9/10 (ingestion/Silver error handling), 11/12 (station/calendar dimensions), 16 (Gold), and 18 (fact loading); `docs/data-contract.md` draft v0.1 and `docs/warehouse-schema.md`. Workbook task ownership/status was read from the existing allocation workbook and was not changed. This entry records review evidence, not task acceptance.
- Objective/scope: Review `origin/feat/gold-etl-and-dwh-loading` at `78873303a4aad41f9b808ea8b3b562fa2bcc7aee` against `origin/main` at `85dd9be39e22848bfb612340a1feed8fb5f295e4`. Fetched remote refs and reviewed the two-commit, ten-file change from an archive in `/private/tmp/bacnx-review.9zutlq`, preserving the active checkout and its existing changes. GitHub PR #5 targets main; public API reported a clean merge and successful `Lint and tests` check at this head. Branch protection and required approvals were not inspected.
- Changes/artifacts: Added `outputs/reviews/bacnx-20261016/review.md`, `test_review_regressions.py`, `offline-regressions.log`, and `gold-integration.log`. Application code, domain contracts, database schema, and remote PR contents were not edited. The probes were copied into the temporary archive's `tests/` directory to use its existing isolation fixtures.
- Checks/environment: Existing Docker image `weather-tests:2.9.3-python3.10` (Python 3.10.14/Airflow 2.9.3), PostgreSQL 16 and MinIO on an isolated internal Compose network with tmpfs. Archived `.venv/bin/ruff` equivalent checks used the repository tooling executable: `ruff check scripts dags tests` and `ruff format --check scripts dags tests` both passed (18 baseline files). Original offline command `docker run --rm --network none --mount type=bind,src=/private/tmp/bacnx-review.9zutlq,dst=/app,readonly --workdir /app -e WEATHER_TESTING=1 -e PYTHONDONTWRITEBYTECODE=1 weather-tests:2.9.3-python3.10 -m 'not integration' -p no:cacheprovider` passed 39 tests, with 6 deselected, in 9.34 s. Original `tests/test_integration.py -m integration -p no:cacheprovider` through the temporary Compose project passed 6 tests in 4.15 s. Existing Airflow deprecation/NumPy binary-size warnings were emitted; they did not fail tests. No fresh image build was performed.
- Failed checks and findings: Supplemental offline `tests/test_review_regressions.py -m 'not integration' -q -p no:cacheprovider` failed 7 probes in 2.02 s, confirming five distinct defects: location-filtered reruns overwrite consolidated monthly Gold; Silver read failures are swallowed; conflicting duplicates are silently kept-first; supplied local timestamps can assign incorrect date keys; and arbitrary `404` substrings in S3 request IDs misclassify server errors in three modules. Supplemental integration `tests/test_review_regressions.py -m integration -p no:cacheprovider -v` passed the happy-path slice and failed SQL recovery (1 passed/1 failed/7 deselected in 1.90 s): one failed month leaves the connection aborted and prevents the next valid month's commit. `git diff --check origin/main...origin/feat/gold-etl-and-dwh-loading` failed on Markdown whitespace/EOF issues in the branch. Review probe lint/format passed after correcting fixture formatting; AST parsing passed for 19 files. Detailed commands and source permalinks are in the review artifact.
- Data: Synthetic observations only. Successful Gold/DW slice: Open-Meteo and Meteostat × HAN/PTO × `[2024-02-29T16:00:00Z, 2024-02-29T19:00:00Z)`, 12 input/Gold/fact rows, still 12 facts after rerun; 8 rows mapped to local March 1 and four source/location rain sums were 3 mm each. Seed checks used 366 generated dates for 2024 and 10 existing registry stations. Failure probe: invalid January source key followed by one valid February observation; February committed zero rows because no rollback occurred. Offline fixtures used tiny in-memory Parquet objects and constructed S3 errors. No weather API calls, historical downloads, or production DW/MinIO access occurred.
- Evidence/cleanup: `outputs/reviews/bacnx-20261016/` contains the report, reproducible probes, and supplemental failure logs. Compose commands used only project `bacnx-review-20261016` with the archived `docker-compose.test.yml`; its two containers and network were removed successfully using `down --volumes --remove-orphans`. Existing services/volumes were not used. Initial sandbox access restrictions were resolved through authorized escalation; a Python HTTPS metadata read failed local certificate validation, and the same public metadata was subsequently read successfully with curl using certificate validation.
- Limits: No independent verification of the branch's claimed 994,145 real rows or full 2016–2025 coverage. Existing six integration tests do not exercise the new Gold loader; the bounded supplemental slice does not establish full acceptance. Local calendar approval claimed in branch progress remains inconsistent with unresolved domain contracts. Workbook status and task acceptance remain separate from these technical checks.
- Next step: Request changes before merge. Fix the six reproduced defects, maintain regression coverage in the main test suite, reconcile approved calendar/documentation decisions, and rerun bounded publication/rerun/failure-resumption checks. No merge or external review/comment was submitted.

## Re-review of bacnx Gold/DW pull request #5 — 2026-10-17

- Related work: workbook IDs 9/10 (ingestion/Silver), 11/12 (dimensions), 16 (Gold), and 18 (observation loading); draft data contract v0.1 and warehouse schema. Read relevant allocation rows from `Theo dõi công việc!A13:O17`, `A20:O20`, and `A22:O22`; ownership/status was not changed. This is review evidence, not full task acceptance.
- Objective/scope: Re-review head `317f06d8fc220627987c45720a730b8c4da7383a` against main `85dd9be39e22848bfb612340a1feed8fb5f295e4`. `git ls-remote --heads origin` confirmed current remote heads; public PR #5 metadata reported open/unmerged/clean mergeability, with successful CI check runs at this head. Archive at `/private/tmp/weather-bacnx-review-20261017` preserved the active checkout and existing uncommitted work.
- Changes/artifacts: Added `outputs/reviews/bacnx-20261017/review.md`, `test_additional_review.py`, `additional-offline.log`, and `additional-integration.log`. Application code, domain contracts, SQL schema, workbook, and remote PR contents were not edited. Appended this entry without replacing earlier local evidence.
- Checks: Repository `.venv/bin/ruff check` and `ruff format --check` on archived `scripts dags tests` passed (19 baseline files); `git diff --check origin/main...origin/feat/gold-etl-and-dwh-loading` passed. Original `docker run --rm --network none --mount type=bind,src=/private/tmp/weather-bacnx-review-20261017,dst=/app,readonly --workdir /app -e WEATHER_TESTING=1 -e PYTHONDONTWRITEBYTECODE=1 weather-tests:2.9.3-python3.10 -m 'not integration' -p no:cacheprovider` passed 43 tests/8 deselected in 10.02 s. Isolated Compose `run --rm --no-deps --volume /private/tmp/weather-bacnx-review-20261017:/app:ro --workdir /app -e PYTHONDONTWRITEBYTECODE=1 tests -m integration -p no:cacheprovider` passed 8 tests/43 deselected in 6.98 s. Maintained regressions for the earlier review scenarios passed; domain calendar descriptions now align with implementation. Existing Airflow deprecation/NumPy binary-size warnings remained. Python AST parsing passed for 20 files including additional probes; probe Ruff check/format passed after formatting.
- Failed checks: Supplemental `tests/test_additional_review.py -m 'not integration' -p no:cacheprovider -v` failed 3 probes in 1.66 s: a listed Silver object returning `NoSuchKey` is skipped without blocking incomplete publication; naive timestamps are silently assigned UTC; out-of-month timestamps pass monthly curation. Supplemental `tests/test_additional_review.py -m integration -p no:cacheprovider -v` failed 1 probe in 1.64 s: after removing one Silver key and rerunning HAN, Gold had 2 keys while DW retained 3 because Gold replaces the scoped rows and DW only upserts. Commands and source permalinks are in the review artifact.
- Data: Synthetic Open-Meteo × HAN/PTO over `[2024-09-01T16:00:00Z, 2024-09-01T18:00:00Z)`: initial 3 Silver/Gold/fact keys; corrected Silver/Gold 2 versus DW 3 after rerun, with PTO preserved. Date seeding generated 366 days for 2024 only in the temporary database. Offline cases used tiny Parquet/error fixtures. No weather API, production data reads/writes, historical downloads, ML training/evaluation, or real ten-year reconciliation occurred.
- Evidence/cleanup: `outputs/reviews/bacnx-20261017/` contains the reproducible probe and full supplemental failure logs. Existing Python 3.10.14/Airflow 2.9.3 test image reused; PostgreSQL 16/MinIO ran only under project `bacnx-review-20261017`, internal network/tmpfs/no host ports. `down --volumes --remove-orphans` successfully removed its containers/network. No existing services/volumes changed. Initial sandboxed fetch/Docker access failed; permitted read-only remote checks and isolated Docker operations succeeded subsequently; no approval review rejection occurred.
- Limits: No fresh image build, branch-protection review, independent calendar approval verification, or verification of the branch's claimed 994,145 real observations/full 2016–2025 coverage. Passing baseline tests and remote CI does not establish full task acceptance.
- Next step: Request changes before merge. Block inconsistent/missing input snapshots; define matching retraction/replacement semantics across Gold and DW without deleting data on incomplete input; enforce aware timestamps and UTC monthly bounds; add maintained regressions and rerun the bounded publication/reconciliation checks. No merge or external review/comment submitted.

## Workbook status update and `main` verification — 2026-10-21

- Related work: all 40 workbook rows in `Theo dõi công việc` (columns M/N/O only); evidence for IDs 1–3, 6–13, 16, 18. User explicitly requested a workbook status update. Assignments, weeks, and dates were not changed.
- Objective/scope: Sync workbook status, percentage, and notes with the evidence recorded above; confirm that PR #5 remediations on `main` (`78ea80c`) pass the maintained tests.
- Changes: Edited sheet XML directly (no library rewrite) and set full recalculation on open so the `Tổng quan` summary refreshes. Updated 13 rows, all Đang làm (30–90%); ID 2 is 90% because Bronze still does not preserve original payloads; every other row unchanged at Chưa bắt đầu (0%). IDs 4, 5, 14, 15, 17 and 19–40 have no evidence. Percentages are estimates, not acceptance. A pre-edit backup is in the session scratchpad.
- Checks actually run on `main` (Docker, synthetic data): `make test` → 46 passed, 9 deselected, 9.30 s; `make test-integration` (PostgreSQL + MinIO) → 9 passed, 6.26 s. Warnings: Airflow `schedule_interval` deprecation, NumPy binary-size. The updated workbook was re-read with openpyxl (values confirmed); it was not opened in Excel/LibreOffice, so summary formulas were not recalculated here.
- Data scope: Status notes rely on the logged real-data runs (Silver 994,145 observation rows; Gold/DW 120 months). They were not independently reconciled in this entry. Sources are Open-Meteo (ECMWF reanalysis, gridded) and Meteostat (station data), both intermediaries, not Vietnam's national service.
- Limits: ID 4 (real GeoJSON), 5 (CDS request), 14/15 (ERA5), 17 (Bronze→DW DAG), labels/features, OLAP, Power BI, CART not started. Calendar basis (UTC+7 for `date_key`) still needs team confirmation.

## Task 6/7 decisions recorded — 2026-10-21

- Related work: workbook IDs 6 (Silver contract) and 7 (RainNext24h definition); also touches 10, 12, 22. Documentation and workbook status only; no code, SQL, or data changes.
- Changes: `docs/data-contract.md` → v0.2 (confirms columns/units/UTC, `pressure_type`, provenance, duplicate handling; station selection and minimum coverage stay open). `docs/rain-label.md` → v0.2 (threshold over `(t, t+24h]`; initially 0.1 mm, changed to 1 mm after the measurement below, Open-Meteo label source for the first experiment, missing hour ⇒ no label, UTC for labels/splits vs UTC+7 for `date_key`). Workbook IDs 6 and 7 set to Đang làm at 90% and 80% with notes.
- Basis: Decisions were confirmed by the project member working on N1/N2 on 2026-10-21. Sign-off from the third member is not recorded, and the documents say so. These are choices for the first experiment, not proven optimal.
- Checks: diffs reviewed (2 docs); workbook re-read with openpyxl. No Markdown link check or service run. Label rates at 0.1 mm versus 1 mm were not measured: this machine's MinIO holds only demo objects, so real Silver data was unavailable.
- Limits: Rain timestamp convention (preceding-hour sum) unverified on a sample; fallback ERA5, station selection, minimum coverage, and recall/false-alarm targets remain open. Both tasks stay below Hoàn thành until the third member confirms and the timestamp check and label-rate count are done.

### Label-rate measurement and threshold change — 2026-10-21

- Related work: workbook ID 7 (and input to ID 22). Measurement only; no label code, MinIO/DW writes, or pipeline changes.
- Data: real Open-Meteo Archive responses, eight requests (one per canonical location, `hourly=precipitation`, `timezone=UTC`, 2024-01-01 to 2024-12-31), saved only in the session scratchpad. 8,784 hours per location, zero nulls; 8,760 labelled hours per location (windows `(t, t+24h]` requiring 24 valid hours). This is one year and one source; Meteostat was not compared.
- Result: positive rate at 0.1 mm was 65.4–73.4% (monthly 24–100%); at 1 mm it was 46.2–54.7%. Annual totals 1,980–2,660 mm. Because an always-Yes baseline would already reach about 71% at 0.1 mm, the threshold in `docs/rain-label.md` and the workbook note for ID 7 was changed to 1 mm.
- Checks/limits: the saved timestamps are hourly UTC, but the preceding-hour versus following-hour rain convention could not be verified from this data and remains open. This machine's MinIO holds only demo objects and the `thuchanh_minio_data` volume is empty, so the logged 10-year Silver/DW data was not available for reconciliation. Third-member sign-off is still not recorded.

- Update 2026-10-21: the user reported that the third team member confirmed the task 6/7 decisions (relayed verbally; no written record in the repository). Remaining: rain timestamp convention check and comparison with the 10-year data.

### Task 6/7 closing checks — 2026-10-21

- Related work: workbook IDs 6 and 7 (input to 10, 11, 22). Documentation, workbook, and read-only measurement only; no code, MinIO, or DW changes.
- Timestamp convention: the Open-Meteo Historical Weather API documentation states hourly `precipitation` is the "sum of the preceding hour". The `(t, t+24h]` window therefore uses values at t+1h…t+24h. Recorded in `docs/rain-label.md`.
- Label rates: eight Open-Meteo Archive requests (one per canonical location, `hourly=precipitation`, `timezone=UTC`, 2016-01-01 to 2025-12-31, 5 s between calls), all HTTP 200, about 2.1 MB each, stored only in the session scratchpad. Each location had 87,672 contiguous UTC hours with zero nulls, giving 701,376 hours in total, which matches the logged Open-Meteo Silver count. Labels requiring 24 valid hours: 701,184. At 1 mm the positive rate was 50.5% overall (train 50.9%, validation 2024 49.3%, test 2025 49.0%), yearly means 42.0–56.5%, monthly 25–78%; LCA was highest at 67.7%. At 0.1 mm the rate was 69.5%. This confirms the 1 mm decision. Train labels near 2023-12-31 and validation labels near 2024-12-31 use rain from the next split and must be purged per `docs/rain-label.md`; test 2025 ends at 2025-12-30 23:00 UTC without 2026 data.
- Station selection/coverage: read `scripts/station_config.py` and grepped the Silver/Gold scripts. All eight canonical locations map 1:1 to a WMO station (superseded the same day: five of those ids were misassigned; see "Station registry correction and RainNext24h label module"); `48806`/`48808` are excluded; coverage is reported by `scripts/quality_checks.py`; no `fillna`/interpolation found. Recorded in `docs/data-contract.md`, with the minimum coverage threshold still open. Earlier statements in this session that "5 of 8 locations have a station" were wrong: 8 are mapped and about 5 returned Meteostat data per the 2026-10-11 log.
- Finding: registry station coordinates are identical to the canonical point coordinates, so station-to-point distances are not real yet (relevant to workbook ID 11). Not fixed in this turn.
- Workbook: IDs 6 and 7 set to Đang làm 98%. They stay below Hoàn thành because the third member's confirmation is only relayed verbally. Re-read with openpyxl; not opened in Excel.

## Station registry correction and RainNext24h label module — 2026-10-21

- Related work: workbook IDs 9 (Meteostat), 11 (DimStation), 18 (fact loading), 22 (label generation); `docs/data-contract.md` v0.2, `docs/rain-label.md` v0.2, `docs/NAMING_CONVENTIONS.md` Table 6.
- Finding: station identities were checked against Meteostat station metadata (`data.meteostat.net/stations/<id>.json`, 10 requests) and NOAA ISD `isd-history.csv` (1 request). Five of the eight Table 6 ids belong to other stations: `48825` is Hà Đông (was PTO), `48826` is Phù Liễn/Hải Phòng (was TNG), `48830` is Lạng Sơn (was HBI), `48838` is Móng Cái (was HPH), `48840` is Thanh Hóa (was NBI). `48816` (TUQ) exists in neither catalogue. Registry coordinates had been copied from the research points. The five stations that returned Meteostat data on 2026-10-11 (`48820`, `48825`, `48826`, `48830`, `48840`) included four misassigned ones. The logged Meteostat Silver/Gold/DW rows for PTO, TNG, HBI, and NBI are therefore observations from other stations.
- Changes: `scripts/station_config.py` now records verified names, coordinates, and elevations with their source. Canonical mappings: HAN `48820`, HPH `48826`, TNG `48831`, LCA `48803`. Misassigned/unverified ids are status `UNMAPPED_MISASSIGNED`/`UNMAPPED_UNVERIFIED` and are rejected by the existing publication guard. Only `48820` and `48826` are served by Meteostat. Updated Table 6, data-contract station row, and `tests/test_silver_transformations.py` (canonical-mapping test rewritten; new rejection test).
- Changes: new `scripts/rain_labels.py` (`build_rain_labels`, `assign_splits`). It uses `(t, t+24h]` on a per-location UTC grid, a 1 mm default threshold, requires 24 valid hours (missing is never No), and rejects naive timestamps and duplicates. Splits are by UTC origin, with `boundary_purge` when `target_end >= split_end`, plus `missing_label`/`out_of_scope` reasons. New `tests/test_rain_labels.py` (10 synthetic tests: t, t+1h, t+24h, t+25h, threshold equality, absent/null hours, end of series, per-location independence, both split boundaries, test end).
- Checks: `.venv/bin/ruff check scripts dags tests` passed; `ruff format --check` passed (21 files). `make test` → 57 passed, 9 deselected, 5.96 s. `make test-integration` → 9 passed, 5.60 s. Real-data run of the label module on the eight Open-Meteo 2016–2025 responses already in the session scratchpad (701,376 hours) took 0.48 s. Results: 701,184 labelled, 700,800 usable, 576 `boundary_purge` (24 h × 8 locations × 3 boundaries); usable positive rate train 50.9%, validation 49.4%, test 49.0%.
- Not done: Bronze/Silver/Gold/DW were not rerun; the corrected registry affects them only on the next run. The machine holding the 10-year MinIO/DW data must rerun Meteostat Silver with `--overwrite`, rerun `seed_dimensions.py`, and reload Gold/DW. The retraction of the misassigned PTO/TNG/HBI/NBI Meteostat facts was not verified. Labels are not yet exported from Silver/Gold. Historical Bronze objects for the misassigned ids keep their raw station ids, so provenance is preserved.
- Workbook: notes updated for IDs 9, 11, 18; ID 22 set to Đang làm 40%.

- Update 2026-10-21 (workbook IDs 6 and 7): the user, acting for the team, stated that their confirmation is sufficient approval. Workbook IDs 6 and 7 were set to Hoàn thành at 100%. Items that remain open and are recorded as such: minimum coverage threshold (ID 6) and ERA5 fallback (ID 7). The approval was given verbally in this session; no separate written record from the third member exists in the repository.

## Full local rebuild with the corrected station registry — 2026-10-21

- Related work: workbook IDs 8, 9, 10, 11, 12, 16, 18. Real data, full 2016–2025 scope, run on the N1/N2 machine (Docker Desktop, Compose project `test_dl_weather`). The user asked for the data to be fetched from the source APIs because the earlier 10-year dataset exists only on another machine.
- Environment fix: the existing `test_dl_weather_postgres_data` volume held a cluster whose first initialization had been interrupted (`airflow` database present but empty, no `weather_dwh`). Ran the project initializer `docker compose exec -T postgres bash /docker-entrypoint-initdb.d/01_init_postgres.sh` (idempotent DDL): created `weather_dwh`, 7 tables, 8 locations, 4 sources. MinIO kept its existing demo objects.
- Commands (each `docker compose run --rm --no-deps -T --entrypoint python airflow-webserver /opt/airflow/scripts/<script>`; working tree with the corrected `station_config.py`):
  - `ingest_open_meteo.py --start-year 2016 --end-year 2025`: rc 0, 326 s, 80/80 Bronze files.
  - `ingest_meteostat.py --start-year 2016 --end-year 2025`: rc 0, 44 s. 15 files (48820 2016–2025; 48826 2021–2025), 25 station-years HTTP 404 (48826 2016–2020, 48831 and 48803 all years), 0 failures.
  - `transform_open_meteo_to_silver.py ... --overwrite`: rc 0, 24 s, 701,376 rows, 0 missing hours, 0 conflicts.
  - `transform_meteostat_to_silver.py ... --overwrite`: rc 0, 12 s, 131,060 rows, 436 missing hours, 0 conflicts.
  - `seed_dimensions.py`: rc 0, 9 s. `dim_date` 4,018 days (20160101–20261231), `dim_time` 24, `dim_station` 11 with verified coordinates.
  - `load_gold_observations.py --start-year 2016 --end-year 2025`: rc 0, 233 s, 832,436 curated/upserted rows, 0 failures.
- Read-only SQL reconciliation on `weather_dwh`:
  - 832,436 rows with 0 duplicates on either the UTC grain or the unique key.
  - Open-Meteo has 87,672 rows for each of the 8 locations (2016-01-01T00Z → 2025-12-31T23Z).
  - Meteostat has only HAN/48820 (87,629) and HPH/48826 (43,431, from 2021-01-01T17Z). 43 + 393 missing hours = 436, matching the Silver report.
  - No Meteostat rows exist for PTO, TNG, HBI, or NBI.
  - 2024 Open-Meteo rain totals per location in the DW equal the independent API sample downloaded earlier today to 0.1 mm (e.g., HAN 2,660.3 mm).
  - MinIO non-demo objects: Bronze 95, Silver 1,143, Gold 126.
- Interpretation: the earlier 994,145-row dataset on the other machine is superseded. Its 292,769 Meteostat rows included stations assigned to the wrong locations. The 161,709-row difference (old Meteostat 292,769 minus new 131,060; Open-Meteo is unchanged at 701,376) is the removed misassigned-station data, partly offset by HPH, which previously had no Meteostat rows. That machine's MinIO/DW still holds the old data until it is rebuilt or the stale Silver objects for 48825/48830/48840 are deleted before reloading.
- Not verified: Airflow DAG execution (scripts were run directly), Bronze raw-payload preservation (known gap), Power BI/OLAP, and a rerun of the Gold load for idempotence on this machine (earlier tests cover it).

## Post-rebuild verification and Meteostat provenance finding — 2026-10-21

- Related work: workbook IDs 9, 10, 16, 18 and `docs/data-contract.md` (`observation_type`). Verification only; no code changed in this entry.
- Checks: `ruff check`/`format --check` passed (21 files); `py_compile` passed for the changed scripts; `git diff --check` passed. A Gold/DW rerun of 2024 (`load_gold_observations.py --start-year 2024 --end-year 2024`, rc 0, 27 s, 87,625 rows upserted) left the DW unchanged at 832,436 rows, 832,436 distinct unique keys, and 198,957.7 mm total rain. The rerun is idempotent on real data.
- Finding (data quality, open): the Meteostat bulk CSVs carry per-variable `<var>_source` columns. Measured on the 15 Bronze files:
  - 48820/HAN precipitation is null for 2016–2021 and 100% `dwd_mosmix` (DWD MOSMIX model forecast) for 2022–2025. Its temperature is 94.8% `isd_lite`, 5.0% `metar`, 0.2% `dwd_mosmix`.
  - 48826/HPH is 100% `metno_forecast` (MET Norway model forecast) for every variable in 2021–2025.
  - So no Meteostat precipitation in the DW is a gauge observation, and HPH Meteostat contains no observations at all.
- Bug: `transform_meteostat_to_silver.py` looks only for `_source`/`source` columns, so every Meteostat row is labeled `observation_type = "station_reported"`. The DW fact table has no `observation_type` column, so the distinction is also lost downstream.
- Decision needed (not taken): (a) keep per-variable source columns in Silver/Gold and exclude model-sourced values from "station" analyses, or (b) drop model-sourced Meteostat values at Silver. Either option changes the data contract, the Silver producer, the Gold/DW consumers, and possibly the DDL (migration). The rain label is unaffected because it uses Open-Meteo only.

## Meteostat model-value filter applied to real data — 2026-10-21

- Related work: workbook IDs 9, 10, 16, 18; `docs/data-contract.md` (new "Meteostat model-filled values" row and `observation_type` domain). The user chose option 2: drop model values at Silver.
- Changes:
  - `scripts/transform_meteostat_to_silver.py`: new `remove_non_observation_values()` uses an allowlist (`isd_lite`, `metar`, `synop`, `national`), nulls every other source per variable (including untagged values in files that have source columns), and drops rows with no remaining measurement. It records `source_filter` statistics. `observation_type` is now `station_reported` only after filtering, or `unverified_source` for legacy payloads without source columns. With `--overwrite`, stale Silver months for a station-year are removed once its Bronze file was read successfully, so Gold cannot reload them. Summary output adds removed values, dropped rows, and removed stale months.
  - `tests/test_silver_transformations.py`: the provenance fixture now uses the real per-variable source format. New tests: per-variable model removal and row drop (including an unknown source), an all-model payload yielding no rows, a legacy payload marked unverified, and an overwrite run removing only the month that lost its observations.
- Checks:
  - `ruff check`/`format --check` passed.
  - `make test` → 61 passed, 9 deselected, 6.54 s.
  - `make test-integration` → 9 passed, 5.66 s.
- Real-data run (same Compose stack and Bronze as the rebuild):
  - `transform_meteostat_to_silver.py --start-year 2016 --end-year 2025 --overwrite`: rc 0, 13 s, 87,485 rows kept, 436,502 model values removed, 43,575 rows dropped, 60 stale Silver months removed (all 48826), 0 failures.
  - `load_gold_observations.py --start-year 2016 --end-year 2025`: rc 0, 788,861 rows upserted, 0 failures. Reported elapsed 3,493 s versus 233 s earlier. The host clock jumped during the run (a later MinIO call failed with `RequestTimeTooSkewed` until the clocks resynchronized), so the machine probably slept. The slowdown is unexplained and was not profiled. Row-by-row retraction deletes are a plausible contributor. **Correction (2026-10-22):** the DQ report of this run (`gold/quality_reports/dq_report_gold_load_20261021_020704.json`) records 291.98 s (02:02:18–02:07:04 UTC); no artifact supports 3,493 s.
- Reconciliation (read-only SQL and MinIO listing from inside the Compose network):
  - DW holds 788,861 rows (701,376 Open-Meteo + 87,485 Meteostat) with 0 unique-key duplicates.
  - Meteostat now has HAN only (2016-01-01T00Z → 2025-12-31T23Z, 8,721–8,782 rows per year): 0 non-null precipitation, 87,484 non-null temperatures. HPH Meteostat rows were retracted from the DW (43,431 → 0), as were HAN rows whose values were all model-derived (87,629 → 87,485). Open-Meteo is unchanged.
  - Silver `weather_hourly/` holds 1,080 objects (960 Open-Meteo, 120 Meteostat 48820, 0 for 48826); Gold holds 128 objects.
  - This also verifies the Gold→DW retraction path on real data.
- Implications/limits:
  - No station precipitation is available for any location from Meteostat. Comparing station and model rainfall would require NOAA ISD/GHCN-h or another gauge source (workbook ID 9 "NOAA" remains open).
  - The DW fact table still has no `observation_type` column; Gold Parquet keeps it.
  - The allowlist was confirmed against sources seen in this data (`isd_lite`, `metar`); `synop`/`national` are included by name without a sample.

## NOAA ISD precipitation availability check — 2026-10-21

- Related work: workbook ID 9 ("Meteostat / NOAA → Bronze"). Read-only investigation with no code, MinIO, or DW changes. Files were saved only in the session scratchpad.
- Scope: 2024 only. 14 northern Vietnam NOAA ISD stations active through 2025 (from `isd-history.csv`, latitude ≥ 19.8). Sources: ISD-Lite `noaa-isd-pds.s3.amazonaws.com/isd-lite/data/2024/<usaf>-99999-2024.gz` and full ISD `.../data/2024/<usaf>-99999-2024.gz`; 28 requests in total. The old `ncei.noaa.gov/pub/data/noaa/isd-lite` paths now return HTTP 301 to the S3 bucket.
- ISD-Lite: no 1-hour precipitation at any station. The 6-hour precipitation field appears only at 06/18 UTC and only on rainy reports (42–209 per station-year), so it is unusable as an hourly or complete series. Temperatures are mostly 3-hourly (about 2,770 reports per year); only the airports 488200 (Nội Bài) and 488251 (Cát Bi) are near-hourly, and both have no precipitation.
- Full ISD `AA` groups: 24-hour totals reported at 12 UTC (window ending 19:00 Vietnam time) for most synoptic stations. Some days carry conflicting duplicate values (52 days at 488260, 61 at 488310). Station-days with a 24-hour value in 2024, nearest research point, and distance:
  - 488260 Phù Liễn: 337/366 days, HPH, 7.6 km
  - 488310 Thái Nguyên: 333/366, TNG, 1.7 km
  - 488030 Lào Cai: 262/366, LCA, 1.6 km
  - 488250 Hà Đông: 260/366, HAN, 11.3 km
  - 488230 Nam Định: 341/366, nearest to NBI at 27.3 km (a different province)
  - No station within reasonable distance of PTO, TUQ, or HBI.
- Plausibility versus Open-Meteo, using 24-hour totals ending 12 UTC on matching days and the maximum where duplicates conflict:
  - Phù Liễn: 2,112 mm vs 2,247 mm, Pearson 0.73, rain ≥ 1 mm day agreement 70.5%
  - Thái Nguyên: 2,218 mm vs 2,156 mm, Pearson 0.61, agreement 75.3%
  - Open-Meteo reports many more rainy days (about 175 vs 102–111), consistent with reanalysis drizzle.
- Conclusion: NOAA ISD provides **daily** gauge rainfall for up to five research points but no hourly rainfall. It can support station-versus-model daily rain comparisons in BI/reports, not the hourly RainNext24h label. Using it requires a contract decision, because the current observation grain is hourly: for example, a separate daily station-rain table with source × station × UTC day ending 12:00, a duplicate-resolution rule, and a coverage report. Other years were not checked.

### NOAA ISD 10-year daily-rain coverage — 2026-10-21

- Scope: full ISD files for 488260 (Phù Liễn/HPH), 488310 (Thái Nguyên/TNG), 488030 (Lào Cai/LCA), 488250 (Hà Đông/HAN), and 488230 (Nam Định/NBI substitute), 2016–2025. 50 requests to `noaa-isd-pds.s3.amazonaws.com/data/<year>/<usaf>-99999-<year>.gz`, all HTTP 200, 5.2 MB, kept in the session scratchpad only. Only `AA` groups with a 24-hour period in reports at 12 UTC were counted.
- Coverage (share of days with a 24-hour value):
  - Phù Liễn, Thái Nguyên, Nam Định: 94–99% per year for 2016–2024.
  - Lào Cai and Hà Đông: 52–79%.
  - 2025: 44–61% for all five, because the ISD history ends on 2025-08-24.
  - Total: 14,724 station-days (Lào Cai 2,268; Nam Định 3,402; Hà Đông 2,269; Phù Liễn 3,393; Thái Nguyên 3,392). Mean annual totals were 1,714–1,906 mm.
- Duplicates: 2,250 station-days carry two different 24-hour values, always from the same FM-12 SYNOP report.
  - 2,223 differ by ≤ 0.5 mm: the same amount encoded twice (`AA2` rounded to whole mm, `AA3` at 0.1 mm).
  - 27 differ by more than 0.5 mm, including one `999.1` value.
  - Proposed rule: within 0.5 mm, keep the 0.1 mm-resolution value; otherwise quarantine; values ≥ 999 are invalid. Under that rule about 14,697 station-days are usable and 27 are quarantined.
- No code, contract, or DW changes yet. The daily-rain table, its grain (source × station × UTC day ending 12:00), and the duplicate rule still need approval before implementation.

## Business specifications for the next N1/N2 tasks — 2026-10-21

- Related work: workbook IDs 19, 20, 9, 17, 8, 4, 5. Documentation only: new `docs/task-specs-weeks-2-5.md`, plus a source-table row in `docs/business-requirements.md`. No code, SQL, or data changes.
- Research performed: read the business requirements, warehouse schema, architecture, DDL, and the DW/BI/ETL skills. Web sources consulted: Microsoft Learn (Power BI PostgreSQL connector), NCEI/NESDIS (ISD superseded by GHCNh; no ISD updates beyond 2025-08-24), ECMWF forum (CDS item limits and licence errors), and Resolution 202/2025/QH15 coverage (63 → 34 provinces).
- Sampled 6 GHCNh files (`by-year/<YYYY>/psv/GHCNh_VMM000<WMO>_<YYYY>.psv`): 2025 for 48803, 48823, 48825, 48826, 48831, and 2024 for 48826. Results: 24-hour precipitation on 358–360 days in 2025, data through 2025-12-31, one value per day, no hourly precipitation. Phù Liễn 2024 total 2,112 mm, identical to ISD. The specification therefore proposes GHCNh instead of ISD for ID 9.
- Verified versus unverified: four of the province mergers relevant to the research points were confirmed by search results (Phú Thọ + Vĩnh Phúc + Hòa Bình; Tuyên Quang + Hà Giang; Lào Cai + Yên Bái; Thái Nguyên + Bắc Kạn); Hải Dương into Hải Phòng was confirmed by Wikipedia. Ninh Bình + Hà Nam + Nam Định is unconfirmed. GeoJSON candidates, licences, and current CDS limits were not verified. Rainy-day (1 mm) and hot-day (35 °C) thresholds are proposals.
- Update 2026-10-21 (ID 4): the N1/N2 member decided to use pre-merger (63-province) boundaries, because the study period is almost entirely before 2025-07-01. `docs/task-specs-weeks-2-5.md` §7 now records this decision, the conflict with workbook ID 4/ID 11 and the risk sheet (which ask for post-merger boundaries and old + new names), and the candidate sources (HDX COD-AB earlier version, geoBoundaries, GADM). Licenses were not verified. The workbook was not edited.

## OLAP views (workbook ID 19) — 2026-10-21

- Related work: workbook ID 19; `docs/task-specs-weeks-2-5.md` §2; `docs/warehouse-schema.md` aggregation rules. Thresholds were decided by the N1/N2 member: rainy day ≥ 1.0 mm and hot day ≥ 35.0 °C, both on complete local days.
- Changes:
  - New `sql/02_olap_views.sql` (idempotent `CREATE OR REPLACE VIEW`): `vw_obs_hourly`, `vw_obs_daily`, `vw_calendar_month`, `vw_obs_monthly`, `vw_obs_yearly`, `vw_coverage`. Local calendar (UTC+7).
  - Rules: aggregates stay within one source × location; means roll up from sums and counts; wind direction uses a vector mean excluding calm or missing-direction hours; incomplete days give NULL flags; a month with no complete day gives NULL rainy/hot counts, not 0. Pressure is compared within one source only, because surface and sea-level pressure share a column.
  - `sql/01_init_postgres.sh` applies the views to new databases and is now executable (mode 100755). `docker-compose.yml` mounts the view file. `Dockerfile` and `.dockerignore` copy it into the test image.
  - New `tests/test_olap_views.py` (3 integration tests; synthetic March 2023; transaction rolled back). Covered: rain sum and inclusive 1 mm flag, hot flag, vector wind mean near north, incomplete day → NULL flag, missing Meteostat rain → NULL instead of 0, monthly mean from sums/counts, yearly = Σ monthly, and sources kept separate.
- Pre-existing bug found and fixed: on this macOS Docker Desktop host, the postgres entrypoint tried to execute the non-executable `01_init_postgres.sh` and failed with `/bin/bash: bad interpreter: Permission denied`, which aborts first-time initialization. The original file from `HEAD` reproduced the failure, and `chmod +x` fixed it. This explains the half-initialized volume found earlier today (`airflow` database present, no `weather_dwh`).
- Checks:
  - `ruff check`/`format --check` passed (22 files); `bash -n` passed; `docker compose --env-file .env.example config --quiet` passed.
  - `make test` → 61 passed, 12 deselected.
  - `make test-integration` → 12 passed.
  - Fresh throwaway `postgres:16-alpine` container (`--network none`) with the three mounts: initialization finished and created all 6 views.
  - Live DW: views applied twice without error.
- Real-data reconciliation (full DW, 788,861 rows):
  - Row counts: hourly 701,376 + 87,485; daily 29,232 Open-Meteo + 3,654 Meteostat; monthly 968 + 121; yearly 88 + 11. The extra local day/month/year comes from the 2026-01-01 local boundary (7 hours).
  - The hourly → daily → monthly → yearly chain against the fact table shows 0 difference in rain, row counts, temperature sums, and temperature counts for all 9 source × location series.
  - Spot check, HAN Open-Meteo July 2024: direct SQL 541.30 mm and 27 rainy days, matching the monthly view.
  - Coverage 2017–2025: Open-Meteo 100%; Meteostat HAN 99.55–99.97% records, 0% rain.
  - Wind direction range 0.0–359.9 after fixing a rounding edge case that produced 360.0.
  - Full-chain query 1.6 s; single views 0.15–0.8 s.
- Observations: Open-Meteo averages 156–211 rainy days per location-year. Meteostat HAN shows 57 hot days in 2024 versus 23 for Open-Meteo at HAN (station maximum temperatures are higher than the grid).
- Limits: no Power BI artifact yet (ID 20). Local-year boundary rows (2016 partial, 2026 = 7 hours) need a year filter in BI. Views are plain (not materialized); ID 26 owns tuning.

## Power BI v1 build guide (workbook ID 20) — 2026-10-21

- Related work: workbook ID 20; `docs/task-specs-weeks-2-5.md` §3. Documentation only. Power BI Desktop cannot run on this macOS host, so **no `.pbix` was created and nothing was tested in Power BI**.
- Changes: new `docs/powerbi-dashboard-v1.md` covering:
  - two ways to reach the DW from Windows (local rebuild, or LAN connection to port 5434);
  - Import of 3 dimensions plus `dim_time`, `vw_obs_daily`, `vw_obs_hourly`, and `vw_coverage`;
  - relationships, date table, and hierarchy;
  - DAX measures that return BLANK across multiple sources and average location totals instead of summing them;
  - five pages mapped to slice, roll-up/drill-down, and dice;
  - a reconciliation table.
  The specification links to the guide.
- Reference values: computed by read-only SQL on the live views (for example, HAN Open-Meteo 2024: 2,660.5 mm, 175 rainy days, 23 hot days, mean 24.62 °C; Meteostat HAN 2024: 25.16 °C, 57 hot days, rain BLANK). These are expected values for the future Power BI check, not evidence that the dashboard works.
- Limits: DAX syntax and behaviour are unverified in Power BI Desktop; Option B requires a firewall change on the host; the `.pbix` storage location is undecided.

## Station daily rainfall from NOAA GHCNh (workbook ID 9) — 2026-10-21

- Related work: workbook ID 9 (NOAA part); `docs/data-contract.md` "Station daily rainfall"; `docs/task-specs-weeks-2-5.md` §4. The N1/N2 member approved: GHCNh instead of ISD; stations 48826→HPH, 48831→TNG, 48803→LCA, 48825 Hà Đông→HAN, 48823 Nam Định→NBI as a substitute; a separate daily table. Pre-merger province scope is unchanged.
- Changes:
  - `scripts/station_config.py`: new `GHCNH_RAIN_STATIONS` mapping, kept separate from the Meteostat canonical mapping; 48823 added to the registry.
  - New `scripts/ingest_ghcnh.py`: raw bytes plus a JSON manifest (URL, retrieval time, size, SHA-256, rows); bounded retries for timeouts/429/5xx with `Retry-After`; 404 recorded; skip only when the stored object matches its manifest size; exit 1 on failure.
  - New `scripts/transform_ghcnh_to_silver.py`: 24-hour totals at 12:00 UTC; quality codes 0/1/4 accepted; trace kept with `is_trace`; accumulated/deleted/missing/estimated/incomplete codes, out-of-range values, and off-schedule reports rejected and counted; duplicates resolved (identical collapsed, conflicting quarantined); unsupported extremes (≥ 300 mm without sub-daily support ≥ 10%) quarantined; per station-year coverage in the DQ report.
  - New `scripts/load_station_daily_rain.py`: one transaction per station-year, set-based retraction plus upsert on `(source_key, station_key, rain_day_utc)`.
  - New `sql/03_station_daily_rain.sql`: `dim_source` row `GHCNh`, `fact_station_daily_rain` with UNIQUE and CHECK constraints and indexes, and `vw_rain_station_vs_model`. The view compares with Open-Meteo over the same `(D-1 12:00, D 12:00]` UTC window and requires 24 model hours.
  - `sql/01_init_postgres.sh` now applies every numbered `NN_*.sql` migration in order. Compose, `Dockerfile`, and `.dockerignore` include the new SQL file.
  - New `tests/test_ghcnh.py`: 7 unit and 2 integration tests (rules and window, rejects, unsupported versus supported extremes, separate mapping, raw bytes and manifest, rerun skip, transient retry and permanent failure, missing Bronze with a manifest, DW rerun and retraction, view window alignment).
  - Docs: data contract, naming conventions (GHCNh keys; ISD marked unused), warehouse schema, specification status.
- Checks:
  - `ruff check`/`format --check` passed.
  - `make test` → 68 passed, 14 deselected.
  - `make test-integration` → 14 passed.
  - Migration applied twice without error.
- Real data, 5 stations × 2016–2025, run on this machine:
  - Ingest: 50/50 files stored (about 2.5 MB each), 0 not found, 0 failures. The run took 8,095 s, mostly while the host slept (no object written 08:18–09:40 UTC). Later runs used `PYTHONUNBUFFERED=1` in the local (non-repository) runner so progress is visible.
  - Silver: 50 station-years, 0 failures, 14 s. Coverage was 95.89–99.18% of days in every station-year, versus 44–79% from ISD for Lào Cai and Hà Đông; 0 conflicting duplicates. One unsupported extreme was quarantined: 48823, 2023-10-13, 700.0 mm. Two `999.x` values (Phù Liễn 2019-10-09, 2020-10-18) were rejected as out of range. **Correction (2026-10-22):** both carry quality code `2` and were rejected by the quality rule; the DQ report `silver/quality_reports/dq_report_ghcnh_20261021_095201.json` shows `out_of_range_rejected` 0 and `quality_rejected` {'2': 22}, so the ≥ 999 guard was not exercised on real data.
  - DW: 17,866 rows; a 2024 rerun and the post-rule reload both left counts consistent, and the 700 mm day was retracted. 2025 station-year totals equal an independent parse of the same GHCNh files done earlier (for example Thái Nguyên 2,888.2 mm, Phù Liễn 1,525.2 mm). Phù Liễn 2024 = 2,111.5 mm, and ISD 2024 = 2,112 mm.
  - Comparison view (2016–2025): Pearson correlation of daily totals HAN 0.57, HPH 0.63, LCA 0.49, NBI 0.61, TNG 0.56. Agreement on rainy day (≥ 1 mm) is 60–74%. Open-Meteo has 1.5–2.1× more rainy days than the gauges (for example LCA 2,394 vs 1,138), consistent with reanalysis drizzle. Mean annual gauge totals were 1,741–1,932 mm.
- Limits: daily resolution only (no hourly gauge rain); the 300 mm / 10% extreme rule rests on one observed failure and should be reviewed by the team; Hà Đông and Nam Định are not co-located with their research points; `observation_type`-style provenance for this table is the `station_role` column; no Power BI page uses the view yet.

## RainNext24h label export to Gold ML (workbook ID 22) — 2026-10-21

- Related work: workbook ID 22; `docs/rain-label.md` v0.2. The N1/N2 member approved: features from Open-Meteo only (GHCNh used later for verification), the dataset as Parquet in Gold ML, and EDA as a notebook. This work belongs to N3's ownership; it was done by the N1/N2 member with the agreement recorded here.
- Changes: new `scripts/export_rain_labels.py`. It reads Silver Open-Meteo hourly data for the 8 canonical locations and refuses to publish if any location-month object is missing. It applies `build_rain_labels` and `assign_splits` (1 mm, `(t, t+24h]`, 24 valid hours, UTC splits with boundary purge) and writes `gold/ml_labels/rain_next24h_labels_v1.parquet`, which keeps excluded rows with a reason, plus a JSON manifest with scope, counts, and positive rates. New `tests/test_export_rain_labels.py` (2 unit tests: a full synthetic year with exact Yes/No origins around a rain hour and boundary purge; refusal on an incomplete Silver snapshot). Naming conventions and the rain-label document were updated.
- Checks: `ruff check`/`format --check` passed; `make test` → 70 passed, 14 deselected.
- Real data: `export_rain_labels.py --start-year 2016 --end-year 2025` took 16 s. Results:
  - 701,376 rows, 700,800 usable, 576 `boundary_purge` (24 h × 8 locations × 3 boundaries).
  - train 560,832 rows (2016-01-01T00Z → 2023-12-30T23Z), positive rate 50.89%.
  - validation 70,080 rows, 49.42%.
  - test 69,888 rows (through 2025-12-30T23Z), 49.03%.
  These match the label-module run on independently downloaded API data earlier today.
- Limits: labels are reanalysis rainfall, not gauge observations. No features yet (ID 23) and no EDA (ID 21).

## RainNext24h EDA notebook (workbook ID 21) — 2026-10-21

- Related work: workbook ID 21 (N3's task, done by the N1/N2 member as agreed); `docs/rain-label.md` leakage rules.
- Changes:
  - New `scripts/rain_eda.py`: joins labels with Silver Open-Meteo variables at origin t plus trailing derivations (rain over the past 24 h, 24 h pressure change, dew-point depression), computed per location on a gap-checked grid. It also provides class balance, month × location rates, missingness, target distribution, standardized mean differences, and binned rates; summaries used for feature choice default to the train split.
  - New `tests/test_rain_eda.py` (4 tests): only past/present hours are used, and changing data after t leaves t unchanged; a gap makes the trailing window missing; split filtering; duplicate rejection.
  - New `notebooks/eda_rain_next24h.ipynb`, committed with executed outputs.
  - New `requirements-notebook.in`/`.txt` (88 hashed pins, uv 0.8.22, universal for Python ≥ 3.10).
  - `Makefile` targets `notebook-setup` and `notebook-run`; `.gitignore` ignores `.venv-*/`; `docs/development.md` notebook section.
  - Charts follow the validated reference palette (single-series blue, sequential blue heatmap, blue/red diverging for signed SMD). Every chart is accompanied by its table. Charts were rendered and inspected; overlapping labels, a misleading zoomed y-axis on the hourly chart, and gridlines over bars were fixed.
- Checks:
  - `ruff check`/`format --check` passed; `make test` → 74 passed, 14 deselected.
  - `make notebook-setup` succeeded on macOS x86_64 / Python 3.12.4 (72 s).
  - `make notebook-run` executed all 9 code cells against the real Silver/Gold data with 0 errors.
- Findings on train 2016–2023 (computed in the notebook):
  - Yes share is 50.89% train, 49.42% validation, 49.03% test.
  - Strong seasonality: the location mean is 78.2% in August and 25.6% in December. Lào Cai stays at 53–79% all year.
  - The origin hour changes the Yes share by under 1 percentage point (50.5–51.3%).
  - Next-24 h rain: 27.8% exactly zero, median 1.1 mm, q99 55.2 mm.
  - No missing values at origin apart from the first trailing hours of 2016.
  - Largest separations (|SMD|): dew point 0.81, past-24 h rain 0.76, humidity 0.75, pressure 0.73 (lower before rain), dew-point depression 0.70, cloud cover 0.66. Wind speed and solar radiation are near 0.
  - Past-24 h rain bins rise from 17% (< 0.1 mm) to 92% (≥ 50 mm).
- Limits: reanalysis inputs only; no feature set or model yet (IDs 23, 28). Notebook figures are static (no interactive layer). Validation/test statistics were not used for any choice.

## README and architecture status refresh — 2026-10-21

- Related work: documentation only; no workbook task status change.
- Objective/scope: bring the summary documents in line with the work logged above (IDs 6, 7, 9, 19, 20, 21, 22 and the 2026-10-21 rebuild).
- Changes:
  - `README.md`: rewrote "Trạng thái hiện tại" (was the 2026-10-10 state saying Silver/Gold, DW loader, and OLAP did not exist). Added the two new docs to the document table and the new scripts, SQL files, notebook, and notebook lockfile to the repository tree.
  - `docs/architecture.md`: component table now reflects the implemented ETL, station sources, OLAP views, ML labels/EDA, and remaining gaps (ERA5, GeoJSON, Bronze manifests ID 8, DAG ID 17, features ID 23, model, PBIX).
  - `docs/rain-label.md`: status sentence now points to the EDA notebook.
- Checks: internal Markdown links and code fences in the three files checked with a local script (0 missing, 0 unbalanced). No services, tests, or data operations were run; figures quoted in the README are copied from the dated entries above.
- Limits: the 2026-10-08 Inventory and Discrepancies tables at the top of this log are kept as a historical snapshot; some rows there (labels, station selection, Silver/Gold ETL, DW loading) were resolved later, as recorded in the dated entries.

## RainNext24h feature dataset (workbook ID 23) — 2026-10-21

- Related work: workbook ID 23 (N3's task, done by the N1/N2 member as agreed); `docs/rain-label.md` v0.2 leakage rules; feature source decision of 2026-10-21 (Open-Meteo only).
- Objective/scope: leakage-free lag, rolling, pressure-tendency, and hour/month features for every usable RainNext24h label, 8 canonical locations, origins 2016–2025.
- Changes:
  - New `scripts/build_rain_features.py`. It reads the ID 22 labels from Gold, refuses labels of another label version, reads Silver Open-Meteo (refusing incomplete snapshots, as in ID 22), and computes 22 features per location on a gap-checked hourly grid. It writes one Parquet file per split to `gold/ml_features/` and a manifest with the ordered features, units, versions, split boundaries, counts, and missing values.
  - New `tests/test_rain_features.py` (6 tests): exact values at t, with every value after t changed leaving all features unchanged; missing history and a missing hour give NaN, while a lag that skips the gap stays valid; no lag crosses locations; UTC+7 hour/month at a year boundary; only usable labels kept and the target amount is not a feature; mock-MinIO export of split files and manifest.
  - Docs: `docs/rain-label.md` feature dataset v1 section; `docs/NAMING_CONVENTIONS.md` ML dataset row now matches the implemented keys (the old path pattern `cart_rain_prediction_<split>_<version>` disagreed with its own examples); `README.md`, `docs/architecture.md`.
  - Workbook ID 23: Đang làm, 90.
- Checks:
  - `ruff format`/`ruff check` passed on the new files.
  - `make test` → 80 passed, 14 deselected. The first run failed one test because the test's expected value was wrong (0.5 mm, not 1.0 mm, over hours 28–30); the code was unchanged.
- Data (real): `build_rain_features.py --start-year 2016 --end-year 2025` in the Airflow image, 17 s.
  - 700,800 rows, equal to the usable labels of ID 22: train 560,832 (Yes 50.89%), validation 70,080 (49.42%), test 69,888 (49.03%).
  - Missing feature values only in the first 24 origin hours of 2016: 192 train rows; validation and test have none.
  - Objects: train 16.9 MB, validation 2.0 MB, test 2.0 MB, plus `cart_rain_features_v1.json`.
  - Independent check: 6 random rows (3 train, 3 validation) recomputed from the raw Silver monthly objects matched for `rain_past_6h_mm`, `rain_past_24h_mm`, `pressure_change_3h_hpa`, `humidity_percent_lag6h`, and `hour_local`.
- Limits: features and labels both come from Open-Meteo reanalysis, so results are retrospective and the features are not proven available in real time. No imputation, encoding, or feature selection yet (ID 28, train only). The feature list was not reviewed by the team. A partial year range (for example only 2024) has no earlier history, so its first 23–24 origins per location miss the long-window features.
- Next step: ID 28, train and tune CART on train with selection on validation 2024.

## CART training and tuning (workbook ID 28) — 2026-10-21

- Related work: workbook ID 28 (N3's task, done by the N1/N2 member as agreed); `docs/rain-label.md` v0.2. Decisions recorded on 2026-10-21: select by validation F1 of the Rain class; drop rows with missing features; no refit on train + validation.
- Objective/scope: fit CART on train 2016–2023 and select parameters and the probability threshold on validation 2024. The test split was not read.
- Changes:
  - New `scripts/train_cart.py`. It reads the ID 23 manifest plus the train and validation files (`read_split` refuses `test`), drops rows with missing features, and builds a pipeline (unscaled numeric features + one-hot `location_code` → `DecisionTreeClassifier`). It fits a 120-configuration grid in parallel, selects the configuration and threshold, computes baselines, and writes `gold/ml_models/cart_rain_next24h_v1/model.joblib` and `report.json`.
  - New `tests/test_train_cart.py` (4 tests): undefined metrics are `None`; tie handling for configuration and threshold; the test split is refused and never requested; reproducible training whose saved model reloads and predicts identically.
  - Docs: `docs/rain-label.md` CART model v1 section; `docs/NAMING_CONVENTIONS.md` `ml_models` row; `README.md`; `docs/architecture.md`. Workbook ID 28: Đang làm, 90.
- Checks: `ruff format`/`ruff check` passed; `make test` → 84 passed, 14 deselected.
- Data (real): `train_cart.py` in the Airflow image (scikit-learn 1.5.1, 4 CPUs), 212 s.
  - Train: 560,640 rows used, 192 dropped. Validation: 70,080 used, 0 dropped.
  - Best F1 by depth limit: 4 → 0.763, 6 → 0.777, 8 → 0.781, 10 → 0.784, 12 → 0.785. The selection rule (within 0.005 of the best, shallowest wins) chose `max_depth=8`, `min_samples_leaf=50`, `class_weight=balanced`, `ccp_alpha=1e-4`: 88 leaves, a 19 KB artifact.
  - Threshold 0.40 on validation: TP 28,476, FP 9,378, TN 26,066, FN 6,160; precision 0.752, recall 0.822, F1 0.786, accuracy 0.778. At 0.50: F1 0.781, recall 0.775.
  - Baseline F1 on validation: persistence 0.753, location × month rate 0.710, always Yes 0.662; always No has recall 0.
  - Train fit at 0.40: F1 0.793, so the gap to validation is small.
  - Largest importances: `rain_past_12h_mm` 0.65, `dew_point_c` 0.12, `precipitation_mm` 0.10, `rain_past_24h_mm` 0.06, `pressure_hpa` 0.03.
  - Validation breakdown (computed after selection, not used for choices):
    - By location, F1 0.758 (HPH) to 0.812 (TNG).
    - By season, wet season May–Oct F1 0.860 (recall 0.903); dry season Nov–Apr F1 0.602 (recall 0.624).
    - Share of Rain cases detected by rain amount: 1–5 mm 72%, 5–20 mm 87%, ≥ 20 mm 94%.
- Limits:
  - Validation chose both the configuration and the threshold, so its metrics are optimistic.
  - Independent evaluation on test 2025 is ID 29.
  - Inputs and labels are Open-Meteo reanalysis, so the result is retrospective.
  - The improvement over persistence is moderate (+3.3 F1 points), and dry-season performance is weak.
  - The feature list, selection rule, and threshold have not been reviewed by the team.
- Next step: ID 29, evaluate the frozen model and threshold once on test 2025; then ID 30, tree figure and rules.

## CART test 2025 evaluation (workbook ID 29) — 2026-10-21

- Related work: workbook ID 29 (N3's task, done by the N1/N2 member as agreed); `docs/rain-label.md` v0.2. The workbook mentions threshold adjustment together with test; as documented in `rain-label.md`, the threshold (0.40) was chosen on validation in ID 28 and only applied here.
- Objective/scope: one evaluation of the frozen ID 28 model on test 2025 (all 8 locations), with baselines and breakdowns.
- Changes:
  - New `scripts/evaluate_cart_test.py`. It loads the model and `report.json` and checks the feature list against the feature manifest. It verifies that the model reproduces the stored validation confusion matrix before reading test, refuses to run if `test_evaluation.json` exists, and writes `test_evaluation.json` and `test_predictions.parquet` next to the model.
  - New `tests/test_evaluate_cart_test.py` (3 tests): frozen threshold, metrics recomputable from the saved predictions, refusal of a second run; a swapped model is rejected before the test file is requested; feature-order mismatch is rejected.
  - `tests/test_train_cart.py` fixture now includes target window columns, as in the real feature files.
  - Docs: `docs/rain-label.md` test 2025 section; `docs/NAMING_CONVENTIONS.md` model row; `README.md`; `docs/architecture.md`. Workbook ID 29: Đang làm, 90.
- Checks:
  - `ruff format`/`ruff check` passed; `make test` → 87 passed, 14 deselected.
  - Before passing, two runs failed on test-fixture issues: missing target columns, and a swapped depth-1 tree that happened to reproduce the same confusion matrix on the one-split synthetic rule, so the test now uses an always-Rain model.
  - A third failure was a real bug: integer month keys in the evaluation dict did not survive the JSON round trip; keys are now strings.
  - None of these runs touched real test data.
- Data (real): `evaluate_cart_test.py` in the Airflow image, 10 s, run once.
  - Validation reproduction passed. Coverage: 69,888 of 69,888 test labels.
  - At threshold 0.40: TP 27,254, FP 8,782, TN 26,840, FN 7,012; precision 0.756, recall 0.795, F1 0.775, accuracy 0.774.
  - Baseline F1: persistence 0.726, location × month rate 0.682, always Yes 0.658; always No has recall 0.
  - By location, F1 0.763 (HBI) to 0.785 (TUQ). By season, wet May–Oct F1 0.835 (recall 0.878), dry Nov–Apr F1 0.621 (recall 0.600). January 0.524, June 0.904.
  - Share of Rain cases detected: 1–5 mm 67%, 5–20 mm 84%, ≥ 20 mm 94%.
  - Model SHA-256 `1f667b91…94b6` (full value in `rain-label.md`).
- Limits:
  - Retrospective result on Open-Meteo reanalysis; labels are reanalysis rain, not gauge rain. A GHCNh station-rain check of predictions is possible later.
  - Test F1 is 1.1 points below validation. The model beats persistence by 4.9 F1 points on test, but performs poorly in the dry season and on light rain.
  - Not yet reviewed by the team.
  - Any new model version must not be evaluated on test 2025 as an independent result.
- Next step: ID 30, tree figure and rules from this artifact; ID 31, predictions into the DW.

## CART tree figure and rules (workbook ID 30) — 2026-10-21

- Related work: workbook ID 30 (N3's task, done by the N1/N2 member as agreed); `docs/rain-label.md`; the artifact evaluated in ID 29.
- Objective/scope: draw and explain the tree and extract classification rules from the evaluated model, with real feature names and units. The model was not refitted or changed.
- Changes:
  - New `scripts/cart_rules.py`, which:
    - loads the model, report, and model SHA-256;
    - merges each root-to-leaf path into one condition per variable, using whole numbers for hour/month and translating one-hot columns into `location = X`, `location ≠ X`, or `location not in {…}`;
    - builds a leaf rule table with train rows, observed Rain share, model probability, and class at the frozen threshold;
    - builds a node table for the top levels and the decision path of one row.
  - New `tests/test_cart_rules.py` (4 tests): condition merging and location/integer wording; leaf rules cover every row and match `predict_proba`, and the learned synthetic rule is recovered with its units; node counts and the yes-branch are verified independently; the decision path length equals the leaf depth.
  - New `notebooks/cart_tree_rules.ipynb`, committed with executed outputs. Its builder lives outside the repository, like the EDA notebook's. The notebook contains:
    - a model SHA-256 check against `test_evaluation.json`;
    - a tree figure of 3 levels in the validated sequential palette, with its node table;
    - the top 10 Rain and No-rain rules;
    - a coverage summary;
    - an importance chart and table;
    - three test-2025 examples chosen by fixed rules;
    - key numbers, an interpretation, and all 88 rules.
  - `Makefile` `notebook-run` accepts `NOTEBOOK=…` (default unchanged), documented in `docs/development.md`.
  - Docs: `docs/rain-label.md` tree and rules section; `README.md`; `docs/architecture.md`. Workbook ID 30: Đang làm, 90.
- Checks:
  - `ruff check`/`format` passed; `make test` → 91 passed, 14 deselected.
  - The first run failed one test whose expected text was too strict (the learned threshold was 79.92 %, not ≥ 80); the code was unchanged.
  - `make notebook-run NOTEBOOK=cart_tree_rules.ipynb` → 0 errors, about 9 s. Both figures were rendered and inspected.
  - The first interpretation draft stated the root branches the wrong way round. It was corrected against the printed node table, and every number in the interpretation was rechecked against cell outputs, including the shallowest split depth of each variable (Lào Cai 5, month 6, `hour_local` 4).
- Data (real): train 2016–2023, 560,640 rows, for the rule statistics; test 2025 predictions from ID 29 for the three examples only.
  - Root split: past-12-hour rain ≤ 0.35 mm (26% Rain, 288,873 rows) versus > 0.35 mm (77% Rain, 271,767 rows).
  - 52 of 88 leaves predict Rain; the 10 largest rules cover 45.6% of train rows; rules have 4–7 merged conditions.
  - Leaf probability versus observed train share differs by at most 0.009.
  - Examples: largest-rain TP at Hà Nội (144.6 mm, p 0.97); highest-probability FP at Hà Nội (0.7 mm, p 0.97); largest-rain FN at Ninh Bình (57.9 mm, p 0.39).
- Limits:
  - The figure shows 3 of 8 levels.
  - Importance and rules describe correlations in reanalysis data, not causes.
  - Surface pressure partly encodes elevation/location.
  - The examples are illustrations, not a full error analysis (ID 36).
  - Not yet reviewed by the team.
- Next step: ID 31, load predictions and `dim_model` into the DW.

## Correction to the ID 30 error interpretation — 2026-10-21

- Related work: workbook ID 30; follow-up to the entry above, at the user's request to check the "missed rain" explanation.
- Problem found:
  - The first notebook interpretation (and the report to the user) generalized from one example. It called "rain without warning" the model's main weakness, based on the largest-rain test miss.
  - That example was selected for its size, so it is not typical. Its hourly data also contradict the wording: rain began one hour after *t*, after 0.1 mm of drizzle, under a fully overcast sky with a northerly wind and pressure 6.9 hPa higher than a day earlier.
  - The verbal report also quoted 89% humidity at *t*; that value was six hours earlier, and humidity was 76% at *t*.
  - Separately, `docs/rain-label.md` said wind direction was excluded from features v1 because the EDA showed no separation, but the EDA never assessed wind direction.
- Changes:
  - `scripts/cart_rules.py`: `hours_to_first_rain` (first rainy hour in the target window, per location on a gap-checked grid) and `missed_rain_profile`.
  - `tests/test_cart_rules.py`: +2 tests (onset with a gap and no-rain cases; profile counts).
  - `notebooks/cart_tree_rules.ipynb`:
    - adds a typical-miss example (probability 0.30–0.40, under 5 mm, closest to the median miss: Lào Cai, 3.0 mm, p 0.36);
    - adds the hourly Silver context of the extreme miss;
    - adds a §5 error profile on validation 2024 with an onset recall chart;
    - corrects the interpretation.
  - `docs/rain-label.md`: error profile; wind-direction statement corrected.
- Checks:
  - `ruff` passed; `make test` → 93 passed, 14 deselected.
  - `make notebook-run NOTEBOOK=cart_tree_rules.ipynb` → 0 errors after one fix (an object-dtype `round` error in the new context cell).
  - The new chart was inspected. The notebook text was checked against the printed hourly table.
- Data (real, validation 2024; describes errors only, no model or threshold change):
  - Missed: 6,160 of 34,636 Rain cases (17.8%); 64.2% of misses under 5 mm; 55.3% with probability 0.30–0.40; 36.1% after a dry 24 h.
  - Recall after a dry 24 h: 17.0% overall, 2.6% in the dry season, 31.7% in the wet season. Recall after a wet 24 h: 87.7%.
  - Recall by onset: 1–3 h 91%, 4–6 h 71%, 7–12 h 66%, 13–18 h 61%, 19–24 h 45%.
  - For reference, on validation a threshold of 0.35 gives recall 0.901, F1 0.784, and 4,423 more false alarms than 0.40. This trade-off is for ID 36; v1 keeps 0.40.
- Limits: improvement options are recorded but not done:
  - feature v2 with wind direction, humidity/cloud trends, and neighbouring-location rain, selected on validation only;
  - a threshold comparison (ID 36);
  - numerical weather prediction inputs (out of scope).
  Any v2 measured on test 2025 is no longer an independent result.

## Predictions and dim_model in the DW (workbook ID 31) — 2026-10-21/22

- Related work: workbook ID 31 (N3 with N2 support; done by the N1/N2 member as agreed); `docs/warehouse-schema.md`; `docs/rain-label.md`. Decisions of 2026-10-21: load only test 2025 predictions (out-of-sample for the frozen model); compute actual rain in a view from the observation fact instead of storing it.
- Changes:
  - New migration `sql/04_predictions.sql` (idempotent):
    - `dim_model`: versions, label and feature sources, both thresholds, horizon, training window, hyperparameters as JSONB, artifact URI and SHA-256, validation and test metrics.
    - New columns in `fact_weather_prediction`: `model_key`, `origin_utc`, target window, `horizon_hours`, `probability_threshold`, `evaluation_split`. A guard refuses to migrate legacy rows without these values. Legacy columns are kept and filled.
    - Constraints: unique key `(model_key, location_key, origin_utc)`; checks on window, probability, and split.
    - Indexes on the prediction foreign keys and origin, and `idx_fwo_location_source_ts` on the observation fact.
    - View `vw_prediction_vs_actual`: actual rain from the label source over `(target_start, target_end]`; actual and outcome are NULL unless all 24 hours exist.
  - New `scripts/load_predictions.py`:
    - upserts `dim_model`;
    - checks model version, threshold, class consistency, duplicates, locations, and window length;
    - derives Vietnam-local date/time keys;
    - replaces the (model, split) scope in one transaction (retract and upsert).
  - New `tests/test_load_predictions.py`:
    - 7 unit cases: local keys across a day boundary, 4 rejection cases, model/evaluation mismatch;
    - 3 integration tests, rolled back: migration applied twice; load, rerun and retraction; view window boundaries (rain stamped at the origin excluded, at the window end included), a NULL outcome for an incomplete window, and unique and window constraints.
  - `Dockerfile`, `.dockerignore`, `docker-compose.yml` include the migration. Docs: `docs/warehouse-schema.md` (table rows, resolved prediction grain and model gaps), `README.md`, `docs/architecture.md`. Workbook ID 31: Đang làm, 90.
- Checks:
  - `ruff` passed.
  - `make test` → 99 passed, 17 deselected.
  - `make test-integration` → 17 passed.
- Data (real, local DW):
  - `04_predictions.sql` applied twice to `weather_dwh` without error.
  - `load_predictions.py` loaded 69,888 rows in 23 s; a rerun took 16 s and gave the same `model_key` with 0 retracted.
  - Prediction rows: 69,888 = 69,888 unique keys, origins 2025-01-01T00Z to 2025-12-30T23Z.
  - `dim_model`: 1 row; SHA-256 prefix `1f667b913657`; `test_f1` 0.7753.
  - The view returned outcomes for all rows (0 unknown) in about 1.4 s.
- **Finding: label bug in ID 22.**
  - The view's confusion matrix (TP 27,467 / FP 8,569 / TN 26,617 / FN 7,235) did not match ID 29 (TP 27,254 / FP 8,782 / TN 26,840 / FN 7,012).
  - All 436 differing rows have a 24-hour total of exactly 1.0 mm. `scripts/rain_labels.py` sums floats, which gives values such as 0.99999999999999, so it labels them No. PostgreSQL sums NUMERIC exactly and labels them Yes, which follows the contract (≥ 1 mm). Otherwise the two sums differ by at most 3e-14 mm.
  - Across all usable labels, 5,147 (0.734%) are wrong in this direction: train 4,237, validation 474, test 436. No error occurs in the opposite direction.
  - Corrected Yes rates: train 51.64% (reported 50.89%), validation 50.10% (49.42%), test 49.65% (49.03%).
  - With corrected test labels and the same frozen model and threshold: precision 0.762, recall 0.792, F1 0.777 (reported 0.756 / 0.795 / 0.775).
  - The earlier "independent" label check used the same function, which is why it did not catch the bug.
- Limits: only test predictions are loaded; there is no scoring DAG (ID 32) and no Power BI page (ID 33). Labels, features, EDA rates, model training, the ID 29 evaluation, and the figures quoted in docs still carry the label bug until the fix agreed on 2026-10-22 (option A: fix labels, rebuild labels/features v2, re-score the frozen v1 on corrected test labels, keep v1 unchanged).

## Label float-sum fix and corrected re-score (IDs 22, 23, 21, 29, 30, 31) — 2026-10-22

- Related work: the label bug found by the ID 31 reconciliation; option A agreed on 2026-10-22: fix labels, rebuild labels/features v2, re-score the frozen v1 on corrected test labels, keep model v1 unchanged.
- Cause: `build_rain_labels` used float rolling sums. For long series, windows whose true total is exactly 1.0 mm could come out as 0.99999999999999 and were labelled No. The same applied to trailing rain features and the EDA rain sums.
- Changes:
  - `scripts/rain_labels.py`: `RAIN_SUM_DECIMALS = 3` and `round_rain`; window sums are rounded before the threshold and storage; `LABEL_VERSION = "rain_next24h_v0.2.1"` (definition unchanged).
  - `scripts/build_rain_features.py`, `scripts/rain_eda.py`: trailing rain sums use `round_rain`.
  - Default dataset versions: labels v2 and features v2. The v1 objects are kept because model v1 was trained on them.
  - New `scripts/rescore_cart_corrected_labels.py`: requires the frozen model to reproduce the stored test probabilities (within 1e-12) and classes exactly on the v2 features, then writes `test_evaluation_corrected_labels.json` once. It also records validation metrics on corrected labels; nothing is reselected.
  - `scripts/load_predictions.py`: `dim_model` test metrics come from the corrected re-score when it exists.
  - Tests:
    - Regression test with a real 28-hour Hà Nội 2025 slice. It was confirmed to fail on the v0.2 code (0.9999999999999999, No) and pass on v0.2.1 (1.0, Rain). A first attempt with ten 0.1 mm hours did not reproduce the bug and was replaced.
    - A feature rounding test.
    - 2 re-score tests: corrected metrics with unchanged predictions and a single run; refusal when inputs change or labels are old.
  - Notebooks `eda_rain_next24h.ipynb` and `cart_tree_rules.ipynb` were re-executed on labels/features v2. The tree notebook's hard-coded figures were updated after rechecking the outputs. The typical-miss example printed probability 0.40 because of 2-decimal rounding; it now prints 3 decimals (0.397). (Clarified 2026-10-22: on corrected labels the fixed selection rule also picked a different case, Ninh Bình 2025-01-26, 2.9 mm, instead of Lào Cai 2025-03-28, 3.0 mm, p 0.36.)
  - Docs: `docs/rain-label.md` (implementation fix, corrected rates and test table, corrected validation figures, profile), `docs/NAMING_CONVENTIONS.md`, `README.md`. Workbook IDs 21, 22, 23, 28, 29, 30, 31 notes updated.
- Checks:
  - `ruff` passed; `make test` → 103 passed, 17 deselected.
  - Both notebooks ran with 0 errors.
- Data (real):
  - Labels v2: 701,376 rows, 700,800 usable, 576 purged. Yes rate: train 51.64%, validation 50.10%, test 49.65%.
  - Features v2: 700,800 rows; missing values only in the first 192 train rows.
  - Re-score: 436 test labels changed; predictions are identical. Precision 0.762, recall 0.792, F1 0.777, accuracy 0.774 (TP 27,467 / FP 8,569 / TN 26,617 / FN 7,235). This equals the DW view `vw_prediction_vs_actual` exactly.
  - Corrected validation at 0.40: precision 0.760, recall 0.819, F1 0.789.
  - Corrected test baselines: persistence F1 0.729, location × month 0.684, always Yes 0.664.
  - `load_predictions.py` rerun: 69,888 upserted, 0 retracted; `dim_model` test_f1 0.7766. `validation_f1` stays 0.7857, the value from the report on v0.2 labels.
  - EDA on v2: August 78.5% and December 26.3% Yes; hourly range 51.3–52.0%; top separations unchanged (dew point, past-24-hour rain, humidity).
  - Tree notebook profile on corrected validation: 18.1% of Rain missed; 64.8% of misses under 5 mm; 54.8% at probability 0.30–0.40; recall after a dry 24 h 16.6% (dry season 2.5%).
- Limits:
  - Model v1 was selected and fitted on v0.2 labels (0.73% wrong, all exact-1.0 mm windows). It was not retrained, because a retrained model could not claim an independent test 2025 result. A future model should train on labels/features v2.
  - The earlier progress entries keep the v0.2 figures as they were measured.

## Audit of earlier tasks and corrections (IDs 3, 4, 5, 7, 8, 9, 11, 12, 13, 16, 18, 20, 21, 28, 29, 30, 31) — 2026-10-22

- Related work: user request to re-check all earlier tasks for errors or deviations. Four read-only audit agents (ETL, DW/BI, ML, docs/workbook) reviewed code, docs, workbook, MinIO, and `weather_dwh`; major findings were re-verified before fixing. One agent claim was wrong (integration tests had been rerun at ID 31) and was dropped. Proposal PDF, workbook, `docs/business-requirements.md` v0.2.
- Objective/scope: fix verified code defects (A), metadata/data (B), and docs/workbook consistency (C); write proposals for items that need a team decision (D) without approving them. No downloads, no volume reset, no model retraining, test 2025 not used for any choice.
- Changes:
  - ML overwrite guards: `scripts/export_rain_labels.py` (`object_exists`, `refuse_overwrite`, `--overwrite`), `scripts/build_rain_features.py` (guard; `feature_version` column now stamps the requested version, previously always the constant), `scripts/train_cart.py` (refuses an existing model/report; never overwrites a model whose `test_evaluation.json` exists). Before this, `python train_cart.py` with defaults would have retrained on features v2 and replaced model v1. `scripts/evaluate_cart_test.py` reuses `object_exists`. `scripts/rescore_cart_corrected_labels.py` hashes the model bytes and requires the SHA-256 recorded in the evaluation instead of copying it.
  - Ingestion/Silver failure semantics: `scripts/ingest_open_meteo.py` collects per-file failures and raises after the loop (exit 1); retries only HTTP 429/5xx and connection errors; comments, logs, and CLI help translated to English. `scripts/transform_open_meteo_to_silver.py` counts a requested but missing Bronze file as a failure unless `--allow-missing`. Ctrl+C exits 130 in five ETL scripts.
  - `scripts/load_gold_observations.py`: retraction scope is the requested scope (`LOADED_SOURCES` Open-Meteo + Meteostat × requested locations × month) instead of the keys present in the incoming rows; a month missing any requested Open-Meteo Silver object fails before anything is deleted; an empty month is a failure; `load_dwh_fact_observations` with a scope and no rows deletes the scope. Gold `station_key` is nullable `Int64` (was float64). Writes `pressure_type`. `--overwrite` is now `--overwrite/--no-overwrite` (it was a no-op `store_true` with default True).
  - `scripts/transform_ghcnh_to_silver.py`: `extreme_is_supported` compares at the 0.1 mm resolution (`0.1 * 338.0` is 33.800000000000004 in floats).
  - Station identity: `scripts/station_config.py` 48820 is Nội Bài airport (ISD 488200 "NOIBAI INTL", METAR VVNB; 21.221, 105.807, 11.9 m from `isd-history.csv`), role `nearest_available`, 22.0 km from HAN; the other canonical stations carry role `representative` and their distance (7.6, 1.7, 1.6 km; recomputed). Meteostat lists 21.0167, 105.8000, 6 m, but its pressure is 97.7% METAR and its near-hourly temperature comes from ISD 488200. `scripts/seed_dimensions.py` writes `station_role`, `distance_km`.
  - New migration `sql/05_metadata_corrections.sql` (idempotent): `dim_model.evaluation_label_version`, `validation_f1_evaluation_labels`, column comments; `dim_station.station_role`, `distance_km`; `fact_weather_observation.pressure_type` with backfill by source and CHECK `ck_fwo_pressure_type`. `scripts/load_predictions.py` fills the new `dim_model` columns from the corrected evaluation. `sql/init_dwh_schema.sql`: comments for ISO `day_of_week` and `period_of_day` (incl. Trưa) corrected. `docker-compose.yml`, `Dockerfile`, `.dockerignore` include `05`.
  - EDA (ID 21): `scripts/rain_eda.py` month matrix uses the Vietnam local month (was UTC); new `variable_correlation` (Spearman, train) and `strong_pairs`. Notebook builder adds §5b correlation and records that ID 23 kept an integer `month_local` instead of the sin/cos first suggested. `notebooks/cart_tree_rules.ipynb`: the wind-direction idea is now "next feature version (v3)" with the disclosure that it came from a test 2025 case.
  - `requirements-notebook.in`/`.txt`: `joblib==1.5.3` (as in the pipeline image that wrote model v1; was 1.6.0), compiled with `uv pip compile` as in `docs/development.md`.
  - Tests: `tests/conftest.py` applies the base schema and every numbered migration (the real initializer order); `tests/test_integration.py` asserts the final state (5 sources, 9 tables, views, new columns); new tests for overwrite guards (labels, features, model, frozen model), rescore SHA, ingestion failure/no-retry on 400, Silver missing Bronze, GHCNh boundary, retraction scope, missing Open-Meteo location, pipeline failing instead of retracting, Int64 station key, source missing from Silver retracted from the DW (integration), local month, correlation, `dim_model` label versions.
  - Docs: `docs/business-requirements.md` v0.2 (decision status; §5a proposals: Recall rule, boundaries, ERA5/GeoJSON plan, N3 review; five new §6 discrepancy rows), `docs/rain-label.md` (v0.2.1 rates incl. local month, Recall statement, test-informed wind idea, feature v3 naming, persistence footnote, overwrite guards, rebuild commit, `test_predictions.parquet` label warning, "Proposal" wording), `docs/warehouse-schema.md` (header, sources, stations, `pressure_type`, `dim_model` label versions, boundaries, loader scope), `docs/powerbi-dashboard-v1.md` v0.2 (COALESCE in day counts, `dim_source` 5, two reconciliation rows), `docs/data-contract.md` (Nội Bài, QNH, known gaps, failure semantics, GHCNh rounding), `docs/NAMING_CONVENTIONS.md` (Silver keys for Meteostat/GHCNh, Meteostat Bronze gap, predictions path as target, Table 6 HAN), `docs/task-specs-weeks-2-5.md` (dims state, GHCNh decided, 3,493 s corrected), `docs/development.md`, `docs/architecture.md`, `docs/implementation-roadmap.md`, `README.md`; superseded banner at the top of `bao_cao_tien_do_du_an.md` (body kept). Two earlier progress claims corrected in place (3,493 s; 999.x reason).
  - Workbook: notes (column O only; status and percent unchanged) for IDs 3, 4, 5, 7, 8, 9, 11, 12, 13, 16, 18, 20, 21, 28, 29, 30, 31; risk sheet column F for rows 4, 7, 9, 10, 12.
- Checks:
  - `ruff check` and `ruff format --check` on `scripts dags tests`: passed.
  - `make test` → 118 passed, 18 deselected (was 103/17).
  - `make test-integration` → 18 passed (first run of the new initializer test failed on an over-strict column assertion that also matched `fact_station_daily_rain.station_role`; narrowed and rerun).
  - `make notebook-run` for both notebooks: 0 errors (EDA 21 s). `cart_tree_rules.ipynb` text outputs are identical to the previous commit (25 of 25), with joblib 1.5.3.
- Data (real, local):
  - `05_metadata_corrections.sql` applied twice to `weather_dwh` (17 s, then 0.2 s with 0 rows updated): `pressure_type` surface 701,376 (Open-Meteo), sea_level 87,485 (Meteostat).
  - `seed_dimensions.py` rc 0, 10 s: 12 stations; 48820 Nội Bài 21.221/105.807/11.9 m, `nearest_available`, 22.0 km.
  - `load_predictions.py` rc 0, 16 s: 69,888 upserted, 0 retracted; `dim_model` v1: `label_version` v0.2, `validation_f1` 0.7857, `evaluation_label_version` v0.2.1, `validation_f1_evaluation_labels` 0.7887, test 0.7622/0.7915/0.7766; view outcomes TP 27,467 / FP 8,569 / TN 26,617 / FN 7,235 (unchanged).
  - Bounded Gold/DW rerun `load_gold_observations.py --start-year 2024 --end-year 2024`: rc 0, 26 s, 12 months, 79,039 rows (Open-Meteo 70,272, Meteostat 8,767), 0 failures; 2024 count and rain total unchanged (79,039; 18,635.50 mm), 0 NULL `pressure_type`; DW total still 788,861. Gold 2024 files now store `station_key` as Int64.
  - Full Gold/DW reload `load_gold_observations.py --start-year 2016 --end-year 2025` (after the checks above): rc 0, 176 s (180 s wall), 120 months, 788,861 rows (Open-Meteo 701,376, Meteostat 87,485), 0 failures, DQ report `gold/quality_reports/dq_report_gold_load_20261022_084153.json`. DW before and after: 788,861 rows = 788,861 distinct (source, location, UTC hour), rain 180,520.90 mm, 0 NULL `pressure_type`. All 120 Gold files store `station_key` as Int64.
  - Validation persistence baseline recomputed: v0.2 labels on features v1 0.752/0.754/0.753 (as reported), on features v2 0.748/0.759/0.754; labels and features v2 0.756/0.757/0.757; 427 rows flip at 1 mm.
  - EDA on v2 with local month: August 78.6%, December 26.5%; strong Spearman pairs humidity–dew-point depression −1.00, temperature–dew point 0.87, dew point–pressure −0.80.
- Limits:
  - Not fixed, documented: Open-Meteo Silver drops grid coordinates/elevation and does not check `utc_offset_seconds`; Meteostat Bronze stores decompressed CSV without manifest/SHA; Open-Meteo Bronze payload modification and size-based skip; GHCNh support ignores sub-daily quality codes and the previous-year file; GHCNh duplicate collapse ignores metadata (0 duplicates found); Meteostat pressure is QNH labelled `sea_level`; `vw_prediction_vs_actual` joins the label source by name; legacy `model_name`/`model_version` columns in the prediction fact; `predicted_at` without time zone; `load_predictions.py` retract/upsert by split is unsafe for the future operational loads of ID 32; the full Gold reload rewrote every Gold month.
  - Ingestion CLIs still default to 2016–2025 when no years are given (the DAG relies on it).
  - Pending decisions (business requirements §5a): Recall rule and target, ERA5/GeoJSON plan and owners, N3 review of the ML decisions. Correction (2026-10-22): the pre-merger boundary choice was first listed here as pending; it was already decided on 2026-10-21 by the N2 owner of IDs 4/11 and is recorded as decided. `.venv/bin/pytest` locally has a stale shebang from the old repository path (tests run in Docker; not changed).
  - Model v1 is unchanged; nothing was retrained or reselected.
- Next step: team decisions in §5a; then ID 36 on validation with a predeclared rule, and ID 32 under the historical scope. Acceptance of the audited IDs stays with the workbook owners.

## Team decisions after the audit (IDs 4, 5, 28/36) — 2026-10-22

- Related work: business requirements §5a.
- Decisions recorded (by the N1/N2 member, who also does N3's ML work; N3 review pending): (1) Recall operating point = highest validation precision with validation recall ≥ 0.85, giving 0.36 for CART v1; 0.40 stays the official test result. (2) ERA5 request scope for ID 5 accepted; N3 must create the CDS account before any download. (3) N3 review via the checklist added to `docs/rain-label.md`. The pre-merger boundary choice was already decided on 2026-10-21 by the N2 owner of IDs 4/11; the audit entry above had listed it as pending, which was corrected.
- Evidence for (1): read-only validation scan of model v1 on corrected labels (`cart_rain_val_2024_v2`): 0.32 P 0.688 R 0.911 F1 0.784 F2 0.856; 0.34 P 0.701 R 0.899 F1 0.788; 0.36 P 0.710 R 0.880 F1 0.786 (FP 12,641, FN 4,217); 0.38 P 0.741 R 0.838; 0.40 P 0.760 R 0.819 F1 0.789 (FP 9,082, FN 6,338). Test 2025 was not read.
- Changes: `docs/business-requirements.md` §5a, `docs/rain-label.md` (decisions table, "Review by N3" checklist), `docs/warehouse-schema.md`, `docs/task-specs-weeks-2-5.md`, `README.md`; workbook notes ID 4, 5, 11 and risk rows 9, 12.
- Limits: no code or data changed; nothing at 0.36 is loaded into the DW yet (ID 36); the 0.85 target was set after viewing the validation scan.

## Province boundary GeoJSON (ID 4; ID 11 point mapping) — 2026-10-22

- Related work: workbook ID 4 (owner N2); decision 2026-10-21 to use pre-merger (63-province) boundaries, `docs/task-specs-weeks-2-5.md` §7; proposal PDF §5 (GeoJSON format). Silver subset agreed with the user on 2026-10-22: the 8 provinces with data plus the other northern provinces as a background.
- Objective/scope: one pinned real boundary file in Bronze with a manifest, a cleaned northern subset in Silver, and the research-point check. No DW change.
- Source review (2026-10-22, read-only web/API checks): HDX `cod-ab-vnm` (API `package_show`) lists only the 34-province files of 2025-09-25, so the 63-province version is not available there. geoBoundaries API `gbOpen/VNM/ADM1`: boundary year 2008, source "geoBoundaries, Wikipedia", license "Public Domain" (Wikimedia Commons), 64 units, build 2023-12-12, download pinned to commit `9469f09`. HDX dataset `geoboundaries-admin-boundaries-for-viet-nam` lists ODbL. GADM was not used (redistribution terms). The 25-province northern definition follows Decisions 368/QĐ-TTg (Red River Delta, 11 provinces) and 369/QĐ-TTg (Northern Midlands and Mountains, 14 provinces), both 2024-05-04; the 14-province list was read from the published decision, the 11-province list from a government page quoting the decision.
- Changes:
  - New `scripts/ingest_boundaries.py`: fetch with bounded retries; checks the current metadata still points to commit `9469f09`; validates before publishing (FeatureCollection, CRS84, 64 units, unique names, 63 provinces + Côn Đảo, valid geometries, 25 northern provinces present, each research point inside exactly its `dim_location.province`); Bronze bytes unchanged with a manifest; identical bytes are not rewritten, different bytes are refused without `--overwrite`; Silver GeoJSON of the 25 northern provinces (names NFC-normalized and trimmed, geometry unchanged, `has_observations` for the 8 with a research point) and a check report.
  - New `tests/test_ingest_boundaries.py` (9 cases, synthetic squares): subset and unchanged geometry, 4 rejection cases (unit count, CRS, invalid geometry, missing northern province), research point in the wrong province, publish-once and refusal of changed bytes, a different upstream build publishes nothing, research provinces/points equal to the `dim_location` seed.
  - Docs: `docs/data-contract.md` (new "Province boundaries" section), `docs/NAMING_CONVENTIONS.md` (Bronze and Silver keys), `docs/task-specs-weeks-2-5.md` §7 (implementation and source findings), `docs/architecture.md`, `docs/business-requirements.md`, `docs/warehouse-schema.md`, `README.md`. Workbook ID 4 and ID 11 notes.
- Checks:
  - `ruff check`/`format` passed; `make test` → 127 passed, 18 deselected; `make test-integration` → 18 passed.
  - Probe before implementation: full file and simplified file downloaded to the session scratchpad; all 64 geometries valid (shapely 2.0.4; 52 Polygon, 12 MultiPolygon); one name with a trailing tab ("Hà Nội\t").
- Data (real):
  - `ingest_boundaries.py` rc 0, 11 s: Bronze `bronze/spatial/boundaries/vnm_adm1_pre2025_geoboundaries_9469f09.geojson` 723,311 bytes, SHA-256 `25dbc2fec9862016710118fc98042d3e803830c72361c91c4e864ae668fa9541`, equal to the independent probe download; manifest 1,805 bytes. Silver `silver/spatial/boundaries/northern_provinces_pre2025_v1.geojson` 316,286 bytes (25 provinces, 8 with observations) and its report.
  - Research points: all 8 inside the expected province; distance to the province border 0.022° (LCA) to 0.163° (PTO); NBI 0.032°.
  - Rerun: rc 0, 10 s, `bronze_written: false`.
  - Silver province names equal the live `dim_location.province` values byte for byte (NFC).
- Limits:
  - The geometry is Wikipedia/Wikimedia-derived and coarse (mean 246 vertices per unit), not an official boundary; adequate for province maps, not for precise spatial analysis. Boundary changes after 2008 were not checked.
  - License stated differently by geoBoundaries (public domain) and HDX (ODbL); attribution is required either way.
  - Power BI Shape Map needs TopoJSON; conversion is part of task 27. Polygons are not loaded into the DW.
  - Quảng Ninh is classified differently in some regional definitions; it only affects the background layer.
- Workbook text (2026-10-22, at the user's request): ID 4 description (F8) now reads "ranh giới TRƯỚC sáp nhập (63 tỉnh)", ID 11 (F15) "chỉ tỉnh trước sáp nhập", risk row 9 handling (C9) the pre-merger rule; each keeps the original wording in quotes with the change date. Owners, weeks, and outputs unchanged.
- Next step: ID 4 is technically delivered; acceptance and the workbook status stay with the owner.

## Week 1 completion (IDs 1, 2, 3, 5; with IDs 8 and 14) — 2026-10-22

- Related work: workbook week 1 (IDs 1–7; IDs 4, 6, 7 handled earlier); `docs/task-specs-weeks-2-5.md` §6 (ID 8 original payload) and §8 (ID 5); business requirements §5a. Re-download of the 80 Open-Meteo Bronze files approved by the user on 2026-10-22 (same 2016–2025 scope).
- ID 1 (Compose end-to-end):
  - `docker compose up -d --wait airflow-webserver airflow-scheduler` with the existing image: healthy in 50 s; `airflow-init` migrated and created the admin user. The Postgres container was recreated by Compose (configuration changed); its volume and data were kept (788,861 observation rows after restart).
  - `/health`: metadatabase and scheduler healthy. `airflow dags list`: 2 DAGs, no import errors.
  - Health DAG triggered through the REST API (`manual__2026-10-22T09:20:26`): `verify_minio_data_lake` and `verify_postgres_data_warehouse` succeeded on the first try; the scheduled run of 2026-10-21 also succeeded. The ingestion DAG was not triggered (0 runs). Verified on one machine (the user's Mac), which is now the whole team's machine.
- ID 3 (star schema): new `docs/diagrams/star-schema.md`, a Mermaid ER diagram generated from the live `weather_dwh` catalog (9 tables, 14 foreign keys, primary keys, all columns), with each fact's grain and unique key. It replaces the 2026-10-13 sketch in the untracked `outputs/` folder, which lacked `dim_model`, `fact_station_daily_rain`, and the `05` columns. "Several stations per point" is already decided in the data contract (at most one station per location; sources kept separate). Rendering was not checked with a Mermaid tool; GitHub renders it on push.
- IDs 2/8 (Open-Meteo Bronze keeps the original payload):
  - `scripts/ingest_open_meteo.py`: stores the response bytes unchanged; writes `open_meteo/manifests/...json` (request parameters, retrieval time, status, bytes, SHA-256, coverage, response grid coordinates/elevation/UTC offset/time zone/units); skips only when the stored SHA-256 matches the manifest and coverage is complete (replaces the size rule); an incomplete year is a failure and is not stored; `--replace-legacy` replaces an object without a manifest only if the new hourly values and units are identical.
  - `scripts/transform_open_meteo_to_silver.py`: verifies each Bronze object against its manifest SHA-256 (mismatch = failure) and reports `bronze_verified_by_manifest` / `bronze_without_manifest`.
  - Tests: `tests/test_ingestion.py` rewritten for bytes and manifests (unchanged bytes, skip by manifest, re-download of a corrupted object, incomplete year, legacy kept/replaced only by identical data, failures); Silver manifest match/mismatch; integration Bronze rerun now checks bytes and manifest.
  - Real run: `ingest_open_meteo.py --start-year 2016 --end-year 2025 --replace-legacy`: rc 0, 339 s, 80 replaced, 0 failed, 0 HTTP 429 lines; every new response had hourly values and units identical to the legacy object. Rerun without flags: rc 0, 8 s, 80 skipped by manifest, no API call.
  - Silver check: `transform_open_meteo_to_silver.py --start-year 2024 --end-year 2024 --overwrite` rc 0, 11 s, 8 files verified by manifest; the 96 Silver 2024 Open-Meteo files (70,272 rows) are row-for-row identical (`pandas.testing.assert_frame_equal`) to a snapshot taken before the change. Other Silver years were not rewritten; their Bronze hourly data were shown identical.
  - The task-spec criterion "stored SHA-256 equals a fresh download" cannot hold for Open-Meteo, because each response contains `generationtime_ms`; recorded in task specs §6.
- IDs 5/14 (ERA5 preparation, no download):
  - New `scripts/ingest_era5.py`: one CDS request per month for `reanalysis-era5-single-levels` (7 variables incl. total cloud cover, area `[23.5, 102.0, 19.5, 107.5]`, NetCDF); checks variables, hourly time steps, and area with xarray before storing; Bronze NetCDF unchanged with a manifest (request, request id, SHA-256, content summary); skips months whose bytes match the manifest; `--dry-run`; requires explicit years within 2016–2025; clear error without credentials. The area unifies the two boxes previously written in task specs §8 and business requirements §5a.
  - Dependency: `cdsapi==0.7.7` in `requirements.in`; `requirements.txt` regenerated with uv 0.8.22 (adds cdsapi 0.7.7, ecmwf-datastores-client 0.5.1, multiurl 0.3.9, tqdm 4.66.4; release dates 2025-09-30, 2026-03-31, 2026-06-18, existing); `requirements-dev.txt` regenerated without change. Runtime and test images rebuilt; `cdsapi` imports in the runtime image. `CDSAPI_URL`/`CDSAPI_KEY` passed from `.env` by Compose; `.env.example` documents them.
  - Checks: dry run for 2024-01 printed the request (rc 0); a real call without credentials stopped with "no CDS credentials" (rc 1); 2026 was rejected by the scope check. CDS has two documented API URLs (`cds.climate.copernicus.eu/api`, `ecds.ecmwf.int/api`); the script does not hardcode one.
  - Tests: `tests/test_ingest_era5.py` (5 cases, synthetic NetCDF via a fake CDS client).
- Checks: `ruff` passed; `make test` → 135 passed, 18 deselected; `make test-integration` → 18 passed (after all changes); `make config-check` passed.
- Limits: Meteostat Bronze still stores decompressed CSV without a manifest (ID 9). Silver does not yet use the Open-Meteo grid metadata. ERA5 has one month (2024-01) in Bronze; the other 119 months of 2016–2025 were not requested (about 6.5 min per uncached request, about 460 MB in total). No ERA5 Silver (ID 15). The CDS key was pasted into the chat; rotating it on the CDS account page is advisable. The Mermaid diagram was not rendered locally.
- First real ERA5 request (after the user created the CDS account, stored `CDSAPI_URL`/`CDSAPI_KEY` in the ignored `.env`, and accepted the licence):
  - Attempts 1–2 (09:49Z, 09:59Z): HTTP 403 "required licences not accepted"; nothing stored, rc 1. The CDS also announced updated Terms of Use from 2026-10-28.
  - Attempt 3 (10:01Z): job `70c1b649-2d18-445e-b460-f3c9007f772c` accepted, running after 80 s, successful after 6 min 27 s, 3.63 MB downloaded; the check failed ("NetCDF: Unknown file format") because the result was a zip, and nothing was stored. The script then accepted a zip of NetCDF files (members checked one by one, variables required exactly once across members; test added).
  - Attempt 4 (10:09Z, cached result, job `8bfe23b5-63c6-41f5-892e-30e47160ba11`): stored `bronze/era5/reanalysis/year=2024/month=01/era5_north_vietnam_hourly_202401.zip`, 3,805,951 bytes, SHA-256 `84dd51f65e8eeaf0b2fcdeb9be534aa6930363454e57a3747a71b72b07e05c10`. Members: `data_stream-oper_stepType-instant.nc` (d2m, sp, t2m, tcc, u10, v10) and `data_stream-oper_stepType-accum.nc` (tp); each 744 hours 2024-01-01T00 to 2024-01-31T23, latitude 19.5–23.5 (17), longitude 102.0–107.5 (23); units K, K, Pa, m s**-1, m s**-1, (0 - 1), m. Rerun: skipped by checksum.
  - The manifest's request id was empty because the client returns the downloaded path; the script now reads it from the results URL (`.../jobs/<id>/results`, checked on a real result). The 2024-01 manifest was rewritten once (job `d6a9bc1b-2fa8-46e6-b727-50738396383b`, cached); the zip bytes were identical (same SHA-256).
  - `make test` → 136 passed after these changes.
- Workbook: IDs 1, 2, 3, 8 Đang làm 95 (criteria met, owner to confirm), ID 5 Đang làm 90, ID 14 Đang làm 60, with notes.
- Next step: with the user's go-ahead, request the remaining months (`ingest_era5.py --start-year 2016 --end-year 2025`, resumable: stored months are skipped). Final status for IDs 1–5 stays with the owner.

## Airflow DAG Bronze → DW (ID 17) — 2026-10-22

- Related work: workbook ID 17 (N1, weeks 4–5); specification `docs/task-specs-weeks-2-5.md` §5; `dags/AGENTS.md`.
- Changes:
  - New `dags/dag_weather_pipeline.py` (`weather_bronze_to_dw_dag`): `validate_window` → (`ingest_open_meteo` → `transform_open_meteo_to_silver`, `ingest_meteostat` → `transform_meteostat_to_silver`) → `seed_dimensions` → `load_gold_observations` → `reconcile`. Each task is a `BashOperator` calling an existing script with `set -euo pipefail`, so a non-zero exit fails the task. Params `start_year`/`end_year` (2016–2025) and `location` (empty = all 8); manual trigger only, `catchup=False`, `max_active_runs=1`; ingestion tasks retry once after 2 minutes, all others do not retry. No network or data access at import; only parameters pass between tasks.
  - New `scripts/reconcile_dw.py`: counts rows per source × location × UTC year in Silver `weather_hourly/` (Open-Meteo, Meteostat; canonical locations) and in `fact_weather_observation`; any difference or one-sided group fails (exit 1); report in `gold/quality_reports/reconcile_<start>_<end>[_<loc>]_<ts>.json`.
  - Tests: `tests/test_dags.py` (task order and settings, rendered commands with and without location, window validation) and new `tests/test_reconcile_dw.py` (pass with report, mismatch and one-sided groups fail, compare). The higher warning count (17) comes from the existing DAGs' deprecated `schedule_interval`, reported once per test that loads the DagBag.
  - Docs: task specs §5 (implemented, deviations), `docs/architecture.md`, `docs/NAMING_CONVENTIONS.md`, `README.md`.
- Checks: `ruff` passed; `make test` → 145 passed, 18 deselected. In the running Airflow: no import errors; `airflow tasks list --tree` shows the expected order.
- Data (real, local; triggered through the REST API):
  - Before: DW 788,861 rows, rain 180,520.90 mm; HAN 2024 Open-Meteo 8,784 rows (2,660.30 mm, mean 24.6176 °C), Meteostat 8,767 (mean 25.1590 °C).
  - Run 1 `manual__2026-10-22T13:19:12` (2024, HAN): success, all 8 tasks on the first try, each task 0.3–11.4 s. Ingestion skipped current objects (Open-Meteo by manifest; Meteostat 2 existing files), no API call. Reconcile: 2 groups, Silver 17,551 = DW 17,551. DW and HAN 2024 figures unchanged.
  - Run 2 `manual__2026-10-22T13:20:38` (rerun, same window): success; DW 788,861 rows = 788,861 distinct (source, location, UTC hour), rain 180,520.90 mm: no new rows.
  - Run 3 `manual__2026-10-22T13:21:45` (forced failure): MinIO stopped before the trigger. Both ingestion tasks failed after their retry ("Failed to resolve 'minio'", exit 1); the five downstream tasks were `upstream_failed`; the run failed at 13:24:21Z. MinIO restarted; `clearTaskInstances` (only failed, with downstream) resumed the same run: `validate_window` was not rerun, ingestion succeeded on try 3, the rest on try 1; run success; DW unchanged.
  - Run 4 `manual__2026-10-22T13:26:31` (2024, all locations): success; reconcile 9 groups, Silver 79,039 = DW 79,039 (`gold/quality_reports/reconcile_2024_2024_20261022T132729Z.json`); DW unchanged.
  - The ERA5 background download was paused during these runs (one in-flight 2016-01 request abandoned) and restarted afterwards; it resumes by skipping stored months.
- Limits: the window is per year (a one-month run is not possible with the current scripts); GHCNh daily rain, boundaries, and ERA5 are not in the DAG; no schedule or watermark (ID 25); data-quality gates beyond row reconciliation are ID 24. Only 2024 was run through the DAG; the 2016–2025 history was loaded by the scripts earlier.
- Next step: ID 24 (quality checks in the DAG) or ID 15 (ERA5 to Silver) once ERA5 is downloaded. Workbook ID 17: Đang làm 90.

## Documentation sweep after the week-1 and ID 17 work — 2026-10-22

- Related work: user request to bring every document up to date with the work done on 2026-10-22 and check for omissions.
- Method: searched README, `docs/`, scoped `AGENTS.md` files, and skills for statements made stale by the audit fixes, IDs 1–5, 8, 14, and 17 (for example "reserializes", "no manifest", "size-based", "not started", "no Airflow DAG", test counts, status dates), and read the workbook notes of IDs 9, 10, 12, 13, 16, 18, 19.
- Corrected: `docs/architecture.md` (status date; Compose, Open-Meteo, and Bronze/Silver/Gold rows), `docs/development.md` (fixed Open-Meteo limitations), `docs/NAMING_CONVENTIONS.md` (raw-preservation status per source), `docs/business-requirements.md` §5a (ERA5/GeoJSON row status), `docs/implementation-roadmap.md` (test counts, week-1 status), `docs/task-specs-weeks-2-5.md` (§5 decision on the test window and the GHCNh chain, §6 wording, §8 implementation), `dags/AGENTS.md` and `scripts/AGENTS.md` (ingester failure and raw-preservation status), `README.md` (Bronze/ERA5 bullet, test counts, DAG listing). Workbook notes of ID 10 (Silver manifest check) and ID 16 (Int64 `station_key` in all 120 Gold files).
- Decision recorded (owner of ID 17, given with the go-ahead on 2026-10-22): the smallest DAG test window is one year × one location; the GHCNh chain joins the DAG; boundaries stay a one-off script; ERA5 joins after ID 15.
- Checks: link and anchor check over README and `docs/` (0 problems); `git diff --check`.
- Limits: `docs/progress.md` entries keep the figures as they were measured; earlier entries are corrected only by dated notes.

## ID 17 completion: GHCNh chain in the DAG and the 2016–2025 acceptance run — 2026-10-22

- Related work: workbook ID 17; decision recorded in `docs/task-specs-weeks-2-5.md` §5 (one year × one location test window; GHCNh joins the DAG).
- Changes:
  - `scripts/station_config.py`: `ghcnh_stations_for(location)` and `resolve_ghcnh_stations(stations, location)`.
  - `scripts/ingest_ghcnh.py`, `scripts/transform_ghcnh_to_silver.py`, `scripts/load_station_daily_rain.py`: new `--location` option (exclusive with `--stations`); a location without a GHCNh station (PTO, TUQ, HBI) prints "nothing to do" and exits 0.
  - `dags/dag_weather_pipeline.py`: `ingest_ghcnh` (one retry) → `transform_ghcnh_to_silver --overwrite` → after `seed_dimensions`, `load_station_daily_rain` → `reconcile`.
  - `scripts/reconcile_dw.py`: also compares Silver GHCNh rows with `fact_station_daily_rain` per station × year of the rain day (a missing Silver station-year counts as 0).
  - Tests: DAG order with the GHCNh tasks, rendered commands, station resolution, daily-rain reconciliation pass/fail. `make test` → 147 passed, 18 deselected; `ruff` passed.
- Data (real, through Airflow):
  - Before: `fact_station_daily_rain` 17,866 rows, 92,090.2 mm (2024: 48803 358, 48823 358, 48825 355, 48826 355, 48831 353 days).
  - Run `manual__2026-10-22T13:49:07` (2024, HAN): 11/11 tasks succeeded on the first try; hourly 17,551 = 17,551; daily rain (48825) 355 = 355.
  - Acceptance run `manual__2026-10-22T13:50:10` (2016–2025, all locations): 11/11 succeeded on the first try, 13:50:10–13:53:53Z (3 min 43 s; `load_gold_observations` 155 s). Ingestion stored nothing: Open-Meteo 80 skipped by manifest, Meteostat 15 existing skipped, GHCNh 50 skipped by manifest. Reconcile (`gold/quality_reports/reconcile_2016_2025_20261022T135347Z.json`): 90 hourly groups, Silver 788,861 = DW 788,861; 50 station-year groups, Silver 17,866 = DW 17,866; passed.
  - After: DW 788,861 rows = 788,861 distinct keys, rain 180,520.90 mm; daily rain 17,866 rows, 92,090.2 mm; `vw_prediction_vs_actual` outcomes unchanged (TP 27,467 / FP 8,569 / TN 26,617 / FN 7,235).
- Limits: the Meteostat ingester still sends its 25 requests for station-years that return HTTP 404 on every run (no data downloaded; it has no manifest to remember them, ID 9). Boundaries and ERA5 are not in the DAG. No schedule (ID 25) and no quality gates beyond row reconciliation (ID 24).
- Next step: ID 24 (quality checks inside the DAG). Workbook ID 17: Đang làm 95.

## Silver quality gate in the pipeline DAG (ID 24) — 2026-10-22

- Related work: workbook ID 24 ("Bước kiểm tra trong Airflow: đếm dòng, % thiếu, khoảng giá trị hợp lệ"); rules from `docs/data-contract.md` v0.2 (valid ranges, keys, UTC, Open-Meteo whole-year Bronze, GHCNh accepted values). Coverage and plausibility thresholds are not agreed in the contract, so none were set.
- Changes:
  - New `scripts/quality_gate.py`: reads Silver for a window (optionally one location) independently of the transform reports. ERROR checks, which fail the script (exit 1): H01 required columns, H02 non-null keys, H03 UTC timestamps, H04 timestamps on the hour inside the partition month, H05 unique source × location × UTC hour, H06 contract ranges (reuses `quality_checks.validate_and_nullify_ranges`), H07 complete hourly coverage for Open-Meteo; GHCNh D01 columns, D02 non-null, D03 unique station × day, D04 window (D-1 12:00 UTC, D 12:00 UTC], D05 0 ≤ `rain_24h_mm` < 999. REPORTED only: H00 objects/rows read, H07 coverage for Meteostat, H08 % missing per variable over expected hours, H09 min/max per variable, D06 day coverage. Report: `silver/quality_reports/quality_gate_<start>_<end>[_<LOCATION>]_<ts>.json`.
  - `dags/dag_weather_pipeline.py`: task `quality_gate` after the three Silver transforms and before `seed_dimensions`, so no DW table is written when it fails.
  - Tests: new `tests/test_quality_gate.py` (clean pass; missing Open-Meteo month and location-year; duplicate hour; humidity 101, rain −0.1, direction 361; nulls reported but not gated; timestamp outside partition and naive timestamps; missing column; GHCNh range, duplicate day, wrong window; CLI exit 1); `tests/test_dags.py` order and rendered command.
  - Docs: data contract (decision-status row; stale Bronze-provenance sentence corrected), `docs/NAMING_CONVENTIONS.md`, `docs/architecture.md`, task specs §5, `dags/AGENTS.md`, `README.md`.
- Checks: `make lint` passed; `make test` → 161 passed, 18 deselected; `make test-integration` → 18 passed.
- Data (real, local):
  - Standalone 2024, all locations: 68 checks, 43 PASS, 0 FAIL, 25 REPORTED, 11 s.
  - Standalone 2016–2025, all locations (`silver/quality_reports/quality_gate_2016_2025_20261022T152839Z.json`): 1,080 Silver objects, 788,861 rows in scope (equal to the DW); 662 checks, 421 PASS, 0 FAIL, 241 REPORTED; 24 s. Open-Meteo 80 location-years complete (701,376 rows), 0% missing in the 8 variables. Meteostat only HAN (48820) in Silver: 87,485 rows, coverage 99.55–99.98% per year; worst-year missing: precipitation and radiation 100% (not provided), cloud cover 89.59%, wind direction 13.14%, pressure 7.53%. GHCNh 50 station-years, 17,866 days, lowest coverage 95.89% (48831, 2022).
  - DAG run `manual__2026-10-22T15:30:28` (2024, HAN): 12/12 tasks succeeded on the first try; `quality_gate` 8.1 s, 16 checks, 9 PASS, 0 FAIL, 7 REPORTED. DW after: 788,861 rows, 180,520.90 mm; daily rain 17,866 rows, 92,090.2 mm (unchanged).
- Limits: the gate's failure path was tested on synthetic data and by the CLI exit code, not by corrupting real Silver; a failed gate in Airflow relies on the same `BashOperator` exit-code behaviour verified in the ID 17 failure run. Coverage minimums (Meteostat, GHCNh) and plausibility thresholds (temperature, pressure, extreme rain) are reported, not gated, until the team sets them. The gate checks Silver only; the DW side is covered by `reconcile`.
- Next step: decide the coverage/plausibility thresholds if they should block; workbook ID 24 can move to Đang làm 90 (pending that decision).

## ERA5 download: concurrent requests rejected, order changed — 2026-10-22

- Related work: workbook IDs 5 and 14.
- Observed: in the sequential run started 13:27Z, 2016-01 returned at once (result of an earlier identical request) and 2016-02 waited 13:28–15:09Z in the CDS queue, then processed in 4 minutes. At that rate the remaining ~118 months take days.
- Tried: a `--workers` option sending four months at once (unit-tested; it also showed that netCDF4/HDF5 crashes when two threads open files). On the real CDS every request was accepted and then rejected after about 50 s with "Maximum number queued requests for this dataset has been reached. Please configure your scripts to request data sequentially" (75 months rejected before the run was stopped, nothing stored). The option was removed (`git checkout`), and the limit is documented in `scripts/ingest_era5.py`, the data contract, and task specs §8.
- Queue clean-up: a stopped run leaves its job queued on the CDS, which blocks the next request; two such 2016-03 jobs were cancelled with `ecmwf.datastores.Client.delete` after checking their year/month (account queue then 0 accepted, 0 running).
- Now running (background): 2024–2025 first (the fallback scope in business requirements §5a), then 2016–2023, sequentially. Bronze before: 2016-01, 2016-02, 2024-01.
- Limits: the CDS queue limit per account is not published in what was read; one queued request per dataset was enough to block the next. Completion time depends on CDS load and is not estimated further.

## ID 24: warning thresholds, extreme-rain check, Airflow failure test — 2026-10-22

- Related work: workbook ID 24. Decision (2026-10-22, the project member holding all roles, after reviewing the proposal): add non-blocking warnings with thresholds derived from the 2016–2025 Silver data; blocking rules unchanged. Recorded in `docs/data-contract.md` (decision table: quality gate, coverage).
- Changes:
  - `scripts/quality_gate.py`: new WARN status (never fails the run; printed as `[WARN]` lines and counted in the report). H07 Meteostat coverage < 95%; H08 missing values (Open-Meteo any of the 8 variables > 0%, Meteostat temperature/humidity > 5% of expected hours; other variables reported); H09 plausible values (temperature 0–45 °C, pressure 950–1050 hPa, hourly rain ≤ 100 mm; min/max kept); D06 GHCNh day coverage < 90%; new D07 GHCNh rain ≥ 300 mm on a day not in the reviewed list (48825 2016-05-25 338.0 mm, 48831 2025-10-07 491.3 mm, the two supported extremes in the data contract).
  - `tests/test_quality_gate.py`: complete synthetic year passes with 0 warnings; low coverage, missing values per source limit, implausible values (45.5 °C, −0.5 °C, 940 hPa, 120 mm/h), and unreviewed versus reviewed 320 mm days warn without blocking. `tests/test_dags.py`: seed and both loaders have `quality_gate` upstream with `all_success`. `tests/test_integration.py`: renders the DAG's `quality_gate` command and runs it in a `BashOperator` against the disposable MinIO: exit 0 on complete Silver, `AirflowException` (exit code 1) after one humidity of 150%, report with exactly one FAIL (`H06_valid_ranges`).
  - Docs: data contract (quality-gate and coverage rows, header, quality section), task specs §5, `README.md`.
- Checks: `ruff` passed; `make test` → 168 passed, 19 deselected; `make test-integration` → 19 passed.
- Data (real, local):
  - DW query before the change: GHCNh days ≥ 300 mm are exactly the two reviewed days; the next largest is 298.0 mm (48823, 2024-09-10).
  - Standalone 2016–2025 (`silver/quality_reports/quality_gate_2016_2025_20261022T161216Z.json`): 712 checks, 421 ERROR PASS, 290 WARN PASS, 0 FAIL, 0 WARN, 1 REPORTED; D07 lists the two reviewed days; 25 s.
  - DAG run `manual__2026-10-22T16:13:04` (2024, HAN): 12/12 tasks on the first try; `quality_gate` 8.3 s, 17 checks, 0 failed, 0 warnings. DW after: 788,861 rows, 180,520.90 mm; daily rain 17,866 rows, 92,090.2 mm (unchanged).
- Limits: the thresholds come from the same data they are checked on, so they show no warning now; they are meant to catch later changes (incremental loads, ID 25). The Airflow failure evidence is the integration test (real `BashOperator`, disposable MinIO) plus the ID 17 run showing that a failed task stops its downstream tasks; no live DAG run was made to fail, because the DAG rewrites Silver from Bronze before the gate, so bad Silver cannot be planted without altering Bronze. The 300 mm check compares against the reviewed list only; supporting sub-daily rain is still checked by the GHCNh Silver transform, not by the gate.
- Workbook (after the user closed Excel): ID 24 Đang làm 95 with the checks and results; stale notes corrected for ID 5 (requests sent sequentially), ID 14 (months in Bronze, download order, CDS queue time), ID 28 (Recall rule decided 2026-10-22), and risk row 4 (ERA5 queue). Statuses of other tasks unchanged.
- Next step: Hoàn thành for ID 24 is the user's decision (technical evidence above).

## Power BI handoff package (IDs 20, 27, 33, 35) — 2026-10-23

- Related work: workbook IDs 20, 27, 33, 35 (owner N2). The user chose to hand the Power BI work to a team member on a Windows machine, using option A (a DW dump restored locally) rather than a LAN connection or a full rebuild.
- Artifacts (in `outputs/`, not in Git): `outputs/powerbi_handoff/weather_dwh.dump` (`pg_dump -Fc -Z 9 --no-owner --no-privileges` of `weather_dwh`, 16.7 MB from a 451 MB database, SHA-256 `7227fdb39918f219999205a5d239127cdf43f129b1e4e568faade460b8d9a865`), `outputs/powerbi_handoff/huong_dan_power_bi.html` (Vietnamese guide, also published as a private artifact), and `outputs/powerbi_handoff.zip` with both.
- Guide content: restore commands for Docker Desktop on Windows (`postgres:16-alpine`, port 5434), Power BI connection, the 9 objects to import, relationships, DAX from `docs/powerbi-dashboard-v1.md` plus new measures for station-versus-model rain and forecast evaluation (TP/FP/TN/FN, accuracy, precision, recall, F1), 8 pages (v1 pages 1–5, map, station versus model, forecast versus actual), a reconciliation table, and the delivery checklist.
- Checks (real data): restored the dump into a disposable `postgres:16-alpine` container (`pg_restore --exit-on-error`, 6 s); row counts of all 9 tables, rain totals (180,520.90 mm hourly, 92,090.2 mm station), 8 views, 14 foreign keys, and the forecast outcomes (TP 27,467 / FP 8,569 / TN 26,617 / FN 7,235) were identical to the live DW (`diff` of the query outputs). Every expected value in the guide was recomputed by SQL: the v1 table (for example HAN 2024 Open-Meteo 2,660.5 mm, 175 rainy days, 23 hot days), 2024 rain per location, station versus model 2024, forecast metrics overall (0.774 / 0.762 / 0.792 / 0.777), by season (F1 0.836 rainy, 0.624 dry), by location, and HAN July 2025. The test container was removed.
- Limits: the Windows commands and Power BI steps were not run on Windows here; the Map visual depends on the Power BI map setting and internet access. The new measures were not executed in Power BI (no Windows machine); their SQL equivalents were. The dump reflects the DW on 2026-10-23; later DW changes need a new dump or a rebuild.
- Next step: the team member builds the dashboard and returns the `.pbix`, screenshots, and the filled reconciliation table; then record ID 35 here.

## ERA5 point time series to Bronze and Silver (IDs 5, 14, 15) — 2026-10-23

- Related work: workbook IDs 5, 14, 15; decision by the user on 2026-10-23 (defence on 2026-10-29) to use the CDS point time-series product, recorded in `docs/data-contract.md` (ERA5 Bronze, ERA5 Silver), task specs §8, and business requirements §5a.
- Probe (2026-10-22, before the decision): one request for HAN, 7 variables, 2016-01-01/2025-12-31, finished in 25 s (accepted 17:45:27Z, successful 17:45:52Z): 87,672 steps, no null values, cell 21.0, 105.75. The CDS process schema lists only `variable`, `data_format`, `area`, but the request with `location` and `date` was accepted and returned the asked window.
- Changes:
  - New `scripts/ingest_era5_timeseries.py` (one request per location for the window; zip of one NetCDF stored unchanged with a manifest; checks variables, unique hourly steps, and that the cell is the nearest one; sequential; skip by manifest SHA-256).
  - `scripts/ingest_era5.py`: `make_cds_client()` shared by both ERA5 scripts (no behaviour change).
  - New `scripts/transform_era5_to_silver.py` (conversions and keys in the data contract; verifies the Bronze SHA-256; fails on a missing or duplicate hour; DQ report `silver/quality_reports/dq_report_era5_<ts>.json`).
  - New `scripts/compare_era5_open_meteo.py` (paired hours only; bias, MAE, RMSE, correlation per location × year; annual rain totals).
  - Tests: new `tests/test_era5_timeseries.py` (dry run, store and skip, wrong steps and wrong cell fail, unit conversions, Magnus humidity, saturation rule, wind direction for the four quarters and calm, UTC and local time, overwrite rules, SHA mismatch fails, comparison pairing and no-pair case).
  - Docs: data contract, `docs/NAMING_CONVENTIONS.md`, `docs/architecture.md`, task specs §8, business requirements §5a, `README.md`.
- Checks: `ruff` passed; `make test` → 181 passed, 19 deselected.
- Data (real):
  - Bronze run 18:08:26–18:13:46Z (320 s): 8 stored, 0 failed, each 87,672 hours, 3.21 MB; cells HAN 21.0/105.75, HBI 20.75/105.25, HPH 20.75/106.75, LCA 22.5/104.0, NBI 20.25/106.0, PTO 21.5/105.25, TNG 21.5/105.75, TUQ 21.75/105.25. Rerun: 8 skipped, no CDS request.
  - Equality with the gridded files at HAN's cell, 2024-01 (744 h) and 2024-02 (696 h): tp, u10, v10, tcc max difference 0; t2m, d2m 0.000244 K; sp 0.0625 Pa. tp minimum 0 (no negative values).
  - Silver run (22 s): 8 locations × 120 monthly objects, 701,376 rows, 0 values nulled by the range check; humidity set to saturation (d2m above t2m by ≤ 0.01 K) HAN 24, HBI 449, HPH 0, LCA 613, NBI 25, PTO 82, TNG 86, TUQ 95 hours. Rerun without `--overwrite`: 0 written, 120 kept per location.
  - Comparison 2016–2025 (`silver/quality_reports/compare_era5_open_meteo_2016_2025_20261022T181922Z.json`, 87,672 pairs per location): temperature bias −1.61 (LCA) to +0.90 °C (TUQ), mean yearly correlation 0.962–0.979; surface-pressure bias −66.6 hPa at LCA and −38.3 hPa at HBI (the ERA5 cell's model terrain differs from the point; a height difference of roughly 550 m at LCA follows from the pressure ratio, an estimate, not a measured elevation), within ±10 hPa elsewhere; mean annual rain ERA5 versus Open-Meteo e.g. HAN 1,905 vs 2,203 mm, LCA 2,064 vs 2,950 mm, HPH 2,010 vs 2,013 mm.
  - DW unchanged (ERA5 is outside `weather_hourly/`).
- Limits: the time series covers the 8 grid cells, not the whole region asked in ID 5; the gridded monthly download (2024–2025, about two hours per month in the CDS queue) is the regional evidence and was still running. ERA5 is not in the DW or the DAG. ERA5 surface pressure at LCA and HBI is not comparable with point data.
- Next step: decide whether ERA5 is loaded into the DW (would change the Power BI handoff dump); workbook IDs 5, 14, 15 notes.

## ERA5 loaded into the DW; ERA5 chain in the DAG; final Power BI dump — 2026-10-23

- Related work: workbook IDs 16, 17, 18, 24 (and 20/27/33/35 handoff). Decision by the user on 2026-10-23: load ERA5 into the DW (proposal §8 integration into the DW, §10 source comparison); recorded in task specs §8, business requirements §5a, data contract, warehouse schema. The user asked that the Power BI package be sent only once, so the DW data used by the dashboard is frozen after this load.
- Changes:
  - `scripts/load_gold_observations.py`: `LOADED_SOURCES` adds ERA5; reads `weather_hourly/` and `era5/weather_hourly/` (`SILVER_HOURLY_PREFIXES`); a missing ERA5 location-month fails the month like Open-Meteo (`missing_era5_objects`, `era5_silver_key`).
  - `scripts/quality_gate.py`: reads both prefixes; ERA5 is a complete-hours source (ERROR), any missing ERA5 value except radiation warns, ERA5 pressure limit 900–1050 hPa (observed 916.89–1039.78; LCA cell lowest).
  - `scripts/reconcile_dw.py`: reads both prefixes.
  - `scripts/ingest_era5_timeseries.py`: the CDS client is created only when a request is needed (a rerun over current objects needs no credentials).
  - `dags/dag_weather_pipeline.py`: `ingest_era5_timeseries` (one retry) → `transform_era5_to_silver --overwrite` → `quality_gate`.
  - Tests: loader scope and ERA5 missing-object test, gate ERA5 tests (missing month blocks, ERA5 pressure limit), DAG order, rerun without credentials. `make test` → 184 passed; `make test-integration` → 19 passed.
  - Docs: data contract, naming, architecture, warehouse schema, task specs §5/§8, business requirements §5a, `docs/powerbi-dashboard-v1.md` (counts, ERA5 reference rows), `README.md`.
- Run (real, DAG `manual__2026-10-22T18:46:10`, 2016–2025, all locations):
  - First attempt: `ingest_era5_timeseries` failed twice with "no CDS credentials": the scheduler container predated the `CDSAPI_*` entries in Compose; the downstream tasks were `upstream_failed` and nothing was written to the DW. Fixed by `docker compose up -d --no-deps airflow-scheduler airflow-webserver` (recreated with the current environment) and the lazy client above; resumed with `clearTaskInstances` (only failed, with downstream).
  - Resumed: all 14 tasks succeeded; ERA5 ingestion skipped 8 current objects; ERA5 Silver rewritten (19.7 s); `quality_gate` 1,032 checks, 0 failed, 0 warnings (35.1 s, `silver/quality_reports/quality_gate_2016_2025_20261022T185143Z.json`); `load_gold_observations` 365.8 s; reconcile 170 hourly groups, Silver 1,490,237 = DW 1,490,237, daily rain 17,866 = 17,866 (`gold/quality_reports/reconcile_2016_2025_20261022T185822Z.json`).
  - DW after: ERA5 701,376 rows (165,505.80 mm), Open-Meteo 701,376 (180,520.90 mm, unchanged), Meteostat 87,485 (unchanged); 1,490,237 rows = 1,490,237 distinct keys; daily rain 17,866; forecast outcomes unchanged (TP 27,467 / FP 8,569 / TN 26,617 / FN 7,235); `vw_obs_daily` 62,118, `vw_coverage` 187.
- Power BI package replaced: `outputs/powerbi_handoff/weather_dwh.dump` 33.5 MB, SHA-256 `8b28049c4779d07b6dd0993b1312431fa39594c126906974220f4bf86a69fc5d`; restored into a disposable PostgreSQL 16 container and identical to the live DW (all table counts, totals, views, keys, outcomes, daily rows per source). Guide updated (ERA5 kept in the source filter, row counts, page 4 compares ERA5 with Open-Meteo at all 8 points with a note on grid-cell pressure, three ERA5 reconciliation rows computed by SQL) and republished at the same link; `outputs/powerbi_handoff.zip` rebuilt.
- Limits: the gridded ERA5 download is unchanged (still running for 2024–2025) and is not loaded. ERA5 surface pressure at LCA and HBI is not comparable with point data; the guide tells the dashboard not to plot pressure across sources.

## Independent review of the ERA5 work; gridded download stopped; doc corrections — 2026-10-23

- Related work: workbook IDs 5, 14, 15, 16–18, 24 (branch `feat/era5-timeseries`, commits `bc79f02`, `296e05f`).
- Review: a separate read-only agent reviewed `git diff main...feat/era5-timeseries` against the data contract and reran `make test` offline (184 passed). It found **no defect that corrupts or misstates DW or Silver data**; conversions, timestamps, keys, retraction scope, gate and reconciliation logic, and downstream consumers (labels, features, station view, prediction view) were checked and found correct. Open findings, **not fixed yet**:
  - M1 (medium): ERA5 Bronze is keyed by the exact window, so a DAG run for another window (default 2024, or one location) sends new CDS requests although 2016–2025 is stored; with CDS or the network down, the ERA5 task fails and, because the loader and the gate require ERA5, the Open-Meteo, Meteostat, and GHCNh loads are blocked too. Fail-closed, no data change.
  - M2 (low–medium): the ingestion checks the number of unique hourly steps, not that they are the requested hours; a series missing one hour but carrying an extra one would be stored and then always skipped as current, while the transform fails that year.
  - L1: the ERA5 transform writes year by year, so a failure in a later year leaves earlier years already overwritten (the DAG stops before the gate; a manual load would mix runs).
  - L2: north can be written as 360.0 or 0.0. L3: calm direction is nulled only for an unrounded speed of exactly 0, and the gate's 0% ERA5 missing limit includes wind direction (calm nulls would warn).
  - L4: `compare_era5_open_meteo.py` summary can raise `KeyError` when a variable has fewer than 2 pairs (after writing the report).
  - L5: the ingestion skip does not compare the stored request with the current coordinates and variables.
  - L6: loader tests return the same listing for both prefixes; no test sends a combined ERA5 + Open-Meteo month through `process_month_observations` (the reviewer's probe did, correctly).
  - L7: stale docs (corrected below); the `sql/02_olap_views.sql` and `sql/05` comments and the seeded `dim_source.update_frequency` 'Monthly Bulk' for ERA5 still describe the earlier state (not changed: a seed value change would alter the DW and the Power BI dump).
- Gridded ERA5 download (`scripts/ingest_era5.py`, 2024–2025 then 2016–2023) stopped on 2026-10-23 before the user closed the laptop; the 2024-03 job, queued since 17:33:33Z, was cancelled (CDS queue then 0 accepted, 0 running). Bronze keeps 2016-01, 2016-02, 2024-01, 2024-02 as evidence of the regional request; the point time series covers the data.
- Docs corrected: data contract (gridded download stopped; quality gate row now names ERA5 complete hours, ERA5 pressure limit, ERA5 missing values), task specs §8, `docs/architecture.md`, `docs/NAMING_CONVENTIONS.md`, `docs/warehouse-schema.md` (`pressure_type` for ERA5), `README.md` (ERA5 in the ETL list, 1,490,237 rows, test counts), `scripts/quality_gate.py` docstring.
- Power BI package: not sent yet; the user waits for the fixes above and the remaining real-data checks (time alignment, rain timing, wind direction against Open-Meteo) before the final dump.
- Next step: fix M1, M2, L1–L4 with tests; run the three real-data checks; reload ERA5 if a fix changes values (L2/L3); final dump.

## ERA5 review fixes, real-data checks, ERA5 reload, final Power BI dump — 2026-10-23

- Related work: workbook IDs 5, 14, 15, 17, 18, 24; the open findings recorded in the previous entry (independent review of `feat/era5-timeseries`).
- Real-data checks first (Silver 2016–2025, ERA5 against Open-Meteo, HAN, LCA, HPH): correlation peaks at lag 0 h for temperature (0.979, 0.961, 0.966), humidity, and hourly precipitation (0.428, 0.383, 0.462), so there is no time shift and the rain hours are aligned with Open-Meteo's preceding-hour totals. Wind direction with both speeds > 2 m/s: median absolute difference 6.8°, 11.3°, 10.5° (92–98% within 45°); a 180° inversion would give about 170°. ERA5 Silver had 140 directions of 360.0 and 5 calm hours with a direction (findings L2, L3 confirmed).
- Fixes (branch `fix/era5-review`):
  - M1, L5: `scripts/ingest_era5_timeseries.py` `find_current()` reuses any stored, checksum-valid series covering the window whose request has today's variables and coordinates (narrowest first); `scripts/transform_era5_to_silver.py` reads that object.
  - M2: a download must contain exactly the requested hours (missing, extra, and duplicate steps are reported).
  - L1: the transform checks every year before writing any Silver object.
  - L2, L3: direction in [0, 360) after rounding; null when the rounded speed is 0.00. The gate's ERA5 0% missing limit no longer includes wind direction.
  - L4: the comparison summary skips statistics a group does not have; correlation is undefined (null), not NaN, when a side is constant.
  - L6: loader test with one Open-Meteo and one ERA5 object in the same month (one row per source, station key null, surface pressure).
  - Tests: 10 new; `make test` → 194 passed (19 warnings, unchanged); `make test-integration` → 19 passed.
- Run: DAG `manual__2026-10-23T07:24:22` (2016–2025, all locations): 14/14 tasks on the first try; ERA5 ingestion "covered by current …20160101_20251231.zip" for all 8; `quality_gate` 1,032 checks, 0 failed, 0 warnings; `load_gold_observations` 307.3 s; reconcile 1,490,237 = 1,490,237 hourly and 17,866 = 17,866 daily. After: ERA5 701,376 rows, 0 directions of 360, 5 null (calm); Open-Meteo, Meteostat, rain totals, daily rain, and forecast outcomes unchanged. A separate run `ingest_era5_timeseries.py --start-year 2024 --end-year 2024 --location HAN` skipped as covered, with no CDS request.
- Final Power BI dump: `outputs/powerbi_handoff/weather_dwh.dump` 33.6 MB, SHA-256 `7e900db14c2b3abc3f5e8cb7eb9d1aaec1d15a219bb88fe19c5b477b7437fdfd`; restored into a disposable PostgreSQL 16 container and identical to the live DW (including the direction counts); guide checksum and size updated and republished; `outputs/powerbi_handoff.zip` rebuilt. Reference values in the guide are unchanged (no wind-direction measure).
- Not changed: Open-Meteo and Meteostat write north as 360 (their own convention, within the contract's [0, 360]); `dim_source.update_frequency` 'Monthly Bulk' for ERA5 (a seed value).
- Next step: the user sends the package; then IDs 36 and 26.
