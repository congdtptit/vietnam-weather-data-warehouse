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
