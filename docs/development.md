# Development Environment

## Shared runtime

- Reference runtime: Linux container, Airflow **2.9.3**, Python **3.10**.
- `Dockerfile` uses `apache/airflow:2.9.3-python3.10` pinned by SHA-256 digest. Compose builds image `weather-airflow:2.9.3-python3.10` for init, webserver, and scheduler.
- Libraries are installed during builds, not on every container startup. Builds run `pip check` to detect incompatible dependencies.
- `.dockerignore` allows only the Dockerfile, lockfiles, test configuration, and code/fixtures required by the test target into the build context, excluding `.env`, data, and logs. Target `runtime` does not copy test code/fixtures or install development tools.
- This describes the current project development environment, not production certification or an Airflow upgrade plan.

## Setup and image checks

Install Docker Desktop, open it, and wait for Docker Engine. If `.env` does not exist, copy `.env.example` to `.env`; preserve an existing configured file. Host Python/weather libraries are not required to run containers.

```bash
docker compose config --quiet
docker compose build airflow-webserver
docker run --rm --network none --entrypoint python weather-airflow:2.9.3-python3.10 --version
docker run --rm --network none --entrypoint pip weather-airflow:2.9.3-python3.10 check
```

These commands do not run ingestion or initialize databases. After a successful build, start the system when needed:

```bash
docker compose up -d --no-build
docker compose ps
```

`up` runs initialization services and may initialize databases/buckets under the configuration. A new build does not replace running containers; use `up` when ready to apply it. Do not remove volumes to update the environment.

`AIRFLOW_IMAGE_NAME` in older `.env` files is no longer used. Compose controls the image name to avoid bypassing the Dockerfile or overwriting official image tags. Update Dockerfile, Compose tags, `.python-version`, constraints, and lockfiles together when changing Airflow/Python.

## Dependencies and updates

- `requirements.in`: direct dependencies and selected project versions.
- `constraints/airflow-2.9.3-python3.10.txt`: unchanged copy of [official constraints](https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt), used as resolver input.
- `requirements.txt`: locked direct/transitive dependencies with SHA-256; Docker installs with `--require-hashes`.
- Locks were generated with **uv 0.8.22**, targeting **Python 3.10/Linux x86_64**. Verify builds before claiming another architecture is supported.

Do not edit lockfiles manually. After changing `requirements.in`, regenerate using uv 0.8.22:

```bash
uv pip compile requirements.in --python-version 3.10 --python-platform linux --generate-hashes --output-file requirements.txt --no-header --quiet
docker compose build airflow-webserver
```

`cdsapi==0.7.7` (added 2026-10-22 for ERA5, ID 5) brings `ecmwf-datastores-client` 0.5.1, `multiurl` 0.3.9, and `tqdm` 4.66.4. Its credentials come from `CDSAPI_URL`/`CDSAPI_KEY` in `.env` (see `.env.example`), passed to the Airflow containers by Compose; the CDS account page shows the URL to use.

Airflow 2.9.3 requires SQLAlchemy 1.4; do not upgrade to 2.x independently. `s3fs` remains at 2023.12.2 under the constraints to align `fsspec`/`aiobotocore`. Older constraints support current-version compatibility; security upgrades require a separate upgrade/testing task.

Python dependencies and the Airflow base image are pinned. Dockerfile installs wheels (`--only-binary=:all:`) without additional compilers/apt packages; healthcheck `curl` is already present in the base. GeoJSON/NetCDF use native libraries bundled with wheels; current workflows do not require GDAL CLI. A new dependency without a target-architecture wheel will fail the build so support can be decided explicitly. MinIO/PostgreSQL retain existing image configuration; byte-identical rebuilds are not certified.

## Optional local Python

`.python-version` specifies **3.10** for Python managers/IDEs; it does not install Python. Use Python 3.10 and a separate virtual environment for scripts outside Docker. System Python 3.12 does not verify container runtime compatibility. The lock is verified for Linux; containers are the reference runtime. Native macOS/Windows environments require separate checks.

## Onboarding and code checks

Requirements: Git, Python >=3.10 for local tooling, GNU Make/bash, and Docker Desktop/Engine with Compose v2. Windows uses WSL2 with Docker integration. Container runtime Python remains 3.10 even if local tooling uses Python 3.12.

After cloning and entering the repository:

```bash
make setup          # Create .venv and install hashed requirements-dev.txt
make hooks          # Install pre-commit for this checkout
make lint           # Check Ruff lint and formatting
make test           # Build test target and run offline unit/runtime/DAG tests
make test-integration
```

`make help` lists commands. `make check` runs lint, Compose validation, and both test suites. Tests do not require project `.env`. Initial setup/build needs network access for packages/images; offline tests run with `--network none`.

Before committing:

```bash
make format         # Apply safe lint fixes and format scripts/dags/tests
make lint
make precommit      # Run hooks on all tracked files
```

Pre-commit also runs during `git commit`. If hooks change files, review the diff and stage again. Hooks use `.venv` Ruff, so run `make setup` per checkout. Makefile places pre-commit cache in `.venv/.cache/`. `make lint` checks untracked new files; hooks check them once staged.

To start development services, create `.env` from `.env.example` if missing, then run `make up`. `make down` stops services and preserves volumes. Do not run integration tests against the development stack.

## Test suite and isolated environment

- `pyproject.toml` configures Ruff/pytest without changing application packaging. Pytest collects only `tests/`; the old upload demo was removed. Parquet uses `pyarrow` consistently.
- `tests/fixtures/open_meteo_hourly.json` contains **three synthetic hours** crossing leap-day/month boundaries, with zero and null rain. It does not represent a complete year.
- Unit tests mock API/MinIO/subprocess and block sockets. They check UTC/units, retry behavior, ingestion scope, and subprocess status. Runtime tests cover Parquet/NetCDF/GeoJSON/CART. DAG tests check imports/dependencies without executing historical DAGs.
- `docker-compose.test.yml` is **standalone**: project `weather-tests`, DB `weather_test`, test credentials, PostgreSQL/MinIO tmpfs, internal network, no host ports, bind mounts, or development volumes. Do not combine it with main Compose.
- `make test-integration` ignores `.env`, applies the existing schema, and runs ingestion with mocked APIs against test services. It cleans up on success/failure and prints logs before cleanup on failure. Use `make test-integration TEST_PROJECT=weather-tests-alice` for a separate test project.
- Integration fixtures check test host/database/user before connecting. They apply the base schema and every numbered migration in initializer order, and verify reruns of that sequence, uniqueness/FKs, health tasks, one Bronze-object rerun, the Gold/DW loader (location-filtered reruns, retraction of removed rows and of a source missing from Silver, transaction recovery), the GHCNh and prediction loaders, and the OLAP views on synthetic rows.
- Pytest requires the `WEATHER_TESTING=1` guard, configured in the test image/Compose. Use `make test` rather than native pytest without required libraries.

Tests do not establish historical data quality or CART accuracy. Size-based skipping and Bronze-payload modification remain [documented limitations](progress.md), not fixes delivered by test setup. Masked per-file Open-Meteo failures were fixed on 2026-10-22.

## Tooling dependencies

`requirements-dev.in` declares pytest/Ruff/pre-commit. `requirements-dev.txt` locks transitive dependencies and hashes with Python/OS markers. Shared dependencies are constrained by the runtime lock. Regenerate with uv 0.8.22:

```bash
uv pip compile requirements-dev.in --python-version 3.10 --universal --generate-hashes --output-file requirements-dev.txt --no-header --quiet
make setup
make check
```

Regenerate the development lock and verify both targets when changing the runtime lock. Main Compose builds `runtime`; pytest builds `test` with tooling/fixtures. `docker build .` defaults to the final stage (`test`); use `docker build --target runtime` for direct runtime builds.

## GitHub Actions

`.github/workflows/ci.yml` runs for new/updated pull requests, pushes to `main`, and manual dispatch. It uses Python 3.10 for tooling and the same Makefile for Ruff, Compose validation, offline tests, and integration tests. CI validates pre-commit configuration rather than repeating the two Ruff hooks already covered by `make lint`; local hooks still run before commits. It uses no production secrets and performs no deployment or historical downloads.

After pushing the configuration, inspect Actions to verify an actual GitHub runner execution. To block merges when CI fails, configure branch protection/rulesets using status check **CI / Lint and tests** as displayed by GitHub. The workflow file does not enable protection automatically. See [progress](progress.md) to distinguish local checks from GitHub runs.

## Analysis notebooks

Notebooks live in `notebooks/` and use a separate environment so that Jupyter never enters the Airflow runtime or test images.

- `requirements-notebook.in` pins the libraries shared with the pipeline to the versions in `requirements.txt` (pandas, pyarrow, numpy, scikit-learn, minio) and adds matplotlib, ipykernel, nbconvert, and nbformat. A `-c requirements.txt` constraint is not used because that lock targets Python 3.10 only.
- Regenerate the lock with uv 0.8.22:

```bash
uv pip compile requirements-notebook.in --python-version 3.10 --universal --generate-hashes --output-file requirements-notebook.txt --no-header --quiet
```

- `make notebook-setup` creates `.venv-notebook` (ignored by Git), installs the hashed lock, and registers the Jupyter kernel `weather-notebook`.
- `make notebook-run` executes `notebooks/eda_rain_next24h.ipynb` in place; `make notebook-run NOTEBOOK=cart_tree_rules.ipynb` executes the CART tree and rules notebook (ID 30). It needs MinIO running with the Silver and Gold data described in `docs/progress.md`. Notebook logic lives in `scripts/` (for example `scripts/rain_eda.py`) and is unit-tested; notebooks only display results.
- Verified on macOS x86_64 with Python 3.12.4. Other platforms were not tested.
