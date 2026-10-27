# Reproduce the Project from Scratch

These are the steps that rebuilt the whole project from an empty data lake and an empty DW on 2026-10-23 (workbook ID 34). The run used a fresh clone of commit `18ae289` and, for the release steps, `6e87190`; the timings come from that run. Evidence and the comparison with the original machine are in [progress](progress.md).

## One command

Since 2026-10-25 the steps below run with one command (`scripts/reproduce.sh`):

```bash
cp .env.example .env        # fill CDSAPI_URL and CDSAPI_KEY
make reproduce MODEL_RELEASE=/path/to/model_release_cart_v1
```

It builds and starts the stack, waits for Airflow, then runs sections 3–5 in order and stops at the first failure: boundaries, `weather_bronze_to_dw_dag` for 2016–2025 (it waits for the run), labels and features, the model release import, the test predictions, `weather_rain_scoring_dag` for 2025-01-01..2025-12-30, the Gold ML pivot, the station check, and a final check of the cubes and DW row counts. Options: `START_YEAR`/`END_YEAR` (default 2016/2025; a smaller window is for testing), `MODEL_RELEASE` (default `outputs/model_release_cart_v1`), `DRY_RUN=1` (print the steps only).

Every step is idempotent, so a rerun on a machine that holds the data only verifies it: sources with current manifests are not downloaded, loads leave identical rows unchanged, the labels/features/pivot scripts skip a version whose manifest exists (`--skip-existing`), the release import skips identical objects, and `load_predictions.py` rewrites a prediction only when a value differs. Verified on 2026-10-25 on the existing stack with `START_YEAR=2025 END_YEAR=2025`: 290 s, the DAG ran 15/15 tasks, and the md5 fingerprints of every DW table were unchanged. A full run on fresh volumes with this command has not been done; the manual steps it automates were verified that way on 2026-10-23.

## 1. Prerequisites

- Git, Docker Desktop or Engine with Compose v2, internet access, and about 10 GB of free disk.
- A Copernicus CDS account that has accepted the ERA5 licence, with its API key (for the ERA5 time series).
- The CART v1 release bundle `model_release_cart_v1/`: 12 files, 28 MB, from the project owner. It is not in Git.
  - Model v1 was trained on labels `rain_next24h_v0.2`, and their float-sum bug was fixed on 2026-10-22, so today's code cannot rebuild the v1 labels and features.
  - The bundle carries them, together with the model and its one-time test evaluation.
  - Everything else is rebuilt from the sources.

## 2. Configure and start

```bash
git clone https://github.com/congdtptit/vietnam-weather-data-warehouse.git
cd vietnam-weather-data-warehouse
cp .env.example .env        # fill CDSAPI_URL and CDSAPI_KEY; keep the other values
docker compose build
docker compose up -d --no-build
docker compose ps           # postgres healthy; minio-init and airflow-init exited with 0
```

Initialization creates the `bronze`, `silver`, and `gold` buckets and the `weather_dwh` schema, applying migrations `02`–`06`. It also seeds `dim_location` (8 rows), `dim_source` (5), and `dim_time` (24), and creates the Airflow user `admin`. Airflow is at <http://localhost:8080>. `weather_incremental_dag` starts paused.

To run one project script inside the Airflow image:

```bash
run() { docker compose run --rm --no-deps -T --entrypoint python airflow-webserver /opt/airflow/scripts/"$@"; }
```

## 3. Data: sources → Bronze → Silver → Gold → DW

```bash
run ingest_boundaries.py    # province boundaries (ID 4), once; 17 s
```

Trigger `weather_bronze_to_dw_dag` with `start_year` 2016, `end_year` 2025, and `location` empty, either in the Airflow UI (Trigger DAG w/ config) or through the API:

```bash
curl -u admin:admin -X POST -H 'Content-Type: application/json' \
  http://localhost:8080/api/v1/dags/weather_bronze_to_dw_dag/dagRuns \
  -d '{"conf": {"start_year": 2016, "end_year": 2025, "location": ""}}'
```

Expected: all tasks succeed in about 15 minutes. The verified run of 2026-10-23 had 14 tasks; since 2026-10-25 the DAG has a 15th, `refresh_olap_cubes`, which refreshes and checks the precomputed sub-cubes (about 5 s on the full data).

- Downloads: Open-Meteo 80 files (349 s), GHCNh 50 (231 s), ERA5 8 point series (53 s), and Meteostat 15 files plus 25 station-years recorded as HTTP 404 (37 s).
- The quality gate runs 1,032 checks with 0 failures.
- The Gold load takes 455 s.
- Reconcile: 1,490,237 hourly rows and 17,866 station-days.

A second run of the same window downloads nothing (manifests are current) and works without internet.

## 4. ML datasets, model, and predictions

```bash
run export_rain_labels.py --start-year 2016 --end-year 2025     # labels v2, 17 s
run build_rain_features.py --start-year 2016 --end-year 2025    # features v2, 21 s
docker compose run --rm --no-deps -T -v "/path/to/model_release_cart_v1:/release:ro" \
  --entrypoint python airflow-webserver /opt/airflow/scripts/model_release.py import --from /release
run load_predictions.py --version v1                            # dim_model + 69,888 predictions, 25 s
```

Then trigger `weather_rain_scoring_dag` with `start_date` 2025-01-01, `end_date` 2025-12-30, `model_version` v1. Expected: 4/4 tasks in about 40 s. The scoring recomputes the features from Silver and finds all 69,888 predictions unchanged.

Do not run `train_cart.py` or `evaluate_cart_test.py` for v1. The model is frozen and test 2025 has been used once; both scripts refuse.

To check that training reproduces the release, which takes about 4 minutes and reads train and validation only:

```bash
run train_cart.py --feature-version v1 --version v1check
```

The report's `objects.reproduces_released_version` must be `v1`. On 2026-10-23 the retrained model had the release's SHA-256 `1f667b91…`.

## 5. Other products and checks

```bash
run build_ml_pivot.py --start-year 2016 --end-year 2025   # Gold ML multi-source pivot (ID 16), 15 s
run assign_stations_geo.py                                # GeoPandas station check (ID 11), 10 s
run benchmark_dw_queries.py                               # DW query timings (ID 26)
```

The web dashboard reads `localhost:5434`, database `weather_dwh`: run `make notebook-setup` once, then `make dashboard` (http://localhost:8501). It replaced Power BI on 2026-10-27; the page specification is [powerbi-dashboard-v1.md](powerbi-dashboard-v1.md).

## 6. Expected content

The run of 2026-10-23 matched the original machine on every DW table and view, compared by natural keys (`location_code`, `source_name`, `station_id`, UTC time):

| Item | Rows |
| --- | --- |
| `fact_weather_observation` | 1,490,237 |
| `fact_station_daily_rain` | 17,866 |
| `fact_weather_prediction` | 69,888 |
| `dim_date`, `dim_time`, `dim_location`, `dim_station`, `dim_source`, `dim_model` | 4,018, 24, 8, 12, 5, 1 |
| `vw_obs_daily` | 62,118 |

Byte-identical with the original machine:

- Silver: 1,080 hourly objects, 960 ERA5 objects, 50 daily-rain objects.
- Gold: 120 observation months, 364 prediction days, labels and features v2, the ML pivot.
- Bronze payloads of ERA5, GHCNh, Meteostat, and the boundaries.

Expected differences, which do not change any data:

- Open-Meteo Bronze payloads differ in `generationtime_ms`, the server's processing time, so their SHA-256 differs. The Silver built from them is identical.
- Manifests and reports carry their own retrieval or generation times.
- Surrogate keys depend on load history. For example, station 48823 has `station_key` 23 on the original machine and 12 on a fresh one. The content by `station_id` is the same.
- `vw_obs_daily` sums wind-direction sines and cosines in floating point, so its last digits can vary between runs on the same database. Rounded to 6 decimals, both machines give the same view.
