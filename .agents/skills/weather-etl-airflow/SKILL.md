---
name: weather-etl-airflow
description: "Build, fix, or review Python/Airflow weather pipelines for Open-Meteo, Meteostat/NOAA, and ERA5 NetCDF, including Bronze/Silver/Gold normalization, backfills, incremental processing, and idempotent loading. Use for weather ingestion, transformation, or orchestration in this project."
---

# Weather ETL and Airflow

## Context and scope

- Project stack: MinIO, Python, Airflow, PostgreSQL. Preserve the architecture unless the user requests a change.
- For ETL work, read `docs/data-contract.md`, `docs/NAMING_CONVENTIONS.md`, scoped `scripts/AGENTS.md`/`dags/AGENTS.md`, and affected producers/consumers. Read `README.md`, `docs/architecture.md`, configuration, and dependencies when setup/orchestration is affected. Paths are relative to the repository root.
- Target observation grain: one source × one location × one hour. Keep sources separate; a pivoted ML dataset is a different product.
- Canonical time is timezone-aware UTC. Derive `timestamp_local` using `Asia/Ho_Chi_Minh`; do not assume naive timestamps are UTC. If the README conflicts with naming conventions, report the conflict and follow the UTC requirement in the proposal/task allocation.
- Fetch only requested periods, locations, and variables. Do not launch a ten-year backfill for a code-writing or review task.

## Data contract

`docs/data-contract.md` owns project column definitions, units, and decision status. Do not duplicate or silently approve pending choices here. Consult `docs/progress.md` before claiming current ingestion complies with the target contract.

Before integrating a source, document `source`, `location_code`, `timestamp_utc`, units, each variable's representative interval, interval-start/end timestamp semantics, null handling, product version, and provenance. Use existing names: `temperature_c`, `humidity_percent`, `pressure_hpa`, `precipitation_mm`, `wind_speed_ms`, `wind_direction_deg`, `cloud_cover_percent`, `solar_radiation_wm2`.

- Bronze: preserve received payloads/files. Store ingestion metadata separately: requests with secrets removed, source/product, retrieval time, checksum, and coverage. Preserve source revisions through versions/manifests rather than overwriting evidence.
- Silver: normalize UTC, units, and keys in Parquet; retain lineage, missing/outlier flags, and deterministic deduplication rules. Do not turn missing precipitation into zero.
- Gold: separate observations serving the DW from ML features; document each product's grain. Do not sum or average sources as though they were independent observations of the same quantity.
- MinIO buckets and object keys are separate: documented `bronze/open_meteo/...` means bucket `bronze` plus key `open_meteo/...`; do not duplicate the bucket name.
- Follow the naming document for stable location codes and annual/monthly windows. Combined-source files retain a source column; separate-source files need distinct keys. Update producers/consumers together when changing paths, and distinguish proposed keys from implemented ones.

## Ingestion and normalization

- During implementation, verify endpoints, variables, units, request limits, and semantics against official documentation for the actual product/version; do not infer semantics from names.
- APIs: explicit timeouts; check HTTP/application errors, array lengths, returned coverage, and source metadata. Split requests into appropriate windows.
- NetCDF: inspect dimensions, coordinates, CRS, `units`, `calendar`, `time`, `valid_time`, `step`, and accumulation metadata. Select grid cells by coordinates and record actual coordinates/offsets. Close datasets after processing; avoid loading ten years into RAM.
- Convert K to °C by subtracting 273.15, Pa to hPa by dividing by 100, precipitation m to mm by multiplying by 1000, and km/h to m/s by dividing by 3.6 when metadata confirms these units. Convert a 0–1 cloud fraction to percent only for the corresponding variable type.
- Distinguish surface from sea-level pressure. Compute wind speed/direction from u/v using the source's meteorological convention; calm wind may have no direction.
- ERA5 and ERA5-Land may package accumulated precipitation differently. Difference values only after verifying forecast cycles/resets and accumulation intervals; do not `diff()` across resets and clip negatives to zero. Convert radiation J/m² to W/m² only after handling accumulation and dividing by the correct interval duration in seconds.
- Do not silently merge subhourly records by flooring timestamps. Define variable-specific resampling rules when needed.

## Orchestration, retries, and reruns

- No API calls, connections, or downloads during DAG import. XCom carries small URIs/keys/manifests, not DataFrames or large payloads. Use Airflow Connections or environment variables for secrets.
- Use DAG data intervals or explicit backfill parameters. Internal windows use `[start, end)`; adapt inclusive API conventions at the adapter boundary. Do not use `now()` to select historical-run data.
- Bound retries for transient timeouts, 429s, and 5xx responses; use exponential backoff/jitter and `Retry-After` when available. Stop and report configuration/authentication/schema failures; do not retry indefinitely or suppress errors to report success.
- Backfills: split by source/location/month, limit concurrency to resources/quotas, support dry runs and resumption. Enable catchup only for the requested historical scope.
- Incremental processing: watermarks per source/location, lookback for late data, and window manifests. Advance watermarks only after successful target publication/commit; account for incomplete source periods using configured publication delays.
- Reruns must be idempotent: grain-based unique keys, staging + transactions + upserts, or explicitly scoped partition replacement. Do not use `created_at` as an observation key. Control overlapping runs on the same partition and preserve previous successful output if publication fails.
- When writing SQL/loading PostgreSQL, read `supabase-postgres-best-practices` if available; apply `weather-dw-design` grain rules when changing schemas.

## Verification and delivery

Test a small window: first run, rerun without additional keys, failure then resumption, late records, month/year boundaries, and UTC-to-Vietnam-time conversion. For accumulation, test resets and missing hours. Run cases relevant to the change.

Report changed files, actual processed scope, before/after row/key counts, watermarks, retry/idempotency results, and unverified parts. Do not claim a ten-year pipeline was validated by a smoke test.
