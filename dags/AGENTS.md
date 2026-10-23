# Airflow instructions

Read `../docs/architecture.md`, relevant portions of `../docs/data-contract.md`, and the invoked scripts before modifying a DAG. Use `weather-etl-airflow`; use `weather-data-quality` for quality gates.

- Check the configured Airflow version before selecting APIs. Current Compose builds the Dockerfile `runtime` target with Airflow 2.9.3/Python 3.10; the separate test stack builds target `test`.
- No network calls or data processing at DAG import. XCom carries small object references/manifests, not datasets.
- Historical and scheduled tasks use explicit windows or data intervals, not wall-clock time. Configure catchup, retries, concurrency, and backfill scope deliberately.
- Only advance incremental watermarks after downstream publication/commit succeeds. Reruns and overlapping windows must not create duplicate observations or forecasts.
- Propagate script failures to task failures. Since 2026-10-22 the Open-Meteo, Meteostat, GHCNh, and ERA5 ingesters exit non-zero when a requested file fails; still, a successful subprocess exit is not evidence that every requested file was loaded, so reconcile outputs (`scripts/reconcile_dw.py`). `weather_bronze_to_dw_dag` runs `scripts/quality_gate.py` before any DW write; keep every DW-writing task downstream of it.
- `weather_rain_scoring_dag` (ID 32) publishes only test-split predictions of a model already in `dim_model`; keep `publish_predictions` downstream of `score_cart` and the `--check` reconciliation last.
- The health-check DAG checks connectivity and basic schema presence, not data completeness, interval correctness, or pipeline completion.
- For changes, check DAG parsing and affected task behavior in an available environment; distinguish syntax-only checks from Airflow execution. Do not trigger a full backfill as a routine check.
