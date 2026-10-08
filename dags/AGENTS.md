# Airflow instructions

Read `../docs/architecture.md`, relevant portions of `../docs/data-contract.md`, and the invoked scripts before modifying a DAG. Use `weather-etl-airflow`; use `weather-data-quality` for quality gates.

- Check the configured Airflow version before selecting APIs. Current Compose defaults to Airflow 2.9.3; it does not build the custom Dockerfile.
- No network calls or data processing at DAG import. XCom carries small object references/manifests, not datasets.
- Historical and scheduled tasks use explicit windows or data intervals, not wall-clock time. Configure catchup, retries, concurrency, and backfill scope deliberately.
- Only advance incremental watermarks after downstream publication/commit succeeds. Reruns and overlapping windows must not create duplicate observations or forecasts.
- Propagate script failures to task failures; current ingestion code can mask per-file failures. A successful subprocess exit is not evidence that every requested file was loaded.
- The health-check DAG checks connectivity and basic schema presence, not data completeness, interval correctness, or pipeline completion.
- For changes, check DAG parsing and affected task behavior in an available environment; distinguish syntax-only checks from Airflow execution. Do not trigger a full backfill as a routine check.
