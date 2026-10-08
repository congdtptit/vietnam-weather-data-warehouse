# ETL instructions

Read `../docs/data-contract.md` and `../docs/NAMING_CONVENTIONS.md` for ingestion or transformation work. Use `weather-etl-airflow`; use `weather-data-quality` when implementing checks. Read `../docs/rain-label.md` when changing ML datasets or labels.

- Keep UTC canonical and use metadata-confirmed units and interval semantics. Missing rain is not zero; subhourly data needs explicit resampling rules.
- Preserve source payloads in Bronze and separate ingestion metadata. Current Open-Meteo behavior differs from this target; see `../docs/progress.md` before claiming compliance.
- Maintain source × location × UTC-hour uniqueness in normalized observations. Resolve conflicting duplicates explicitly and retain provenance.
- Give failures observable failure status so Airflow can retry. Do not report successful ingestion when requested files failed.
- Use bounded requests/retries and explicit scopes. A CLI default spanning 2016–2025 must not become an automatic validation run.
- `test_minio_upload.py` uploads synthetic samples and writes objects. It is not a read-only unit test, a real boundary dataset, or evidence of a complete Silver/Gold pipeline.
- Validate changed transformations with small representative samples, including relevant missing-hour, unit, boundary, and rerun cases. Do not contact external sources for a documentation-only change.
