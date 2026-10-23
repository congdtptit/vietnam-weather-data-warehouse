# ETL instructions

Read `../docs/data-contract.md` and `../docs/NAMING_CONVENTIONS.md` for ingestion or transformation work. Use `weather-etl-airflow`; use `weather-data-quality` when implementing checks. Read `../docs/rain-label.md` when changing ML datasets or labels.

- Keep UTC canonical and use metadata-confirmed units and interval semantics. Missing rain is not zero; subhourly data needs explicit resampling rules.
- Preserve source payloads in Bronze and separate ingestion metadata. Every source complies (Meteostat since 2026-10-23: received `.csv.gz` with a manifest, 404s recorded); the Open-Meteo, Meteostat, and ERA5 Silver transforms verify Bronze against its manifest checksum.
- Maintain source × location × UTC-hour uniqueness in normalized observations. Resolve conflicting duplicates explicitly and retain provenance.
- Give failures observable failure status so Airflow can retry. Do not report successful ingestion when requested files failed.
- Use bounded requests/retries and explicit scopes. A CLI default spanning 2016–2025 must not become an automatic validation run.
- Synthetic upload/runtime checks live in `tests/` and run through the isolated test stack. They are not evidence of a complete Silver/Gold pipeline.
- Validate changed transformations with small representative samples, including relevant missing-hour, unit, boundary, and rerun cases. Do not contact external sources for a documentation-only change.
