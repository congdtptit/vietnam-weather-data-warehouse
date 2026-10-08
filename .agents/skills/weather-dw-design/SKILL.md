---
name: weather-dw-design
description: "Design or review a PostgreSQL weather Data Warehouse: Star Schema at source × location × hour grain, dimensions/facts, deduplication keys, loading, aggregation rules, and OLAP views. Use for weather schemas, migrations, or analytical SQL, not general ETL or ML training."
---

# Weather Data Warehouse

## Read before designing

Read `sql/AGENTS.md`, `docs/warehouse-schema.md`, `sql/init_dwh_schema.sql`, and affected ETL/BI consumers relative to the repository root. Read relevant `docs/data-contract.md` definitions for measurement/loading changes. Read `supabase-postgres-best-practices` before writing/changing SQL, schemas, or PostgreSQL data operations if available; select relevant references rather than loading all of them.

`docs/warehouse-schema.md` owns actual/proposed keys, calendar basis, and project aggregation rules. Distinguish existing constraints from pending designs; this skill supplies reusable design and verification procedures.

Distinguish current schema from proposed design. Initialization is not migration: `CREATE TABLE IF NOT EXISTS` does not update existing tables. Provide migrations and consumer compatibility checks for schema changes; do not reset the database automatically.

## Grain and keys

- `fact_weather_observation`: one source × one stable research location × one UTC hour. Proposed business unique key: `(source_key, location_key, timestamp_utc)`; inspect existing constraints before changing them.
- `date_key`/`time_key` use the same documented basis, UTC or Vietnam local time. Keep UTC canonical; if BI needs both calendars, define separate key sets/roles or views. Test 17:00 UTC becoming the next calendar day in Vietnam.
- Dimension surrogate keys support joins; business keys support lookups/upserts. Do not create dimensions arbitrarily for each fact row or use changing province names as location identifiers.
- Use `DimDate`, `DimTime`, `DimLocation`, `DimSource`, `DimStation`; add `DimModel` for predictions. Check dimension uniqueness and foreign keys before publication.
- Station IDs may be unique only within a source; namespace by source when needed. Grid sources may have no station: distinguish not-applicable from failed lookups in quality checks.
- Multiple stations for the same source/location/hour violate target grain. Define station selection or aggregation with provenance before loading. If retaining all stations, use a separate station-grain fact or agree on a grain change. Do not add `station_key` to uniqueness while retaining the original grain description.
- `DimLocation` represents a coordinate point with old/new province names, administrative codes, and mapping versions/effective dates where needed. Distinguish current-boundary maps from historical-boundary reporting; use SCD2 only when history is required.
- Prediction grain: location × UTC forecast origin × horizon × model version, plus source/scenario for multiple configurations. Distinguish `forecast_origin` from execution time `predicted_at`; retain target window, probability, class, threshold, and label version for idempotent reruns.

## OLAP aggregation rules

| Variable/metric | Rule |
| --- | --- |
| Hourly precipitation | SUM over nonoverlapping intervals for the same source/location; disclose coverage |
| Temperature/humidity/pressure/wind speed | AVG/MIN/MAX for the question; never SUM; roll up means from sums/counts or appropriate weights |
| Wind direction | Circular/vector mean following the convention, not linear AVG of degrees |
| Rainy/hot days | Compute flags at source × location × day grain with explicit thresholds/coverage, then count |
| Forecast probability | Summarize a defined forecast population; never SUM or interpret AVG as regional probability |

- Do not SUM rain across sources at the same location/hour, or across locations and call it regional mean precipitation. Compare sources separately or use a documented fusion rule.
- Do not average daily/monthly averages with unequal counts/coverage. Missing-hour rain totals are partial or incomplete, not complete daily totals.
- Distinguish surface/sea-level pressure and instantaneous/interval variables before comparison; prevent join fan-out.
- Daily/monthly/yearly views retain source, location, calendar basis, `valid_count`, and `expected_count`. Support year-month-day-hour and geographic roll-ups with traceability to source facts.

## Loading and performance

- Load dimensions before facts; resolve keys once per batch and check missing keys. Stage/merge in a transaction; unique constraints prevent concurrent duplicates.
- Null measurements differ from zero. Choose numeric types for source precision/range. Use `TIMESTAMPTZ` for UTC instants; derive local time consistently.
- Add indexes/partitions/materialized views only when workloads/data demonstrate benefits. Inspect representative query plans; document materialized-view refresh behavior.
- Verify preservation of fact history, surrogate keys, BI consumers, and idempotency after migrations. Total counts alone do not establish join correctness.

## Required output

Document grain, business/surrogate keys, an ERD or relationship table, calendar basis, aggregation rules, and SQL/migrations within scope. Reconcile sample hour → day → month roll-ups, check fan-out, unique dimension keys, and duplicate-free reruns. Distinguish SQL written from SQL executed.
