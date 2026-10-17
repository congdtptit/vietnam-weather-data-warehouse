# Weather Data Warehouse Schema

Inventory date: 2026-10-08, based on `sql/init_dwh_schema.sql`. Distinguish existing DDL from proposals requiring migrations. The live database was not verified during this inventory.

## Existing model

| Table | DDL keys and relationships |
| --- | --- |
| `dim_date` | PK `date_key`; day/month/quarter/year and season/weekend flags |
| `dim_time` | PK `time_key`; hour/minute/time-of-day; seeds all 24 hours |
| `dim_location` | PK `location_key`; UNIQUE `location_code`; name/province/coordinates/elevation; eight seed points |
| `dim_source` | PK `source_key`; UNIQUE `source_name`; type/format/frequency; four seed sources |
| `dim_station` | PK `station_key`; UNIQUE `station_id`; FK `location_key` |
| `fact_weather_observation` | PK `fact_id`; date/time/location/source/station FKs; nullable `station_key` |
| `fact_weather_prediction` | PK `prediction_id`; prediction date/time, target date, and location FKs; model name/version |

The DDL does not seed `dim_date`/`dim_station` and does not include `DimModel`, a real loader, or OLAP views. Fact schema existence does not establish data population.

## Grain and keys

- Required observation grain: one source × one research point × one UTC hour.
- Existing constraint: UNIQUE `(date_key, time_key, location_key, source_key)`. `date_key` (YYYYMMDD) and `time_key` (HH00) use Vietnam Local Time (`Asia/Ho_Chi_Minh`, UTC+7), derived deterministically from canonical `timestamp_utc`.
- Proposed direct business key: `(source_key, location_key, timestamp_utc)`. This is not an existing constraint; migration and consumer checks are required before changing it.
- Surrogate keys support dimension/fact joins; business keys support lookups/upserts. Do not use `created_at` as an observation key.
- Multiple stations per source/location/hour require selection/aggregation before loading, or a separate station-grain fact if all stations are retained. `station_key` must not silently expand the current grain.

## Open decisions and gaps

| Item | Current state | Decision/implementation needed |
| --- | --- | --- |
| Calendar basis | Resolved: Vietnam Local Time (`Asia/Ho_Chi_Minh`, UTC+7) | `date_key` (YYYYMMDD) and `time_key` (HH00) derived deterministically from `timestamp_utc` |
| Date dimension | Seeded (2016–2026) | 4,018 daily records seeded via `scripts/seed_dimensions.py` |
| Boundaries | One `province` column | Old/new province mapping, codes, version/effective dates; SCD2 if history is required |
| Stations | Globally unique `station_id` | Verify source-specific namespaces, distance mapping, and selection rules |
| Prediction grain | No unique key or UTC origin/target window | Location × origin × horizon × model version, plus source/scenario when needed |
| Models | Fact stores name/version | Design `dim_model` and appropriate artifact metadata |
| Loader | Implemented and verified | `scripts/load_gold_observations.py` performs idempotent transactional upsert with rollback on failure |

`predicted_at` is currently a TIMESTAMP row-creation time, not forecast origin. One `target_date_key` cannot represent every rolling 24-hour window. Probability/threshold/label version and UTC target start/end require design during ML integration.

## Aggregation rules

| Metric | Aggregation |
| --- | --- |
| Hourly rain | SUM nonoverlapping intervals for the same source/location; disclose coverage |
| Temperature/humidity/pressure/wind speed | Appropriate AVG/MIN/MAX; roll up means from sums/counts or weights |
| Wind direction | Circular/vector aggregation, not a linear AVG of degrees |
| Rainy/hot days | Create source × location × day flags under agreed thresholds/coverage, then count |
| Forecast probability | Summarize a defined forecast population; do not SUM or interpret mean as regional probability |

Do not add rain across sources at the same point/hour. Do not add rainfall across points and call it regional mean rainfall. Partial totals are not complete daily totals. Day/month/year views need source/location/calendar basis and valid/expected counts; joins must not multiply rows.

## Migrations and verification

Initialization is for new databases only: `CREATE TABLE IF NOT EXISTS` does not change existing structures. `01_init_postgres.sh` uses literal `weather_dwh`; configurable `DWH_DB` is not fully supported. Do not delete volumes to change schemas.

Before changes, read the PostgreSQL skill, provide a separate migration, and check dimension uniqueness/FKs, fact grain, calendar boundaries, reruns, and hour → day → month rollups. Distinguish authored SQL from executed SQL and report the tested data scope.
