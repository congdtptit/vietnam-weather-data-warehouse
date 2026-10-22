# Weather Data Warehouse Schema

Inventory 2026-10-08, based on `sql/init_dwh_schema.sql`; updated through 2026-10-22 with migrations `02`–`05`. Rows marked resolved or verified were checked against the live `weather_dwh` on the date given in [progress](progress.md). Distinguish existing DDL from proposals requiring migrations.

## Existing model

| Table | DDL keys and relationships |
| --- | --- |
| `dim_date` | PK `date_key`; day/month/quarter/year and season/weekend flags |
| `dim_time` | PK `time_key`; hour/minute/time-of-day; seeds all 24 hours |
| `dim_location` | PK `location_key`; UNIQUE `location_code`; name/province/coordinates/elevation; eight seed points |
| `dim_source` | PK `source_key`; UNIQUE `source_name`; type/format/frequency; four seed sources (Open-Meteo, ERA5, Meteostat, NOAA) plus GHCNh from `03`. ERA5 and NOAA have no rows in any fact, and GHCNh only in `fact_station_daily_rain`; BI source slicers should list only sources with data |
| `dim_station` | PK `station_key`; UNIQUE `station_id`; FK `location_key` (the Meteostat/hourly mapping); `station_role` (`representative` or `nearest_available`) and `distance_km` to the research point from `05`. `location_key` is NULL for unmapped ids, including 48825 Hà Đông and 48823 Nam Định, which are mapped only for GHCNh daily rain: join `fact_station_daily_rain` on its own `location_key`, not through `dim_station`. 48820 is Nội Bài airport, 22.0 km from HAN |
| `fact_weather_observation` | PK `fact_id`; date/time/location/source/station FKs; nullable `station_key`; `pressure_type` (`surface` for Open-Meteo, `sea_level` for Meteostat, from `05`): the two pressures are not comparable, so never mix sources in a pressure visual |
| `fact_weather_prediction` (extended by migration `sql/04_predictions.sql`) | PK `prediction_id`; UNIQUE `(model_key, location_key, origin_utc)`; FKs model/location/prediction date and time/target date; `origin_utc`, target window `(target_start_utc, target_end_utc]` with `target_start_utc = origin_utc` and a CHECK on `horizon_hours`; `rain_probability`, `rain_next_24h`, `probability_threshold`, `evaluation_split`. `prediction_date_key`/`prediction_time_key` are the Vietnam local date and hour of the origin; `target_date_key` is the local date of `target_end_utc`; `predicted_at` is load time |
| `dim_model` (migration `sql/04_predictions.sql`) | PK `model_key`; UNIQUE `(model_name, model_version)`; algorithm, label/feature versions and sources, label threshold (mm), probability threshold, horizon, training window, hyperparameters (JSONB), artifact URI and SHA-256, validation/test metrics, availability note. From `05`: `label_version` and `validation_f1` describe training and selection; `evaluation_label_version` and `validation_f1_evaluation_labels` give the labels the `test_*` metrics were measured on. CART v1: trained and selected on `rain_next24h_v0.2` (validation F1 0.7857); test metrics re-scored on `rain_next24h_v0.2.1` (validation F1 on those labels 0.7887). Compare test with `validation_f1_evaluation_labels`, not with `validation_f1` |
| `fact_station_daily_rain` (migration `sql/03_station_daily_rain.sql`) | PK `daily_rain_id`; UNIQUE `(source_key, station_key, rain_day_utc)`; FKs source/station/location; 24-hour gauge rain for `(D-1 12:00, D 12:00]` UTC; view `vw_rain_station_vs_model` |

The DDL does not seed `dim_date`/`dim_station`. `dim_model` and the prediction keys come from `sql/04_predictions.sql` (extended by `sql/05_metadata_corrections.sql`), which also adds index `idx_fwo_location_source_ts` on the observation fact and view `vw_prediction_vs_actual` (prediction versus label-source rain over the same window; actual and outcome are NULL unless all 24 hours exist). OLAP views live in `sql/02_olap_views.sql` (applied by the initializer for new databases and manually for existing ones): `vw_obs_hourly`, `vw_obs_daily`, `vw_obs_monthly`, `vw_obs_yearly`, `vw_coverage`, and helper `vw_calendar_month`. Fact schema existence does not establish data population.

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
| Boundaries | One `province` column holding pre-merger names | Decided 2026-10-21 by the N2 owner of IDs 4/11: pre-merger (63-province) boundaries, no old/new mapping or SCD2 (task specs §7). The workbook ID 11 description was rewritten on 2026-10-22 to this scope and quotes the original "tỉnh cũ + tỉnh mới". Province polygons (ID 4) are in Silver `spatial/boundaries/`, joined to `dim_location.province` by name for maps; they are not loaded into the DW |
| Stations | Globally unique `station_id`; at most one hourly station per location; roles and distances from `scripts/station_config.py` | Distances are computed once (haversine), not with GeoPandas as ID 11 suggests. Source-specific namespaces remain unverified beyond WMO ids |
| Prediction grain | Resolved in `sql/04_predictions.sql` (2026-10-21): model version × location × UTC origin, fixed 24 h horizon per model | Add a scenario/source column only if several input configurations are scored |
| Models | Resolved: `dim_model` with versions, thresholds, artifact hash, and metrics | Loader `scripts/load_predictions.py` upserts on `(model_name, model_version)` |
| Loader | Implemented and verified | `scripts/load_gold_observations.py` performs idempotent transactional upsert with rollback on failure. Since 2026-10-22 the per-month retraction covers the requested scope (Open-Meteo and Meteostat × requested locations), so a source that disappears from Silver is removed; a missing Open-Meteo Silver location-month fails the month instead of deleting its rows |

`predicted_at` is a row-load time, not the forecast origin; use `origin_utc`. A rolling 24-hour window usually spans two local dates, so `target_date_key` (date of the window end) is only a calendar convenience; reconcile on `target_start_utc`/`target_end_utc`.

## Aggregation rules

| Metric | Aggregation |
| --- | --- |
| Hourly rain | SUM nonoverlapping intervals for the same source/location; disclose coverage |
| Temperature/humidity/pressure/wind speed | Appropriate AVG/MIN/MAX; roll up means from sums/counts or weights |
| Wind direction | Circular/vector aggregation, not a linear AVG of degrees |
| Rainy/hot days | Source × location × local-day flags, then count. Decided 2026-10-21: rainy day = 24 valid rain hours and daily rain ≥ 1.0 mm; hot day = 24 valid temperature hours and daily maximum ≥ 35.0 °C. Incomplete days are unknown (NULL), and a month with no complete day reports NULL, not 0 |
| Forecast probability | Summarize a defined forecast population; do not SUM or interpret mean as regional probability |

Do not add rain across sources at the same point/hour. Do not add rainfall across points and call it regional mean rainfall. Partial totals are not complete daily totals. Day/month/year views need source/location/calendar basis and valid/expected counts; joins must not multiply rows.

## Migrations and verification

Initialization is for new databases only: `CREATE TABLE IF NOT EXISTS` does not change existing structures. `01_init_postgres.sh` uses literal `weather_dwh`; configurable `DWH_DB` is not fully supported. Do not delete volumes to change schemas.

Before changes, read the PostgreSQL skill, provide a separate migration, and check dimension uniqueness/FKs, fact grain, calendar boundaries, reruns, and hour → day → month rollups. Distinguish authored SQL from executed SQL and report the tested data scope.
