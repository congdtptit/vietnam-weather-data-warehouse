# Weather Data Contract

Document version: v0.2, 2026-10-21. v0.2 confirms the Silver columns, units, UTC, pressure types, and duplicate handling below (confirmed by the project member working on N1/N2 on 2026-10-21; that member accepted their confirmation as team approval). Station selection is recorded below; coverage has warning thresholds (quality gate, 2026-10-22) but no blocking minimum. This is the target implementation contract; the Silver stage has been executed on real data per `docs/progress.md`, but full-task acceptance is separate.

## Decision status

| Item | Status and basis |
| --- | --- |
| Canonical UTC, standard units, and Parquet | Existing naming and task-allocation conventions |
| Observations: source × location × hour | Proposal requirement |
| Main variable names below | Existing conventions used by the DDL |
| Conflicting duplicates: identical rows collapse deterministically; conflicting rows are excluded from clean output and saved separately | Confirmed in v0.2 (matches implemented behavior) |
| `pressure_type` (`surface` for Open-Meteo, `sea_level` for Meteostat) and `source_name`/`station_id` provenance retained; the two pressures are never merged | Confirmed in v0.2. Kept through Gold and, since migration `05`, in `fact_weather_observation`. The retained Meteostat 48820 pressure is mostly METAR QNH (altimeter setting), which approximates but is not mean sea-level pressure |
| Station selection | Decided 2026-10-21 (implemented in `scripts/station_config.py`): at most one verified WMO station per canonical location. Verified mappings: HAN `48820`, HPH `48826`, TNG `48831`, LCA `48803`. `48820` is listed by Meteostat as "Ha Noi", but its hourly values come from Nội Bài airport (NOAA ISD 488200 "NOIBAI INTL", METAR VVNB; 21.221, 105.807, 11.9 m), 22.0 km from the HAN point, so it is recorded as `nearest_available` (2026-10-22; the others are `representative`, 1.6–7.6 km); PTO, TUQ, HBI, NBI have no verified station and use gridded sources only. Stations without a confirmed mapping are rejected at publication, including the misassigned Table 6 ids (`48825`, `48830`, `48838`, `48840`), the unverified `48816`, and regional `48806`/`48808`. No multi-station aggregation. Open-Meteo and Meteostat stay separate sources and are never summed. Only `48820` and `48826` are currently served by Meteostat |
| Meteostat model-filled values | Decided 2026-10-21 (implemented in `scripts/transform_meteostat_to_silver.py`): a Meteostat value is kept only when its `<variable>_source` is a station observation source (`isd_lite`, `metar`, `synop`, `national`). Model sources such as `dwd_mosmix` and `metno_forecast`, and unknown names, are nulled. Rows left without any measurement are dropped, and the counts are reported in the DQ report (`source_filter`). With `--overwrite`, Silver months that no longer have observations are removed. Payloads without any `<variable>_source` columns are kept as `unverified_source`. Effect on 2016–2025: Meteostat keeps HAN/48820 only and contains no precipitation, because all Meteostat rain for both stations was model-filled |
| Coverage | Decided 2026-10-21: report record coverage per source × location × window (`scripts/quality_checks.py`); do not impute missing hours. Since 2026-10-22 coverage below the warning thresholds of the quality gate (row below) raises a warning; no coverage minimum blocks publication, except complete hours for Open-Meteo |
| Product-specific interval semantics and pressure/radiation mapping | Verify source metadata/documentation during implementation |
| Additional lineage/DQ column names and types | Contract proposals requiring producer/consumer agreement |
| Known implementation gaps (audit 2026-10-22) | Open-Meteo Silver drops the API's grid coordinates and elevation, and assigns UTC to the naive `time` values without checking `utc_offset_seconds` (the requests set `timezone=UTC`, and stored payloads show offset 0); since 2026-10-22 the Bronze manifest keeps the grid coordinates, elevation, `utc_offset_seconds`, and time zone of each response, but Silver does not yet use them. Meteostat Bronze stores the decompressed CSV without a manifest or checksum. GHCNh extreme support takes sub-daily values without checking their quality codes and does not read the previous year's file for 1 January windows. Not fixed; recorded so they are not claimed as compliant |
| ERA5 Bronze | `reanalysis-era5-single-levels`, area `[23.5, 102.0, 19.5, 107.5]` (N, W, S, E), 7 variables (`t2m`, `d2m`, `tp`, `sp`, `u10`, `v10`, `tcc`), one request per month, NetCDF. The CDS returns a zip of two NetCDF files (`stepType-instant`: 6 variables; `stepType-accum`: `tp`), stored unchanged; the manifest records the CDS request and job id, SHA-256, and per member the variables, units, 744/720/… hourly steps on `valid_time`, and the grid (0.25°, 17 × 23 points). Units as delivered: K, Pa, m s**-1, (0 - 1), and m for `tp`; conversion belongs to Silver (ID 15). Not yet compared with Open-Meteo. Requests must be sequential: on 2026-10-22 the CDS rejected four concurrent requests ("Maximum number queued requests for this dataset has been reached"), and a job left queued by a stopped run blocks the next one until it is cancelled. **Point time series (decided 2026-10-23, used for Silver):** `reanalysis-era5-single-levels-timeseries`, the same ERA5 hourly data served from the nearest 0.25° cell of each research point, one request per location for the whole window (`scripts/ingest_era5_timeseries.py`; 8 × 2016–2025 in 320 s on 2026-10-22, against about two hours of queue per gridded month). Each response (a zip with one NetCDF) is stored unchanged with a manifest (request, job id, SHA-256, 87,672 hourly steps, grid cell, units, null counts). On 2024-01 and 2024-02 the series equals the gridded files at the same cell (tp, u10, v10, tcc identical; t2m, d2m within 0.00025 K; sp within 0.07 Pa). The gridded monthly download was stopped on 2026-10-23 with 4 months stored (2016-01/02, 2024-01/02); its queued 2024-03 job was cancelled; those months remain as evidence of the regional request |
| ERA5 Silver (ID 15) | Implemented 2026-10-23 (`scripts/transform_era5_to_silver.py`), keys `era5/weather_hourly/year=YYYY/month=MM/era5_hourly_<LOCATION>_<YYYYMM>.parquet`, outside `weather_hourly/`, which only Open-Meteo and Meteostat use; since 2026-10-23 the Gold loader also reads this prefix and loads ERA5 into the DW (decided by the user). The DW rounds values to 2 decimals, so ERA5 rain is stored to 0.01 mm. The quality gate treats ERA5 like Open-Meteo (every hour required, no missing value except radiation) with its own pressure limit 900–1050 hPa, since the grid cell's surface pressure reaches 916.9 hPa at LCA. Standard Silver columns with `source_name` `ERA5`, `observation_type` `model_reanalysis`, `pressure_type` `surface`, plus `dew_point_c`, `grid_latitude`, `grid_longitude`. Conversions: K − 273.15; Pa / 100; tp m × 1000 (ERA5 hourly tp is the total over the hour ending at `valid_time`, so no de-accumulation, unlike ERA5-Land); wind speed and the direction the wind blows from, from u10/v10 (direction null in calm); tcc × 100; relative humidity from t2m and d2m by the Magnus formula (a = 17.625, b = 243.04 °C), set to 100% where d2m exceeds t2m by at most 0.01 K (float precision; 0–613 hours per location, counted in the report). Rounded to 2 decimals (temperature, pressure, wind speed), 1 (humidity, direction, cloud), 3 (rain). `solar_radiation_wm2` is null (not requested). A Bronze object that does not match its manifest SHA-256, a missing hour, or a duplicate hour fails the location. Cross-source comparison: `scripts/compare_era5_open_meteo.py` |
| Open-Meteo Bronze | Since 2026-10-22 each object is the response body exactly as received, with a manifest (request parameters, retrieval time, HTTP status, bytes, SHA-256, hourly coverage, response metadata). An object is current only if its SHA-256 matches the manifest and coverage is complete; a response not covering the whole year is a failure and is not stored. Objects written before that date carried an injected `_pipeline_metadata`; all 80 were replaced with `--replace-legacy`, which replaces only when the new hourly values and units are identical. The Silver transform verifies each object against its manifest SHA-256 and counts objects without one |
| Quality gate before the DW (ID 24) | Implemented 2026-10-22 in `scripts/quality_gate.py`, task `quality_gate` of `weather_bronze_to_dw_dag` (after the Silver transforms, before any DW write). **Blocking (ERROR)**, only rules this contract already states: required columns and non-null keys; timezone-aware UTC timestamps on the hour inside their partition month; unique source × location × UTC hour; humidity/cloud in [0, 100], rain, wind speed and radiation ≥ 0, wind direction in [0, 360]; complete hourly coverage for Open-Meteo, whose Bronze accepts only whole years, and for ERA5 (since 2026-10-23); for GHCNh unique station × rain day, the window (D-1 12:00 UTC, D 12:00 UTC], and 0 ≤ `rain_24h_mm` < 999. **Warnings (WARN, never blocking)**, thresholds decided 2026-10-22 by the project member holding all roles, derived from the 2016–2025 Silver data with a margin: Meteostat hourly coverage < 95% per location-year (lowest observed 99.55%); GHCNh day coverage < 90% per station-year (lowest 95.89%); any missing Open-Meteo value in the 8 standard variables (none observed; Open-Meteo feeds the labels); Meteostat temperature or humidity missing > 5% of expected hours (worst 0.45%; other Meteostat variables are reported only, since stations often omit them); temperature outside 0–45 °C (observed 3.8–42.9); pressure outside 950–1050 hPa for Open-Meteo and Meteostat (observed 963.3–1041.0) and 900–1050 hPa for ERA5 (grid-cell terrain, observed 916.89–1039.78; since 2026-10-23); any missing ERA5 value except radiation; hourly rain > 100 mm (observed 69.4); GHCNh 24-hour rain ≥ 300 mm on a day not yet reviewed (the reviewed days are the two supported extremes listed under "Unsupported extremes"). A warning asks for a human review; it does not change data. Report: `silver/quality_reports/quality_gate_<start>_<end>[_<LOCATION>]_<timestamp>.json`; warnings are also printed as `[WARN]` lines in the task log |
| Failure semantics | Since 2026-10-22 Open-Meteo ingestion exits 1 when any requested file fails (after attempting the rest; only HTTP 429/5xx and connection errors are retried), and the Open-Meteo Silver transform counts a requested but missing Bronze file as a failure unless `--allow-missing` is passed. Ctrl+C exits 130 in the ETL scripts |

## Station daily rainfall (NOAA GHCNh)

Decided 2026-10-21 (workbook ID 9; specification in [task specifications §4](task-specs-weeks-2-5.md)). This is a separate product from the hourly observations and never changes their grain.

| Item | Contract |
| --- | --- |
| Source | NOAA NCEI GHCNh by-year PSV files (successor of ISD). Bronze keeps the received bytes plus a JSON manifest (URL, retrieval time, byte size, SHA-256, row count) |
| Stations → research points | `GHCNH_RAIN_STATIONS` in `scripts/station_config.py`: 48826 Phù Liễn → HPH, 48831 Thái Nguyên → TNG, 48803 Lào Cai → LCA (representative); 48825 Hà Đông → HAN (nearest available, 11.3 km); 48823 Nam Định → NBI (substitute from the neighbouring pre-merger province, 27.3 km). PTO, TUQ, HBI have no station. This mapping is separate from the Meteostat mapping |
| Variable | `precipitation_24_hour` (mm) from FM-12/SYNOP reports at 12:00 UTC. GHCNh has no hourly precipitation for these stations |
| Grain and window | Source × station × rain day D (UTC); window `(D-1 12:00 UTC, D 12:00 UTC]` (19:00 Vietnam time) |
| Accepted values | Quality code `0` (not checked), `1` (good), or `4` (calculated); measurement code empty or trace (`T`/`2`, value kept and `is_trace` set); 0 ≤ value < 999 mm. Suspect/erroneous/removed codes, letter QC flags, accumulated/deleted/missing/estimated/incomplete measurement codes, and off-schedule reports are rejected and counted |
| Duplicates | Identical rows for the same day collapse; conflicting values are quarantined (`ghcnh/quarantine/...` in Silver) |
| Unsupported extremes | A 24-hour total ≥ 300 mm is quarantined unless a sub-daily accumulation (3–21 h) reported inside the same window reaches ≥ 10% of it. Added 2026-10-21 after Nam Định 2023-10-13 reported 700.0 mm (quality code "good") with no sub-daily rain and 0.1 mm on both neighbouring days. Supported events such as 338 mm (Hà Đông 2016-05-25) and 491.3 mm (Thái Nguyên 2025-10-07) are kept. The comparison is rounded to the 0.1 mm data resolution (2026-10-22), because in binary floats 10% of 338.0 is 33.800000000000004. Quarantined rows keep `quarantine_reason` |
| Missing data | A day without a usable report has no row: missing, never 0 mm |
| DW | `fact_station_daily_rain` and `vw_rain_station_vs_model` (`sql/03_station_daily_rain.sql`); the view compares with Open-Meteo summed over the same UTC window and requires 24 model hours |
| Use | Station-versus-model comparison in BI and reports. Not used for the hourly RainNext24h label |

## Province boundaries (GeoJSON)

Implemented 2026-10-22 (workbook ID 4; decision and source review in [task specifications §7](task-specs-weeks-2-5.md#7-id-4--real-boundary-geojson-n2-week-1-backlog)). Boundaries describe provinces; they never change the research points, which stay coordinates.

| Item | Contract |
| --- | --- |
| Basis | Pre-merger provinces (63, before 2025-07-01), decided 2026-10-21 |
| Source | geoBoundaries gbOpen VNM ADM1, release commit `9469f09592ced973a3448cf66b6100b741b64c0d`; boundary year 2008; source "geoBoundaries, Wikipedia". License: public domain per geoBoundaries metadata, ODbL per HDX; attribute geoBoundaries and Wikimedia Commons |
| Bronze | The received GeoJSON unchanged (64 units, CRS84) plus a JSON manifest: URLs, commit, source metadata, license note, CRS, feature count, bytes, SHA-256, retrieval time. A rerun with identical bytes does not rewrite it; different bytes are refused without `--overwrite` |
| Silver | `northern_provinces_pre2025_v1.geojson`: the 25 northern provinces of the Red River Delta (Decision 368/QĐ-TTg) and Northern Midlands and Mountains (Decision 369/QĐ-TTg) regions, both of 2024-05-04. Properties `province_name` (NFC, trimmed; the source has a trailing tab in "Hà Nội"), `region`, `shape_iso`, `source_shape_id`, `location_code` and `has_observations` (true for the 8 provinces with a research point). Geometry is copied unchanged. The other 17 northern provinces are a map background without data |
| Checks (all must pass before publishing) | FeatureCollection in CRS84; 64 units with unique names, 63 provinces plus Côn Đảo; every geometry valid; all 25 northern provinces present; each research point inside exactly one province, equal to its `dim_location.province`. A changed upstream build (metadata pointing to another commit) stops the run |
| DW | No change: `dim_location.province` already holds the pre-merger names |
| Limits | Coarse geometry (mean 246 vertices per unit; the Lào Cai point is about 0.022° from the border); not an official government boundary; changes after 2008 not checked. Power BI Shape Map needs TopoJSON, which is a task 27 conversion |

## Grain, identifiers, and time

- Target Silver observations are unique by `(source_name, location_code, timestamp_utc)`. Source names map consistently to `DimSource`. Raw station data may have another grain before station selection/aggregation.
- Preserve codes `HAN`, `PTO`, `TNG`, `TUQ`, `LCA`, `HBI`, `HPH`, `NBI`. These identify research points, not current administrative provinces. Retain requested coordinates and actual station/grid coordinates for traceability.
- `timestamp_utc`: timezone-aware UTC timestamp at the defined hourly grid after agreed resampling. Assign timezone to naive source timestamps only when metadata confirms it.
- Internal processing windows are `[start, end)`. Accumulated measurements may be timestamped at interval start/end; record those semantics and distinguish processing windows from future labels.
- `timestamp_local`: derived deterministically from `timestamp_utc` using `Asia/Ho_Chi_Minh` (UTC+7). Stored in PostgreSQL as a timestamp without timezone.
- Calendar basis for `date_key`/`time_key`: confirmed as Vietnam Local Time (`Asia/Ho_Chi_Minh`, UTC+7 / `timestamp_local`). `date_key` is formatted as integer `YYYYMMDD` and `time_key` as integer `HH00` (`hour * 100`), derived deterministically from canonical `timestamp_utc`.

## Standard observation columns

The following types are proposed Silver exchange types; they do not change PostgreSQL's existing NUMERIC types.

| Column | Target Silver type | Unit / meaning |
| --- | --- | --- |
| `source_name` | non-null string | Source identity; preserve separate sources |
| `location_code` | non-null string | Stable research point |
| `station_id` | nullable string | Source station identifier (e.g. WMO station ID for Meteostat/NOAA) |
| `timestamp_utc` | non-null UTC timestamp | Verified representative timestamp |
| `timestamp_local` | derived timestamp | Vietnam local time |
| `temperature_c` | nullable float | °C |
| `humidity_percent` | nullable float | Relative humidity, % |
| `pressure_hpa` | nullable float | Atmospheric pressure in hPa |
| `pressure_type` | non-null string | Explicit pressure type (`surface` for Open-Meteo, `sea_level` for Meteostat) |
| `precipitation_mm` | nullable float | Total precipitation over one hourly interval, not a multi-hour accumulation |
| `wind_speed_ms` | nullable float | m/s; measurement height depends on the product |
| `wind_direction_deg` | nullable float | Degrees under the source's meteorological convention |
| `cloud_cover_percent` | nullable float | % |
| `solar_radiation_wm2` | nullable float | W/m²; agree radiation type and interval before mapping |
| `observation_type` | nullable string | Provenance indicator (`station_reported`: every retained value passed the Meteostat source filter; `model_reanalysis`: Open-Meteo; `unverified_source`: Meteostat payload without per-variable sources) |

Do not merge different variables such as direct-normal irradiance and shortwave radiation merely because units match. Surface and sea-level pressure are not equivalent: Open-Meteo produces surface pressure while Meteostat station archive provides sea-level pressure (`pressure_type` prevents downstream conflation). Retain extra variables in Bronze or introduce versioned schema additions when needed.

## Layers and provenance

- Target Bronze preserves original source bytes. Retrieval metadata belongs separately or in object metadata/manifests: source/product, secret-free request, retrieval time, checksum, coverage, and version. Open-Meteo, GHCNh, ERA5, and boundaries comply since 2026-10-22; Meteostat Bronze still stores the decompressed CSV without a manifest (see "Known implementation gaps" above).
- Silver retains source, units, canonical time, location/station mapping, lineage, and quality flags. Additional column names require agreement; do not imply existing code supports them.
- Gold observations preserve the multi-source grain. ML pivot/fusion datasets have their own grain, feature names, and explicit source policy. Future labels belong to ML datasets, not automatically to observation facts.
- Follow [naming conventions](NAMING_CONVENTIONS.md); update consumers together with key/schema changes.

## Missing data, duplicates, and quality

- Null differs from zero. Missing-hour records differ from records with null measurements; report both. Do not fill missing rain with zero or create No labels automatically.
- Identical duplicates may be removed under a deterministic rule. Conflicting duplicates require quarantine/errors or an agreed precedence policy; do not arbitrarily keep the first record.
- Do not floor subhourly records without variable-specific resampling rules. Do not interpolate labels; feature imputation must follow [rain label](rain-label.md).
- Validate keys, schema, UTC, duplicates, missing hours, and units before publication. Humidity/cloud percentages are in [0,100], rain/wind speed are nonnegative. Outlier and coverage limits are purpose-specific: the quality gate uses the warning thresholds in the decision table; other consumers (labels, BI) keep their own rules.
- Resolve ERA5 accumulation resets/cycles for the actual product before deriving hourly rain. Reconcile equivalent intervals, locations, and measurements; related reanalysis sources are not independent confirmation.

## Contract changes

Record version, changes, and unresolved decisions. Update affected adapters, DQ, Gold, loaders, and BI/ML consumers. Verify samples and duplicate-free reruns before claiming compliance. A draft is not approval of every proposal.
