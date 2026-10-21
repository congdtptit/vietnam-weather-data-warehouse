# Weather Data Contract

Document version: v0.2, 2026-10-21. v0.2 confirms the Silver columns, units, UTC, pressure types, and duplicate handling below (confirmed by the project member working on N1/N2 on 2026-10-21; that member accepted their confirmation as team approval). Station selection is recorded below; the minimum coverage threshold remains open. This is the target implementation contract; the Silver stage has been executed on real data per `docs/progress.md`, but full-task acceptance is separate.

## Decision status

| Item | Status and basis |
| --- | --- |
| Canonical UTC, standard units, and Parquet | Existing naming and task-allocation conventions |
| Observations: source × location × hour | Proposal requirement |
| Main variable names below | Existing conventions used by the DDL |
| Conflicting duplicates: identical rows collapse deterministically; conflicting rows are excluded from clean output and saved separately | Confirmed in v0.2 (matches implemented behavior) |
| `pressure_type` (`surface` for Open-Meteo, `sea_level` for Meteostat) and `source_name`/`station_id` provenance retained; the two pressures are never merged | Confirmed in v0.2 |
| Station selection | Decided 2026-10-21 (implemented in `scripts/station_config.py`): at most one verified WMO station per canonical location. Verified mappings: HAN `48820`, HPH `48826`, TNG `48831`, LCA `48803`; PTO, TUQ, HBI, NBI have no verified station and use gridded sources only. Stations without a confirmed mapping are rejected at publication, including the misassigned Table 6 ids (`48825`, `48830`, `48838`, `48840`), the unverified `48816`, and regional `48806`/`48808`. No multi-station aggregation. Open-Meteo and Meteostat stay separate sources and are never summed. Only `48820` and `48826` are currently served by Meteostat |
| Meteostat model-filled values | Decided 2026-10-21 (implemented in `scripts/transform_meteostat_to_silver.py`): a Meteostat value is kept only when its `<variable>_source` is a station observation source (`isd_lite`, `metar`, `synop`, `national`). Model sources such as `dwd_mosmix` and `metno_forecast`, and unknown names, are nulled. Rows left without any measurement are dropped, and the counts are reported in the DQ report (`source_filter`). With `--overwrite`, Silver months that no longer have observations are removed. Payloads without any `<variable>_source` columns are kept as `unverified_source`. Effect on 2016–2025: Meteostat keeps HAN/48820 only and contains no precipitation, because all Meteostat rain for both stations was model-filled |
| Coverage | Decided 2026-10-21: report record coverage per source × location × window (`scripts/quality_checks.py`); do not impute missing hours. A minimum coverage threshold is **not** set and remains open |
| Product-specific interval semantics and pressure/radiation mapping | Verify source metadata/documentation during implementation |
| Additional lineage/DQ column names and types | Contract proposals requiring producer/consumer agreement |

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

- Target Bronze preserves original source bytes. Retrieval metadata belongs separately or in object metadata/manifests: source/product, secret-free request, retrieval time, checksum, coverage, and version. Current ingestion adds `_pipeline_metadata` to JSON and therefore does not preserve the original payload.
- Silver retains source, units, canonical time, location/station mapping, lineage, and quality flags. Additional column names require agreement; do not imply existing code supports them.
- Gold observations preserve the multi-source grain. ML pivot/fusion datasets have their own grain, feature names, and explicit source policy. Future labels belong to ML datasets, not automatically to observation facts.
- Follow [naming conventions](NAMING_CONVENTIONS.md); update consumers together with key/schema changes.

## Missing data, duplicates, and quality

- Null differs from zero. Missing-hour records differ from records with null measurements; report both. Do not fill missing rain with zero or create No labels automatically.
- Identical duplicates may be removed under a deterministic rule. Conflicting duplicates require quarantine/errors or an agreed precedence policy; do not arbitrarily keep the first record.
- Do not floor subhourly records without variable-specific resampling rules. Do not interpolate labels; feature imputation must follow [rain label](rain-label.md).
- Validate keys, schema, UTC, duplicates, missing hours, and units before publication. Humidity/cloud percentages are in [0,100], rain/wind speed are nonnegative; outlier/coverage thresholds are purpose-specific and not yet agreed project-wide.
- Resolve ERA5 accumulation resets/cycles for the actual product before deriving hourly rain. Reconcile equivalent intervals, locations, and measurements; related reanalysis sources are not independent confirmation.

## Contract changes

Record version, changes, and unresolved decisions. Update affected adapters, DQ, Gold, loaders, and BI/ML consumers. Verify samples and duplicate-free reruns before claiming compliance. A draft is not approval of every proposal.
