# Business Specifications: Next N1/N2 Tasks (Weeks 2–5)

Version 0.1, 2026-10-21. Proposed specifications for workbook IDs 19, 20, 9, 17, 8, 4, and 5, written after the full local rebuild recorded in [progress](progress.md). Each section states the business purpose, scope, rules, acceptance criteria, and open decisions. Items marked **Proposed** are not approved requirements until the team confirms them in the owning document ([data contract](data-contract.md), [warehouse schema](warehouse-schema.md), or [rain label](rain-label.md)). Task ownership and status stay in the allocation workbook.

## 0. Starting point (verified 2026-10-21)

| Item | State |
| --- | --- |
| DW `fact_weather_observation` | 788,861 rows, 0 duplicate keys: Open-Meteo 8 locations × 87,672 hours (2016–2025); Meteostat HAN only (87,485 rows, temperature/humidity/wind, no precipitation) |
| Dimensions | `dim_date` 4,018 days (2016–2026, Vietnam local calendar), `dim_time` 24, `dim_location` 8, `dim_source` 4, `dim_station` 11 verified stations (state on 2026-10-21 before ID 9; now `dim_source` 5 with GHCNh and `dim_station` 12 with 48823; 48816 is unverified with NULL coordinates) |
| Calendar basis | `date_key`/`time_key` in Vietnam local time (UTC+7); `timestamp_utc` canonical |
| Labels | `scripts/rain_labels.py`: 1 mm over `(t, t+24h]`, Open-Meteo |
| Orchestration | Scripts run manually; only `open_meteo_ingest_to_bronze_dag` (manual) and the health-check DAG exist |
| Known gaps | Bronze Open-Meteo rewrites the payload; no station precipitation; no OLAP views, BI artifacts, boundaries, or ERA5 |

## 1. Recommended order and dependencies

| Order | ID | Owner | Depends on | Unblocks |
| --- | --- | --- | --- | --- |
| 1 | 19 OLAP views | N2 | Clean DW (done) | 20, 27, 35 |
| 2 | 20 Power BI v1 | N2 | 19; a Windows machine | 27, 33, 35 |
| 3 | 9 Station daily rain (GHCNh) | N1 | Contract decision in §4.3 | 20/27 comparison pages, 37 |
| 4 | 17 Bronze → DW DAG | N1 | Existing scripts | 24, 25, 32, 34 |
| 5 | 8 Bronze raw payload | N1 | — | Closes 2 and 8 |
| — | 4 Boundary GeoJSON | N2 | Source choice in §4.6 | 11, 27 maps |
| — | 5 ERA5 CDS request | N3 | CDS account | 14, 15 |

IDs 19 and 9 can run in parallel. ID 5 should be submitted immediately because CDS queues can take hours to days.

## 2. ID 19 — OLAP views (N2, weeks 4–5)

**Business purpose.** Provide one trusted SQL layer that Power BI and reports read, so that every total, average, and rainy-day count is computed once with the project aggregation rules instead of being re-derived in each dashboard.

**Business questions served.**

1. How do temperature and rainfall change by hour, day, month, season, and year at each location?
2. How many rainy days and hot days does each location have per month and year?
3. How do sources differ at the same location and period (Open-Meteo vs Meteostat now; vs station daily rain after ID 9)?
4. How complete is each series (coverage) for the period shown?

**Scope.** SQL views in a new `sql/` migration file; no change to existing tables.

| View (proposed name) | Grain | Content |
| --- | --- | --- |
| `vw_obs_hourly` | source × location × UTC hour | Fact joined to dimension attributes (names, local date parts, season flag), without aggregation |
| `vw_obs_daily` | source × location × local date | Rain sum, temperature mean/min/max, humidity mean, wind speed mean, vector-mean wind direction, `valid_hours` per variable, `expected_hours` (24), `is_complete_day` |
| `vw_obs_monthly` | source × location × local month | Sums/means rolled up from hourly sums and counts (not averages of daily averages), valid/expected hours, complete-day count, rainy-day and hot-day counts |
| `vw_obs_yearly` | source × location × local year | Same rules as monthly |
| `vw_coverage` | source × location × year | Expected vs present hours per variable, to display data completeness |

**Business rules.**

- Rain totals are sums over hours of the **same source and location**. Never add sources together, and never add locations and call it regional rainfall.
- A day is **complete** for a variable only with 24 valid hours. Totals from incomplete days are labelled partial, not hidden and not treated as zero.
- **Rainy day (decided 2026-10-21):** complete day with daily rain ≥ 1 mm, matching the label threshold.
- **Hot day (decided 2026-10-21):** complete day with daily maximum temperature ≥ 35 °C.
- **Rainy season:** use `dim_date.is_rainy_season` (May–October) as seeded; document it as the project definition.
- Means roll up from sums and counts; wind direction uses vector mean of (sin, cos); calm hours without direction are excluded from the direction mean.
- Days follow Vietnam local time (current calendar basis). State this on every view and dashboard page.

**Acceptance criteria.**

- One hour → day → month → year chain reconciles exactly: for a chosen location/source/month, the monthly rain equals the sum of the daily rain, which equals the sum of the hourly rain from the fact table.
- Row counts per view match the expected grain (for example 8 × 3,653 Open-Meteo location-days, give or take the first/last local-day boundary), with no join fan-out.
- `vw_coverage` shows 100% for Open-Meteo and the real gaps for Meteostat.
- Query time for each view on the full 10-year data is recorded; materialized views or indexes are added only if a measured query is too slow (ID 26 owns tuning).

**Status (2026-10-21).** Implemented in `sql/02_olap_views.sql`; see [progress](progress.md). **Open decision:** whether views also expose a UTC-day variant for ML reconciliation.

## 3. ID 20 — Power BI dashboard v1 (N2, weeks 4–5)

**Business purpose.** Show that the DW answers the historical-analysis questions and demonstrate the four OLAP operations required by the proposal (roll-up, drill-down, slice, dice).

**Users.** The team, the supervisor at reviews, and the final demo audience.

**Data connection.** Power BI Desktop's built-in PostgreSQL connector (bundled Npgsql since December 2019) in **Import** mode against the ID 19 views. Connect to `localhost:5434`, database `weather_dwh`. Import keeps the demo independent of the Docker stack once refreshed. Power BI Desktop runs on Windows only.

**Model (Proposed).** A star model on the views: `dim_date` marked as the date table, `dim_location`, `dim_source`, single-direction 1:* relationships. No direct fact-to-fact joins.

**Pages for v1.**

| Page | Content | OLAP operation shown |
| --- | --- | --- |
| Overview | KPI cards (period total rain, mean temperature, rainy days, coverage) with source and location slicers | Slice |
| Trends | Monthly rain and temperature lines with a year → month → day → hour hierarchy | Roll-up / drill-down |
| Seasons and locations | Matrix of location × month (rain, rainy days) with rainy-season filter | Dice |
| Source comparison | Same location and period, sources as separate series, with coverage tooltip | Slice + dice |
| Data quality | Coverage by source × location × year from `vw_coverage` | — |

**Business rules.** Explicit DAX measures with units (mm, °C); no default SUM on temperature; BLANK for missing data, never zero; totals across sources disabled or clearly separated; tooltips show source, coverage, and calendar basis.

**Acceptance criteria.**

- A real `.pbix` file saved in the agreed shared location (not in Git unless the team decides otherwise).
- For one day, one month, and one year, three numbers per page (rain total, mean temperature, rainy days) match the SQL views under identical filters; the check is recorded.
- Each OLAP operation is demonstrable with filters visibly changing the data.

**Build guide.** [Power BI dashboard v1 build guide](powerbi-dashboard-v1.md) (model, DAX, pages, reference values).

**Open decisions.** Where the `.pbix` is stored; which machine refreshes it; whether a map is included in v1 (it needs ID 4 boundaries; points alone are possible earlier).

## 4. ID 9 — Station daily rainfall from NOAA GHCNh (N1, weeks 2–3, follow-up)

**Business purpose.** Add an independent gauge-based rainfall source so that the project is genuinely multi-source and the dashboards can compare model rainfall (Open-Meteo) with measured rainfall. This does **not** change the hourly label or the hourly observation fact.

### 4.1 Source choice

NOAA superseded the Integrated Surface Database (ISD) with **GHCNh** (Global Historical Climatology Network – hourly); ISD has no updates beyond 2025-08-24, and NCEI ended its own ISD download service (ISD remains on the NOAA Open Data portal). Checks on 2026-10-21:

| Check | ISD (full format) | GHCNh |
| --- | --- | --- |
| 2025 coverage | Ends 2025-08-24 (44–61% of days) | Through 2025-12-31 (358–360 days per station) |
| Lào Cai / Hà Đông 24 h rain days | 44–79% per year | ~98% in 2025 |
| Duplicates | 2,250 station-days with two encodings of the same report | One `precipitation_24_hour` value per day |
| Quality flags | Per group | Per variable (`_Quality_Code`, `_Measurement_Code`, `_Source_Code`) |
| Phù Liễn 2024 total | 2,112 mm | 2,112 mm (identical) |

**Proposed, then decided 2026-10-21 (see the [data contract](data-contract.md#station-daily-rainfall-noaa-ghcnh)):** use GHCNh, files `https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year/<YYYY>/psv/GHCNh_VMM000<WMO>_<YYYY>.psv`. GHCNh has no hourly precipitation for these stations; the usable field is the 24-hour total reported at 12 UTC (19:00 Vietnam time). Only 2024 and 2025 of GHCNh were sampled; the 10-year coverage must be measured during implementation.

### 4.2 Stations

| Research point | Station (WMO) | Distance | Role |
| --- | --- | --- | --- |
| HPH | Phù Liễn (48826) | 7.6 km | Representative |
| TNG | Thái Nguyên (48831) | 1.7 km | Representative |
| LCA | Lào Cai (48803) | 1.6 km | Representative |
| HAN | Hà Đông (48825) | 11.3 km | Nearest available; Nội Bài/Láng have no rain in NOAA |
| NBI | Nam Định (48823) | 27.3 km | Substitute from the neighbouring (pre-merger) Nam Định province; label as such |
| PTO, TUQ, HBI | — | — | No station within a reasonable distance |

Mapping 48825 and 48823 to research points is a station-selection change to `scripts/station_config.py` and the data contract and needs team approval. Hà Đông is currently `UNMAPPED_MISASSIGNED` because it was wrongly labelled Việt Trì, not because it is unsuitable for HAN.

### 4.3 Grain and storage (proposed here; decided 2026-10-21 in the data contract and implemented in `sql/03_station_daily_rain.sql`)

- New table `fact_station_daily_rain`: one source × one station × one **rain day ending 12:00 UTC**, with `rain_24h_mm`, quality code, measurement code, report type, and the window `(D-1 12:00 UTC, D 12:00 UTC]`.
- Kept separate from `fact_weather_observation`; the hourly grain is unchanged. Add `NOAA` (already seeded in `dim_source`) or a `GHCNh` source row.
- Bronze keeps each `.psv` file unchanged; Silver keeps the 24-hour series with quality flags; failed-QC values are excluded and counted.
- A day without a report is **missing**, not 0 mm.
- For comparison, Open-Meteo is aggregated over the same 12 UTC–12 UTC window (a view), not over the local calendar day.

**Status (2026-10-21).** Implemented: `scripts/ingest_ghcnh.py`, `scripts/transform_ghcnh_to_silver.py`, `scripts/load_station_daily_rain.py`, `sql/03_station_daily_rain.sql`; results in [progress](progress.md).

**Acceptance criteria.** Coverage report per station × year; first run and rerun produce identical row counts; for one station-year the yearly total matches the raw file; a comparison view returns daily station vs Open-Meteo rain for the five points.

## 5. ID 17 — Airflow DAG Bronze → DW (N1, weeks 4–5)

**Business purpose.** Replace the manual sequence used on 2026-10-21 with a scheduled, observable, rerunnable pipeline so that the DW can be rebuilt or extended by anyone on the team without reading logs by hand.

**Scope (Proposed).** One DAG with explicit year (or year-month) parameters running the existing scripts in order:

```text
ingest_open_meteo ─┐
ingest_meteostat  ─┼─> transform_open_meteo_to_silver ─┐
                   └─> transform_meteostat_to_silver ──┼─> seed_dimensions ─> load_gold_observations ─> reconcile
```

**Business rules.**

- Window parameters, not `now()`; manual trigger with `start_year`/`end_year`; catchup off. Incremental scheduling is ID 25.
- Each task fails visibly when its script exits non-zero (the scripts already return non-zero on recorded failures).
- Bounded retries for transient network errors only; no retries for configuration errors.
- A final `reconcile` task checks DW row counts per source × location × year against Silver and fails on a mismatch.
- Large data never passes through XCom; tasks exchange only window parameters and report keys.

**Acceptance criteria.** A one-month run, a rerun without new rows, and a forced failure followed by a successful resume are demonstrated and logged; DAG import has no network calls; parsing alone is not accepted as evidence.

**Decision (2026-10-22, owner of ID 17):** the smallest test window is **one UTC year × one location**, not one month, because Bronze objects, transforms, and the Gold loader work per year; a one-year, one-location run takes about one minute. The GHCNh daily-rain chain (`ingest_ghcnh` → `transform_ghcnh_to_silver` → `load_station_daily_rain`) is added to the DAG so that it rebuilds every DW fact it can; boundaries stay a one-off script (a static file) and ERA5 joins after its Silver step (ID 15) and a decision on whether ERA5 is loaded into the DW.

**Implemented 2026-10-22.** `dags/dag_weather_pipeline.py` (`weather_bronze_to_dw_dag`) and `scripts/reconcile_dw.py`. Parameters `start_year`, `end_year` (2016–2025, checked by a first `validate_window` task) and optional `location`; `schedule=None`, `catchup=False`, `max_active_runs=1`; one retry after 2 minutes for the two ingestion tasks only; transforms run with `--overwrite`. The GHCNh daily-rain chain was added the same day (`ingest_ghcnh` → `transform_ghcnh_to_silver` → `load_station_daily_rain`, with a `--location` option on the three scripts), and `reconcile` also compares daily rain per station × year. Deviations from this section: the smallest window is one year (optionally one location), not one month (decision below); boundaries and ERA5 are not part of this DAG. Acceptance run on 2026-10-22: 2016–2025, all locations, no download, 3 min 43 s, reconciliation 788,861 = 788,861 hourly rows and 17,866 = 17,866 station-days. Evidence (runs, rerun, failure and resume): [progress](progress.md).

**Added 2026-10-22 (ID 24).** A `quality_gate` task now runs after the three Silver transforms and before `seed_dimensions`, so no DW table is written when Silver breaks a contract rule. Blocking checks and the warning thresholds (decided 2026-10-22) are listed in the [data contract](data-contract.md#decision-status). Runs: 2016–2025 standalone (712 checks, 0 failed, 0 warnings) and DAG runs 2024 × HAN (12/12 tasks); an integration test runs the DAG's own `quality_gate` command in a `BashOperator` against a disposable MinIO and sees it fail on bad Silver. Evidence: [progress](progress.md).

**Known constraint.** The last full Gold load took 292 s (DQ report `dq_report_gold_load_20261021_020704.json`; an earlier version of this line said 3,493 s, which was not supported by any run artifact; corrected 2026-10-22). The DAG should run per year so that one slow or failed year can be retried alone.

## 6. ID 8 — Bronze keeps the original payload (N1)

**Business purpose.** Bronze must be the evidence of what the source returned. Until 2026-10-22 the Open-Meteo ingester added `_pipeline_metadata` and re-serialized the JSON, so the stored file was not the original response, and size-based skipping could not detect a wrong or truncated file.

**Required behaviour.**

- Store the response bytes exactly as received (current key layout unchanged).
- Write a separate manifest per object: source, request parameters without secrets, retrieval time (UTC), HTTP status, byte size, SHA-256, and coverage (first/last timestamp, hour count).
- Skip decisions use the manifest (checksum and coverage), not file size.
- The Silver transform reads the original bytes and the manifest; its outputs must not change.

**Acceptance criteria.** For a sample year the SHA-256 of the stored object equals the SHA-256 of a fresh download; the Silver output for that year is byte-for-byte or row-for-row identical before and after the change; existing tests and a rerun pass.

**Implemented 2026-10-22 (Open-Meteo).** The first criterion cannot hold literally: every Open-Meteo response contains `generationtime_ms`, which changes per request, so two downloads never have the same bytes. Implemented instead: the stored bytes match the SHA-256 recorded at download, and a replacement is accepted only if its hourly values and units equal the previous object. All 80 objects (8 points × 2016–2025) were replaced on that basis, and Silver 2024 was row-for-row identical (70,272 rows) before and after. See [progress](progress.md).

## 7. ID 4 — Real boundary GeoJSON (N2, week 1 backlog)

**Business purpose.** Province maps in Power BI (ID 27) and a correct province name for each research point (ID 11).

**Decision (2026-10-21, N1/N2 member): use pre-merger boundaries (63 provinces).** Almost the whole study period (2016-01-01 to 2025-06-30) falls before the 2025 reorganization, so reports and maps use the provinces that existed during that period. Weather data is unaffected by this choice: research points are coordinates, not provinces. **Discrepancy:** the workbook asks for post-merger boundaries (ID 4: "ranh giới sau sáp nhập 1/7/2025"; risk sheet: "GeoJSON dùng ranh giới mới") and old + new names (ID 11). On 2026-10-22 the workbook ID 4 and ID 11 descriptions and the risk row 9 handling were rewritten to this decision, each quoting the original wording; the ID 4/11 notes record it too.

**Administrative context (reference only).** National Assembly Resolution 202/2025/QH15 (12 June 2025) reduced 63 provincial units to 34; the new provincial governments operate from 1 July 2025. The mapping below is kept for the report's background section and is not loaded into the DW under this decision:

| Point | Province before 2025-07-01 | Province from 2025-07-01 (merged from) |
| --- | --- | --- |
| HAN | Hà Nội | Hà Nội (unchanged) |
| PTO | Phú Thọ | Phú Thọ (Phú Thọ + Vĩnh Phúc + Hòa Bình) |
| HBI | Hòa Bình | Phú Thọ (same merged province as PTO) |
| TNG | Thái Nguyên | Thái Nguyên (Thái Nguyên + Bắc Kạn) |
| TUQ | Tuyên Quang | Tuyên Quang (Tuyên Quang + Hà Giang) |
| LCA | Lào Cai | Lào Cai (Lào Cai + Yên Bái) |
| HPH | Hải Phòng | Hải Phòng (Hải Phòng + Hải Dương) |
| NBI | Ninh Bình | Ninh Bình (Ninh Bình + Hà Nam + Nam Định; not confirmed by a source in this review) |

PTO and HBI fall in the same new province; they remain separate research points because points are coordinates, not provinces.

**Candidate sources for 63-province (pre-merger) boundaries.**

| Source | Notes |
| --- | --- |
| OCHA HDX COD-AB Viet Nam, earlier version (files dated 2020/2022) | Humanitarian common operational dataset; listed license Creative Commons Attribution for Intergovernmental Organisations. The current HDX version (September 2025) has 34 provinces, so the earlier version must be selected explicitly |
| geoBoundaries VNM ADM1 | Open-licensed, standardized; temporal coverage 2008–2020 |
| GADM | Widely used, but its terms restrict redistribution; avoid unless the license is acceptable |

None of these licenses, versions, or geometries were verified in this review.

**Implemented 2026-10-22 (`scripts/ingest_boundaries.py`).** Checked sources: the HDX `cod-ab-vnm` dataset now serves only the 34-province version (the earlier 63-province files are no longer listed), so it could not be used. geoBoundaries gbOpen VNM ADM1 was chosen, pinned to release commit `9469f09`: 64 units (63 provinces, with Côn Đảo split from Bà Rịa–Vũng Tàu), boundary year 2008, CRS84, all 64 geometries valid, and all 8 research points inside their expected province. Its metadata gives the license as public domain (from Wikimedia Commons), while HDX lists the same data under ODbL; the manifest records both. The geometry is coarse (mean 246 vertices per unit) and comes from Wikipedia/Wikimedia, not from the government: suitable for maps, not for precise spatial analysis. Boundary changes after 2008 were not checked. Details: [data contract](data-contract.md#province-boundaries-geojson).

**Required output.** Bronze GeoJSON (63 provinces) with a manifest (source URL, license, version or date, retrieval date, CRS, feature count); a geometry-validity check; a point-in-polygon check that each research point falls inside its expected pre-merger province (HAN Hà Nội, PTO Phú Thọ, TNG Thái Nguyên, TUQ Tuyên Quang, LCA Lào Cai, HBI Hòa Bình, HPH Hải Phòng, NBI Ninh Bình). The existing `dim_location.province` values already hold the pre-merger names, so no DW migration is needed under this decision.

## 8. ID 5 — ERA5 request on Copernicus CDS (N3, week 1 backlog)

**Business purpose.** Satisfy the proposal's NetCDF source and provide a second reanalysis series. Open-Meteo is largely derived from ERA5, so ERA5 is a format and cross-check source, not independent ground truth.

**Required steps.**

1. Create a CDS account and accept the dataset licence and the general Copernicus licence; unaccepted licences cause HTTP 403.
2. Store the API key in `~/.cdsapirc` or an environment variable outside Git.
3. Choose the product: `reanalysis-era5-single-levels` (hourly) or ERA5-Land (hourly, finer grid). The choice affects accumulation handling in ID 15.
4. Request northern Vietnam only (implemented area north 23.5, west 102, south 19.5, east 107.5; total cloud cover was added to the variables below on 2026-10-22), only the variables needed (2 m temperature, 2 m dewpoint, total precipitation, surface pressure, 10 m u/v wind), 2016–2025, **one month per request**. ECMWF reports per-request item limits (60,000 items for hourly ERA5 and 6,000 for ERA5-Land in 2024; limits change), and NetCDF output allows less data than GRIB.

**Acceptance criteria.** Evidence that requests were submitted (request IDs, area, variables, product, window); credentials absent from the repository; no claim of a completed download without files in Bronze.

**Implemented 2026-10-22 (`scripts/ingest_era5.py`).** Product `reanalysis-era5-single-levels`, one request per month, NetCDF, credentials from `.env` (ignored by Git). The first real request (job `70c1b649-2d18-445e-b460-f3c9007f772c`) ran 6.5 minutes; the CDS returned a zip of two NetCDF files (instantaneous and accumulated variables), which is stored unchanged with a manifest listing both. 2024-01 is in Bronze; the 2016–2025 download was started the same day and resumes by skipping stored months. Measured on 2026-10-22: one month waited 1 h 41 min in the CDS queue and then processed in 4 minutes, so the remaining months take days, not hours; concurrent requests were rejected by the CDS, so requests stay sequential. The fallback scope recorded in [business requirements](business-requirements.md) §5a (2024–2025) is therefore downloaded first, then 2016–2023. Details and counts: [progress](progress.md).

**Decided and implemented 2026-10-23 (point time series).** With the defence on 2026-10-29, the user approved using the CDS product `reanalysis-era5-single-levels-timeseries` for the Silver step: the same ERA5 hourly data at the nearest grid cell of each research point, which is what ID 15 asks for ("lấy ô lưới gần nhất"). All 8 locations × 2016–2025 were stored in 320 s (`scripts/ingest_era5_timeseries.py`), checked equal to the gridded files on 2024-01/02, and transformed to Silver (`scripts/transform_era5_to_silver.py`, 701,376 rows). Deviation from ID 5's wording ("vùng miền Bắc"): the series covers grid cells at the 8 points, not the whole region; the gridded regional download keeps running for 2024–2025 as evidence of the regional request and then stops. Whether ERA5 is loaded into the DW stays open; it is kept out so the DW and the Power BI handoff do not change.

## 9. Sources consulted

- [Power Query PostgreSQL connector (Microsoft Learn)](https://learn.microsoft.com/power-query/connectors/postgresql)
- [NCEI GHCNh product page](https://ncei.noaa.gov/products/global-historical-climatology-network-hourly) and [NESDIS notice on ISD/GHCNh](https://www.nesdis.noaa.gov/news/global-historical-climate-network-hourly-integrated-surface-data-global-hourly)
- [ECMWF forum: ERA5 request limits](https://forum.ecmwf.int/t/limitation-change-on-netcdf-era5-requests/12477) and [licence errors](https://forum.ecmwf.int/t/required-licences-not-accepted/6014)
- [Resolution 202/2025/QH15 (LuatVietnam)](https://luatvietnam.vn/hanh-chinh/nghi-quyet-202-2025-qh15-cua-quoc-hoi-ve-viec-sap-xep-don-vi-hanh-chinh-cap-tinh-402728-d1.html) and [2025 Vietnamese administrative reform (Wikipedia)](https://en.wikipedia.org/wiki/2025_Vietnamese_administrative_reform)
- [OCHA HDX: Viet Nam subnational administrative boundaries](https://data.humdata.org/m/dataset/cod-ab-vnm) and [geoBoundaries Viet Nam record (BTAA)](https://geo.btaa.org/catalog/3cb544a9-9d04-4f54-94e2-93230efd8ceb)
- [thanglequoc/vietnamese-provinces-database](https://github.com/thanglequoc/vietnamese-provinces-database), [nguyenduy1133/Free-GIS-Data](https://github.com/nguyenduy1133/Free-GIS-Data)
