# Power BI Dashboard v1: Build Guide

Version 0.1, 2026-10-21. Workbook ID 20 (N2). Builds the first dashboard on the OLAP views in `sql/02_olap_views.sql` (ID 19). The specification is in [task specifications §3](task-specs-weeks-2-5.md).

This guide was written on macOS, where Power BI Desktop cannot run. **No `.pbix` exists yet.** The model, DAX, and reference numbers below were derived from the live DW; none of them has been tested inside Power BI Desktop. Record the actual build and the reconciliation results in [progress](progress.md).

## 1. Prerequisites

- Windows machine with current Power BI Desktop. Its built-in PostgreSQL connector bundles the Npgsql provider, so no driver installation is needed.
- A reachable `weather_dwh` that contains the views (`vw_obs_hourly`, `vw_obs_daily`, `vw_obs_monthly`, `vw_obs_yearly`, `vw_coverage`).

Choose one way to reach the database:

| Option | Steps | Notes |
| --- | --- | --- |
| A. Rebuild on the Windows machine (recommended) | `git pull`, `docker compose up -d postgres minio minio-init`, then run the pipeline scripts as recorded in [progress](progress.md) ("Full local rebuild" and "Meteostat model-value filter") | About 15 minutes of API download and loading; the result is independent of other machines |
| B. Connect over the LAN to the machine that holds the data | Server `<host-ip>:5434`; allow inbound TCP 5434 on that host's firewall | Compose publishes 5434 on all interfaces; use only on a trusted network and do not share the password |

If the database already existed before 2026-10-21, apply the views once with:

```bash
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d weather_dwh' < sql/02_olap_views.sql
```

## 2. Connect and import

1. **Get data → PostgreSQL database.** Server `localhost:5434` (or `<host-ip>:5434`), database `weather_dwh`, data connectivity mode **Import**.
2. Credentials: **Database** tab, with the values from `.env` (`POSTGRES_USER`/`POSTGRES_PASSWORD`).
3. Select these objects:

| Object | Rows (2016–2025 data) | Role |
| --- | --- | --- |
| `public.dim_date` | 4,018 | Date dimension (mark as date table) |
| `public.dim_time` | 24 | Hour of day |
| `public.dim_location` | 8 | Research points |
| `public.dim_source` | 4 | Sources |
| `public.vw_obs_daily` | 32,886 | Main daily fact |
| `public.vw_obs_hourly` | 788,861 | Hourly fact for hour drill-down |
| `public.vw_coverage` | 99 | Data-quality page |

4. In Power Query, keep only the columns used; set `dim_date[full_date]` to type Date.

## 3. Model

- Relationships (single direction, one-to-many from dimension to fact):
  - `dim_date[date_key]` → `vw_obs_daily[date_key]` and `vw_obs_hourly[date_key]`
  - `dim_location[location_key]` → `vw_obs_daily[location_key]` and `vw_obs_hourly[location_key]`
  - `dim_source[source_key]` → `vw_obs_daily[source_key]` and `vw_obs_hourly[source_key]`
  - `dim_time[time_key]` → `vw_obs_hourly[time_key]`
  - `dim_location[location_code]` → `vw_coverage[location_code]` and `dim_source[source_name]` → `vw_coverage[source_name]`
- No relationship between `vw_obs_daily` and `vw_obs_hourly` (no fact-to-fact joins).
- **Mark as date table:** `dim_date`, using `full_date`. Sort `month_name` by `month`.
- Hierarchy on `dim_date`: `year` → `quarter` → `month_name` → `full_date`; add `dim_time[hour_24]` in hourly visuals.
- Report-level filter: `dim_date[year]` between 2016 and 2025. The DW is loaded by UTC year, so local 2016-01-01 has only 17 hours and local 2026 only 7.
- Hide the key columns and the raw `*_sum`/`*_valid_hours` columns from report view.

## 4. Measures (DAX)

Create a table `_Measures` and add the following. Each measure returns BLANK when more than one source is in the filter context, because sources must never be added together.

```dax
Selected Source OK = HASONEVALUE ( dim_source[source_name] )

Rain per Location (mm) =
IF (
    [Selected Source OK],
    AVERAGEX (
        VALUES ( dim_location[location_key] ),
        CALCULATE ( SUM ( vw_obs_daily[rain_mm] ) )
    )
)
-- One location: its total. Several locations: mean of the location totals, never their sum.

Temperature Mean (°C) =
IF (
    [Selected Source OK],
    DIVIDE ( SUM ( vw_obs_daily[temp_sum] ), SUM ( vw_obs_daily[temp_valid_hours] ) )
)

Temperature Max (°C) = IF ( [Selected Source OK], MAX ( vw_obs_daily[temp_max_c] ) )
Temperature Min (°C) = IF ( [Selected Source OK], MIN ( vw_obs_daily[temp_min_c] ) )

Complete Rain Days =
CALCULATE ( COUNTROWS ( vw_obs_daily ), vw_obs_daily[is_complete_rain_day] = TRUE () )

Rainy Days (location-days) =
IF (
    [Selected Source OK] && [Complete Rain Days] > 0,
    CALCULATE ( COUNTROWS ( vw_obs_daily ), vw_obs_daily[is_rainy_day] = TRUE () )
)

Rainy Days per Location =
IF (
    [Selected Source OK],
    AVERAGEX ( VALUES ( dim_location[location_key] ), [Rainy Days (location-days)] )
)

Complete Temp Days =
CALCULATE ( COUNTROWS ( vw_obs_daily ), vw_obs_daily[is_complete_temp_day] = TRUE () )

Hot Days (location-days) =
IF (
    [Selected Source OK] && [Complete Temp Days] > 0,
    CALCULATE ( COUNTROWS ( vw_obs_daily ), vw_obs_daily[is_hot_day] = TRUE () )
)

Hot Days per Location =
IF (
    [Selected Source OK],
    AVERAGEX ( VALUES ( dim_location[location_key] ), [Hot Days (location-days)] )
)

Rain Hourly (mm) =
IF ( [Selected Source OK], SUM ( vw_obs_hourly[precipitation_mm] ) )

Temperature Hourly Mean (°C) =
IF ( [Selected Source OK], AVERAGE ( vw_obs_hourly[temperature_c] ) )

Rain Coverage (%) =
DIVIDE ( SUM ( vw_coverage[rain_valid_hours] ), SUM ( vw_coverage[expected_hours] ) )
```

Format rain with 1 decimal place and "mm", temperatures with 2 decimal places and "°C", and coverage as a percentage. Do not replace BLANK with 0: Meteostat has no rain, and that must show as empty, not as "0 mm".

## 5. Pages

| Page | Visuals | OLAP operation to demonstrate |
| --- | --- | --- |
| 1. Overview | Cards: Rain per Location, Temperature Mean, Rainy Days per Location, Hot Days per Location; slicers: source (single select), location, year | **Slice** (change source or year) |
| 2. Trends | Line chart: Rain per Location and Temperature Mean on the date hierarchy; separate hourly line chart using `dim_time[hour_24]` | **Roll-up / Drill-down** (year → month → day; day → hour on the hourly chart) |
| 3. Seasons and locations | Matrix: rows `dim_location[location_name]`, columns `dim_date[month_name]`, values Rain per Location and Rainy Days; slicer `is_rainy_season` | **Dice** (location × month × season) |
| 4. Source comparison | Line chart with `dim_source[source_name]` in the legend for one location; temperature measures only (Meteostat has no rain) | Slice + dice |
| 5. Data quality | Table from `vw_coverage`: source, location, year, record/rain/temperature coverage % | Shows why Meteostat rain is blank |

Every page subtitle should state: "Local calendar (UTC+7); rainy day ≥ 1 mm and hot day ≥ 35 °C on complete days; sources are never added together."

## 6. Reconciliation (required before calling v1 done)

Set the filters in Power BI and compare with these values, which were computed from the views on 2026-10-21. Record each comparison in `docs/progress.md`.

| Filters | Measure | Expected |
| --- | --- | --- |
| Open-Meteo, HAN, 2024 | Rain per Location | 2,660.5 mm |
| Open-Meteo, HAN, 2024 | Rainy Days (location-days) | 175 |
| Open-Meteo, HAN, 2024 | Hot Days | 23 |
| Open-Meteo, HAN, 2024 | Temperature Mean / Min / Max | 24.62 / 7.20 / 40.20 °C |
| Meteostat, HAN, 2024 | Temperature Mean / Hot Days | 25.16 °C / 57 |
| Meteostat, HAN, 2024 | Rain per Location / Rainy Days | BLANK / BLANK |
| Open-Meteo, HAN, July 2024 | Rain per Location / Rainy Days / Hot Days | 541.3 mm / 27 / 4 |
| Open-Meteo, HAN, 2024-07-15 | Rain / Temp Mean / Max | 28.9 mm / 27.56 / 31.60 °C |
| Open-Meteo, all 8 locations, 2024 | Rain per Location / Rainy Days (location-days) | 2,330.3 mm / 1,466 |
| Open-Meteo, HAN, 2024-07-15 14:00 (hourly page) | Rain Hourly / Temperature | 1.0 mm / 28.0 °C |
| Two sources selected | Any rain or temperature measure | BLANK |

Ten-year Open-Meteo reference (2016–2025, mean per year): HAN 2,202.4 mm and 179.2 rainy days, LCA 2,949.4 mm and 247.3 rainy days (wettest), NBI 2,012.4 mm and 170.6 rainy days.

## 7. Delivery

- Save as `weather-dashboard-v1.pbix` in the agreed shared location. Whether `.pbix` files go into Git is still open.
- Record in `docs/progress.md`: Power BI Desktop version, data option used (A or B), refresh time, the reconciliation table with actual values, and screenshots of each OLAP operation.
- Update workbook ID 20 only after the reconciliation passes.
