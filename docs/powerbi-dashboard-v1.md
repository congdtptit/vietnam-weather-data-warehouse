# Power BI Dashboard: Build Guide

Version 0.3, 2026-10-23 (eight-page specification, new measures and calculated columns; 0.2: 2026-10-22, day-count DAX fix; 0.1: 2026-10-21). Workbook IDs 20, 27, 33, 35 (N2). Builds the dashboard on the OLAP views in `sql/02_olap_views.sql` (ID 19), the station view in `sql/03_station_daily_rain.sql` (ID 9), and the prediction view in `sql/04_predictions.sql` (ID 31). The v1 specification is in [task specifications §3](task-specs-weeks-2-5.md).

This guide was written on macOS, where Power BI Desktop cannot run. **No `.pbix` exists yet.** The model, DAX, and reference numbers below were derived from the live DW; none of them has been tested inside Power BI Desktop. Record the actual build and the reconciliation results in [progress](progress.md).

The team member building the dashboard works from the Vietnamese handoff guide `outputs/powerbi_handoff/huong_dan_power_bi.html` (not in Git; also published as a link), which carries the same model, DAX, and page specification as this document, plus a 16:9 layout sketch per page. Keep the two consistent when either changes.

## 1. Prerequisites

- Windows machine with current Power BI Desktop. Its built-in PostgreSQL connector bundles the Npgsql provider, so no driver installation is needed.
- A reachable `weather_dwh` that contains the views used below.

Choose one way to reach the database:

| Option | Steps | Notes |
| --- | --- | --- |
| A. Restore the handoff dump (used since 2026-10-23) | `outputs/powerbi_handoff/weather_dwh.dump` restored into a local `postgres:16-alpine` container on port 5434 (commands in the handoff guide) | The DW content of 2026-10-23, frozen for the dashboard |
| B. Rebuild on the Windows machine | `git pull`, then [reproduce from scratch](reproduce-from-scratch.md) | About 15 minutes of API download and loading |
| C. Connect over the LAN to the machine that holds the data | Server `<host-ip>:5434`; allow inbound TCP 5434 on that host's firewall | Compose publishes 5434 on all interfaces; use only on a trusted network and do not share the password |

## 2. Connect and import

1. **Get data → PostgreSQL database.** Server `localhost:5434` (or `<host-ip>:5434`), database `weather_dwh`, data connectivity mode **Import**.
2. Credentials: **Database** tab, with the values from `.env` (`POSTGRES_USER`/`POSTGRES_PASSWORD`) or those chosen for the restored container.
3. Select these objects:

| Object | Rows | Role |
| --- | --- | --- |
| `public.dim_date` | 4,018 (local 2016-01-01 to 2026-12-31) | Date dimension (mark as date table) |
| `public.dim_time` | 24 | Hour of day |
| `public.dim_location` | 8 | Research points |
| `public.dim_source` | 5 | Sources (NOAA and GHCNh have no rows in the observation views: in Power Query keep only Open-Meteo, Meteostat, and ERA5) |
| `public.vw_obs_daily` | 62,118 | Main daily fact |
| `public.vw_obs_hourly` | 1,490,237 | Hourly fact for hour drill-down |
| `public.vw_coverage` | 187 | Data-quality page |
| `public.vw_rain_station_vs_model` | 17,866 | Station versus Open-Meteo rain (page 7) |
| `public.vw_prediction_vs_actual` | 69,888 | Forecast versus actual, test 2025 (page 8) |

4. In Power Query, keep only the columns used; set `dim_date[full_date]` to type Date.

## 3. Model

- Relationships (single direction, one-to-many from dimension to fact):
  - `dim_date[date_key]` → `vw_obs_daily[date_key]`, `vw_obs_hourly[date_key]`, and `vw_prediction_vs_actual[prediction_date_key]` (the local date of the forecast origin)
  - `dim_location[location_key]` → `vw_obs_daily[location_key]` and `vw_obs_hourly[location_key]`
  - `dim_location[location_code]` → `vw_coverage[location_code]`, `vw_rain_station_vs_model[location_code]`, and `vw_prediction_vs_actual[location_code]`
  - `dim_source[source_key]` → `vw_obs_daily[source_key]` and `vw_obs_hourly[source_key]`; `dim_source[source_name]` → `vw_coverage[source_name]`
  - `dim_time[time_key]` → `vw_obs_hourly[time_key]`
- No relationship between fact views (no fact-to-fact joins).
- `vw_rain_station_vs_model` is not related to `dim_date`: its rain day is the 24 hours ending 12:00 UTC (19:00 local), not a calendar day. Page 7 filters on the view's own `year` and `month`.
- **Mark as date table:** `dim_date`, using `full_date`. Sort `month_name` by `month`.
- Turn off **Auto date/time** (Options → Current file → Data Load), so `full_date` is not split into an automatic hierarchy.
- Hierarchy on `dim_date`: `year` → `quarter` → `month_name` → `full_date`; page 2 adds `dim_time[hour_24]` below `full_date` in the visual axis.
- `dim_location[latitude]`/`[longitude]`: data category Latitude/Longitude.
- Report-level filter: `dim_date[year]` between 2016 and 2025. The DW is loaded by UTC year, so local 2016-01-01 has only 17 hours and local 2026 only 7. `vw_coverage` is not related to `dim_date`, so page 5 filters `vw_coverage[year]` to 2016–2025 itself.
- Hide the key columns and the raw `*_sum`/`*_valid_hours` columns from report view.
- Calculated columns (added 2026-10-23):

```dax
-- dim_date: season label for slicers (rainy season = months 5-10 in dim_date)
Season = IF ( dim_date[is_rainy_season], "Mùa mưa (T5–T10)", "Mùa khô (T11–T4)" )

-- vw_prediction_vs_actual: labels for the confusion matrix
Actual Label =
SWITCH (
    TRUE (),
    ISBLANK ( vw_prediction_vs_actual[actual_rain] ), "Chưa đủ 24 giờ số liệu",
    vw_prediction_vs_actual[actual_rain], "Thực tế: mưa",
    "Thực tế: không mưa"
)

Predicted Label =
IF ( vw_prediction_vs_actual[predicted_rain], "Dự báo: mưa", "Dự báo: không mưa" )
```

The label values are Vietnamese because they appear on the dashboard. All 69,888 test predictions have a complete 24-hour actual, so "Chưa đủ 24 giờ số liệu" does not occur in the current data.

## 4. Measures (DAX)

Create a table `_Measures` and add the following. Each observation measure returns BLANK when more than one source is in the filter context, because sources must never be added together.

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
    COALESCE (
        CALCULATE ( COUNTROWS ( vw_obs_daily ), vw_obs_daily[is_rainy_day] = TRUE () ),
        0
    )
)
-- COUNTROWS of an empty table is BLANK; with complete days and no rainy day the answer is 0.

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
    COALESCE (
        CALCULATE ( COUNTROWS ( vw_obs_daily ), vw_obs_daily[is_hot_day] = TRUE () ),
        0
    )
)

Hot Days per Location =
IF (
    [Selected Source OK],
    AVERAGEX ( VALUES ( dim_location[location_key] ), [Hot Days (location-days)] )
)

Rain Hourly (mm) =
IF ( [Selected Source OK], SUM ( vw_obs_hourly[precipitation_mm] ) )
-- Sums across locations: use it with one location selected (page 2 forces single select).

Temperature Hourly Mean (°C) =
IF ( [Selected Source OK], AVERAGE ( vw_obs_hourly[temperature_c] ) )

Rain Coverage (%) =
DIVIDE ( SUM ( vw_coverage[rain_valid_hours] ), SUM ( vw_coverage[expected_hours] ) )
```

Added 2026-10-23 for the page specification in §5:

```dax
Rain per Location per Year (mm) =
IF (
    [Selected Source OK],
    AVERAGEX ( VALUES ( dim_date[year] ), [Rain per Location (mm)] )
)
-- Mean per year: with ten years selected, the "Tháng 7" cell is the mean July total.

Rainy Days per Location per Year =
IF ( [Selected Source OK], AVERAGEX ( VALUES ( dim_date[year] ), [Rainy Days per Location] ) )

Hot Days per Location per Year =
IF ( [Selected Source OK], AVERAGEX ( VALUES ( dim_date[year] ), [Hot Days per Location] ) )

Rain Hours Coverage (%) =
IF (
    [Selected Source OK],
    DIVIDE (
        SUM ( vw_obs_daily[rain_valid_hours] ),
        24 * COUNTROWS ( dim_date ) * COUNTROWS ( VALUES ( dim_location[location_key] ) )
    )
)
-- Valid rain hours over expected hours of the selected days and locations; Meteostat gives 0%.

Temp Hours Coverage (%) =
IF (
    [Selected Source OK],
    DIVIDE (
        SUM ( vw_obs_daily[temp_valid_hours] ),
        24 * COUNTROWS ( dim_date ) * COUNTROWS ( VALUES ( dim_location[location_key] ) )
    )
)

Record Hours = SUM ( vw_coverage[record_hours] )

Temp Coverage (%) =
DIVIDE ( SUM ( vw_coverage[temp_valid_hours] ), SUM ( vw_coverage[expected_hours] ) )
```

Station versus model (page 7) and forecast evaluation (page 8):

```dax
Station Rain (mm) = SUM ( vw_rain_station_vs_model[station_rain_mm] )

Model Rain, complete days (mm) =
CALCULATE (
    SUM ( vw_rain_station_vs_model[model_rain_mm] ),
    vw_rain_station_vs_model[model_valid_hours] = 24
)

Station Rainy Days =
COALESCE ( CALCULATE ( COUNTROWS ( vw_rain_station_vs_model ), vw_rain_station_vs_model[station_is_rainy] = TRUE () ), 0 )

Model Rainy Days =
COALESCE ( CALCULATE ( COUNTROWS ( vw_rain_station_vs_model ), vw_rain_station_vs_model[model_is_rainy] = TRUE () ), 0 )

Compared Days = COUNTROWS ( vw_rain_station_vs_model )   -- added 2026-10-23

Predictions = COALESCE ( COUNTROWS ( vw_prediction_vs_actual ), 0 )
TP = COALESCE ( CALCULATE ( COUNTROWS ( vw_prediction_vs_actual ), vw_prediction_vs_actual[outcome] = "TP" ), 0 )
FP = COALESCE ( CALCULATE ( COUNTROWS ( vw_prediction_vs_actual ), vw_prediction_vs_actual[outcome] = "FP" ), 0 )
TN = COALESCE ( CALCULATE ( COUNTROWS ( vw_prediction_vs_actual ), vw_prediction_vs_actual[outcome] = "TN" ), 0 )
FN = COALESCE ( CALCULATE ( COUNTROWS ( vw_prediction_vs_actual ), vw_prediction_vs_actual[outcome] = "FN" ), 0 )

Accuracy = DIVIDE ( [TP] + [TN], [Predictions] )
Precision = DIVIDE ( [TP], [TP] + [FP] )
Recall = DIVIDE ( [TP], [TP] + [FN] )
F1 = DIVIDE ( 2 * [TP], 2 * [TP] + [FP] + [FN] )
Rain Probability Mean = AVERAGE ( vw_prediction_vs_actual[rain_probability] )

-- added 2026-10-23: share of 24-hour windows with actual rain, and share predicted as rain
Actual Rain Rate = DIVIDE ( [TP] + [FN], [TP] + [FP] + [TN] + [FN] )
Predicted Rain Rate = DIVIDE ( [TP] + [FP], [TP] + [FP] + [TN] + [FN] )
```

Format rain with 1 decimal place and "mm", temperatures with 2 decimal places and "°C", day counts with 1 decimal place, coverage as a percentage, and model metrics with 3 decimal places. Do not replace BLANK with 0 in general: Meteostat has no rain, and that must show as empty, not as "0 mm". The day counts are the exception: inside the completeness guard they return 0, because `AVERAGEX` skips BLANK and would otherwise average only over locations that had at least one such day (corrected 2026-10-22; before the fix March 2024 showed 1.33 hot days per location instead of 0.50).

## 5. Pages

Common rules: 16:9 canvas; a title and the subtitle "Local calendar (UTC+7); rainy day ≥ 1 mm and hot day ≥ 35 °C on complete days; sources are never added together" (pages 7 and 8 state their own basis); dropdown slicers in one row at the top; no dual-axis visuals (rain and temperature never share a chart); Vietnamese visual titles with units. Fixed colours, checked for colour-vision deficiency with the dataviz palette validator: Open-Meteo `#2A78D6`, ERA5 `#EB6834`, Meteostat `#1BAF7A`, GHCNh station `#008300`; single-series rain `#2A78D6`, temperature and hot days `#E34948`; dry season `#EDA100` (with data labels, below 3:1 contrast); forecast actual `#EB6834`, predicted and F1 `#6250D6`.

| Page | Question | Visuals (field wells) | OLAP |
| --- | --- | --- | --- |
| 1. Overview | How much rain, how hot, how complete, for the chosen source and period? | Slicers source (single), location, year. Cards: Rain per Location, Temperature Mean, Rainy Days per Location, Hot Days per Location, Rain Hours Coverage. Clustered bar: Y `location_name`, X Rain per Location, sorted descending. Clustered column: X `year`, Y Rainy Days per Location and Hot Days per Location | Slice |
| 2. Trends | How do rain and temperature at one location change by year, month, day, hour? | Slicers source (single), location (**single**, because Rain Hourly sums locations), year. Column chart: X `year`, `month_name`, `full_date`, `dim_time[hour_24]` (drill hierarchy), Y Rain Hourly. Line chart: same axis, Y Temperature Hourly Mean. Column chart: X `hour_24`, Y Rain Hourly. Line chart: X `hour_24`, Y Temperature Hourly Mean | Roll-up / drill-down year → month → day → hour |
| 3. Seasons and locations | Which months, seasons, and locations are wettest; where do rainy and hot days fall? | Slicers source (single), year, `Season`, location. Matrix: rows `location_name`, columns `month_name`, values Rain per Location per Year with a background gradient; column subtotal "Cả năm", row subtotal "TB 8 điểm". Matrix: rows `location_name`, columns `Season`, values Rainy and Hot Days per Location per Year. Stacked bar: Y `location_name`, X Rain per Location per Year, legend `Season` | Dice (location × month × season × year) |
| 4. Source comparison | How do the three sources differ at the same place and time? | Slicers location (single, default HAN), year, source (multi; not synced). Line chart: X `year`, `month_name`, Y Temperature Mean, legend `source_name`, tooltip Temp Hours Coverage. Clustered column: X `year`, Y Rain per Location, legend `source_name`, tooltip Rain Hours Coverage. Matrix: rows `location_name`, columns `source_name`, values Temperature Mean and Rain per Location per Year, column subtotals off, location slicer interaction set to None. Text box on the ERA5 grid cell (pressure not compared) | Slice + dice |
| 5. Data quality | Is each source complete; why is Meteostat rain blank? | Page filter `vw_coverage[year]` 2016–2025. Clustered bar: Y `source_name`, X Record Hours. Matrices: rows `source_name` → `vw_coverage[location_code]`, columns `vw_coverage[year]`, values Rain Coverage and Temp Coverage. Text box: Meteostat is HAN only (48820, Nội Bài airport), its rain was model-filled and removed; local 2016 lacks 7 hours | — |
| 6. Map | How is rain distributed across the 8 points? | Slicers source (single), year. **Azure Maps** visual: Latitude, Longitude, Size Rain per Location, tooltips name, province, Temperature Mean, Rainy Days per Location; style Grayscale light, province borders off (the base map does not show the pre-merger boundaries the project uses). Table of the 8 points (`elevation_m` not summarized). Fallbacks: the classic Map visual, or a scatter chart of longitude × latitude sized by rain | Slice |
| 7. Station and model | Does Open-Meteo rain match the ground stations? | Slicers `vw_rain_station_vs_model[year]` and `[location_name]` (single; interaction None on the column chart and station table). Cards Station Rainy Days, Model Rainy Days, Compared Days. Clustered column: X `location_name`, Y Station Rain and Model Rain, complete days. Line chart: X view `year` → `month`, same values. Table: location, `station_id`, `station_name`, `station_role`, Compared Days | Drill-down year → month |
| 8. Forecast and actual | How well does CART v1 predict rain in the next 24 hours, by location and season? | Slicers location, `Season`, `full_date` between (visual filter year 2025). Text box: CART v1, threshold 0.40, label ≥ 1 mm over the next 24 hours from Open-Meteo, test 2025 used once, retrospective; persistence F1 0.729. Cards Predictions, Accuracy, Precision, Recall, F1. Matrix: rows Actual Label, columns Predicted Label, values Predictions. Clustered column: X `location_name`, Y F1. Line chart: X `month_name`, Y Actual Rain Rate and Predicted Rain Rate, tooltips F1 and Rain Probability Mean. Detail table: `origin_local`, `location_code`, `rain_probability`, Predicted Label, `actual_rain_mm`, Actual Label, `outcome` | Slice |

## 6. Reconciliation (required before calling the dashboard done)

Set the filters in Power BI and compare with these values, computed by SQL on the views (v1 rows on 2026-10-21, the monthly hot-day rows on 2026-10-22, ERA5, station, and forecast rows on 2026-10-23, the rows for the new measures on 2026-10-23 evening). Record each comparison in `docs/progress.md`.

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
| Open-Meteo, all 8 locations, March 2024 | Hot Days per Location | 0.50 (4 hot days at 8 locations) |
| Open-Meteo, all 8 locations, January 2024 | Hot Days per Location | 0 (not BLANK) |
| ERA5, HAN, 2024 | Rain per Location / Rainy Days / Hot Days | 2,239.3 mm / 190 / 41 |
| ERA5, HAN, 2024 | Temperature Mean / Min / Max | 25.26 / 8.78 / 39.28 °C |
| ERA5, all 8 locations, 2024 | Rain per Location / Rainy Days (location-days) | 2,406.3 mm / 1,572 |
| Two sources selected | Any rain or temperature measure | BLANK |
| Map, Open-Meteo, 2024 | Rain per Location LCA / NBI | 2,392.6 / 1,979.7 mm |
| Station versus model, HAN (Hà Đông), year 2024 | Station Rain / Model Rain, complete days | 2,604.7 / 2,531.2 mm |
| Station versus model, HAN, year 2024 | Station Rainy Days / Model Rainy Days / Compared Days | 119 / 169 / 355 |
| Forecast, no filter | TP / FP / TN / FN | 27,467 / 8,569 / 26,617 / 7,235 |
| Forecast, no filter | Accuracy / Precision / Recall / F1 | 0.774 / 0.762 / 0.792 / 0.777 |
| Forecast, rainy / dry season | F1 | 0.836 / 0.624 |
| Forecast, LCA | F1 | 0.767 |
| Forecast, HAN, July 2025 | Predictions / TP / FN | 744 / 538 / 26 |
| Open-Meteo, HAN, 2016–2025 | Rain per Location per Year (all months / August) | 2,202.4 / 432.9 mm |
| Open-Meteo, all 8 locations, 2016–2025 | Rain / Rainy Days / Hot Days per Location per Year | 2,256.4 mm / 187.6 / 19.2 |
| Open-Meteo, HAN, 2024 | Rain Hours Coverage | 100.00% |
| Meteostat, HAN, 2024 | Rain Hours Coverage / Temp Hours Coverage | 0.00% / 99.81% |
| Page 5, Meteostat, HAN, year 2024 | Temp Coverage | 99.81% |
| Page 5, 2016–2025 | Record Hours Open-Meteo / ERA5 / Meteostat | 701,320 / 701,320 / 87,478 (the remaining 56 and 7 rows fall in local 2026) |
| Forecast, no filter | Actual Rain Rate / Predicted Rain Rate / Rain Probability Mean | 0.497 / 0.516 / 0.481 |

Ten-year Open-Meteo reference (2016–2025, mean per year): HAN 2,202.4 mm and 179.2 rainy days, LCA 2,949.4 mm and 247.3 rainy days (wettest), NBI 2,012.4 mm and 170.6 rainy days. The handoff guide lists further per-page values (for example the hourly profile and the monthly forecast rates), all computed by SQL on the same DW.

## 7. Delivery

- Save as `weather-dashboard.pbix` in the agreed shared location. Whether `.pbix` files go into Git is still open.
- Record in `docs/progress.md`: Power BI Desktop version, data option used, which map visual page 6 uses, refresh time, the reconciliation table with actual values, and screenshots of each OLAP operation.
- Update workbook IDs 20, 27, 33, 35 only after the reconciliation passes.
