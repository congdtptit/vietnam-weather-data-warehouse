-- ==============================================================================
-- 02_olap_views.sql: OLAP views over fact_weather_observation (workbook ID 19)
--
-- Idempotent (CREATE OR REPLACE VIEW); safe to apply to an existing weather_dwh.
-- Specification: docs/task-specs-weeks-2-5.md section 2; rules: docs/warehouse-schema.md.
--
-- Calendar basis: date_key/time_key are Vietnam local time (Asia/Ho_Chi_Minh, UTC+7),
-- so days, months, and years below are local. timestamp_utc stays available hourly.
-- Business thresholds (decided 2026-10-21):
--   rainy day = complete day (24 valid rain hours) with daily rain >= 1.0 mm
--   hot day   = complete day (24 valid temperature hours) with daily max >= 35.0 degC
-- Rules: every aggregate stays within one source x one location; rain is never summed
-- across sources or locations; means roll up from sums and counts; wind direction uses
-- a vector mean that excludes calm or missing-direction hours; missing is never zero.
-- pressure_hpa is surface pressure for Open-Meteo and sea-level pressure for Meteostat,
-- so compare pressure only within a source.
-- ==============================================================================

-- 1. Hourly: fact rows with dimension attributes, no aggregation
CREATE OR REPLACE VIEW vw_obs_hourly AS
SELECT
    f.fact_id,
    f.source_key,
    s.source_name,
    f.location_key,
    l.location_code,
    l.location_name,
    l.province,
    f.station_key,
    f.timestamp_utc,
    f.timestamp_local,
    f.date_key,
    f.time_key,
    d.full_date AS local_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_of_month,
    d.day_of_week,
    d.is_weekend,
    d.is_rainy_season,
    t.hour_24 AS local_hour,
    t.period_of_day,
    f.temperature_c,
    f.humidity_percent,
    f.pressure_hpa,
    f.precipitation_mm,
    f.wind_speed_ms,
    f.wind_direction_deg,
    f.cloud_cover_percent,
    f.solar_radiation_wm2
FROM fact_weather_observation f
JOIN dim_source s ON s.source_key = f.source_key
JOIN dim_location l ON l.location_key = f.location_key
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_time t ON t.time_key = f.time_key;

-- 2. Daily: source x location x local date
CREATE OR REPLACE VIEW vw_obs_daily AS
WITH day_agg AS (
    SELECT
        f.source_key,
        f.location_key,
        f.date_key,
        count(*) AS record_hours,
        sum(f.precipitation_mm) AS rain_mm,
        count(f.precipitation_mm) AS rain_valid_hours,
        sum(f.temperature_c) AS temp_sum,
        count(f.temperature_c) AS temp_valid_hours,
        min(f.temperature_c) AS temp_min_c,
        max(f.temperature_c) AS temp_max_c,
        sum(f.humidity_percent) AS humidity_sum,
        count(f.humidity_percent) AS humidity_valid_hours,
        sum(f.wind_speed_ms) AS wind_speed_sum,
        count(f.wind_speed_ms) AS wind_speed_valid_hours,
        sum(sin(radians(f.wind_direction_deg)))
            FILTER (WHERE f.wind_direction_deg IS NOT NULL AND f.wind_speed_ms > 0)
            AS wind_dir_sin_sum,
        sum(cos(radians(f.wind_direction_deg)))
            FILTER (WHERE f.wind_direction_deg IS NOT NULL AND f.wind_speed_ms > 0)
            AS wind_dir_cos_sum,
        count(*) FILTER (WHERE f.wind_direction_deg IS NOT NULL AND f.wind_speed_ms > 0)
            AS wind_dir_valid_hours
    FROM fact_weather_observation f
    GROUP BY f.source_key, f.location_key, f.date_key
)
SELECT
    a.source_key,
    s.source_name,
    a.location_key,
    l.location_code,
    l.location_name,
    l.province,
    a.date_key,
    d.full_date AS local_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_of_month,
    d.is_weekend,
    d.is_rainy_season,
    24 AS expected_hours,
    a.record_hours,
    a.rain_mm,
    a.rain_valid_hours,
    (a.rain_valid_hours = 24) AS is_complete_rain_day,
    a.temp_sum,
    a.temp_valid_hours,
    (a.temp_sum / NULLIF(a.temp_valid_hours, 0))::NUMERIC(5, 2) AS temp_mean_c,
    a.temp_min_c,
    a.temp_max_c,
    (a.temp_valid_hours = 24) AS is_complete_temp_day,
    a.humidity_sum,
    a.humidity_valid_hours,
    (a.humidity_sum / NULLIF(a.humidity_valid_hours, 0))::NUMERIC(5, 2) AS humidity_mean_percent,
    a.wind_speed_sum,
    a.wind_speed_valid_hours,
    (a.wind_speed_sum / NULLIF(a.wind_speed_valid_hours, 0))::NUMERIC(5, 2) AS wind_speed_mean_ms,
    a.wind_dir_sin_sum,
    a.wind_dir_cos_sum,
    a.wind_dir_valid_hours,
    CASE WHEN a.wind_dir_valid_hours > 0 THEN
        mod(round((degrees(atan2(a.wind_dir_sin_sum, a.wind_dir_cos_sum)) + 360)::NUMERIC, 1), 360)::NUMERIC(5, 1)
    END AS wind_dir_vector_mean_deg,
    CASE WHEN a.rain_valid_hours = 24 THEN a.rain_mm >= 1.0 END AS is_rainy_day,
    CASE WHEN a.temp_valid_hours = 24 THEN a.temp_max_c >= 35.0 END AS is_hot_day
FROM day_agg a
JOIN dim_source s ON s.source_key = a.source_key
JOIN dim_location l ON l.location_key = a.location_key
JOIN dim_date d ON d.date_key = a.date_key;

-- Calendar sizes for expected-hour denominators (full local months/years, not only
-- the days that happen to have rows)
CREATE OR REPLACE VIEW vw_calendar_month AS
SELECT year, month, count(*) AS days_in_month
FROM dim_date
GROUP BY year, month;

-- 3. Monthly: source x location x local month, rolled up from daily sums and counts
CREATE OR REPLACE VIEW vw_obs_monthly AS
WITH month_agg AS (
    SELECT
        source_key,
        location_key,
        year,
        month,
        count(*) AS days_with_records,
        sum(record_hours) AS record_hours,
        sum(rain_mm) AS rain_mm,
        sum(rain_valid_hours) AS rain_valid_hours,
        sum(temp_sum) AS temp_sum,
        sum(temp_valid_hours) AS temp_valid_hours,
        min(temp_min_c) AS temp_min_c,
        max(temp_max_c) AS temp_max_c,
        sum(humidity_sum) AS humidity_sum,
        sum(humidity_valid_hours) AS humidity_valid_hours,
        sum(wind_speed_sum) AS wind_speed_sum,
        sum(wind_speed_valid_hours) AS wind_speed_valid_hours,
        sum(wind_dir_sin_sum) AS wind_dir_sin_sum,
        sum(wind_dir_cos_sum) AS wind_dir_cos_sum,
        sum(wind_dir_valid_hours) AS wind_dir_valid_hours,
        count(*) FILTER (WHERE is_complete_rain_day) AS complete_rain_days,
        count(*) FILTER (WHERE is_complete_temp_day) AS complete_temp_days,
        count(*) FILTER (WHERE is_rainy_day) AS rainy_days,
        count(*) FILTER (WHERE is_hot_day) AS hot_days
    FROM vw_obs_daily
    GROUP BY source_key, location_key, year, month
)
SELECT
    a.source_key,
    s.source_name,
    a.location_key,
    l.location_code,
    l.location_name,
    l.province,
    a.year,
    a.month,
    (a.year * 100 + a.month) AS year_month,
    c.days_in_month,
    c.days_in_month * 24 AS expected_hours,
    a.days_with_records,
    a.record_hours,
    a.rain_mm,
    a.rain_valid_hours,
    (a.rain_valid_hours = c.days_in_month * 24) AS is_complete_rain_month,
    a.temp_sum,
    a.temp_valid_hours,
    (a.temp_sum / NULLIF(a.temp_valid_hours, 0))::NUMERIC(5, 2) AS temp_mean_c,
    a.temp_min_c,
    a.temp_max_c,
    a.humidity_sum,
    a.humidity_valid_hours,
    (a.humidity_sum / NULLIF(a.humidity_valid_hours, 0))::NUMERIC(5, 2) AS humidity_mean_percent,
    a.wind_speed_sum,
    a.wind_speed_valid_hours,
    (a.wind_speed_sum / NULLIF(a.wind_speed_valid_hours, 0))::NUMERIC(5, 2) AS wind_speed_mean_ms,
    a.wind_dir_sin_sum,
    a.wind_dir_cos_sum,
    a.wind_dir_valid_hours,
    CASE WHEN a.wind_dir_valid_hours > 0 THEN
        mod(round((degrees(atan2(a.wind_dir_sin_sum, a.wind_dir_cos_sum)) + 360)::NUMERIC, 1), 360)::NUMERIC(5, 1)
    END AS wind_dir_vector_mean_deg,
    a.complete_rain_days,
    a.complete_temp_days,
    -- Unknown (NULL), not zero, when no day in the month is complete
    CASE WHEN a.complete_rain_days > 0 THEN a.rainy_days END AS rainy_days,
    CASE WHEN a.complete_temp_days > 0 THEN a.hot_days END AS hot_days
FROM month_agg a
JOIN vw_calendar_month c ON c.year = a.year AND c.month = a.month
JOIN dim_source s ON s.source_key = a.source_key
JOIN dim_location l ON l.location_key = a.location_key;

-- 4. Yearly: source x location x local year, rolled up from monthly sums and counts
CREATE OR REPLACE VIEW vw_obs_yearly AS
WITH year_agg AS (
    SELECT
        source_key,
        location_key,
        year,
        sum(record_hours) AS record_hours,
        sum(rain_mm) AS rain_mm,
        sum(rain_valid_hours) AS rain_valid_hours,
        sum(temp_sum) AS temp_sum,
        sum(temp_valid_hours) AS temp_valid_hours,
        min(temp_min_c) AS temp_min_c,
        max(temp_max_c) AS temp_max_c,
        sum(humidity_sum) AS humidity_sum,
        sum(humidity_valid_hours) AS humidity_valid_hours,
        sum(wind_speed_sum) AS wind_speed_sum,
        sum(wind_speed_valid_hours) AS wind_speed_valid_hours,
        sum(complete_rain_days) AS complete_rain_days,
        sum(complete_temp_days) AS complete_temp_days,
        sum(rainy_days) AS rainy_days,
        sum(hot_days) AS hot_days
    FROM vw_obs_monthly
    GROUP BY source_key, location_key, year
),
calendar_year AS (
    SELECT year, sum(days_in_month) AS days_in_year
    FROM vw_calendar_month
    GROUP BY year
)
SELECT
    a.source_key,
    s.source_name,
    a.location_key,
    l.location_code,
    l.location_name,
    l.province,
    a.year,
    c.days_in_year,
    c.days_in_year * 24 AS expected_hours,
    a.record_hours,
    a.rain_mm,
    a.rain_valid_hours,
    (a.rain_valid_hours = c.days_in_year * 24) AS is_complete_rain_year,
    a.temp_sum,
    a.temp_valid_hours,
    (a.temp_sum / NULLIF(a.temp_valid_hours, 0))::NUMERIC(5, 2) AS temp_mean_c,
    a.temp_min_c,
    a.temp_max_c,
    (a.humidity_sum / NULLIF(a.humidity_valid_hours, 0))::NUMERIC(5, 2) AS humidity_mean_percent,
    (a.wind_speed_sum / NULLIF(a.wind_speed_valid_hours, 0))::NUMERIC(5, 2) AS wind_speed_mean_ms,
    a.complete_rain_days,
    a.complete_temp_days,
    a.rainy_days,
    a.hot_days
FROM year_agg a
JOIN calendar_year c ON c.year = a.year
JOIN dim_source s ON s.source_key = a.source_key
JOIN dim_location l ON l.location_key = a.location_key;

-- 5. Coverage: share of expected local-year hours present per variable
-- The first and last local years of a UTC-bounded load are partial by construction
-- (for 2016-2025 UTC data: 2016 lacks 7 local hours; 2026 holds only 7 hours).
CREATE OR REPLACE VIEW vw_coverage AS
SELECT
    source_name,
    location_code,
    year,
    expected_hours,
    record_hours,
    rain_valid_hours,
    temp_valid_hours,
    round(100.0 * record_hours / expected_hours, 2) AS record_coverage_pct,
    round(100.0 * rain_valid_hours / expected_hours, 2) AS rain_coverage_pct,
    round(100.0 * temp_valid_hours / expected_hours, 2) AS temp_coverage_pct
FROM vw_obs_yearly;
