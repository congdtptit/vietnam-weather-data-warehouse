-- ==============================================================================
-- 03_station_daily_rain.sql: station daily rainfall from NOAA GHCNh (workbook ID 9)
--
-- Idempotent migration; safe to apply to an existing weather_dwh. It adds a separate fact
-- and does not change fact_weather_observation (hourly grain is unchanged).
-- Contract: docs/data-contract.md ("Station daily rainfall"); spec:
-- docs/task-specs-weeks-2-5.md section 4.
--
-- Grain: one source x one station x one rain day (UTC). The value for rain day D is the
-- 24-hour gauge total for the window (D-1 12:00 UTC, D 12:00 UTC], reported at 12:00 UTC
-- (19:00 Vietnam time). A day without a usable report has no row: missing, not 0 mm.
-- location_key is the research point the station represents (GHCNH_RAIN_STATIONS in
-- scripts/station_config.py); station_role says whether it is a substitute.
-- ==============================================================================

INSERT INTO dim_source (source_name, source_type, data_format, update_frequency)
VALUES ('GHCNh', 'Station Dataset', 'PSV', 'Daily (24 h total at 12 UTC)')
ON CONFLICT (source_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS fact_station_daily_rain (
    daily_rain_id BIGSERIAL PRIMARY KEY,
    source_key INT NOT NULL REFERENCES dim_source(source_key),
    station_key INT NOT NULL REFERENCES dim_station(station_key),
    location_key INT NOT NULL REFERENCES dim_location(location_key),
    rain_day_utc DATE NOT NULL,
    window_start_utc TIMESTAMPTZ NOT NULL,
    window_end_utc TIMESTAMPTZ NOT NULL,
    rain_24h_mm NUMERIC(6, 1) NOT NULL,      -- source precision is 0.1 mm
    is_trace BOOLEAN NOT NULL,
    station_role VARCHAR(40) NOT NULL,
    quality_code VARCHAR(4),
    measurement_code VARCHAR(4),
    report_type VARCHAR(10),
    source_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_station_daily_rain UNIQUE (source_key, station_key, rain_day_utc),
    CONSTRAINT ck_station_daily_rain_window
        CHECK (window_end_utc - window_start_utc = INTERVAL '24 hours'),
    CONSTRAINT ck_station_daily_rain_nonnegative CHECK (rain_24h_mm >= 0)
);

CREATE INDEX IF NOT EXISTS idx_fsdr_location_day ON fact_station_daily_rain(location_key, rain_day_utc);
CREATE INDEX IF NOT EXISTS idx_fsdr_station ON fact_station_daily_rain(station_key);

-- Station rain versus Open-Meteo over the same (D-1 12:00, D 12:00] UTC window.
-- Open-Meteo hourly rain is the sum of the preceding hour, so the hour stamped h belongs
-- to rain day D when D-1 12:00 < h <= D 12:00. Model rain is NULL unless all 24 hours exist.
CREATE OR REPLACE VIEW vw_rain_station_vs_model AS
WITH model_window AS (
    SELECT
        f.location_key,
        ((f.timestamp_utc AT TIME ZONE 'UTC') - INTERVAL '12 hours' - INTERVAL '1 microsecond')::DATE + 1
            AS rain_day_utc,
        sum(f.precipitation_mm) AS model_rain_sum,
        count(f.precipitation_mm) AS model_valid_hours
    FROM fact_weather_observation f
    JOIN dim_source s ON s.source_key = f.source_key
    WHERE s.source_name = 'Open-Meteo'
    GROUP BY 1, 2
)
SELECT
    r.rain_day_utc,
    extract(YEAR FROM r.rain_day_utc)::INT AS year,
    extract(MONTH FROM r.rain_day_utc)::INT AS month,
    l.location_code,
    l.location_name,
    st.station_id,
    st.station_name,
    r.station_role,
    r.rain_24h_mm AS station_rain_mm,
    r.is_trace AS station_is_trace,
    (r.rain_24h_mm >= 1.0) AS station_is_rainy,
    m.model_valid_hours,
    CASE WHEN m.model_valid_hours = 24 THEN m.model_rain_sum END AS model_rain_mm,
    CASE WHEN m.model_valid_hours = 24 THEN m.model_rain_sum >= 1.0 END AS model_is_rainy
FROM fact_station_daily_rain r
JOIN dim_location l ON l.location_key = r.location_key
JOIN dim_station st ON st.station_key = r.station_key
LEFT JOIN model_window m ON m.location_key = r.location_key AND m.rain_day_utc = r.rain_day_utc;
