
-- ==============================================================================
-- init_dwh_schema.sql: Star Schema Data Warehouse Khí tượng Miền Bắc
-- Đề tài: Xây dựng kho dữ liệu khí tượng đa nguồn phục vụ phân tích xu hướng
--        và dự báo khả năng mưa tại miền Bắc Việt Nam
-- ==============================================================================

-- 1. Bảng Dimension: DimDate (Thời gian theo ngày)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,              -- Định dạng YYYYMMDD (ví dụ: 20240501)
    full_date DATE NOT NULL,
    day_of_week INT NOT NULL,              -- ISO weekday: 1 = Monday (Thứ Hai) ... 7 = Sunday (Chủ Nhật)
    day_name VARCHAR(15) NOT NULL,         -- Thứ Hai, Thứ Ba...
    day_of_month INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_rainy_season BOOLEAN NOT NULL       -- Mùa mưa miền Bắc (thường tháng 5 -> tháng 10)
);

-- 2. Bảng Dimension: DimTime (Thời gian theo giờ trong ngày)
CREATE TABLE IF NOT EXISTS dim_time (
    time_key INT PRIMARY KEY,              -- Định dạng HHMM (ví dụ: 0000, 0100... 2300)
    hour_24 INT NOT NULL,                  -- 0 -> 23
    minute INT NOT NULL,                   -- 0 -> 59
    period_of_day VARCHAR(20) NOT NULL     -- Sáng 05-10, Trưa 11-13, Chiều 14-17, Tối 18-21, Đêm 22-04 (local hour)
);

-- 3. Bảng Dimension: DimLocation (Địa điểm khu vực miền Bắc)
CREATE TABLE IF NOT EXISTS dim_location (
    location_key SERIAL PRIMARY KEY,
    location_code VARCHAR(10) UNIQUE NOT NULL, -- HAN, PTO, TNG, TUQ, LCA, HBI, HPH, NBI
    location_name VARCHAR(100) NOT NULL,
    province VARCHAR(100) NOT NULL,
    region VARCHAR(50) DEFAULT 'Miền Bắc',
    latitude NUMERIC(9, 6) NOT NULL,
    longitude NUMERIC(9, 6) NOT NULL,
    elevation_m NUMERIC(7, 2)
);

-- 4. Bảng Dimension: DimSource (Nguồn dữ liệu đa nguồn)
CREATE TABLE IF NOT EXISTS dim_source (
    source_key SERIAL PRIMARY KEY,
    source_name VARCHAR(50) UNIQUE NOT NULL, -- Open-Meteo, ERA5, Meteostat, NOAA
    source_type VARCHAR(50) NOT NULL,        -- REST API, Reanalysis Dataset, Station Dataset
    data_format VARCHAR(20) NOT NULL,        -- JSON, NetCDF, CSV/Text
    update_frequency VARCHAR(50) NOT NULL    -- Theo giờ (Hourly), Hàng ngày (Daily), Bulk
);

-- 5. Bảng Dimension: DimStation (Trạm quan trắc mặt đất)
CREATE TABLE IF NOT EXISTS dim_station (
    station_key SERIAL PRIMARY KEY,
    station_id VARCHAR(50) UNIQUE NOT NULL,  -- WMO ID hoặc Station Code
    station_name VARCHAR(100) NOT NULL,
    wmo_id VARCHAR(20),
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6),
    elevation_m NUMERIC(7, 2),
    location_key INT REFERENCES dim_location(location_key)
);

-- 6. Bảng Fact: FactWeatherObservation (Quan trắc thời tiết lịch sử theo giờ)
-- Grain: 1 nguồn dữ liệu × 1 vị trí × 1 thời điểm theo giờ
CREATE TABLE IF NOT EXISTS fact_weather_observation (
    fact_id BIGSERIAL PRIMARY KEY,
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    time_key INT NOT NULL REFERENCES dim_time(time_key),
    location_key INT NOT NULL REFERENCES dim_location(location_key),
    source_key INT NOT NULL REFERENCES dim_source(source_key),
    station_key INT REFERENCES dim_station(station_key),
    timestamp_utc TIMESTAMPTZ NOT NULL,
    timestamp_local TIMESTAMP NOT NULL,      -- Giờ địa phương Việt Nam (UTC+7)
    
    -- Các chỉ số khí tượng chuẩn hóa
    temperature_c NUMERIC(5, 2),             -- Nhiệt độ (°C)
    humidity_percent NUMERIC(5, 2),          -- Độ ẩm (%)
    pressure_hpa NUMERIC(7, 2),              -- Áp suất khí quyển (hPa)
    precipitation_mm NUMERIC(6, 2),          -- Lượng mưa (mm)
    wind_speed_ms NUMERIC(5, 2),             -- Tốc độ gió (m/s)
    wind_direction_deg NUMERIC(5, 2),        -- Hướng gió (độ 0-360)
    cloud_cover_percent NUMERIC(5, 2),       -- Độ che phủ mây (%)
    solar_radiation_wm2 NUMERIC(7, 2),       -- Bức xạ mặt trời (W/m²)
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_weather_obs UNIQUE (date_key, time_key, location_key, source_key)
);

-- Index tối ưu truy vấn OLAP & BI
CREATE INDEX IF NOT EXISTS idx_fwo_date ON fact_weather_observation(date_key);
CREATE INDEX IF NOT EXISTS idx_fwo_location ON fact_weather_observation(location_key);
CREATE INDEX IF NOT EXISTS idx_fwo_source ON fact_weather_observation(source_key);
CREATE INDEX IF NOT EXISTS idx_fwo_local_ts ON fact_weather_observation(timestamp_local);

-- 7. Bảng Fact: FactWeatherPrediction (Lưu kết quả dự báo ML - Decision Tree CART)
CREATE TABLE IF NOT EXISTS fact_weather_prediction (
    prediction_id BIGSERIAL PRIMARY KEY,
    prediction_date_key INT NOT NULL REFERENCES dim_date(date_key),
    prediction_time_key INT NOT NULL REFERENCES dim_time(time_key),
    target_date_key INT NOT NULL REFERENCES dim_date(date_key),
    location_key INT NOT NULL REFERENCES dim_location(location_key),
    model_name VARCHAR(50) DEFAULT 'DecisionTree_CART',
    model_version VARCHAR(20) NOT NULL,
    rain_next_24h BOOLEAN NOT NULL,          -- True: Có mưa (Yes), False: Không mưa (No)
    rain_probability NUMERIC(5, 4),          -- Xác suất xuất hiện mưa (0.0000 -> 1.0000)
    predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Chèn dữ liệu mẫu cho các Dimension cố định ban đầu
-- 1. DimLocation (8 địa điểm đại diện miền Bắc trong phạm vi nghiên cứu)
INSERT INTO dim_location (location_code, location_name, province, latitude, longitude, elevation_m)
VALUES
    ('HAN', 'Hà Nội', 'Hà Nội', 21.0285, 105.8542, 10.0),
    ('PTO', 'Phú Thọ', 'Phú Thọ', 21.3986, 105.2280, 25.0),
    ('TNG', 'Thái Nguyên', 'Thái Nguyên', 21.5942, 105.8482, 35.0),
    ('TUQ', 'Tuyên Quang', 'Tuyên Quang', 21.8236, 105.2153, 50.0),
    ('LCA', 'Lào Cai', 'Lào Cai', 22.4856, 103.9707, 80.0),
    ('HBI', 'Hòa Bình', 'Hòa Bình', 20.8172, 105.3376, 20.0),
    ('HPH', 'Hải Phòng', 'Hải Phòng', 20.8449, 106.6881, 4.0),
    ('NBI', 'Ninh Bình', 'Ninh Bình', 20.2506, 105.9745, 5.0)
ON CONFLICT (location_code) DO NOTHING;

-- 2. DimSource (Các nguồn dữ liệu dự kiến)
INSERT INTO dim_source (source_name, source_type, data_format, update_frequency)
VALUES
    ('Open-Meteo', 'REST API', 'JSON', 'Hourly'),
    ('ERA5', 'Reanalysis Dataset', 'NetCDF', 'Monthly Bulk'),
    ('Meteostat', 'Station Dataset', 'CSV', 'Hourly'),
    ('NOAA', 'Station Dataset', 'Text/CSV', 'Hourly')
ON CONFLICT (source_name) DO NOTHING;

-- 3. DimTime (24 giờ trong ngày)
DO $$
DECLARE
    h INT;
    t_key INT;
    p_name VARCHAR(20);
BEGIN
    FOR h IN 0..23 LOOP
        t_key := h * 100;
        IF h >= 5 AND h < 11 THEN
            p_name := 'Sáng (Morning)';
        ELSIF h >= 11 AND h < 14 THEN
            p_name := 'Trưa (Noon)';
        ELSIF h >= 14 AND h < 18 THEN
            p_name := 'Chiều (Afternoon)';
        ELSIF h >= 18 AND h < 22 THEN
            p_name := 'Tối (Evening)';
        ELSE
            p_name := 'Đêm (Night)';
        END IF;
        
        INSERT INTO dim_time (time_key, hour_24, minute, period_of_day)
        VALUES (t_key, h, 0, p_name)
        ON CONFLICT (time_key) DO NOTHING;
    END LOOP;
END $$;
