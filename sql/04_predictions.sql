-- ==============================================================================
-- 04_predictions.sql: dim_model, prediction fact keys, and prediction-vs-actual view
-- (workbook ID 31)
--
-- Idempotent migration; safe to apply to an existing weather_dwh and to rerun.
-- It extends fact_weather_prediction from init_dwh_schema.sql without dropping columns.
--
-- Grain: one model version x one research location x one UTC forecast origin. The horizon
-- is fixed per model (24 h for RainNext24h); the target window is (target_start, target_end]
-- with target_start = origin, matching docs/rain-label.md. Legacy columns are kept and
-- filled by scripts/load_predictions.py:
--   prediction_date_key / prediction_time_key  Vietnam local date/hour of the origin
--   target_date_key                            Vietnam local date of target_end_utc
--   predicted_at                               row load time, not the forecast origin
-- ==============================================================================

CREATE TABLE IF NOT EXISTS dim_model (
    model_key SERIAL PRIMARY KEY,
    model_name VARCHAR(50) NOT NULL,
    model_version VARCHAR(20) NOT NULL,
    algorithm TEXT NOT NULL,
    label_version TEXT NOT NULL,
    label_source TEXT NOT NULL,                 -- source of the actual rain used by the label
    feature_version TEXT NOT NULL,
    feature_source TEXT NOT NULL,
    label_threshold_mm NUMERIC(4, 1) NOT NULL,  -- rain total that defines a Rain label
    probability_threshold NUMERIC(5, 4) NOT NULL, -- probability that defines a Rain prediction
    horizon_hours SMALLINT NOT NULL,
    train_start_utc TIMESTAMPTZ NOT NULL,
    train_end_utc TIMESTAMPTZ NOT NULL,         -- exclusive
    hyperparameters JSONB NOT NULL,
    artifact_uri TEXT NOT NULL,
    artifact_sha256 CHAR(64) NOT NULL,
    validation_f1 NUMERIC(5, 4),
    test_precision NUMERIC(5, 4),
    test_recall NUMERIC(5, 4),
    test_f1 NUMERIC(5, 4),
    availability_note TEXT NOT NULL,            -- e.g. retrospective reanalysis experiment
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_dim_model UNIQUE (model_name, model_version),
    CONSTRAINT ck_dim_model_thresholds CHECK (
        label_threshold_mm > 0 AND probability_threshold > 0 AND probability_threshold < 1
    ),
    CONSTRAINT ck_dim_model_horizon CHECK (horizon_hours > 0),
    CONSTRAINT ck_dim_model_train_window CHECK (train_end_utc > train_start_utc)
);

ALTER TABLE fact_weather_prediction
    ADD COLUMN IF NOT EXISTS model_key INT,
    ADD COLUMN IF NOT EXISTS origin_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS target_start_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS target_end_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS horizon_hours SMALLINT,
    ADD COLUMN IF NOT EXISTS probability_threshold NUMERIC(5, 4),
    ADD COLUMN IF NOT EXISTS evaluation_split VARCHAR(20);

-- Rows loaded before this migration have no origin or model and cannot be reconciled.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM fact_weather_prediction
        WHERE model_key IS NULL OR origin_utc IS NULL OR target_start_utc IS NULL
           OR target_end_utc IS NULL OR horizon_hours IS NULL
           OR probability_threshold IS NULL OR evaluation_split IS NULL
           OR rain_probability IS NULL
    ) THEN
        RAISE EXCEPTION 'fact_weather_prediction has rows without model/origin/window; '
                        'reload them with scripts/load_predictions.py before migrating';
    END IF;
END $$;

ALTER TABLE fact_weather_prediction
    ALTER COLUMN model_key SET NOT NULL,
    ALTER COLUMN origin_utc SET NOT NULL,
    ALTER COLUMN target_start_utc SET NOT NULL,
    ALTER COLUMN target_end_utc SET NOT NULL,
    ALTER COLUMN horizon_hours SET NOT NULL,
    ALTER COLUMN probability_threshold SET NOT NULL,
    ALTER COLUMN evaluation_split SET NOT NULL,
    ALTER COLUMN rain_probability SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_fwp_model') THEN
        ALTER TABLE fact_weather_prediction
            ADD CONSTRAINT fk_fwp_model FOREIGN KEY (model_key) REFERENCES dim_model(model_key);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_fwp_model_location_origin') THEN
        ALTER TABLE fact_weather_prediction
            ADD CONSTRAINT uq_fwp_model_location_origin
            UNIQUE (model_key, location_key, origin_utc);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_fwp_window') THEN
        ALTER TABLE fact_weather_prediction
            ADD CONSTRAINT ck_fwp_window CHECK (
                target_start_utc = origin_utc
                AND target_end_utc - target_start_utc = horizon_hours * INTERVAL '1 hour'
            );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_fwp_probability') THEN
        ALTER TABLE fact_weather_prediction
            ADD CONSTRAINT ck_fwp_probability CHECK (
                rain_probability BETWEEN 0 AND 1
                AND probability_threshold > 0 AND probability_threshold < 1
            );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_fwp_split') THEN
        ALTER TABLE fact_weather_prediction
            ADD CONSTRAINT ck_fwp_split CHECK (
                evaluation_split IN ('train', 'validation', 'test', 'operational')
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_fwp_model ON fact_weather_prediction(model_key);
CREATE INDEX IF NOT EXISTS idx_fwp_location_origin ON fact_weather_prediction(location_key, origin_utc);
CREATE INDEX IF NOT EXISTS idx_fwp_target_date ON fact_weather_prediction(target_date_key);

-- Supports the 24-hour window sums in vw_prediction_vs_actual (one index range per row)
CREATE INDEX IF NOT EXISTS idx_fwo_location_source_ts
    ON fact_weather_observation(location_key, source_key, timestamp_utc);

-- Prediction versus actual rain over the same (target_start, target_end] window, from the
-- label source in fact_weather_observation (hourly values stamped at interval end). The actual
-- is NULL unless every hour of the window has a value: missing is not "no rain".
CREATE OR REPLACE VIEW vw_prediction_vs_actual AS
SELECT
    p.prediction_id,
    m.model_name,
    m.model_version,
    p.evaluation_split,
    l.location_code,
    l.location_name,
    p.origin_utc,
    (p.origin_utc AT TIME ZONE 'Asia/Ho_Chi_Minh') AS origin_local,
    p.prediction_date_key,
    p.prediction_time_key,
    p.target_start_utc,
    p.target_end_utc,
    p.target_date_key,
    p.rain_probability,
    p.probability_threshold,
    p.rain_next_24h AS predicted_rain,
    m.label_threshold_mm,
    a.valid_hours AS actual_valid_hours,
    CASE WHEN a.valid_hours = p.horizon_hours THEN a.rain_mm END AS actual_rain_mm,
    CASE WHEN a.valid_hours = p.horizon_hours THEN a.rain_mm >= m.label_threshold_mm END
        AS actual_rain,
    CASE
        WHEN a.valid_hours IS DISTINCT FROM p.horizon_hours THEN NULL
        WHEN p.rain_next_24h AND a.rain_mm >= m.label_threshold_mm THEN 'TP'
        WHEN p.rain_next_24h THEN 'FP'
        WHEN a.rain_mm >= m.label_threshold_mm THEN 'FN'
        ELSE 'TN'
    END AS outcome
FROM fact_weather_prediction p
JOIN dim_model m ON m.model_key = p.model_key
JOIN dim_location l ON l.location_key = p.location_key
LEFT JOIN dim_source s ON s.source_name = m.label_source
LEFT JOIN LATERAL (
    SELECT count(f.precipitation_mm)::INT AS valid_hours, sum(f.precipitation_mm) AS rain_mm
    FROM fact_weather_observation f
    WHERE f.location_key = p.location_key
      AND f.source_key = s.source_key
      AND f.timestamp_utc > p.target_start_utc
      AND f.timestamp_utc <= p.target_end_utc
) a ON TRUE;
