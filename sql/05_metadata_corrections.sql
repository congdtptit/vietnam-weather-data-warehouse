-- ==============================================================================
-- 05_metadata_corrections.sql: metadata found missing by the 2026-10-22 audit
--
-- Idempotent migration; safe to apply to an existing weather_dwh and to rerun.
-- It only adds nullable columns and constraints; no fact grain or key changes.
--
-- 1. dim_model: which labels each metric was measured on. CART v1 was selected on
--    rain_next24h_v0.2 labels (validation_f1) and its test metrics were re-scored on the
--    corrected rain_next24h_v0.2.1 labels (docs/rain-label.md). Filled by
--    scripts/load_predictions.py.
-- 2. dim_station: role and distance of the station relative to its research point
--    (scripts/station_config.py). Filled by scripts/seed_dimensions.py.
-- 3. fact_weather_observation.pressure_type: surface (Open-Meteo) or sea_level
--    (Meteostat), as in Silver/Gold (docs/data-contract.md). The two are never comparable.
--    Existing rows are backfilled from their source; scripts/load_gold_observations.py
--    writes it for new loads.
-- ==============================================================================

ALTER TABLE dim_model
    ADD COLUMN IF NOT EXISTS evaluation_label_version TEXT,
    ADD COLUMN IF NOT EXISTS validation_f1_evaluation_labels NUMERIC(5, 4);

COMMENT ON COLUMN dim_model.label_version IS
    'Labels the model was trained and selected on';
COMMENT ON COLUMN dim_model.validation_f1 IS
    'Validation F1 at probability_threshold on label_version (the selection value)';
COMMENT ON COLUMN dim_model.evaluation_label_version IS
    'Labels the test_* metrics and validation_f1_evaluation_labels were measured on';
COMMENT ON COLUMN dim_model.validation_f1_evaluation_labels IS
    'Validation F1 at probability_threshold on evaluation_label_version (reference only)';

ALTER TABLE dim_station
    ADD COLUMN IF NOT EXISTS station_role VARCHAR(40),
    ADD COLUMN IF NOT EXISTS distance_km NUMERIC(5, 1);

COMMENT ON COLUMN dim_station.station_role IS
    'representative or nearest_available for the mapped research location; NULL if unmapped';
COMMENT ON COLUMN dim_station.distance_km IS
    'Great-circle distance from the station to its research location point';

ALTER TABLE fact_weather_observation
    ADD COLUMN IF NOT EXISTS pressure_type VARCHAR(20);

UPDATE fact_weather_observation f
SET pressure_type = CASE s.source_name
        WHEN 'Open-Meteo' THEN 'surface'
        WHEN 'Meteostat' THEN 'sea_level'
    END
FROM dim_source s
WHERE s.source_key = f.source_key
  AND f.pressure_type IS NULL
  AND s.source_name IN ('Open-Meteo', 'Meteostat');

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_fwo_pressure_type') THEN
        ALTER TABLE fact_weather_observation
            ADD CONSTRAINT ck_fwo_pressure_type
            CHECK (pressure_type IN ('surface', 'sea_level'));
    END IF;
END $$;

COMMENT ON COLUMN fact_weather_observation.pressure_type IS
    'surface (Open-Meteo surface_pressure) or sea_level (Meteostat; METAR QNH for 48820)';
