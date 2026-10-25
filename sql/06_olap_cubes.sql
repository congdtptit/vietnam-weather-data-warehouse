-- ==============================================================================
-- 06_olap_cubes.sql: two precomputed sub-cubes (materialized views) of the observation cube
--
-- Idempotent (IF NOT EXISTS); safe to apply to an existing weather_dwh. Adds objects only:
-- no existing table or view changes, so Power BI reports built on the vw_* views are unaffected.
--
-- Cube lattice: dimensions source x location x time (hour -> day -> month -> year). The base
-- cuboid is fact_weather_observation (one source x location x UTC hour). Precomputed:
--   mv_obs_daily   source x location x local day   (about 24x fewer rows than the base)
--   mv_obs_monthly source x location x local month (serves month, season, and year questions)
-- Why these two: every daily/monthly/yearly view aggregates the whole fact table before any
-- filter applies (about 1.3 s each, docs/warehouse-schema.md, ID 26); the daily cuboid answers
-- rainy/hot-day questions, the monthly one answers trend and seasonal questions, and the
-- yearly cuboid is small enough to compute from the monthly one at query time. The hourly
-- base stays in the fact table.
--
-- Contents equal vw_obs_daily and vw_obs_monthly (02_olap_views.sql), so the aggregation
-- rules there apply unchanged. Refresh after every DW load: scripts/refresh_olap_cubes.py
-- (task refresh_olap_cubes of weather_bronze_to_dw_dag), which also checks each cube against
-- its view. The unique indexes allow REFRESH ... CONCURRENTLY, so readers are not blocked.
-- ==============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_obs_daily AS
SELECT * FROM vw_obs_daily
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_obs_daily
    ON mv_obs_daily (source_key, location_key, date_key);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_obs_monthly AS
SELECT * FROM vw_obs_monthly
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_obs_monthly
    ON mv_obs_monthly (source_key, location_key, year, month);
