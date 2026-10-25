"""Integration tests for the OLAP views in sql/02_olap_views.sql (workbook ID 19).

Synthetic rows only. Everything runs in one transaction that is rolled back, so the shared
test database keeps the schema that other integration tests expect.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[1]
VN = timezone(timedelta(hours=7))


def _seed_march_2023_dates(cur):
    cur.execute(
        """
        INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month,
                              month, month_name, quarter, year, is_weekend, is_rainy_season)
        SELECT to_char(d, 'YYYYMMDD')::int, d::date, extract(isodow FROM d)::int,
               to_char(d, 'FMDay'), extract(day FROM d)::int, 3, 'March', 1, 2023,
               extract(isodow FROM d) IN (6, 7), false
        FROM generate_series('2023-03-01'::date, '2023-03-31'::date, interval '1 day') AS d
        ON CONFLICT (date_key) DO NOTHING;
        """
    )


def _insert_local_day(cur, source, day, hours, rain, temp, wind_dir=None, wind_speed=None):
    """Insert one row per local hour of `day` for HAN; values are callables of the hour."""
    cur.execute("SELECT source_key FROM dim_source WHERE source_name = %s", (source,))
    source_key = cur.fetchone()[0]
    cur.execute("SELECT location_key FROM dim_location WHERE location_code = 'HAN'")
    location_key = cur.fetchone()[0]
    for h in hours:
        local = datetime(2023, 3, day, h, tzinfo=VN)
        cur.execute(
            """
            INSERT INTO fact_weather_observation (
                date_key, time_key, location_key, source_key, timestamp_utc, timestamp_local,
                temperature_c, precipitation_mm, wind_speed_ms, wind_direction_deg)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(local.strftime("%Y%m%d")),
                h * 100,
                location_key,
                source_key,
                local.astimezone(timezone.utc),
                local.replace(tzinfo=None),
                temp(h),
                rain(h),
                wind_speed(h) if wind_speed else None,
                wind_dir(h) if wind_dir else None,
            ),
        )


@pytest.fixture
def olap_cursor(pg_connection):
    cur = pg_connection.cursor()
    try:
        cur.execute((ROOT / "sql/02_olap_views.sql").read_text())
        cur.execute((ROOT / "sql/02_olap_views.sql").read_text())  # idempotent rerun
        _seed_march_2023_dates(cur)
        # Day 1: complete, 24 x 0.05 mm = 1.2 mm (rainy), one hour at 36 degC (hot),
        # wind alternating 350/10 degrees so the vector mean is north, not 180.
        _insert_local_day(
            cur,
            "Open-Meteo",
            1,
            range(24),
            rain=lambda h: 0.05,
            temp=lambda h: 36.0 if h == 14 else 30.0,
            wind_dir=lambda h: 350.0 if h % 2 else 10.0,
            wind_speed=lambda h: 2.0,
        )
        # Day 2: 23 hours only, 23 x 0.2 = 4.6 mm: incomplete, so the rainy flag is unknown.
        _insert_local_day(cur, "Open-Meteo", 2, range(23), rain=lambda h: 0.2, temp=lambda h: 25.0)
        # Meteostat day 1: temperature only, rain missing for every hour.
        _insert_local_day(cur, "Meteostat", 1, range(24), rain=lambda h: None, temp=lambda h: 31.0)
        yield cur
    finally:
        pg_connection.rollback()
        cur.close()


def _one(cur, sql, params=()):
    cur.execute(sql, params)
    rows = cur.fetchall()
    assert len(rows) == 1, rows
    return rows[0]


def test_daily_flags_completeness_and_vector_wind(olap_cursor):
    cur = olap_cursor
    rain, valid, complete, rainy, hot, temp_max, wdir = _one(
        cur,
        """SELECT rain_mm, rain_valid_hours, is_complete_rain_day, is_rainy_day, is_hot_day,
                  temp_max_c, wind_dir_vector_mean_deg
           FROM vw_obs_daily WHERE source_name='Open-Meteo' AND location_code='HAN' AND date_key=20230301""",
    )
    assert float(rain) == pytest.approx(1.2)
    assert valid == 24 and complete is True
    assert rainy is True and hot is True and float(temp_max) == 36.0
    assert float(wdir) < 1.0 or float(wdir) >= 359.0  # north, not the linear mean 180

    rain2, complete2, rainy2 = _one(
        cur,
        """SELECT rain_mm, is_complete_rain_day, is_rainy_day FROM vw_obs_daily
           WHERE source_name='Open-Meteo' AND location_code='HAN' AND date_key=20230302""",
    )
    assert float(rain2) == pytest.approx(4.6)
    assert complete2 is False and rainy2 is None

    ms_rain, ms_valid, ms_rainy, ms_hot = _one(
        cur,
        """SELECT rain_mm, rain_valid_hours, is_rainy_day, is_hot_day FROM vw_obs_daily
           WHERE source_name='Meteostat' AND location_code='HAN' AND date_key=20230301""",
    )
    assert ms_rain is None and ms_valid == 0 and ms_rainy is None and ms_hot is False


def test_monthly_and_yearly_roll_up_from_hourly(olap_cursor):
    cur = olap_cursor
    hourly_rain, hourly_n = _one(
        cur,
        """SELECT sum(precipitation_mm), count(*) FROM vw_obs_hourly
           WHERE source_name='Open-Meteo' AND location_code='HAN' AND year=2023 AND month=3""",
    )
    m = _one(
        cur,
        """SELECT rain_mm, record_hours, expected_hours, days_in_month, is_complete_rain_month,
                  complete_rain_days, rainy_days, hot_days, temp_mean_c
           FROM vw_obs_monthly WHERE source_name='Open-Meteo' AND location_code='HAN' AND year_month=202303""",
    )
    rain, n, expected, days, complete_month, complete_days, rainy, hot, temp_mean = m
    assert rain == hourly_rain and n == hourly_n == 47
    assert days == 31 and expected == 31 * 24 and complete_month is False
    assert complete_days == 1 and rainy == 1 and hot == 1
    # Mean from sums and counts: (23 x 30 + 36 + 23 x 25) / 47, not the mean of daily means
    assert float(temp_mean) == pytest.approx((23 * 30 + 36 + 23 * 25) / 47, abs=0.01)

    # The yearly row must equal the sum of its monthly rows (other tests may add months)
    y_rain, y_n, m_rain, m_n = _one(
        cur,
        """SELECT y.rain_mm, y.record_hours, sum(m.rain_mm), sum(m.record_hours)
           FROM vw_obs_yearly y
           JOIN vw_obs_monthly m USING (source_name, location_code, year)
           WHERE y.source_name='Open-Meteo' AND y.location_code='HAN' AND y.year=2023
           GROUP BY y.rain_mm, y.record_hours""",
    )
    assert y_rain == m_rain and y_n == m_n


def test_missing_rain_is_unknown_not_zero_and_sources_stay_separate(olap_cursor):
    cur = olap_cursor
    ms = _one(
        cur,
        """SELECT rain_mm, rainy_days, hot_days FROM vw_obs_monthly
           WHERE source_name='Meteostat' AND location_code='HAN' AND year_month=202303""",
    )
    assert ms[0] is None and ms[1] is None  # no complete rain day: unknown, not 0
    assert ms[2] == 0  # temperature days are complete and none reach 35 degC

    cur.execute(
        """SELECT source_name, count(*) FROM vw_obs_monthly
           WHERE year_month=202303 AND location_code='HAN' GROUP BY 1"""
    )
    assert dict(cur.fetchall()) == {"Open-Meteo": 1, "Meteostat": 1}

    cov = _one(
        cur,
        """SELECT record_coverage_pct, rain_coverage_pct, temp_coverage_pct FROM vw_coverage
           WHERE source_name='Meteostat' AND location_code='HAN' AND year=2023""",
    )
    assert float(cov[1]) == 0.0 and float(cov[0]) == float(cov[2]) > 0


def test_precomputed_cubes_equal_their_views_after_refresh(olap_cursor):
    from refresh_olap_cubes import CUBES, compare

    cur = olap_cursor
    cur.execute((ROOT / "sql/06_olap_cubes.sql").read_text())
    cur.execute((ROOT / "sql/06_olap_cubes.sql").read_text())  # idempotent rerun
    for cube in CUBES:
        cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {cube}")
    for cube, view in CUBES.items():
        result = compare(cur, cube, view)
        assert result["only_in_view"] == result["only_in_cube"] == 0, (cube, result)

    # A new load makes the cubes stale until they are refreshed
    _insert_local_day(cur, "Open-Meteo", 3, range(24), rain=lambda h: 0.1, temp=lambda h: 20.0)
    assert compare(cur, "mv_obs_daily", "vw_obs_daily")["only_in_view"] == 1
    assert compare(cur, "mv_obs_monthly", "vw_obs_monthly")["only_in_view"] == 1
    for cube in CUBES:
        cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {cube}")
    for cube, view in CUBES.items():
        result = compare(cur, cube, view)
        assert result["only_in_view"] == result["only_in_cube"] == 0, (cube, result)

    rain, rainy = _one(
        cur,
        """SELECT rain_mm, rainy_days FROM mv_obs_monthly
           WHERE source_name='Open-Meteo' AND location_code='HAN' AND year_month=202303""",
    )
    assert float(rain) == pytest.approx(1.2 + 4.6 + 2.4) and rainy == 2


def test_olap_operations_on_the_cubes(olap_cursor, pg_connection):
    import olap_queries as oq

    cur = olap_cursor
    cur.execute((ROOT / "sql/06_olap_cubes.sql").read_text())
    for cube in ("mv_obs_daily", "mv_obs_monthly"):
        cur.execute(f"REFRESH MATERIALIZED VIEW {cube}")

    # Roll-up: the quarter subtotal and the year total equal the month row (only March exists)
    r = oq.roll_up(pg_connection, "Open-Meteo", "HAN", 2023).set_index("level")
    assert list(r.index) == ["month", "quarter", "year"]
    assert r["rain_mm"].nunique() == 1 and r.loc["year", "rain_mm"] == pytest.approx(5.8)
    assert r.loc["year", "rainy_days"] == 1 and r.loc["year", "complete_days"] == 1

    # Drill-down: month -> two days -> 24 hours of day 1
    days = oq.drill_down(pg_connection, "Open-Meteo", "HAN", 2023, 3)
    assert len(days) == 2 and list(days["rain_valid_hours"]) == [24, 23]
    hours = oq.drill_down(pg_connection, "Open-Meteo", "HAN", 2023, 3, 1)
    assert len(hours) == 24 and hours["precipitation_mm"].sum() == pytest.approx(1.2)

    # Slice on the year keeps sources in separate columns: Meteostat rain is not added
    s = oq.slice_year(pg_connection, 2023).set_index("location_code")
    assert s.loc["HAN", "rain_open_meteo_mm"] == pytest.approx(5.8)
    assert s.loc["HAN", "temp_meteostat_c"] == pytest.approx(31.0)

    # The month is incomplete, so the pivot of monthly means leaves it out
    assert oq.pivot_monthly_rain(pg_connection, "Open-Meteo", (2023, 2023)).empty

    # CUBE: one location x one season, the two subtotals, and the grand total
    c = oq.cube_rainy_days(pg_connection, "Open-Meteo", (2023, 2023))
    assert len(c) == 4 and set(c["location"]) == {"HAN", "(all 8)"}
    assert set(c["season"]) == {"dry (11-4)", "(all)"}
    assert (c["rainy_day_pct"] == 100.0).all() and (c["complete_days"] == 1).all()
