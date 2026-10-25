"""scripts/olap_queries.py.

The five OLAP operations (roll-up, drill-down, slice, dice, pivot) and a GROUP BY CUBE query
on the weather cube, as read-only SQL over the precomputed sub-cubes of
sql/06_olap_cubes.sql (`mv_obs_daily`, `mv_obs_monthly`) and the hourly view. Used by
notebooks/olap_demo.ipynb.

Aggregation rules of docs/warehouse-schema.md hold in every query: rain is summed only within
one source x one location; sources are never added together; means come from sums and
counts; rates are ratios of summed counts, so they roll up across any dimension; months that
miss rain hours are excluded where a monthly mean is shown. Seasons follow dim_date: rainy
season = local months 5-10.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pandas as pd

SEASON_SQL = "CASE WHEN month BETWEEN 5 AND 10 THEN 'rainy (5-10)' ELSE 'dry (11-4)' END"


def fetch(conn, sql: str, params: Any = None) -> pd.DataFrame:
    """Run a query and return a DataFrame; NUMERIC values become floats."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=columns)
    for col in df.columns:
        if df[col].map(lambda v: isinstance(v, Decimal)).any():
            df[col] = df[col].astype("float64")
    # Nullable dtypes keep whole numbers (counts, months) as integers next to NULLs
    return df.convert_dtypes(convert_string=False)


def roll_up(conn, source: str, location: str, year: int) -> pd.DataFrame:
    """Day -> month -> quarter -> year with GROUP BY ROLLUP: one row per month, a subtotal per
    quarter, and the year total (`level` names the row type)."""
    sql = """
        SELECT CASE WHEN GROUPING(quarter) = 1 THEN 'year'
                    WHEN GROUPING(month) = 1 THEN 'quarter' ELSE 'month' END AS level,
               year, quarter, month,
               sum(rain_mm) AS rain_mm,
               count(*) FILTER (WHERE is_rainy_day) AS rainy_days,
               count(*) FILTER (WHERE is_complete_rain_day) AS complete_days,
               round(sum(temp_sum) / sum(temp_valid_hours), 2) AS temp_mean_c
        FROM mv_obs_daily
        WHERE source_name = %s AND location_code = %s AND year = %s
        GROUP BY ROLLUP (year, quarter, month)
        HAVING GROUPING(year) = 0
        ORDER BY year, quarter NULLS LAST, month NULLS LAST
    """
    return fetch(conn, sql, (source, location, year))


def drill_down(
    conn, source: str, location: str, year: int, month: int | None = None, day: int | None = None
) -> pd.DataFrame:
    """The level below the given one: year -> months, month -> days, day -> hours."""
    if month is None:
        sql = """
            SELECT year, month, rain_mm, rainy_days, complete_rain_days, temp_mean_c
            FROM mv_obs_monthly
            WHERE source_name = %s AND location_code = %s AND year = %s
            ORDER BY month
        """
        return fetch(conn, sql, (source, location, year))
    if day is None:
        sql = """
            SELECT local_date, rain_mm, rain_valid_hours, is_rainy_day, temp_mean_c, temp_max_c
            FROM mv_obs_daily
            WHERE source_name = %s AND location_code = %s AND year = %s AND month = %s
            ORDER BY local_date
        """
        return fetch(conn, sql, (source, location, year, month))
    sql = """
        SELECT local_hour, timestamp_utc, precipitation_mm, temperature_c, humidity_percent
        FROM vw_obs_hourly
        WHERE source_name = %s AND location_code = %s AND local_date = make_date(%s, %s, %s)
        ORDER BY local_hour
    """
    return fetch(conn, sql, (source, location, year, month, day))


def slice_year(conn, year: int) -> pd.DataFrame:
    """Slice time = one year: the remaining source x location plane, one row per location and
    one column per source and measure. Sources stay in separate columns, never added."""
    sql = """
        SELECT location_code,
               sum(rain_mm) FILTER (WHERE source_name = 'Open-Meteo') AS rain_open_meteo_mm,
               sum(rain_mm) FILTER (WHERE source_name = 'ERA5') AS rain_era5_mm,
               round(sum(temp_sum) FILTER (WHERE source_name = 'Open-Meteo')
                     / sum(temp_valid_hours) FILTER (WHERE source_name = 'Open-Meteo'), 2)
                   AS temp_open_meteo_c,
               round(sum(temp_sum) FILTER (WHERE source_name = 'ERA5')
                     / sum(temp_valid_hours) FILTER (WHERE source_name = 'ERA5'), 2)
                   AS temp_era5_c,
               round(sum(temp_sum) FILTER (WHERE source_name = 'Meteostat')
                     / NULLIF(sum(temp_valid_hours) FILTER (WHERE source_name = 'Meteostat'), 0),
                     2) AS temp_meteostat_c
        FROM mv_obs_monthly
        WHERE year = %s
        GROUP BY location_code
        ORDER BY location_code
    """
    return fetch(conn, sql, (year,))


def dice(
    conn,
    source: str,
    locations: list[str],
    months: tuple[int, int],
    years: tuple[int, int],
) -> pd.DataFrame:
    """Select a sub-cube on several dimensions at once: one source, a set of locations, a range
    of months, and a range of years; one row per location x year."""
    sql = """
        SELECT location_code, year,
               sum(rain_mm) AS rain_mm,
               sum(rainy_days)::int AS rainy_days,
               sum(complete_rain_days)::int AS complete_days,
               round(100.0 * sum(rainy_days) / NULLIF(sum(complete_rain_days), 0), 1)
                   AS rainy_day_pct
        FROM mv_obs_monthly
        WHERE source_name = %s AND location_code = ANY(%s)
          AND month BETWEEN %s AND %s AND year BETWEEN %s AND %s
        GROUP BY location_code, year
        ORDER BY location_code, year
    """
    return fetch(conn, sql, (source, list(locations), *months, *years))


def pivot_monthly_rain(conn, source: str, years: tuple[int, int]) -> pd.DataFrame:
    """Mean monthly rain (mm) with locations as rows and months as columns, over complete
    months only; `months_used` counts the location-months behind each row."""
    month_cols = ",\n".join(
        f"round(avg(rain_mm) FILTER (WHERE month = {m}), 0) AS m{m:02d}" for m in range(1, 13)
    )
    sql = f"""
        SELECT location_code,
               {month_cols},
               count(*) AS months_used
        FROM mv_obs_monthly
        WHERE source_name = %s AND year BETWEEN %s AND %s AND is_complete_rain_month
        GROUP BY location_code
        ORDER BY location_code
    """
    return fetch(conn, sql, (source, *years))


def cube_rainy_days(conn, source: str, years: tuple[int, int]) -> pd.DataFrame:
    """GROUP BY source, CUBE(location, season): every combination of location and season plus
    their subtotals and the grand total for one source. Rainy-day share = rainy days / complete
    days, and mean temperature = sum / valid hours, so subtotals are exact, not averages of
    averages."""
    sql = f"""
        SELECT source_name,
               CASE WHEN GROUPING(location_code) = 1 THEN '(all 8)'
                    ELSE location_code END AS location,
               CASE WHEN GROUPING(season) = 1 THEN '(all)' ELSE season END AS season,
               sum(complete_rain_days)::int AS complete_days,
               sum(rainy_days)::int AS rainy_days,
               round(100.0 * sum(rainy_days) / NULLIF(sum(complete_rain_days), 0), 1)
                   AS rainy_day_pct,
               round(sum(temp_sum) / NULLIF(sum(temp_valid_hours), 0), 2) AS temp_mean_c
        FROM (SELECT *, {SEASON_SQL} AS season
              FROM mv_obs_monthly
              WHERE source_name = %s AND year BETWEEN %s AND %s) m
        GROUP BY source_name, CUBE (location_code, season)
        ORDER BY GROUPING(location_code), location, GROUPING(season), season
    """
    return fetch(conn, sql, (source, *years))
