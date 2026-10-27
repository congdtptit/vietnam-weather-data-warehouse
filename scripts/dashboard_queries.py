"""scripts/dashboard_queries.py.

Read-only queries and measures for the web dashboard (dashboard/app.py), which replaces the
Power BI report specified in docs/powerbi-dashboard-v1.md (decided by the user on 2026-10-27).
Each query function returns the DataFrame and the exact SQL that ran, so every page can show
the query it sends to the warehouse.

Sources: the precomputed sub-cubes of sql/06_olap_cubes.sql (`mv_obs_daily`, `mv_obs_monthly`),
`vw_obs_hourly`, `vw_calendar_month`, `fact_weather_observation` (column completeness),
`vw_rain_station_vs_model`, and `vw_prediction_vs_actual`.

Aggregation rules of docs/warehouse-schema.md, the same ones the Power BI measures followed:
- rain and temperature are shown for one source at a time; sources are never added together;
- rain over several locations or years is the mean of the location-year totals, never their sum;
- means come from sums and valid-hour counts, not from averages of averages;
- rainy and hot days count complete days only; a location-year without a complete day has an
  unknown count (NaN), and an unknown value is displayed as a dash, never as zero;
- forecast metrics come from TP/FP/TN/FN of the filtered rows, never from averaged subgroup F1.
The calendar is Vietnam local time (UTC+7); the rainy season is local months 5-10.
"""

from __future__ import annotations

import math
import time
from decimal import Decimal
from typing import Any

import numpy as np
import olap_queries
import pandas as pd

YEARS = (2016, 2025)
# Silver object written by scripts/ingest_boundaries.py (SILVER_KEY); 25 northern provinces in
# pre-2025 boundaries. Kept as a literal because that module needs shapely, which the dashboard
# environment does not install; tests/test_dashboard_queries.py checks that both stay equal.
BOUNDARY_KEY = "spatial/boundaries/northern_provinces_pre2025_v1.geojson"
SOURCES = ("Open-Meteo", "ERA5", "Meteostat")
RAINY_SEASON_MONTHS = (5, 6, 7, 8, 9, 10)
CUBE_RELATIONS = {"view": "vw_obs_monthly", "cube": "mv_obs_monthly"}
FACT_MEASURES = (
    "temperature_c",
    "humidity_percent",
    "pressure_hpa",
    "precipitation_mm",
    "wind_speed_ms",
    "wind_direction_deg",
    "cloud_cover_percent",
    "solar_radiation_wm2",
)


# --- Running queries -------------------------------------------------------------------------


def frame(columns: list[str], rows: list[tuple]) -> pd.DataFrame:
    """Rows to a DataFrame: NUMERIC becomes float, NULL becomes NaN in numeric columns."""
    df = pd.DataFrame(rows, columns=columns)
    for col in df.columns:
        values = df[col]
        if values.map(lambda v: isinstance(v, Decimal)).any():
            df[col] = values.map(lambda v: np.nan if v is None else float(v)).astype("float64")
    return df.infer_objects()


def run(conn, sql: str, params: Any = None) -> tuple[pd.DataFrame, str]:
    """Execute a read-only query; return the rows and the SQL exactly as sent."""
    with conn.cursor() as cur:
        shown = cur.mogrify(sql, params).decode()
        cur.execute(sql, params)
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return frame(columns, rows), shown


class _RecordingCursor:
    """Cursor stand-in that keeps the SQL it sends, for the olap_queries functions."""

    def __init__(self, cursor, log: list[str]):
        self._cursor, self._log = cursor, log

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self._cursor.close()

    def execute(self, sql: str, params: Any = None) -> None:
        self._log.append(self._cursor.mogrify(sql, params).decode())
        self._cursor.execute(sql, params)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


class _RecordingConnection:
    def __init__(self, conn):
        self._conn, self.sql = conn, []

    def cursor(self) -> _RecordingCursor:
        return _RecordingCursor(self._conn.cursor(), self.sql)


def olap(conn, name: str, *args) -> tuple[pd.DataFrame, str]:
    """Run an olap_queries function (roll_up, cube_rainy_days, ...) and keep its SQL."""
    recorder = _RecordingConnection(conn)
    df = getattr(olap_queries, name)(recorder, *args)
    return df, recorder.sql[-1]


def locations(conn) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT location_code, location_name, province, latitude, longitude, elevation_m
        FROM dim_location
        ORDER BY location_key
    """
    return run(conn, sql)


def calendar_hours(conn, years: tuple[int, int], months: list[int]) -> tuple[pd.DataFrame, str]:
    """Expected hours of one location over the selected local years and months."""
    sql = """
        SELECT coalesce(sum(days_in_month), 0) * 24 AS hours
        FROM vw_calendar_month
        WHERE year BETWEEN %(year_from)s AND %(year_to)s AND month = ANY(%(months)s)
    """
    return run(conn, sql, {"year_from": years[0], "year_to": years[1], "months": list(months)})


def monthly_rows(
    conn, source: str, location_codes: list[str], years: tuple[int, int], months: list[int]
) -> tuple[pd.DataFrame, str]:
    """The monthly sub-cube for one source, filtered on location, year, and month (a dice)."""
    sql = """
        SELECT location_code, location_name, year, month, days_in_month, record_hours,
               rain_mm, rain_valid_hours, temp_sum, temp_valid_hours, temp_min_c, temp_max_c,
               complete_rain_days, rainy_days, complete_temp_days, hot_days
        FROM mv_obs_monthly
        WHERE source_name = %(source)s
          AND location_code = ANY(%(locations)s)
          AND year BETWEEN %(year_from)s AND %(year_to)s
          AND month = ANY(%(months)s)
        ORDER BY location_code, year, month
    """
    params = {
        "source": source,
        "locations": list(location_codes),
        "year_from": years[0],
        "year_to": years[1],
        "months": list(months),
    }
    return run(conn, sql, params)


def trend_years(conn, source: str, location: str) -> tuple[pd.DataFrame, str]:
    """Top of the time hierarchy: one row per local year (roll-up of the monthly cube)."""
    sql = """
        SELECT year,
               sum(rain_mm) AS rain_mm,
               sum(rain_valid_hours) AS rain_valid_hours,
               round(sum(temp_sum) / NULLIF(sum(temp_valid_hours), 0), 2) AS temp_mean_c,
               CASE WHEN sum(complete_rain_days) > 0 THEN coalesce(sum(rainy_days), 0) END
                   AS rainy_days,
               CASE WHEN sum(complete_temp_days) > 0 THEN coalesce(sum(hot_days), 0) END
                   AS hot_days
        FROM mv_obs_monthly
        WHERE source_name = %(source)s AND location_code = %(location)s
          AND year BETWEEN %(year_from)s AND %(year_to)s
        GROUP BY year
        ORDER BY year
    """
    params = {"source": source, "location": location, "year_from": YEARS[0], "year_to": YEARS[1]}
    return run(conn, sql, params)


def trend_months(conn, source: str, location: str, year: int) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT month, rain_mm, rain_valid_hours, temp_mean_c, temp_min_c, temp_max_c,
               rainy_days, hot_days, complete_rain_days
        FROM mv_obs_monthly
        WHERE source_name = %(source)s AND location_code = %(location)s AND year = %(year)s
        ORDER BY month
    """
    return run(conn, sql, {"source": source, "location": location, "year": year})


def trend_days(conn, source: str, location: str, year: int, month: int) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT local_date, day_of_month, rain_mm, rain_valid_hours, temp_mean_c, temp_min_c,
               temp_max_c, is_rainy_day, is_hot_day
        FROM mv_obs_daily
        WHERE source_name = %(source)s AND location_code = %(location)s
          AND year = %(year)s AND month = %(month)s
        ORDER BY local_date
    """
    params = {"source": source, "location": location, "year": year, "month": month}
    return run(conn, sql, params)


def trend_hours(conn, source: str, location: str, day: Any) -> tuple[pd.DataFrame, str]:
    """Bottom of the hierarchy: the hours of one local day (rain stamped at interval end)."""
    sql = """
        SELECT local_hour, timestamp_utc, precipitation_mm, temperature_c, humidity_percent
        FROM vw_obs_hourly
        WHERE source_name = %(source)s AND location_code = %(location)s
          AND local_date = %(day)s
        ORDER BY local_hour
    """
    return run(conn, sql, {"source": source, "location": location, "day": day})


def hour_profile(
    conn, source: str, location: str, years: tuple[int, int]
) -> tuple[pd.DataFrame, str]:
    """Rain summed and temperature averaged by local hour of day over the selected years."""
    sql = """
        SELECT local_hour,
               sum(precipitation_mm) AS rain_mm,
               count(precipitation_mm) AS rain_valid_hours,
               round(avg(temperature_c), 2) AS temp_mean_c
        FROM vw_obs_hourly
        WHERE source_name = %(source)s AND location_code = %(location)s
          AND year BETWEEN %(year_from)s AND %(year_to)s
        GROUP BY local_hour
        ORDER BY local_hour
    """
    params = {"source": source, "location": location, "year_from": years[0], "year_to": years[1]}
    return run(conn, sql, params)


def source_months(conn, location: str, year: int) -> tuple[pd.DataFrame, str]:
    """All sources side by side at one location and year (a slice; sources stay separate)."""
    sql = """
        SELECT source_name, month, days_in_month, record_hours, rain_mm, rain_valid_hours,
               temp_sum, temp_valid_hours, temp_mean_c, complete_rain_days, rainy_days,
               complete_temp_days, hot_days
        FROM mv_obs_monthly
        WHERE location_code = %(location)s AND year = %(year)s
        ORDER BY source_name, month
    """
    return run(conn, sql, {"location": location, "year": year})


def source_years(conn, location: str) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT source_name, year,
               sum(rain_mm) AS rain_mm,
               round(sum(temp_sum) / NULLIF(sum(temp_valid_hours), 0), 2) AS temp_mean_c
        FROM mv_obs_monthly
        WHERE location_code = %(location)s AND year BETWEEN %(year_from)s AND %(year_to)s
        GROUP BY source_name, year
        ORDER BY source_name, year
    """
    return run(conn, sql, {"location": location, "year_from": YEARS[0], "year_to": YEARS[1]})


def coverage(conn) -> tuple[pd.DataFrame, str]:
    """Present hours over expected local-year hours per source x location x year, as in
    vw_coverage but read from the monthly cube."""
    sql = """
        SELECT m.source_name, m.location_code, m.year,
               c.days_in_year * 24 AS expected_hours,
               sum(m.record_hours) AS record_hours,
               sum(m.rain_valid_hours) AS rain_valid_hours,
               sum(m.temp_valid_hours) AS temp_valid_hours
        FROM mv_obs_monthly m
        JOIN (SELECT year, sum(days_in_month) AS days_in_year
              FROM vw_calendar_month GROUP BY year) c ON c.year = m.year
        WHERE m.year BETWEEN %(year_from)s AND %(year_to)s
        GROUP BY m.source_name, m.location_code, m.year, c.days_in_year
        ORDER BY m.source_name, m.location_code, m.year
    """
    return run(conn, sql, {"year_from": YEARS[0], "year_to": YEARS[1]})


def column_completeness(conn) -> tuple[pd.DataFrame, str]:
    """Rows and non-null values of every measure column of the hourly fact, per source."""
    counts = ",\n               ".join(f"count(f.{c}) AS {c}" for c in FACT_MEASURES)
    sql = f"""
        SELECT s.source_name, count(*) AS rows,
               {counts}
        FROM fact_weather_observation f
        JOIN dim_source s ON s.source_key = f.source_key
        GROUP BY s.source_name
        ORDER BY s.source_name
    """
    return run(conn, sql)


def station_rows(conn, years: tuple[int, int]) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT location_code, location_name, station_id, station_name, station_role,
               rain_day_utc, year, month, station_rain_mm, station_is_rainy,
               model_valid_hours, model_rain_mm, model_is_rainy
        FROM vw_rain_station_vs_model
        WHERE year BETWEEN %(year_from)s AND %(year_to)s
        ORDER BY location_code, rain_day_utc
    """
    return run(conn, sql, {"year_from": years[0], "year_to": years[1]})


def prediction_rows(conn) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT model_name, model_version, evaluation_split, location_code, location_name,
               origin_local, extract(MONTH FROM origin_local)::INT AS month,
               rain_probability, probability_threshold, predicted_rain,
               actual_valid_hours, actual_rain_mm, actual_rain, outcome
        FROM vw_prediction_vs_actual
        WHERE evaluation_split = 'test'
        ORDER BY origin_local, location_code
    """
    return run(conn, sql)


def cube_sizes(conn) -> tuple[pd.DataFrame, str]:
    sql = """
        SELECT 'mv_obs_daily' AS cube, 'nguồn × điểm × ngày' AS grain, count(*) AS rows
        FROM mv_obs_daily
        UNION ALL
        SELECT 'mv_obs_monthly', 'nguồn × điểm × tháng', count(*) FROM mv_obs_monthly
    """
    return run(conn, sql)


def timed_monthly_query(conn, relation: str, source: str, location: str, year: int):
    """The same monthly question on the view or on the cube (relation 'view' or 'cube');
    returns the rows, the SQL, and the elapsed milliseconds."""
    table = CUBE_RELATIONS[relation]  # whitelisted identifier, never user text
    sql = f"""
        SELECT month, rain_mm, rainy_days, temp_mean_c
        FROM {table}
        WHERE source_name = %(source)s AND location_code = %(location)s AND year = %(year)s
        ORDER BY month
    """
    start = time.perf_counter()
    df, shown = run(conn, sql, {"source": source, "location": location, "year": year})
    return df, shown, (time.perf_counter() - start) * 1000


# --- Measures ----------------------------------------------------------------------------------


def _sum_or_nan(values: pd.Series) -> float:
    return float(values.sum(min_count=1))


def location_year_totals(monthly: pd.DataFrame) -> pd.DataFrame:
    """One row per location x year. Day counts are NaN when the year has no complete day."""
    agg = monthly.groupby(["location_code", "year"]).agg(
        rain_mm=("rain_mm", _sum_or_nan),
        rain_valid_hours=("rain_valid_hours", "sum"),
        temp_sum=("temp_sum", _sum_or_nan),
        temp_valid_hours=("temp_valid_hours", "sum"),
        temp_min_c=("temp_min_c", "min"),
        temp_max_c=("temp_max_c", "max"),
        complete_rain_days=("complete_rain_days", "sum"),
        rainy_days=("rainy_days", "sum"),
        complete_temp_days=("complete_temp_days", "sum"),
        hot_days=("hot_days", "sum"),
        record_hours=("record_hours", "sum"),
    )
    agg["rainy_days"] = agg["rainy_days"].where(agg["complete_rain_days"] > 0)
    agg["hot_days"] = agg["hot_days"].where(agg["complete_temp_days"] > 0)
    return agg.reset_index()


def _ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else math.nan


def summary(monthly: pd.DataFrame, n_locations: int, hours_per_location: float) -> dict:
    """Headline measures for one source over the selected locations, years, and months.

    Rain and day counts are means per location per year; temperature is the mean of all valid
    hours; coverage divides by the expected hours of every selected location (the calendar
    hours of the selected years and months), including locations without rows."""
    if monthly.empty:
        return {
            k: math.nan
            for k in (
                "rain_mm",
                "temp_mean_c",
                "temp_min_c",
                "temp_max_c",
                "rainy_days",
                "hot_days",
                "rain_coverage",
                "temp_coverage",
            )
        }
    ly = location_year_totals(monthly)
    expected = hours_per_location * n_locations
    return {
        "rain_mm": ly["rain_mm"].mean(),
        "temp_mean_c": _ratio(monthly["temp_sum"].sum(), monthly["temp_valid_hours"].sum()),
        "temp_min_c": monthly["temp_min_c"].min(),
        "temp_max_c": monthly["temp_max_c"].max(),
        "rainy_days": ly["rainy_days"].mean(),
        "hot_days": ly["hot_days"].mean(),
        "rain_coverage": _ratio(monthly["rain_valid_hours"].sum(), expected),
        "temp_coverage": _ratio(monthly["temp_valid_hours"].sum(), expected),
    }


def by_location(monthly: pd.DataFrame, hours_per_location: float) -> pd.DataFrame:
    """Per-location means per year (rain, rainy/hot days), temperature, and rain coverage."""
    ly = location_year_totals(monthly)
    grouped = ly.groupby("location_code")
    out = pd.DataFrame(
        {
            "rain_mm": grouped["rain_mm"].mean(),
            "temp_mean_c": grouped["temp_sum"].sum() / grouped["temp_valid_hours"].sum(),
            "temp_min_c": grouped["temp_min_c"].min(),
            "temp_max_c": grouped["temp_max_c"].max(),
            "rainy_days": grouped["rainy_days"].mean(),
            "hot_days": grouped["hot_days"].mean(),
            "rain_coverage": grouped["rain_valid_hours"].sum() / hours_per_location,
        }
    )
    out.loc[grouped["temp_valid_hours"].sum() == 0, "temp_mean_c"] = math.nan
    return out.reset_index()


def by_month(monthly: pd.DataFrame) -> pd.DataFrame:
    """Per local month: mean monthly rain per location-year and the mean temperature."""
    grouped = monthly.groupby("month")
    out = pd.DataFrame(
        {
            "rain_mm": grouped["rain_mm"].mean(),
            "temp_mean_c": grouped["temp_sum"].sum() / grouped["temp_valid_hours"].sum(),
        }
    )
    return out.reset_index()


def month_pivot(monthly: pd.DataFrame) -> pd.DataFrame:
    """Locations x months of mean monthly rain per year, with a row total ('Cả kỳ', the mean
    total of the selected months per year) and a column mean over locations ('TB các điểm')."""
    cell = monthly.groupby(["location_code", "month"])["rain_mm"].mean().unstack("month")
    cell = cell.reindex(columns=sorted(cell.columns))
    cell.columns = [f"T{m}" for m in cell.columns]
    cell["Cả kỳ"] = cell.sum(axis=1, min_count=1)
    cell.loc["TB các điểm"] = cell.mean(axis=0)
    return cell


def season_days(monthly: pd.DataFrame) -> pd.DataFrame:
    """Rainy and hot days per location per year, split by season (rainy = months 5-10)."""
    df = monthly.assign(
        season=np.where(
            monthly["month"].isin(RAINY_SEASON_MONTHS), "Mùa mưa (5–10)", "Mùa khô (11–4)"
        )
    )
    agg = df.groupby(["location_code", "season", "year"]).agg(
        complete_rain_days=("complete_rain_days", "sum"),
        rainy_days=("rainy_days", "sum"),
        complete_temp_days=("complete_temp_days", "sum"),
        hot_days=("hot_days", "sum"),
        rain_mm=("rain_mm", _sum_or_nan),
    )
    agg["rainy_days"] = agg["rainy_days"].where(agg["complete_rain_days"] > 0)
    agg["hot_days"] = agg["hot_days"].where(agg["complete_temp_days"] > 0)
    return (
        agg.groupby(["location_code", "season"])[["rain_mm", "rainy_days", "hot_days"]]
        .mean()
        .reset_index()
    )


def coverage_pct(cov: pd.DataFrame) -> pd.DataFrame:
    out = cov.copy()
    for name, col in (
        ("record", "record_hours"),
        ("rain", "rain_valid_hours"),
        ("temp", "temp_valid_hours"),
    ):
        out[f"{name}_pct"] = 100.0 * out[col] / out["expected_hours"]
    return out


def column_missing_pct(counts: pd.DataFrame) -> pd.DataFrame:
    """Share of rows with an empty value, per source and measure column (percent)."""
    out = counts[["source_name", "rows"]].copy()
    for col in FACT_MEASURES:
        out[col] = 100.0 * (1 - counts[col] / counts["rows"])
    return out


def station_summary(stations: pd.DataFrame) -> pd.DataFrame:
    """Per location and station: station rain, model rain on complete model days, rainy days."""
    grouped = stations.groupby(
        ["location_code", "location_name", "station_id", "station_name", "station_role"]
    )
    out = grouped.agg(
        compared_days=("station_rain_mm", "size"),
        station_rain_mm=("station_rain_mm", _sum_or_nan),
        model_rain_mm=("model_rain_mm", _sum_or_nan),
        station_rainy_days=("station_is_rainy", lambda s: int(s.fillna(False).astype(bool).sum())),
        model_rainy_days=("model_is_rainy", lambda s: int(s.fillna(False).astype(bool).sum())),
    )
    return out.reset_index()


def classification_counts(df: pd.DataFrame, threshold: float | None = None) -> dict[str, int]:
    """TP/FP/TN/FN over forecasts whose 24-hour actual is complete. With no threshold (or the
    model's own) the stored classes are used; another threshold reclassifies the stored
    probabilities, which is an exploration, not the evaluated result."""
    known = df[df["actual_rain"].notna()]
    actual = known["actual_rain"].astype(bool).to_numpy()
    stored = known["probability_threshold"].iloc[0] if len(known) else None
    if threshold is None or (stored is not None and abs(threshold - stored) < 1e-9):
        predicted = known["predicted_rain"].astype(bool).to_numpy()
    else:
        predicted = (known["rain_probability"] >= threshold).to_numpy()
    return {
        "tp": int((predicted & actual).sum()),
        "fp": int((predicted & ~actual).sum()),
        "tn": int((~predicted & ~actual).sum()),
        "fn": int((~predicted & actual).sum()),
    }


def classification_metrics(counts: dict[str, int]) -> dict[str, float]:
    """Accuracy, Precision, Recall, F1 and rates from counts; NaN when undefined."""
    tp, fp, tn, fn = counts["tp"], counts["fp"], counts["tn"], counts["fn"]
    n = tp + fp + tn + fn
    return {
        "n": n,
        "accuracy": _ratio(tp + tn, n),
        "precision": _ratio(tp, tp + fp),
        "recall": _ratio(tp, tp + fn),
        "f1": _ratio(2 * tp, 2 * tp + fp + fn),
        "actual_rate": _ratio(tp + fn, n),
        "predicted_rate": _ratio(tp + fp, n),
    }


def metrics_by(df: pd.DataFrame, key: str, threshold: float | None = None) -> pd.DataFrame:
    """Metrics recomputed from the counts of each group (no averaging of subgroup F1)."""
    rows = []
    for value, group in df.groupby(key):
        counts = classification_counts(group, threshold)
        rows.append({key: value, **counts, **classification_metrics(counts)})
    return pd.DataFrame(rows)


def season_of_month(month: int) -> str:
    return "Mùa mưa (5–10)" if month in RAINY_SEASON_MONTHS else "Mùa khô (11–4)"


def boundary_lines(geojson: dict) -> tuple[list, list]:
    """Province outlines as one lon/lat polyline per ring, separated by None, for a plain
    (tile-free, offline) plot. Polygon and MultiPolygon geometries are supported."""
    lons: list = []
    lats: list = []
    for feature in geojson["features"]:
        geometry = feature["geometry"]
        polygons = (
            [geometry["coordinates"]] if geometry["type"] == "Polygon" else geometry["coordinates"]
        )
        for polygon in polygons:
            for ring in polygon:
                lons.extend(point[0] for point in ring)
                lats.extend(point[1] for point in ring)
                lons.append(None)
                lats.append(None)
    return lons, lats


# --- Display -----------------------------------------------------------------------------------


def vn_number(value: Any, decimals: int = 1, unit: str = "") -> str:
    """Vietnamese number format (thousands '.', decimals ','); unknown values become a dash."""
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return "—"
    text = f"{float(value):,.{decimals}f}".replace(",", "\0").replace(".", ",").replace("\0", ".")
    return f"{text} {unit}" if unit else text
