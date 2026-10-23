#!/usr/bin/env python3
"""scripts/build_ml_pivot.py.

Pivot the Gold hourly observations to one row per location x UTC hour, with one column per
source and variable (workbook ID 16). This is the Gold ML exchange product for multi-source
experiments. CART v1 and its feature datasets (IDs 23, 28) use Open-Meteo only and are not
changed by it.

Source policy (data contract, "Gold observations preserve the multi-source grain"):
- Columns are named <variable>__<source tag>. Values are never combined across sources:
  no fusion, no averaging, and no rain summed across sources.
- Pressure keeps its source meaning. Open-Meteo and ERA5 give surface pressure (ERA5 at its
  grid cell's terrain height); Meteostat gives sea-level pressure (mostly METAR QNH). These
  values are not comparable; the manifest records the pressure type of each column.
- A missing value stays missing: an hour without a row for a source has nulls in its columns.

Input: Gold `fact_weather_observation/year=YYYY/month=MM/fact_weather_observation_<YYYYMM>.parquet`
(the DW load product, reconciled with the DW by scripts/reconcile_dw.py).

Checks before anything is written (the run fails otherwise):
- the pivot key (location_code, timestamp_utc) is unique;
- for every source, the rows carrying any of its values equal the Gold rows of that source;
- for every source and variable, the non-null count and the sum equal those of the Gold
  long table;
- rows per source and year equal the DW (fact_weather_observation), read-only.

Output (bucket gold), written once unless --overwrite:
  ml_pivot/weather_hourly_by_source_<version>/year=YYYY/weather_hourly_by_source_<YYYY>.parquet
  ml_pivot/weather_hourly_by_source_<version>.json   manifest: grain, columns, units, source
                                                     policy, counts, lineage (Gold SHA-256)
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from export_rain_labels import refuse_overwrite
from load_gold_observations import BUCKET_GOLD, get_db_connection, get_minio_client
from minio import Minio
from train_cart import _get

PIVOT_VERSION = "v1"
SOURCES = {"Open-Meteo": "open_meteo", "ERA5": "era5", "Meteostat": "meteostat"}
VARIABLES = {
    "temperature_c": "degC",
    "humidity_percent": "%",
    "pressure_hpa": "hPa (see pressure_type per source)",
    "precipitation_mm": "mm over the hour ending at the timestamp",
    "wind_speed_ms": "m/s",
    "wind_direction_deg": "degrees, direction the wind blows from",
    "cloud_cover_percent": "%",
    "solar_radiation_wm2": "W/m2",
}
KEY = ["location_code", "timestamp_utc"]
CALENDAR = ["date_key", "time_key"]  # Vietnam local calendar keys, as in the DW


def gold_key(year: int, month: int) -> str:
    return (
        f"fact_weather_observation/year={year}/month={month:02d}/"
        f"fact_weather_observation_{year}{month:02d}.parquet"
    )


def pivot_keys(year: int, version: str = PIVOT_VERSION) -> tuple[str, str]:
    base = f"ml_pivot/weather_hourly_by_source_{version}"
    return f"{base}/year={year}/weather_hourly_by_source_{year}.parquet", f"{base}.json"


def column(variable: str, source: str) -> str:
    return f"{variable}__{SOURCES[source]}"


def pivot(long: pd.DataFrame) -> pd.DataFrame:
    """Wide table: one row per location x UTC hour, one column per source x variable."""
    unknown = sorted(set(long["source_name"]) - set(SOURCES))
    if unknown:
        raise ValueError(f"sources without a pivot policy: {unknown}")
    if long.duplicated(["source_name", *KEY]).any():
        raise ValueError("Gold has duplicate (source, location, hour) rows")
    calendar = long.drop_duplicates(KEY).set_index(KEY)[CALENDAR]
    wide = long.pivot(index=KEY, columns="source_name", values=list(VARIABLES))
    wide.columns = [column(v, s) for v, s in wide.columns]
    for source in SOURCES:  # a source absent from the window still gets its columns
        for variable in VARIABLES:
            if column(variable, source) not in wide.columns:
                wide[column(variable, source)] = np.nan
    ordered = [column(v, s) for s in SOURCES for v in VARIABLES]
    wide = calendar.join(wide[ordered]).reset_index()
    return wide.sort_values(KEY).reset_index(drop=True)


def reconcile(long: pd.DataFrame, wide: pd.DataFrame) -> dict[str, Any]:
    """Counts and sums of the pivot against the long Gold table; raise on any difference."""
    if wide.duplicated(KEY).any():
        raise ValueError("pivot key (location_code, timestamp_utc) is not unique")
    if len(wide) != len(long.drop_duplicates(KEY)):
        raise ValueError("pivot rows differ from the distinct Gold keys")
    out: dict[str, Any] = {}
    for source in SOURCES:
        g = long[long["source_name"] == source]
        cols = [column(v, source) for v in VARIABLES]
        rows = int(wide[cols].notna().any(axis=1).sum())
        # A Gold row with every variable null cannot be seen in the pivot; count those apart
        empty = int(g[list(VARIABLES)].isna().all(axis=1).sum())
        if rows != len(g) - empty:
            raise ValueError(f"{source}: {rows} pivot rows with values, Gold has {len(g) - empty}")
        per_variable = {}
        for variable in VARIABLES:
            col = column(variable, source)
            n_long, n_wide = int(g[variable].notna().sum()), int(wide[col].notna().sum())
            s_long, s_wide = float(g[variable].sum()), float(wide[col].sum())
            if n_long != n_wide or not np.isclose(s_long, s_wide, rtol=0, atol=1e-6):
                raise ValueError(
                    f"{col}: pivot {n_wide} values / {s_wide}, Gold {n_long} / {s_long}"
                )
            per_variable[variable] = {"non_null": n_wide, "sum": round(s_wide, 3)}
        out[source] = {
            "gold_rows": len(g),
            "gold_rows_without_values": empty,
            "pivot_rows_with_values": rows,
            "variables": per_variable,
        }
    return out


def read_gold_year(client: Minio, year: int) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    frames, lineage = [], []
    for month in range(1, 13):
        key = gold_key(year, month)
        body = _get(client, key)
        frames.append(pd.read_parquet(io.BytesIO(body)))
        lineage.append({"key": f"{BUCKET_GOLD}/{key}", "sha256": hashlib.sha256(body).hexdigest()})
    long = pd.concat(frames, ignore_index=True)
    long["timestamp_utc"] = pd.to_datetime(long["timestamp_utc"], utc=True)
    return long, lineage


def dw_counts(conn, start_year: int, end_year: int) -> dict[tuple[str, int], int]:
    """Rows per source and UTC year in fact_weather_observation."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.source_name, extract(year FROM f.timestamp_utc AT TIME ZONE 'UTC')::int, count(*)
            FROM fact_weather_observation f JOIN dim_source s ON s.source_key = f.source_key
            WHERE f.timestamp_utc >= make_timestamptz(%s, 1, 1, 0, 0, 0, 'UTC')
              AND f.timestamp_utc < make_timestamptz(%s, 1, 1, 0, 0, 0, 'UTC')
            GROUP BY 1, 2
            """,
            (start_year, end_year + 1),
        )
        return {(s, y): n for s, y, n in cur.fetchall()}


def build(
    start_year: int = 2016,
    end_year: int = 2025,
    version: str = PIVOT_VERSION,
    overwrite: bool = False,
    client: Minio | None = None,
    conn=None,
) -> dict[str, Any]:
    if not 2016 <= start_year <= end_year <= 2025:
        raise ValueError(f"window {start_year}-{end_year} is outside 2016-2025 or reversed")
    client = client or get_minio_client()
    years = range(start_year, end_year + 1)
    manifest_key = pivot_keys(start_year, version)[1]
    refuse_overwrite(client, [*(pivot_keys(y, version)[0] for y in years), manifest_key], overwrite)

    own = conn is None
    conn = conn or get_db_connection()
    try:
        dw = dw_counts(conn, start_year, end_year)
    finally:
        if own:
            conn.close()

    by_year, lineage, tables = {}, [], {}
    for year in years:
        long, objects = read_gold_year(client, year)
        wide = pivot(long)
        checks = reconcile(long, wide)
        for source in SOURCES:
            gold_rows = checks[source]["gold_rows"]
            if gold_rows != dw.get((source, year), 0):
                raise ValueError(
                    f"{source} {year}: Gold {gold_rows} rows, DW {dw.get((source, year), 0)}"
                )
        tables[year] = wide
        lineage += objects
        by_year[str(year)] = {"rows": len(wide), "sources": checks}
        print(
            f"  {year}: {len(wide):,} rows; "
            + ", ".join(f"{s} {c['gold_rows']:,}" for s, c in checks.items())
        )

    # Write only after every year passed its checks
    for year, wide in tables.items():
        buf = io.BytesIO()
        wide.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
        body = buf.getvalue()
        client.put_object(BUCKET_GOLD, pivot_keys(year, version)[0], io.BytesIO(body), len(body))

    manifest = {
        "dataset": f"weather_hourly_by_source_{version}",
        "workbook_id": 16,
        "grain": "location_code x timestamp_utc (one row per research point and UTC hour)",
        "key": KEY,
        "calendar_columns": {
            "date_key": "Vietnam local date (UTC+7)",
            "time_key": "local hour x 100",
        },
        "source_policy": (
            "one column per source and variable; values are never combined across sources; "
            "missing stays missing"
        ),
        "columns": [
            {
                "name": column(v, s),
                "source": s,
                "variable": v,
                "unit": u,
                **(
                    {"pressure_type": "sea_level" if s == "Meteostat" else "surface"}
                    if v == "pressure_hpa"
                    else {}
                ),
            }
            for s in SOURCES
            for v, u in VARIABLES.items()
        ],
        "notes": {
            "Meteostat": "HAN only (station 48820, Nội Bài airport, 22 km from the point); no rain",
            "ERA5": "nearest 0.25-degree cell; surface pressure at the cell's terrain height",
            "Open-Meteo": "the label and feature source of CART v1",
        },
        "window_utc_years": [start_year, end_year],
        "rows": sum(v["rows"] for v in by_year.values()),
        "by_year": by_year,
        "dw_rows_checked": True,
        "lineage": {"input": "Gold fact_weather_observation monthly objects", "objects": lineage},
        "objects": [f"{BUCKET_GOLD}/{pivot_keys(y, version)[0]}" for y in years],
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    body = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    client.put_object(BUCKET_GOLD, manifest_key, io.BytesIO(body), len(body), "application/json")
    print(json.dumps({"rows": manifest["rows"], "manifest": f"{BUCKET_GOLD}/{manifest_key}"}))
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the Gold ML multi-source pivot (ID 16).")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--version", default=PIVOT_VERSION)
    parser.add_argument("--overwrite", action="store_true", help="Replace an existing version")
    args = parser.parse_args()
    try:
        build(args.start_year, args.end_year, args.version, args.overwrite)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
