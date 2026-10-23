#!/usr/bin/env python3
"""scripts/quality_gate.py.

Quality gate on Silver before the DW load (workbook ID 24, task `quality_gate` of
dags/dag_weather_pipeline.py). Reads the Silver objects of a window of UTC years
independently of the transform reports and checks:

- hourly `weather_hourly/` (the sources loaded into fact_weather_observation): required
  columns, non-null keys, UTC timestamps on the hour inside their partition month, unique
  source x location x UTC hour, the valid ranges of docs/data-contract.md, and complete
  hourly coverage for sources whose Bronze contract only accepts whole years (Open-Meteo);
- GHCNh daily rain: required columns, unique station x rain day, the rain window
  (D-1 12:00 UTC, D 12:00 UTC], and 0 <= rain_24h_mm < 999.

ERROR checks block the DW load (exit 1). WARN checks never block: they flag values worth a
human look (coverage drops, missing values, implausible but possible values, unreviewed
extreme rain). Their thresholds were decided on 2026-10-22 by the project member holding all
roles, from the 2016-2025 Silver data with a margin (docs/data-contract.md, "Quality gate").
Row counts and min/max per variable are REPORTED. A JSON report is written to
silver/quality_reports/quality_gate_<start>_<end>[_<location>]_<timestamp>.json.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from load_gold_observations import (
    BUCKET_SILVER,
    LOADED_SOURCES,
    SILVER_HOURLY_PREFIXES,
    get_minio_client,
    requested_locations,
)
from minio import Minio
from minio.error import S3Error
from quality_checks import compute_hourly_coverage, validate_and_nullify_ranges
from station_config import GHCNH_RAIN_STATIONS, ghcnh_stations_for
from transform_ghcnh_to_silver import silver_keys as ghcnh_silver_keys

HOURLY_KEY = ["source_name", "location_code", "timestamp_utc"]
HOURLY_REQUIRED = HOURLY_KEY + ["pressure_type"]
MEASUREMENTS = [
    "temperature_c",
    "humidity_percent",
    "pressure_hpa",
    "precipitation_mm",
    "wind_speed_ms",
    "wind_direction_deg",
    "cloud_cover_percent",
    "solar_radiation_wm2",
]
# Sources complete for every hour by contract: Open-Meteo Bronze stores only whole years, and
# ERA5 Silver fails a location with a missing hour (docs/data-contract.md).
WHOLE_YEAR_SOURCES = {"Open-Meteo", "ERA5"}
RAIN_REQUIRED = ["station_id", "rain_day_utc", "window_start_utc", "window_end_utc", "rain_24h_mm"]
RAIN_MAX_MM = 999.0  # GHCNh accepted values: 0 <= value < 999 mm (data contract)
SAMPLE = 5

# WARN thresholds (decided 2026-10-22; observed 2016-2025 values in comments).
MIN_COVERAGE_PCT = {"Meteostat": 95.0}  # lowest year 99.55%
MIN_RAIN_DAY_COVERAGE_PCT = 90.0  # GHCNh, lowest station-year 95.89%
MAX_MISSING_PCT = {  # % of expected hours; variables not listed are reported only
    "Open-Meteo": dict.fromkeys(MEASUREMENTS, 0.0),  # never missing; feeds the ML labels
    "Meteostat": {"temperature_c": 5.0, "humidity_percent": 5.0},  # worst year 0.45%
    # ERA5 (added 2026-10-23): never missing; radiation is not requested, so not limited.
    "ERA5": {c: 0.0 for c in MEASUREMENTS if c != "solar_radiation_wm2"},
}
PLAUSIBLE = {  # variable: (low, high); observed temperature 3.8-42.9, pressure 963.3-1041.0
    "temperature_c": (0.0, 45.0),
    "pressure_hpa": (950.0, 1050.0),
    "precipitation_mm": (None, 100.0),  # one hour; observed maximum 69.4 mm
}
# ERA5 surface pressure is at the grid cell's model terrain: 916.9 hPa (LCA) to 1039.8 observed.
PLAUSIBLE_BY_SOURCE = {"ERA5": {"pressure_hpa": (900.0, 1050.0)}}
EXTREME_RAIN_MM = 300.0  # the data contract's "unsupported extremes" limit
# Days >= EXTREME_RAIN_MM already reviewed and recorded as real in docs/data-contract.md.
REVIEWED_EXTREME_RAIN_DAYS = {("48825", "2016-05-25"), ("48831", "2025-10-07")}


def check(check_id: str, severity: str, passed: bool | None, **fields: Any) -> dict[str, Any]:
    """One report row. passed=None: REPORTED metric; a failed WARN check is WARN, not FAIL."""
    if passed is None:
        status = "REPORTED"
    elif passed:
        status = "PASS"
    else:
        status = "FAIL" if severity == "ERROR" else "WARN"
    return {"check_id": check_id, "severity": severity, "status": status, **fields}


def _read_parquet(client: Minio, key: str) -> pd.DataFrame | None:
    try:
        resp = client.get_object(BUCKET_SILVER, key)
    except S3Error as err:
        if getattr(err, "code", None) in ("NoSuchKey", "NoSuchObject"):
            return None
        raise
    try:
        return pd.read_parquet(io.BytesIO(resp.read()))
    finally:
        resp.close()
        resp.release_conn()


def _partition_month(key: str) -> tuple[int, int] | None:
    parts = dict(p.split("=", 1) for p in key.split("/") if "=" in p)
    try:
        return int(parts["year"]), int(parts["month"])
    except (KeyError, ValueError):
        return None


def check_hourly_object(key: str, df: pd.DataFrame) -> list[dict[str, Any]]:
    """Object-level checks: columns, non-null keys, UTC timestamps inside the partition."""
    out = []
    missing = [c for c in HOURLY_REQUIRED + MEASUREMENTS if c not in df.columns]
    out.append(check("H01_columns", "ERROR", not missing, object=key, missing_columns=missing))
    if any(c in missing for c in HOURLY_REQUIRED):
        return out
    nulls = {c: int(df[c].isna().sum()) for c in HOURLY_REQUIRED if df[c].isna().any()}
    out.append(check("H02_non_null_keys", "ERROR", not nulls, object=key, null_counts=nulls))
    ts = df["timestamp_utc"]
    is_utc = isinstance(ts.dtype, pd.DatetimeTZDtype) and str(ts.dt.tz) == "UTC"
    out.append(check("H03_utc_timestamps", "ERROR", is_utc, object=key, dtype=str(ts.dtype)))
    if not is_utc:
        return out
    off_hour = ts[(ts.dt.minute != 0) | (ts.dt.second != 0) | (ts.dt.microsecond != 0)]
    month = _partition_month(key)
    outside = ts[(ts.dt.year != month[0]) | (ts.dt.month != month[1])] if month else ts.iloc[:0]
    bad = pd.concat([off_hour, outside]).drop_duplicates()
    out.append(
        check(
            "H04_timestamps_in_partition",
            "ERROR",
            month is not None and bad.empty,
            object=key,
            partition=list(month) if month else None,
            actual=int(len(bad)),
            expected=0,
            sample_keys=[t.isoformat() for t in bad.head(SAMPLE)],
        )
    )
    return out


def check_hourly_groups(df: pd.DataFrame, years: range) -> list[dict[str, Any]]:
    """Duplicate keys, valid ranges, row counts, coverage, % missing, and min/max."""
    out = []
    dup = df[df.duplicated(HOURLY_KEY, keep=False)]
    out.append(
        check(
            "H05_unique_key",
            "ERROR",
            dup.empty,
            key=HOURLY_KEY,
            actual=int(len(dup)),
            expected=0,
            sample_keys=dup[HOURLY_KEY].astype(str).head(SAMPLE).to_dict("records"),
        )
    )
    for (src, loc, year), group in df.groupby(["source_name", "location_code", "year"]):
        scope = {"source": src, "location": loc, "year": int(year)}
        _, violations = validate_and_nullify_ranges(group)
        bad = {k: v for k, v in violations.items() if v}
        out.append(check("H06_valid_ranges", "ERROR", not bad, **scope, violations=bad))
        start = datetime(int(year), 1, 1, tzinfo=timezone.utc)
        end = datetime(int(year) + 1, 1, 1, tzinfo=timezone.utc)
        cov = compute_hourly_coverage(group, start, end, MEASUREMENTS)
        whole_year = src in WHOLE_YEAR_SOURCES
        min_cov = 100.0 if whole_year else MIN_COVERAGE_PCT.get(src)
        if whole_year:
            coverage_ok = cov["missing_hours"] == 0
        else:
            coverage_ok = None if min_cov is None else cov["record_coverage_pct"] >= min_cov
        out.append(
            check(
                "H07_hourly_coverage",
                "ERROR" if whole_year else "WARN",
                coverage_ok,
                **scope,
                rows=int(len(group)),
                expected_hours=cov["expected_hours"],
                present_hours=cov["present_expected_hours"],
                missing_hours=cov["missing_hours"],
                coverage_pct=cov["record_coverage_pct"],
                threshold="100% (whole-year Bronze)" if whole_year else min_cov,
                sample_keys=cov["sample_missing_timestamps"][:SAMPLE],
            )
        )
        expected = cov["expected_hours"]
        missing_pct = {
            c: (expected - m["non_null_count"]) / expected * 100.0
            for c, m in cov["value_completeness"].items()
        }
        limits = MAX_MISSING_PCT.get(src, {})
        exceeded = {
            c: round(missing_pct[c], 3) for c, lim in limits.items() if missing_pct[c] > lim
        }
        out.append(
            check(
                "H08_missing_values",
                "WARN",
                not exceeded,
                **scope,
                denominator="expected hours",
                expected_hours=expected,
                missing_pct={c: round(v, 3) for c, v in missing_pct.items()},
                max_missing_pct=limits,
                exceeded=exceeded,
            )
        )
        outside, samples = {}, {}
        limits = {**PLAUSIBLE, **PLAUSIBLE_BY_SOURCE.get(src, {})}
        for col, (low, high) in limits.items():
            values = group[col]
            mask = pd.Series(False, index=group.index)
            if low is not None:
                mask |= values < low
            if high is not None:
                mask |= values > high
            if mask.any():
                outside[col] = int(mask.sum())
                samples[col] = [
                    [t.isoformat(), float(v)]
                    for t, v in zip(group.loc[mask, "timestamp_utc"].head(SAMPLE), values[mask])
                ]
        out.append(
            check(
                "H09_plausible_values",
                "WARN",
                not outside,
                **scope,
                limits={c: list(v) for c, v in limits.items()},
                outside=outside,
                sample_keys=samples,
                min_max={
                    c: [float(group[c].min()), float(group[c].max())]
                    for c in MEASUREMENTS
                    if group[c].notna().any()
                },
            )
        )
    # A whole-year source with no row at all for a requested location-year is a missing input.
    present = set(zip(df["source_name"], df["location_code"], df["year"]))
    for src in sorted(WHOLE_YEAR_SOURCES):
        for loc in sorted(df.attrs.get("locations", [])):
            for year in years:
                if (src, loc, year) not in present:
                    out.append(
                        check(
                            "H07_hourly_coverage",
                            "ERROR",
                            False,
                            source=src,
                            location=loc,
                            year=year,
                            rows=0,
                            missing_hours=None,
                            threshold="100% (whole-year Bronze)",
                            note="no Silver rows for this location-year",
                        )
                    )
    return out


def gate_hourly(
    client: Minio, start_year: int, end_year: int, locations: list[str]
) -> list[dict[str, Any]]:
    out, frames = [], []
    objects = rows_read = failed_objects = 0
    listings = (
        client.list_objects(BUCKET_SILVER, prefix=f"{base}/year={year}/", recursive=True)
        for year in range(start_year, end_year + 1)
        for base in SILVER_HOURLY_PREFIXES
    )
    for listing in listings:
        for obj in listing:
            if not obj.object_name.endswith(".parquet"):
                continue
            df = _read_parquet(client, obj.object_name)
            if df is None:
                continue
            objects += 1
            rows_read += len(df)
            object_failures = [
                c for c in check_hourly_object(obj.object_name, df) if c["status"] == "FAIL"
            ]
            if object_failures:  # only failures are listed per object, to keep the report short
                out.extend(object_failures)
                failed_objects += 1
                continue
            df = df[df["source_name"].isin(LOADED_SOURCES)].copy()
            df["location_code"] = df["location_code"].astype(str).str.strip().str.upper()
            df = df[df["location_code"].isin(locations)]
            if not df.empty:
                frames.append(df[HOURLY_KEY + MEASUREMENTS])
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=HOURLY_KEY)
    out.append(
        check(
            "H00_objects_read",
            "INFO",
            None,
            objects=objects,
            objects_failing_object_checks=failed_objects,
            rows_read=rows_read,
            rows_in_scope=int(len(df)),
            note="rows of other sources or locations are ignored, as by the Gold loader",
        )
    )
    df["year"] = pd.to_datetime(df["timestamp_utc"], utc=True).dt.year
    df.attrs["locations"] = locations
    out.extend(check_hourly_groups(df, range(start_year, end_year + 1)))
    return out


def check_rain_frame(sid: str, year: int, df: pd.DataFrame) -> list[dict[str, Any]]:
    scope = {"source": "GHCNh", "station": sid, "year": year}
    missing = [c for c in RAIN_REQUIRED if c not in df.columns]
    out = [check("D01_columns", "ERROR", not missing, **scope, missing_columns=missing)]
    if missing:
        return out
    nulls = {c: int(df[c].isna().sum()) for c in RAIN_REQUIRED if df[c].isna().any()}
    out.append(check("D02_non_null", "ERROR", not nulls, **scope, null_counts=nulls))
    day = pd.to_datetime(df["rain_day_utc"])
    dup = df[day.duplicated(keep=False)]
    out.append(
        check(
            "D03_unique_station_day",
            "ERROR",
            dup.empty,
            **scope,
            actual=int(len(dup)),
            expected=0,
            sample_keys=dup["rain_day_utc"].astype(str).head(SAMPLE).tolist(),
        )
    )
    end = pd.to_datetime(df["window_end_utc"], utc=True)
    start = pd.to_datetime(df["window_start_utc"], utc=True)
    expected_end = day.dt.tz_localize("UTC") + pd.Timedelta(hours=12)
    bad_window = (
        (end != expected_end) | (start != end - pd.Timedelta(hours=24)) | (day.dt.year != year)
    )
    out.append(
        check(
            "D04_rain_window",
            "ERROR",
            not bad_window.any(),
            **scope,
            rule="(D-1 12:00 UTC, D 12:00 UTC], D inside the partition year",
            actual=int(bad_window.sum()),
            expected=0,
            sample_keys=df.loc[bad_window, "rain_day_utc"].astype(str).head(SAMPLE).tolist(),
        )
    )
    rain = df["rain_24h_mm"]
    days = day.dt.strftime("%Y-%m-%d")
    extreme_days = days[rain >= EXTREME_RAIN_MM].tolist()
    unreviewed = [d for d in extreme_days if (sid, d) not in REVIEWED_EXTREME_RAIN_DAYS]
    out.append(
        check(
            "D07_extreme_rain",
            "WARN",
            not unreviewed,
            **scope,
            rule=f"rain_24h_mm >= {EXTREME_RAIN_MM:g} on a day not yet reviewed",
            unreviewed_days=unreviewed,
            reviewed_days=[d for d in extreme_days if d not in unreviewed],
        )
    )
    bad_range = (rain < 0) | (rain >= RAIN_MAX_MM)
    out.append(
        check(
            "D05_rain_range",
            "ERROR",
            not bad_range.any(),
            **scope,
            rule=f"0 <= rain_24h_mm < {RAIN_MAX_MM:g}",
            actual=int(bad_range.sum()),
            expected=0,
            sample_keys=df.loc[bad_range, "rain_day_utc"].astype(str).head(SAMPLE).tolist(),
        )
    )
    return out


def gate_daily_rain(
    client: Minio, start_year: int, end_year: int, stations: list[str]
) -> list[dict[str, Any]]:
    out = []
    for sid in stations:
        for year in range(start_year, end_year + 1):
            key, _ = ghcnh_silver_keys(sid, year)
            df = _read_parquet(client, key)
            days = (datetime(year + 1, 1, 1) - datetime(year, 1, 1)).days
            rows = 0 if df is None else len(df)
            if df is not None:
                out.extend(check_rain_frame(sid, year, df))
            coverage = rows / days * 100.0
            out.append(
                check(
                    "D06_day_coverage",
                    "WARN",
                    coverage >= MIN_RAIN_DAY_COVERAGE_PCT,
                    source="GHCNh",
                    station=sid,
                    year=year,
                    rows=rows,
                    days_in_year=days,
                    coverage_pct=round(coverage, 2),
                    threshold=MIN_RAIN_DAY_COVERAGE_PCT,
                    note="no Silver object (no usable day)" if df is None else None,
                )
            )
    return out


def quality_gate(
    start_year: int,
    end_year: int,
    location: str | None = None,
    client: Minio | None = None,
) -> dict[str, Any]:
    locations = requested_locations(location)
    stations = ghcnh_stations_for(location) if location else list(GHCNH_RAIN_STATIONS)
    client = client or get_minio_client()
    checks = gate_hourly(client, start_year, end_year, locations)
    checks += gate_daily_rain(client, start_year, end_year, stations)
    failed = [c for c in checks if c["status"] == "FAIL"]
    warnings = [c for c in checks if c["status"] == "WARN"]
    report = {
        "check": "quality gate on Silver before the DW load",
        "window_utc_years": [start_year, end_year],
        "locations": locations,
        "hourly_sources": list(LOADED_SOURCES),
        "daily_rain_stations": stations,
        "checks_run": len(checks),
        "failed": len(failed),
        "warnings": len(warnings),
        "by_status": {
            s: sum(c["status"] == s for c in checks) for s in ("PASS", "FAIL", "WARN", "REPORTED")
        },
        "passed": not failed,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
    }
    tag = f"{start_year}_{end_year}" + (f"_{locations[0]}" if location else "")
    key = f"quality_reports/quality_gate_{tag}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    body = json.dumps(report, indent=2, default=str).encode("utf-8")
    client.put_object(BUCKET_SILVER, key, io.BytesIO(body), len(body), "application/json")
    report["report_key"] = f"{BUCKET_SILVER}/{key}"
    print(json.dumps({k: v for k, v in report.items() if k != "checks"}, indent=2))
    for w in warnings:  # visible in the Airflow task log; the run continues
        print("[WARN] " + json.dumps(w, default=str))
    if failed:
        raise RuntimeError(
            f"{len(failed)} quality check(s) failed: " + json.dumps(failed[:SAMPLE], default=str)
        )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Quality gate on Silver before the DW load.")
    parser.add_argument("--start-year", type=int, required=True, help="First UTC year")
    parser.add_argument("--end-year", type=int, required=True, help="Last UTC year")
    parser.add_argument("--location", default=None, help="One canonical location code")
    args = parser.parse_args()
    try:
        quality_gate(args.start_year, args.end_year, args.location)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
