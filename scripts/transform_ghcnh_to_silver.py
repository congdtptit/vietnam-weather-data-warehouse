#!/usr/bin/env python3
"""scripts/transform_ghcnh_to_silver.py.

Turn Bronze GHCNh PSV files into a Silver daily station-rainfall series (workbook ID 9).

Semantics (GHCNh documentation, v1.1.0):
- DATE is UTC (ISO 8601). `precipitation_24_hour` is the liquid accumulation (mm) reported
  in FM-12/SYNOP. Vietnamese stations report it at 12:00 UTC, so the value at D 12:00 UTC
  covers the window (D-1 12:00 UTC, D 12:00 UTC]; the rain day is D.
- Quality codes for source 220 (legacy): 0 not checked, 1 good, 2 suspect, 3 erroneous,
  4 calculated, 5 removed; letters are GHCNh QC failure flags.
- Measurement code "T" (or "2") marks trace precipitation; codes for accumulated, deleted,
  missing, estimated, or incomplete periods make the value unusable as a 24-hour total.

A day without a usable report is missing, never 0 mm. Totals >= EXTREME_MM without a supporting
sub-daily accumulation in the same window are quarantined (see EXTREME_MM below).

Silver keys (bucket silver):
  ghcnh/station_daily_rain/year=YYYY/ghcnh_daily_rain_<wmo>_<YYYY>.parquet
  ghcnh/quarantine/year=YYYY/ghcnh_daily_rain_<wmo>_<YYYY>_quarantine.parquet
  quality_reports/dq_report_ghcnh_<timestamp>.json
"""

import argparse
import calendar
import io
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from ingest_ghcnh import bronze_keys, get_minio_client, is_s3_not_found_error, read_manifest
from minio import Minio
from minio.error import S3Error
from quality_checks import resolve_duplicates_with_quarantine
from station_config import GHCNH_RAIN_STATIONS

BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")

SOURCE_NAME = "GHCNh"
REPORT_HOUR_UTC = 12
ACCEPTED_QUALITY_CODES = frozenset({"0", "1", "4"})
TRACE_MEASUREMENT_CODES = frozenset({"T", "2"})
MAX_VALID_MM = 999.0
P24 = "precipitation_24_hour"
# A 24-hour total at or above EXTREME_MM must be supported by a sub-daily accumulation in the
# same window of at least EXTREME_SUPPORT_RATIO of its value; otherwise it is quarantined.
# Example: Nam Dinh 2023-10-13 reported 700.0 mm with no sub-daily rain and 0.1 mm on both
# neighbouring days, while heavy real events (338 mm, 491 mm) are supported.
EXTREME_MM = 300.0
EXTREME_SUPPORT_RATIO = 0.1

SUB_DAILY_COLUMNS = [f"precipitation_{h}_hour" for h in (3, 6, 9, 12, 15, 18, 21)]

SILVER_COLUMNS = [
    "source_name",
    "station_id",
    "location_code",
    "station_role",
    "rain_day_utc",
    "window_start_utc",
    "window_end_utc",
    "rain_24h_mm",
    "is_trace",
    "quality_code",
    "measurement_code",
    "report_type",
    "source_code",
]


def extreme_is_supported(support_mm: float, total_mm: float) -> bool:
    """Whether a sub-daily accumulation supports a 24 h total (missing support: no).

    Values are reported in 0.1 mm, so the comparison is rounded: in binary floats
    0.1 * 338.0 is 33.800000000000004, which would reject an exact 33.8 mm support.
    """
    return bool(round(support_mm, 1) >= round(EXTREME_SUPPORT_RATIO * total_mm, 2))


def silver_keys(wmo_id: str, year: int) -> tuple[str, str]:
    name = f"ghcnh_daily_rain_{wmo_id}_{year}"
    return (
        f"ghcnh/station_daily_rain/year={year}/{name}.parquet",
        f"ghcnh/quarantine/year={year}/{name}_quarantine.parquet",
    )


def clean_ghcnh_daily_rain(
    raw_psv: bytes, wmo_id: str, year: int
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Return (clean daily rows, quarantined rows, DQ stats) for one station-year file.

    Quarantined rows carry `quarantine_reason`: `conflicting_duplicate` or `unsupported_extreme`.
    """
    mapping = GHCNH_RAIN_STATIONS.get(wmo_id)
    if mapping is None:
        raise ValueError(f"Station {wmo_id} is not in GHCNH_RAIN_STATIONS")

    cols = [P24, f"{P24}_Measurement_Code", f"{P24}_Quality_Code"]
    cols += [f"{P24}_Report_Type", f"{P24}_Source_Code"]
    df = pd.read_csv(io.BytesIO(raw_psv), sep="|", dtype=str, keep_default_na=False)
    missing_cols = [c for c in ["DATE", *cols] if c not in df.columns]
    if missing_cols:
        raise ValueError(f"GHCNh {wmo_id} {year}: missing columns {missing_cols}")

    stats: dict[str, Any] = {"station_id": wmo_id, "year": year, "input_rows": len(df)}
    all_times = pd.to_datetime(df["DATE"], format="ISO8601").dt.tz_localize("UTC")
    present_sub = [c for c in SUB_DAILY_COLUMNS if c in df.columns]
    sub_daily_max = (
        pd.concat([pd.to_numeric(df[c], errors="coerce") for c in present_sub], axis=1).max(axis=1)
        if present_sub
        else pd.Series(float("nan"), index=df.index)
    )
    df = df.loc[df[P24].str.strip() != "", ["DATE", *cols]].copy()
    stats["rows_with_24h_value"] = len(df)

    df["window_end_utc"] = pd.to_datetime(df["DATE"], format="ISO8601").dt.tz_localize("UTC")
    on_schedule = (df["window_end_utc"].dt.hour == REPORT_HOUR_UTC) & (
        df["window_end_utc"].dt.minute == 0
    )
    stats["off_schedule_rows_ignored"] = int((~on_schedule).sum())
    df = df.loc[on_schedule & (df["window_end_utc"].dt.year == year)]

    quality = df[f"{P24}_Quality_Code"].str.strip()
    bad_quality = ~quality.isin(ACCEPTED_QUALITY_CODES)
    stats["quality_rejected"] = {
        str(k): int(v) for k, v in quality[bad_quality].value_counts().items()
    }

    measurement = df[f"{P24}_Measurement_Code"].str.strip()
    bad_measurement = (measurement != "") & ~measurement.isin(TRACE_MEASUREMENT_CODES)
    stats["measurement_rejected"] = {
        str(k): int(v)
        for k, v in measurement[bad_measurement & ~bad_quality].value_counts().items()
    }

    value = pd.to_numeric(df[P24], errors="coerce")
    out_of_range = value.isna() | (value < 0) | (value >= MAX_VALID_MM)
    stats["out_of_range_rejected"] = int((out_of_range & ~bad_quality & ~bad_measurement).sum())

    keep = ~(bad_quality | bad_measurement | out_of_range)
    df = df.loc[keep].copy()
    df["rain_24h_mm"] = value[keep].astype("float64")
    df["is_trace"] = measurement[keep].isin(TRACE_MEASUREMENT_CODES)

    df["source_name"] = SOURCE_NAME
    df["station_id"] = wmo_id
    df["location_code"] = mapping["location_code"]
    df["station_role"] = mapping["role"]
    df["window_start_utc"] = df["window_end_utc"] - pd.Timedelta(hours=24)
    df["rain_day_utc"] = df["window_end_utc"].dt.date
    df["quality_code"] = df[f"{P24}_Quality_Code"].str.strip()
    df["measurement_code"] = df[f"{P24}_Measurement_Code"].str.strip().replace("", None)
    df["report_type"] = df[f"{P24}_Report_Type"].str.strip()
    df["source_code"] = df[f"{P24}_Source_Code"].str.strip()
    df = df[SILVER_COLUMNS]

    extreme = df["rain_24h_mm"] >= EXTREME_MM
    unsupported = pd.Series(False, index=df.index)
    for idx in df.index[extreme]:
        in_window = (all_times > df.at[idx, "window_start_utc"]) & (
            all_times <= df.at[idx, "window_end_utc"]
        )
        support = sub_daily_max[in_window].max()
        unsupported[idx] = not extreme_is_supported(support, df.at[idx, "rain_24h_mm"])
    extremes_quarantined = df.loc[unsupported].assign(quarantine_reason="unsupported_extreme")
    df = df.loc[~unsupported]

    clean, conflicts, dedup = resolve_duplicates_with_quarantine(
        df=df,
        key_cols=["source_name", "station_id", "rain_day_utc"],
        measurement_cols=["rain_24h_mm"],
    )
    clean = clean.sort_values("rain_day_utc").reset_index(drop=True)[SILVER_COLUMNS]

    expected_days = 366 if calendar.isleap(year) else 365
    stats.update(
        {
            "identical_duplicates_removed": dedup["identical_duplicates_removed"],
            "conflicting_rows_quarantined": dedup["conflicting_duplicates_count"],
            "unsupported_extreme_quarantined": len(extremes_quarantined),
            "clean_days": len(clean),
            "expected_days": expected_days,
            "missing_days": expected_days - len(clean),
            "coverage_pct": round(100.0 * len(clean) / expected_days, 2),
            "trace_days": int(clean["is_trace"].sum()) if not clean.empty else 0,
            "rain_total_mm": round(float(clean["rain_24h_mm"].sum()), 1)
            if not clean.empty
            else 0.0,
        }
    )
    if not conflicts.empty:
        conflicts = conflicts[SILVER_COLUMNS].assign(quarantine_reason="conflicting_duplicate")
    quarantined = (
        pd.concat([q for q in (conflicts, extremes_quarantined) if not q.empty], ignore_index=True)
        if not (conflicts.empty and extremes_quarantined.empty)
        else pd.DataFrame()
    )
    return clean, quarantined, stats


def _put_parquet(client: Minio, key: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    data = buf.getvalue()
    client.put_object(
        BUCKET_SILVER, key, io.BytesIO(data), len(data), content_type="application/octet-stream"
    )


def _remove_if_exists(client: Minio, key: str) -> bool:
    try:
        client.stat_object(BUCKET_SILVER, key)
    except S3Error as err:
        if is_s3_not_found_error(err):
            return False
        raise
    client.remove_object(BUCKET_SILVER, key)
    return True


def process_ghcnh_to_silver(
    start_year: int = 2016,
    end_year: int = 2025,
    station_ids: list[str] | None = None,
    overwrite: bool = False,
    client: Minio | None = None,
) -> dict[str, Any]:
    """Transform every requested station-year present in Bronze. Raises on any failure."""
    client = client or get_minio_client()
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)
    stations = station_ids or list(GHCNH_RAIN_STATIONS)
    report: dict[str, Any] = {
        "source": SOURCE_NAME,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "station_years": {},
        "skipped_existing": 0,
        "not_in_bronze": [],
        "stale_removed": 0,
        "failures": [],
    }

    for wmo_id in stations:
        for year in range(start_year, end_year + 1):
            data_key, manifest_key = bronze_keys(wmo_id, year)
            clean_key, conflict_key = silver_keys(wmo_id, year)
            try:
                if not overwrite:
                    try:
                        client.stat_object(BUCKET_SILVER, clean_key)
                        report["skipped_existing"] += 1
                        continue
                    except S3Error as err:
                        if not is_s3_not_found_error(err):
                            raise
                try:
                    resp = client.get_object(BUCKET_BRONZE, data_key)
                    try:
                        raw = resp.read()
                    finally:
                        resp.close()
                        resp.release_conn()
                except S3Error as err:
                    if not is_s3_not_found_error(err):
                        raise
                    if read_manifest(client, manifest_key) is not None:
                        raise RuntimeError(
                            f"Manifest exists but Bronze object is missing: {data_key}"
                        )
                    report["not_in_bronze"].append({"station_id": wmo_id, "year": year})
                    continue

                clean, conflicts, stats = clean_ghcnh_daily_rain(raw, wmo_id, year)
                report["station_years"][f"{wmo_id}_{year}"] = stats
                if clean.empty:
                    if overwrite and _remove_if_exists(client, clean_key):
                        report["stale_removed"] += 1
                else:
                    _put_parquet(client, clean_key, clean)
                if not conflicts.empty:
                    _put_parquet(client, conflict_key, conflicts)
                elif overwrite:
                    _remove_if_exists(client, conflict_key)
                print(
                    f"  {wmo_id} {year}: {stats['clean_days']}/{stats['expected_days']} days "
                    f"({stats['coverage_pct']}%), {stats['rain_total_mm']} mm, "
                    f"conflicts {stats['conflicting_rows_quarantined']}, "
                    f"unsupported extremes {stats['unsupported_extreme_quarantined']}"
                )
            except Exception as ex:
                report["failures"].append({"station_id": wmo_id, "year": year, "error": str(ex)})
                print(f"  [!] {wmo_id} {year}: {ex}")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    rbytes = json.dumps(report, indent=2, default=str).encode("utf-8")
    client.put_object(
        BUCKET_SILVER,
        f"quality_reports/dq_report_ghcnh_{stamp}.json",
        io.BytesIO(rbytes),
        len(rbytes),
        content_type="application/json",
    )
    years_done = len(report["station_years"])
    print(
        f"GHCNh Silver: {years_done} station-years, skipped {report['skipped_existing']}, "
        f"not in Bronze {len(report['not_in_bronze'])}, failures {len(report['failures'])}"
    )
    if report["failures"]:
        raise RuntimeError(f"GHCNh Silver had {len(report['failures'])} failure(s)")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Transform Bronze GHCNh files into Silver daily rain."
    )
    parser.add_argument("--start-year", type=int, required=True, help="First year (inclusive)")
    parser.add_argument("--end-year", type=int, required=True, help="Last year (inclusive)")
    parser.add_argument("--stations", type=str, default=None, help="Comma-separated WMO ids")
    parser.add_argument("--overwrite", action="store_true", help="Rewrite existing Silver objects")
    args = parser.parse_args()
    stations = [s.strip() for s in args.stations.split(",")] if args.stations else None
    try:
        process_ghcnh_to_silver(args.start_year, args.end_year, stations, args.overwrite)
    except RuntimeError as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
