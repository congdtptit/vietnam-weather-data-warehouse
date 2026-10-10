"""Quality check functions for weather ETL pipelines.

Implements expected hourly grid coverage, range validation without clipping,
and order-invariant deduplication separating identical and conflicting records.
Follows docs/data-contract.md and .agents/skills/weather-data-quality/SKILL.md.
"""

from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd


def compute_hourly_coverage(
    df: pd.DataFrame,
    start_time: datetime | pd.Timestamp | str,
    end_time: datetime | pd.Timestamp | str,
    measurement_cols: list[str] | None = None,
) -> dict[str, Any]:
    """Compute expected chronological UTC hourly grid coverage over [start, end).

    Does not infer expected hours from data min/max.
    Identifies missing hours, out-of-bounds timestamps, and unaligned hours.
    Differentiates record presence coverage from value non-null completeness.
    Handles leap years (e.g. 2024 = 8,784h) and empty DataFrames.
    """
    start_utc = pd.to_datetime(start_time, utc=True)
    end_utc = pd.to_datetime(end_time, utc=True)

    if start_utc >= end_utc:
        raise ValueError(f"start_time ({start_utc}) must precede end_time ({end_utc})")

    # Half-open interval [start, end)
    expected_grid = pd.date_range(start=start_utc, end=end_utc, freq="h", inclusive="left")
    expected_hours = len(expected_grid)

    eval_cols = measurement_cols or []

    if df.empty or "timestamp_utc" not in df.columns:
        null_metrics = {
            col: {
                "null_count": 0,
                "non_null_count": 0,
                "non_null_pct_of_records": 0.0,
                "completeness_pct_of_expected": 0.0,
            }
            for col in eval_cols
        }
        return {
            "window_start_utc": start_utc.isoformat(),
            "window_end_utc": end_utc.isoformat(),
            "expected_hours": expected_hours,
            "actual_records": 0,
            "present_expected_hours": 0,
            "missing_hours": expected_hours,
            "record_coverage_pct": 0.0,
            "out_of_bounds_count": 0,
            "unaligned_hours_count": 0,
            "start_gap_hours": expected_hours,
            "end_gap_hours": expected_hours,
            "sample_missing_timestamps": [ts.isoformat() for ts in expected_grid[:10]],
            "value_completeness": null_metrics,
        }

    ts_series = pd.to_datetime(df["timestamp_utc"], utc=True)

    # Check alignment (must be on exact hour :00:00)
    unaligned_mask = (
        (ts_series.dt.minute != 0) | (ts_series.dt.second != 0) | (ts_series.dt.microsecond != 0)
    )
    unaligned_count = int(unaligned_mask.sum())

    # Check bounds [start, end)
    out_of_bounds_mask = (ts_series < start_utc) | (ts_series >= end_utc)
    out_of_bounds_count = int(out_of_bounds_mask.sum())

    valid_mask = ~unaligned_mask & ~out_of_bounds_mask
    valid_ts_set = set(ts_series[valid_mask])
    expected_ts_set = set(expected_grid)

    missing_ts_set = expected_ts_set - valid_ts_set
    missing_hours_count = len(missing_ts_set)
    present_hours_count = len(valid_ts_set & expected_ts_set)
    coverage_pct = (
        round(present_hours_count / expected_hours * 100.0, 2) if expected_hours > 0 else 0.0
    )

    # Start and end gap analysis
    if valid_ts_set:
        min_present = min(valid_ts_set)
        max_present = max(valid_ts_set)
        start_gap = len(expected_grid[expected_grid < min_present])
        end_gap = len(expected_grid[expected_grid > max_present])
    else:
        start_gap = expected_hours
        end_gap = expected_hours

    sorted_missing = sorted(list(missing_ts_set))
    sample_missing = [ts.isoformat() for ts in sorted_missing[:10]]

    # Evaluate non-null value completeness for measurement columns
    null_metrics = {}
    actual_records = len(df)
    for col in eval_cols:
        if col in df.columns:
            non_null_count = int(df[col].notnull().sum())
            null_count = int(df[col].isnull().sum())
            pct_records = (
                round(non_null_count / actual_records * 100.0, 2) if actual_records > 0 else 0.0
            )
            pct_expected = (
                round(non_null_count / expected_hours * 100.0, 2) if expected_hours > 0 else 0.0
            )
            null_metrics[col] = {
                "null_count": null_count,
                "non_null_count": non_null_count,
                "non_null_pct_of_records": pct_records,
                "completeness_pct_of_expected": pct_expected,
            }

    return {
        "window_start_utc": start_utc.isoformat(),
        "window_end_utc": end_utc.isoformat(),
        "expected_hours": expected_hours,
        "actual_records": actual_records,
        "present_expected_hours": present_hours_count,
        "missing_hours": missing_hours_count,
        "record_coverage_pct": coverage_pct,
        "out_of_bounds_count": out_of_bounds_count,
        "unaligned_hours_count": unaligned_count,
        "start_gap_hours": start_gap,
        "end_gap_hours": end_gap,
        "sample_missing_timestamps": sample_missing,
        "value_completeness": null_metrics,
    }


def validate_and_nullify_ranges(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Validate physical meteorological ranges without clipping invalid values to valid numbers.

    Rules:
    - precipitation_mm: must be >= 0.0. Negative rain is set to NaN (not 0.0).
    - humidity_percent: must be in [0.0, 100.0]. Out of range is set to NaN.
    - cloud_cover_percent: must be in [0.0, 100.0]. Out of range is set to NaN.
    - wind_speed_ms: must be >= 0.0. Negative speed is set to NaN.
    - wind_direction_deg: must be in [0.0, 360.0]. Out of range is set to NaN.
    - solar_radiation_wm2: must be >= 0.0. Negative radiation is set to NaN.
    """
    df_out = df.copy()
    violations: dict[str, int] = {}

    if "precipitation_mm" in df_out.columns:
        neg_mask = df_out["precipitation_mm"] < 0.0
        neg_count = int(neg_mask.sum())
        violations["negative_precipitation"] = neg_count
        if neg_count > 0:
            df_out.loc[neg_mask, "precipitation_mm"] = np.nan

    if "humidity_percent" in df_out.columns:
        bad_mask = (df_out["humidity_percent"] < 0.0) | (df_out["humidity_percent"] > 100.0)
        bad_count = int(bad_mask.sum())
        violations["out_of_range_humidity"] = bad_count
        if bad_count > 0:
            df_out.loc[bad_mask, "humidity_percent"] = np.nan

    if "cloud_cover_percent" in df_out.columns:
        bad_mask = (df_out["cloud_cover_percent"] < 0.0) | (df_out["cloud_cover_percent"] > 100.0)
        bad_count = int(bad_mask.sum())
        violations["out_of_range_cloud_cover"] = bad_count
        if bad_count > 0:
            df_out.loc[bad_mask, "cloud_cover_percent"] = np.nan

    if "wind_speed_ms" in df_out.columns:
        neg_mask = df_out["wind_speed_ms"] < 0.0
        neg_count = int(neg_mask.sum())
        violations["negative_wind_speed"] = neg_count
        if neg_count > 0:
            df_out.loc[neg_mask, "wind_speed_ms"] = np.nan

    if "wind_direction_deg" in df_out.columns:
        bad_mask = (df_out["wind_direction_deg"] < 0.0) | (df_out["wind_direction_deg"] > 360.0)
        bad_count = int(bad_mask.sum())
        violations["invalid_wind_direction"] = bad_count
        if bad_count > 0:
            df_out.loc[bad_mask, "wind_direction_deg"] = np.nan

    if "solar_radiation_wm2" in df_out.columns:
        neg_mask = df_out["solar_radiation_wm2"] < 0.0
        neg_count = int(neg_mask.sum())
        violations["negative_solar_radiation"] = neg_count
        if neg_count > 0:
            df_out.loc[neg_mask, "solar_radiation_wm2"] = np.nan

    return df_out, violations


def resolve_duplicates_with_quarantine(
    df: pd.DataFrame,
    key_cols: list[str],
    measurement_cols: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Deduplicate records symmetrically and deterministically.

    Separates identical duplicates (keep one deterministically) from conflicting duplicates
    (exclude from clean data and quarantine, because station selection / conflict precedence
    remains an open decision per docs/data-contract.md).
    Order of rows in input does NOT affect which rows are kept or quarantined.
    """
    if df.empty:
        return (
            df.copy(),
            pd.DataFrame(),
            {
                "identical_duplicates_removed": 0,
                "conflicting_duplicates_count": 0,
                "conflicting_groups": 0,
                "conflicting_details": [],
            },
        )

    # Identify duplicate key groups
    dup_key_mask = df.duplicated(subset=key_cols, keep=False)
    if not dup_key_mask.any():
        return (
            df.copy(),
            pd.DataFrame(),
            {
                "identical_duplicates_removed": 0,
                "conflicting_duplicates_count": 0,
                "conflicting_groups": 0,
                "conflicting_details": [],
            },
        )

    clean_rows_indices = []
    quarantine_rows_indices = []
    identical_removed_count = 0
    conflicting_groups_count = 0
    conflicting_details = []

    # Non-duplicate records go directly to clean
    non_dup_indices = df.index[~dup_key_mask].tolist()
    clean_rows_indices.extend(non_dup_indices)

    # Group duplicate keys
    dup_df = df[dup_key_mask]
    grouped = dup_df.groupby(key_cols, sort=True)

    available_measurements = [c for c in measurement_cols if c in df.columns]

    for key_vals, group in grouped:
        # Check if all rows in the group have identical measurements (or both NaN)
        # Symmetrically compare all rows to the first row in sorted group
        sorted_group = group.sort_index()
        is_conflicting = False
        first_row = sorted_group.iloc[0]

        for _, row in sorted_group.iloc[1:].iterrows():
            for mcol in available_measurements:
                v1 = first_row[mcol]
                v2 = row[mcol]
                if pd.isna(v1) and pd.isna(v2):
                    continue
                if v1 != v2:
                    is_conflicting = True
                    break
            if is_conflicting:
                break

        if is_conflicting:
            conflicting_groups_count += 1
            quarantine_rows_indices.extend(sorted_group.index.tolist())
            conflicting_details.append(
                {
                    "key": dict(
                        zip(key_cols, key_vals if isinstance(key_vals, tuple) else [key_vals])
                    ),
                    "row_count": len(sorted_group),
                    "stations": sorted_group["station_id"].tolist()
                    if "station_id" in sorted_group.columns
                    else [],
                }
            )
        else:
            # Identical duplicates: keep deterministically the one with min index (or any, since identical)
            # Reversing input does not alter the values kept
            clean_rows_indices.append(sorted_group.index[0])
            identical_removed_count += len(sorted_group) - 1

    clean_df = df.loc[clean_rows_indices].sort_values(key_cols).reset_index(drop=True)
    quarantine_df = (
        df.loc[quarantine_rows_indices].sort_values(key_cols).reset_index(drop=True)
        if quarantine_rows_indices
        else pd.DataFrame()
    )

    stats = {
        "identical_duplicates_removed": identical_removed_count,
        "conflicting_duplicates_count": len(quarantine_rows_indices),
        "conflicting_groups": conflicting_groups_count,
        "conflicting_details": conflicting_details,
    }

    return clean_df, quarantine_df, stats
