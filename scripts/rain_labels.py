"""RainNext24h label generation and chronological split assignment.

Implements the label contract in docs/rain-label.md (v0.2; implementation v0.2.1):
- Hourly precipitation is timestamped at interval end ("sum of the preceding hour"), so the
  value at hour h covers (h - 1h, h]. The target window (t, t+24h] uses the values
  timestamped t+1h through t+24h.
- A label exists only when all 24 target hours are present and non-null; missing is not No.
- Labels are computed per location on a gap-checked UTC hourly grid, never with a plain
  positional shift.
- Splits use the UTC forecast origin. A label whose target window reaches the next split
  boundary (target_end >= split_end) is purged, because the rain value timestamped exactly
  at the boundary already belongs to the next split.
"""

from __future__ import annotations

import pandas as pd

LABEL_VERSION = "rain_next24h_v0.2.1"  # v0.2 definition; v0.2.1 fixes float sums at the threshold
DEFAULT_THRESHOLD_MM = 1.0
HORIZON_HOURS = 24
# Rain sums are rounded before comparison or storage. Float addition of 0.1 mm values can
# give 0.99999999999999 for an exact 1.0 mm total, which turned such windows into No in
# rain_next24h_v0.2. Sources resolve 0.01-0.1 mm, so 3 decimals remove only float error.
RAIN_SUM_DECIMALS = 3


def round_rain(values: pd.Series) -> pd.Series:
    """Round summed rain to RAIN_SUM_DECIMALS (removes float error, keeps NaN)."""
    return values.round(RAIN_SUM_DECIMALS)


# UTC split boundaries as [start, end) on forecast origin (docs/rain-label.md).
SPLITS: tuple[tuple[str, pd.Timestamp, pd.Timestamp], ...] = (
    ("train", pd.Timestamp("2016-01-01", tz="UTC"), pd.Timestamp("2024-01-01", tz="UTC")),
    ("validation", pd.Timestamp("2024-01-01", tz="UTC"), pd.Timestamp("2025-01-01", tz="UTC")),
    ("test", pd.Timestamp("2025-01-01", tz="UTC"), pd.Timestamp("2026-01-01", tz="UTC")),
)


def _require_aware_utc(ts: pd.Series) -> pd.Series:
    if not isinstance(ts.dtype, pd.DatetimeTZDtype):
        raise ValueError("timestamp_utc must be timezone-aware; naive timestamps are rejected")
    return ts.dt.tz_convert("UTC")


def build_rain_labels(
    df: pd.DataFrame,
    threshold_mm: float = DEFAULT_THRESHOLD_MM,
    location_col: str = "location_code",
    time_col: str = "timestamp_utc",
    precip_col: str = "precipitation_mm",
) -> pd.DataFrame:
    """Build one RainNext24h row per location and origin hour.

    Input must hold a single source with unique (location, timestamp) rows. Returns columns:
    location_code, origin_utc, target_start_utc, target_end_utc, rain_next24h_mm,
    valid_hours, rain_next24h (nullable boolean), threshold_mm, label_version.
    """
    if threshold_mm <= 0:
        raise ValueError("threshold_mm must be positive")
    if df.duplicated([location_col, time_col]).any():
        raise ValueError("Duplicate (location, timestamp) rows; resolve duplicates before labeling")

    data = df[[location_col, time_col, precip_col]].copy()
    data[time_col] = _require_aware_utc(data[time_col])
    if (data[time_col] != data[time_col].dt.floor("h")).any():
        raise ValueError("timestamp_utc values must lie on the hourly grid")

    frames = []
    for loc, g in data.groupby(location_col, sort=True):
        series = g.set_index(time_col)[precip_col].astype(float).sort_index()
        grid = pd.date_range(series.index.min(), series.index.max(), freq="h", tz="UTC")
        hourly = series.reindex(grid)  # absent hours become NaN, never zero

        # Reverse rolling over 24 values starting at t+1h gives the (t, t+24h] window.
        future = hourly.shift(-1)
        reversed_future = future.iloc[::-1]
        window_sum = reversed_future.rolling(HORIZON_HOURS, min_periods=HORIZON_HOURS).sum()
        valid_hours = (
            reversed_future.notna().astype(int).rolling(HORIZON_HOURS, min_periods=1).sum()
        )
        window_sum = round_rain(window_sum.iloc[::-1])
        valid_hours = valid_hours.iloc[::-1]

        out = pd.DataFrame(
            {
                "location_code": loc,
                "origin_utc": grid,
                "target_start_utc": grid,
                "target_end_utc": grid + pd.Timedelta(hours=HORIZON_HOURS),
                "rain_next24h_mm": window_sum.to_numpy(),
                "valid_hours": valid_hours.to_numpy().astype(int),
            }
        )
        complete = out["valid_hours"] == HORIZON_HOURS
        label = pd.Series(pd.NA, index=out.index, dtype="boolean")
        label[complete] = out.loc[complete, "rain_next24h_mm"] >= threshold_mm
        out["rain_next24h"] = label
        out.loc[~complete, "rain_next24h_mm"] = float("nan")
        frames.append(out)

    if not frames:
        return pd.DataFrame(
            columns=[
                "location_code",
                "origin_utc",
                "target_start_utc",
                "target_end_utc",
                "rain_next24h_mm",
                "valid_hours",
                "rain_next24h",
                "threshold_mm",
                "label_version",
            ]
        )
    labels = pd.concat(frames, ignore_index=True)
    labels["threshold_mm"] = threshold_mm
    labels["label_version"] = LABEL_VERSION
    return labels


def assign_splits(labels: pd.DataFrame) -> pd.DataFrame:
    """Add `split` (by UTC origin) and `exclusion_reason`; usable rows have no reason.

    Reasons: `missing_label` (incomplete 24h window), `boundary_purge` (target reaches the next
    split), `out_of_scope` (origin outside every split).
    """
    out = labels.copy()
    split = pd.Series(pd.NA, index=out.index, dtype="string")
    reason = pd.Series(pd.NA, index=out.index, dtype="string")

    for name, start, end in SPLITS:
        in_split = (out["origin_utc"] >= start) & (out["origin_utc"] < end)
        split[in_split] = name
        reason[in_split & (out["target_end_utc"] >= end)] = "boundary_purge"

    reason[split.isna()] = "out_of_scope"
    reason[reason.isna() & out["rain_next24h"].isna()] = "missing_label"

    out["split"] = split
    out["exclusion_reason"] = reason
    return out
