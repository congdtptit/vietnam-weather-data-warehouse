"""Exploratory data analysis helpers for RainNext24h (workbook ID 21).

The notebook `notebooks/eda_rain_next24h.ipynb` only plots what these functions return,
so the numbers are testable and reproducible outside Jupyter.

Leakage rule (docs/rain-label.md): anything used to *choose* features is computed on the
train split only. Class balance per split is reported for all splits because it describes
the labels, not the features.
"""

from __future__ import annotations

import io

import numpy as np
import pandas as pd

# Silver Open-Meteo variables explored at the forecast origin t
ORIGIN_VARIABLES = [
    "temperature_c",
    "dew_point_c",
    "humidity_percent",
    "pressure_hpa",
    "cloud_cover_percent",
    "wind_speed_ms",
    "solar_radiation_wm2",
    "precipitation_mm",
]
DERIVED_VARIABLES = ["dew_point_depression_c", "rain_past_24h_mm", "pressure_change_24h_hpa"]


def load_labels(client, bucket: str, key: str) -> pd.DataFrame:
    resp = client.get_object(bucket, key)
    try:
        return pd.read_parquet(io.BytesIO(resp.read()))
    finally:
        resp.close()
        resp.release_conn()


def add_origin_variables(labels: pd.DataFrame, hourly: pd.DataFrame) -> pd.DataFrame:
    """Join variables observed at origin t and trailing derivations (past data only).

    `hourly` must hold one row per location and UTC hour. Trailing windows are computed per
    location on a gap-checked hourly grid, so a missing hour yields NaN instead of a window
    that silently spans more than 24 hours.
    """
    if hourly.duplicated(["location_code", "timestamp_utc"]).any():
        raise ValueError("hourly data has duplicate (location, timestamp) rows")
    parts = []
    for loc, g in hourly.groupby("location_code", sort=True):
        g = g.set_index("timestamp_utc").sort_index()
        grid = pd.date_range(g.index.min(), g.index.max(), freq="h", tz="UTC")
        g = g.reindex(grid)
        g["location_code"] = loc
        # Values stamped <= t only: trailing windows ending at t
        g["rain_past_24h_mm"] = g["precipitation_mm"].rolling(24, min_periods=24).sum()
        g["pressure_change_24h_hpa"] = g["pressure_hpa"] - g["pressure_hpa"].shift(24)
        g["dew_point_depression_c"] = g["temperature_c"] - g["dew_point_c"]
        parts.append(g.rename_axis("origin_utc").reset_index())
    origin = pd.concat(parts, ignore_index=True)
    cols = ["location_code", "origin_utc", *ORIGIN_VARIABLES, *DERIVED_VARIABLES]
    return labels.merge(origin[cols], on=["location_code", "origin_utc"], how="left")


def usable(df: pd.DataFrame) -> pd.DataFrame:
    """Rows with a label and no exclusion reason."""
    return df[df["exclusion_reason"].isna() & df["rain_next24h"].notna()]


def class_balance(df: pd.DataFrame) -> pd.DataFrame:
    u = usable(df)
    out = u.groupby("split")["rain_next24h"].agg(
        rows="size", positive_rate=lambda s: s.astype(float).mean()
    )
    return out.reindex(["train", "validation", "test"])


def positive_rate_by(df: pd.DataFrame, by: list[str], split: str = "train") -> pd.DataFrame:
    u = usable(df)
    u = u[u["split"] == split]
    return u.groupby(by)["rain_next24h"].agg(
        rows="size", positive_rate=lambda s: s.astype(float).mean()
    )


def month_location_matrix(df: pd.DataFrame, split: str = "train") -> pd.DataFrame:
    u = usable(df)
    u = u[u["split"] == split].assign(month=lambda x: x["origin_utc"].dt.month)
    return u.pivot_table(
        index="location_code",
        columns="month",
        values="rain_next24h",
        aggfunc=lambda s: s.astype(float).mean(),
    )


def missingness(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    """Share of missing values per column among usable rows (all splits)."""
    u = usable(df)
    return u[columns].isna().mean().rename("missing_share")


def target_rain_distribution(df: pd.DataFrame, split: str = "train") -> pd.Series:
    u = usable(df)
    r = u.loc[u["split"] == split, "rain_next24h_mm"].astype(float)
    q = r.quantile([0.25, 0.5, 0.75, 0.9, 0.99])
    q.index = [f"q{int(i * 100)}" for i in q.index]
    return pd.concat(
        [pd.Series({"share_zero": float((r == 0).mean()), "mean": float(r.mean())}), q]
    )


def variable_separation(df: pd.DataFrame, columns: list[str], split: str = "train") -> pd.DataFrame:
    """Per-variable mean by class and standardized mean difference (train split by default).

    SMD = (mean_yes - mean_no) / pooled_std. |SMD| >= 0.5 is a medium separation; it guides
    which raw variables are worth turning into features in ID 23.
    """
    u = usable(df)
    u = u[u["split"] == split]
    yes = u[u["rain_next24h"].astype(bool)]
    no = u[~u["rain_next24h"].astype(bool)]
    rows = []
    for col in columns:
        a, b = yes[col].dropna(), no[col].dropna()
        pooled = np.sqrt((a.var(ddof=1) + b.var(ddof=1)) / 2)
        rows.append(
            {
                "variable": col,
                "mean_yes": a.mean(),
                "mean_no": b.mean(),
                "smd": (a.mean() - b.mean()) / pooled if pooled > 0 else np.nan,
            }
        )
    out = pd.DataFrame(rows).set_index("variable")
    return out.reindex(out["smd"].abs().sort_values(ascending=False).index)


def binned_positive_rate(
    df: pd.DataFrame, column: str, bins: list[float], split: str = "train"
) -> pd.DataFrame:
    u = usable(df)
    u = u[(u["split"] == split) & u[column].notna()]
    cut = pd.cut(u[column], bins=bins, include_lowest=True, right=False)
    return u.groupby(cut, observed=True)["rain_next24h"].agg(
        rows="size", positive_rate=lambda s: s.astype(float).mean()
    )
