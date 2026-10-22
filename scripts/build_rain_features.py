#!/usr/bin/env python3
"""scripts/build_rain_features.py.

Build the CART RainNext24h feature dataset in Gold ML (workbook ID 23).

Inputs:
  gold/ml_labels/rain_next24h_labels_<label dataset version>.parquet   (ID 22)
  silver/weather_hourly/...  Silver Open-Meteo hourly observations (feature source decided
                             on 2026-10-21: Open-Meteo only)

Leakage rule (docs/rain-label.md): every feature at origin t uses only values stamped at
or before t, computed per location on a gap-checked hourly grid. A lag or trailing window
that touches a missing hour is NaN; nothing is filled. Imputation and feature selection
belong to model training (ID 28) and are fitted on train only.

Outputs (bucket gold), usable labels only, one row per location x UTC origin hour:
  ml_features/cart_rain_train_2016_2023_<version>.parquet
  ml_features/cart_rain_val_2024_<version>.parquet
  ml_features/cart_rain_test_2025_<version>.parquet
  ml_features/cart_rain_features_<version>.json      manifest: ordered features, units,
                                                     versions, counts, missing shares

Open-Meteo is reanalysis published with a delay, so this dataset supports a retrospective
experiment, not a verified real-time forecast.
"""

import argparse
import io
import json
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from export_rain_labels import DATASET_VERSION as LABEL_DATASET_VERSION
from export_rain_labels import LABEL_SOURCE, labels_keys, read_open_meteo_silver, refuse_overwrite
from load_gold_observations import BUCKET_GOLD, get_minio_client
from minio import Minio
from rain_labels import LABEL_VERSION, SPLITS, round_rain

FEATURE_VERSION = "v2"  # v2: labels v0.2.1 and rounded rain sums; v1 kept for model v1
FEATURE_SOURCE = "Open-Meteo"
LOCAL_OFFSET = pd.Timedelta(hours=7)  # Vietnam, UTC+7, no daylight saving

SILVER_COLUMNS = [
    "location_code",
    "timestamp_utc",
    "temperature_c",
    "dew_point_c",
    "humidity_percent",
    "pressure_hpa",
    "cloud_cover_percent",
    "wind_speed_ms",
    "precipitation_mm",
]

# Ordered feature list with units. Model training must use exactly this order.
FEATURES: dict[str, str] = {
    # Values at origin t
    "temperature_c": "degC",
    "dew_point_c": "degC",
    "dew_point_depression_c": "degC",
    "humidity_percent": "%",
    "pressure_hpa": "hPa (surface)",
    "cloud_cover_percent": "%",
    "wind_speed_ms": "m/s",
    "precipitation_mm": "mm (hour ending at t)",
    # Lags
    "humidity_percent_lag3h": "%",
    "humidity_percent_lag6h": "%",
    "cloud_cover_percent_lag3h": "%",
    "cloud_cover_percent_lag6h": "%",
    # Trailing windows ending at t
    "rain_past_3h_mm": "mm",
    "rain_past_6h_mm": "mm",
    "rain_past_12h_mm": "mm",
    "rain_past_24h_mm": "mm",
    "humidity_mean_6h_percent": "%",
    "cloud_cover_mean_6h_percent": "%",
    # Pressure tendency
    "pressure_change_3h_hpa": "hPa",
    "pressure_change_24h_hpa": "hPa",
    # Calendar (UTC+7 local time of the origin)
    "hour_local": "hour 0-23",
    "month_local": "month 1-12",
}
CATEGORICAL_FEATURES = {"location_code": "location code; encode in the training pipeline"}

ID_COLUMNS = ["location_code", "origin_utc", "target_start_utc", "target_end_utc", "split"]
TARGET_COLUMNS = ["rain_next24h", "rain_next24h_mm"]  # rain_next24h_mm is never a feature

SPLIT_FILE_TAGS = {"train": "train_2016_2023", "validation": "val_2024", "test": "test_2025"}


def features_keys(version: str = FEATURE_VERSION) -> tuple[dict[str, str], str]:
    split_keys = {
        split: f"ml_features/cart_rain_{tag}_{version}.parquet"
        for split, tag in SPLIT_FILE_TAGS.items()
    }
    return split_keys, f"ml_features/cart_rain_features_{version}.json"


def compute_features(hourly: pd.DataFrame) -> pd.DataFrame:
    """Return one feature row per location and UTC hour of its gap-checked grid.

    `hourly` must hold one row per location and UTC hour. Lags use time offsets on the
    reindexed grid, so a missing hour becomes NaN instead of shifting to an older hour.
    """
    if hourly.duplicated(["location_code", "timestamp_utc"]).any():
        raise ValueError("hourly data has duplicate (location, timestamp) rows")
    parts = []
    for loc, g in hourly.groupby("location_code", sort=True):
        g = g.set_index("timestamp_utc").sort_index()
        grid = pd.date_range(g.index.min(), g.index.max(), freq="h", tz="UTC")
        g = g.reindex(grid)  # absent hours become NaN rows; values are never filled

        f = pd.DataFrame(index=grid)
        for col in (
            "temperature_c",
            "dew_point_c",
            "humidity_percent",
            "pressure_hpa",
            "cloud_cover_percent",
            "wind_speed_ms",
            "precipitation_mm",
        ):
            f[col] = g[col]
        f["dew_point_depression_c"] = g["temperature_c"] - g["dew_point_c"]

        for col in ("humidity_percent", "cloud_cover_percent"):
            for lag in (3, 6):
                f[f"{col}_lag{lag}h"] = g[col].shift(lag)

        # Trailing windows (t - n + 1h .. t]; incomplete windows stay NaN
        for n in (3, 6, 12, 24):
            f[f"rain_past_{n}h_mm"] = round_rain(
                g["precipitation_mm"].rolling(n, min_periods=n).sum()
            )
        f["humidity_mean_6h_percent"] = g["humidity_percent"].rolling(6, min_periods=6).mean()
        f["cloud_cover_mean_6h_percent"] = g["cloud_cover_percent"].rolling(6, min_periods=6).mean()

        f["pressure_change_3h_hpa"] = g["pressure_hpa"] - g["pressure_hpa"].shift(3)
        f["pressure_change_24h_hpa"] = g["pressure_hpa"] - g["pressure_hpa"].shift(24)

        local = grid + LOCAL_OFFSET
        f["hour_local"] = local.hour.astype("int8")
        f["month_local"] = local.month.astype("int8")

        f.insert(0, "location_code", loc)
        parts.append(f.rename_axis("origin_utc").reset_index())
    out = pd.concat(parts, ignore_index=True)
    return out[["location_code", "origin_utc", *FEATURES]]


def build_feature_dataset(
    labels: pd.DataFrame, hourly: pd.DataFrame, version: str = FEATURE_VERSION
) -> pd.DataFrame:
    """Join usable labels with the features at their origin."""
    if (labels["label_version"] != LABEL_VERSION).any():
        raise ValueError(f"labels are not {LABEL_VERSION}; rebuild labels before features")
    usable = labels[labels["exclusion_reason"].isna()].copy()
    if usable["rain_next24h"].isna().any():
        raise ValueError("usable labels contain missing values")
    usable["rain_next24h"] = usable["rain_next24h"].astype(bool)

    feats = compute_features(hourly)
    data = usable.merge(feats, on=["location_code", "origin_utc"], how="left", indicator=True)
    no_origin = int((data["_merge"] != "both").sum())
    if no_origin:
        raise ValueError(f"{no_origin} usable labels have no hourly origin row in Silver")
    data = data.drop(columns="_merge")
    data["feature_version"] = version
    cols = [
        *ID_COLUMNS,
        *FEATURES,
        *TARGET_COLUMNS,
        "threshold_mm",
        "label_version",
        "feature_version",
    ]
    return data[cols].sort_values(["split", "location_code", "origin_utc"]).reset_index(drop=True)


def read_labels(client: Minio, version: str = LABEL_DATASET_VERSION) -> pd.DataFrame:
    key, _ = labels_keys(version)
    resp = client.get_object(BUCKET_GOLD, key)
    try:
        labels = pd.read_parquet(io.BytesIO(resp.read()))
    finally:
        resp.close()
        resp.release_conn()
    if (labels["label_source"] != LABEL_SOURCE).any():
        raise ValueError(f"{key}: labels are not from {LABEL_SOURCE}")
    return labels


def summarize(dataset: pd.DataFrame) -> dict[str, Any]:
    by_split = {}
    for split, g in dataset.groupby("split"):
        by_split[split] = {
            "rows": len(g),
            "positive_rate": round(float(g["rain_next24h"].mean()), 4),
            "first_origin_utc": g["origin_utc"].min().isoformat(),
            "last_origin_utc": g["origin_utc"].max().isoformat(),
            "rows_with_any_missing_feature": int(g[list(FEATURES)].isna().any(axis=1).sum()),
        }
    missing = dataset[list(FEATURES)].isna().sum()
    return {
        "rows": len(dataset),
        "by_split": by_split,
        "missing_values_per_feature": {k: int(v) for k, v in missing.items() if v},
    }


def export_features(
    start_year: int = 2016,
    end_year: int = 2025,
    version: str = FEATURE_VERSION,
    label_dataset_version: str = LABEL_DATASET_VERSION,
    client: Minio | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    client = client or get_minio_client()
    split_keys, manifest_key = features_keys(version)
    refuse_overwrite(client, [*split_keys.values(), manifest_key], overwrite)
    labels = read_labels(client, label_dataset_version)
    first = pd.Timestamp(year=start_year, month=1, day=1, tz="UTC")
    after = pd.Timestamp(year=end_year + 1, month=1, day=1, tz="UTC")
    labels = labels[(labels["origin_utc"] >= first) & (labels["origin_utc"] < after)]
    hourly = read_open_meteo_silver(client, start_year, end_year, columns=SILVER_COLUMNS)
    dataset = build_feature_dataset(labels, hourly, version)

    usable_labels = int(labels["exclusion_reason"].isna().sum())
    if len(dataset) != usable_labels:
        raise ValueError(f"{len(dataset)} feature rows for {usable_labels} usable labels")

    objects = {}
    for split, g in dataset.groupby("split"):
        buf = io.BytesIO()
        g.reset_index(drop=True).to_parquet(
            buf, index=False, engine="pyarrow", compression="snappy"
        )
        data = buf.getvalue()
        client.put_object(BUCKET_GOLD, split_keys[split], io.BytesIO(data), len(data))
        objects[split] = {"object": f"{BUCKET_GOLD}/{split_keys[split]}", "bytes": len(data)}

    manifest = {
        "dataset": "cart_rain_features",
        "version": version,
        "feature_source": FEATURE_SOURCE,
        "availability": "retrospective: reanalysis values, publication latency not modelled",
        "label_dataset": f"{BUCKET_GOLD}/{labels_keys(label_dataset_version)[0]}",
        "label_version": LABEL_VERSION,
        "features": [{"name": k, "unit": v} for k, v in FEATURES.items()],
        "categorical_features": [{"name": k, "note": v} for k, v in CATEGORICAL_FEATURES.items()],
        "id_columns": ID_COLUMNS,
        "target_columns": TARGET_COLUMNS,
        "leakage_rule": "values stamped <= origin t, per location, gap-checked hourly grid, "
        "no filling",
        "splits_utc": {name: [s.isoformat(), e.isoformat()] for name, s, e in SPLITS},
        "input_scope": {
            "silver_prefix": "weather_hourly/",
            "years": [start_year, end_year],
            "hourly_rows": len(hourly),
            "usable_labels": usable_labels,
        },
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "objects": objects,
        **summarize(dataset),
    }
    mbytes = json.dumps(manifest, indent=2).encode("utf-8")
    client.put_object(
        BUCKET_GOLD, manifest_key, io.BytesIO(mbytes), len(mbytes), content_type="application/json"
    )
    print(
        json.dumps(
            {k: manifest[k] for k in ("rows", "by_split", "missing_values_per_feature")},
            indent=2,
        )
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Build RainNext24h CART features in Gold ML.")
    parser.add_argument("--start-year", type=int, required=True, help="First year (inclusive)")
    parser.add_argument("--end-year", type=int, required=True, help="Last year (inclusive)")
    parser.add_argument("--version", default=FEATURE_VERSION, help="Feature dataset version")
    parser.add_argument(
        "--label-version", default=LABEL_DATASET_VERSION, help="Label dataset version to read"
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Replace an existing dataset of this version"
    )
    args = parser.parse_args()
    try:
        export_features(
            args.start_year,
            args.end_year,
            args.version,
            args.label_version,
            overwrite=args.overwrite,
        )
    except (RuntimeError, ValueError) as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
