#!/usr/bin/env python3
"""scripts/export_rain_labels.py.

Export the RainNext24h label table to Gold ML (workbook ID 22).

Input: Silver Open-Meteo hourly observations (the label source decided in
docs/rain-label.md v0.2). Labels use scripts/rain_labels.py: 1 mm over (t, t+24h],
24 valid hours required, UTC splits with boundary purge.

Output (bucket gold):
  ml_labels/rain_next24h_labels_<version>.parquet   one row per location x UTC origin hour,
                                                     including excluded rows with a reason
  ml_labels/rain_next24h_labels_<version>.json      manifest: scope, counts, positive rates
"""

import argparse
import io
import json
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from load_gold_observations import BUCKET_GOLD, BUCKET_SILVER, get_minio_client
from minio import Minio
from rain_labels import DEFAULT_THRESHOLD_MM, LABEL_VERSION, assign_splits, build_rain_labels
from station_config import CANONICAL_LOCATIONS

LABEL_SOURCE = "Open-Meteo"
DATASET_VERSION = "v2"  # v2: labels rain_next24h_v0.2.1 (float-sum fix); v1 kept for model v1


def object_exists(client: Minio, key: str, bucket: str = BUCKET_GOLD) -> bool:
    try:
        client.stat_object(bucket, key)
        return True
    except Exception as ex:
        if getattr(ex, "code", None) in ("NoSuchKey", "NoSuchObject"):
            return False
        raise


def refuse_overwrite(client: Minio, keys: list[str], overwrite: bool = False) -> None:
    """Refuse to replace published Gold ML objects unless the caller asks explicitly.

    Versioned datasets and models are referenced by later steps (features, model reports,
    test evaluations, dim_model), so a silent rewrite under the same tag breaks lineage.
    """
    if overwrite:
        return
    existing = [k for k in keys if object_exists(client, k)]
    if existing:
        raise RuntimeError(
            f"{', '.join(existing)} already exist; use a new --version, or pass --overwrite "
            "only if nothing downstream uses this version"
        )


def already_published(client: Minio, manifest_key: str) -> bool:
    """True when a dataset's manifest exists, so `--skip-existing` can leave the dataset as it is.

    Every Gold ML writer puts its manifest last, after the data objects, so the manifest marks a
    completed write. Data objects without a manifest are a partial write; `refuse_overwrite`
    still stops them.
    """
    return object_exists(client, manifest_key)


def labels_keys(version: str = DATASET_VERSION) -> tuple[str, str]:
    base = f"ml_labels/rain_next24h_labels_{version}"
    return f"{base}.parquet", f"{base}.json"


def read_open_meteo_silver(
    client: Minio,
    start_year: int,
    end_year: int,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Read Silver Open-Meteo hourly rows for the canonical locations and year range.

    Raises if any canonical location-month in the range is missing, so labels are never
    built from an incomplete snapshot without notice.
    """
    columns = columns or ["location_code", "timestamp_utc", "precipitation_mm"]
    frames: list[pd.DataFrame] = []
    missing: list[str] = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            for loc in sorted(CANONICAL_LOCATIONS):
                key = f"weather_hourly/year={year}/month={month:02d}/weather_hourly_{loc}_{year}{month:02d}.parquet"
                try:
                    resp = client.get_object(BUCKET_SILVER, key)
                except Exception as ex:
                    if getattr(ex, "code", None) == "NoSuchKey":
                        missing.append(key)
                        continue
                    raise
                try:
                    df = pd.read_parquet(io.BytesIO(resp.read()))
                finally:
                    resp.close()
                    resp.release_conn()
                if "source_name" in df.columns and (df["source_name"] != LABEL_SOURCE).any():
                    raise ValueError(f"{key}: unexpected source in Open-Meteo Silver object")
                frames.append(df[columns])
    if missing:
        raise RuntimeError(f"{len(missing)} Silver Open-Meteo objects missing, e.g. {missing[:3]}")
    data = pd.concat(frames, ignore_index=True)
    data["timestamp_utc"] = pd.to_datetime(data["timestamp_utc"], utc=True)
    return data.sort_values(["location_code", "timestamp_utc"]).reset_index(drop=True)


def summarize(labels: pd.DataFrame) -> dict[str, Any]:
    usable = labels[labels["exclusion_reason"].isna()]
    by_split = {}
    for split, g in usable.groupby("split"):
        by_split[split] = {
            "rows": len(g),
            "positive_rate": round(float(g["rain_next24h"].astype(float).mean()), 4),
            "first_origin_utc": g["origin_utc"].min().isoformat(),
            "last_origin_utc": g["origin_utc"].max().isoformat(),
        }
    by_location = {
        loc: {
            "rows": len(g),
            "positive_rate": round(float(g["rain_next24h"].astype(float).mean()), 4),
        }
        for loc, g in usable.groupby("location_code")
    }
    return {
        "rows_total": len(labels),
        "rows_usable": len(usable),
        "excluded": {
            str(k): int(v) for k, v in labels["exclusion_reason"].value_counts(dropna=True).items()
        },
        "by_split": by_split,
        "by_location": by_location,
    }


def export_labels(
    start_year: int = 2016,
    end_year: int = 2025,
    threshold_mm: float = DEFAULT_THRESHOLD_MM,
    version: str = DATASET_VERSION,
    client: Minio | None = None,
    overwrite: bool = False,
    skip_existing: bool = False,
) -> dict[str, Any]:
    client = client or get_minio_client()
    parquet_key, manifest_key = labels_keys(version)
    if skip_existing and not overwrite and already_published(client, manifest_key):
        print(f"[=] {BUCKET_GOLD}/{manifest_key} already exists; nothing written (--skip-existing)")
        return {}
    refuse_overwrite(client, [parquet_key, manifest_key], overwrite)
    hourly = read_open_meteo_silver(client, start_year, end_year)
    labels = assign_splits(build_rain_labels(hourly, threshold_mm=threshold_mm))
    labels.insert(0, "label_source", LABEL_SOURCE)

    buf = io.BytesIO()
    labels.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    data = buf.getvalue()
    client.put_object(BUCKET_GOLD, parquet_key, io.BytesIO(data), len(data))

    manifest = {
        "dataset": "rain_next24h_labels",
        "version": version,
        "label_version": LABEL_VERSION,
        "label_source": LABEL_SOURCE,
        "threshold_mm": threshold_mm,
        "window": "(t, t+24h] on hourly values stamped at interval end",
        "input_scope": {
            "silver_prefix": "weather_hourly/",
            "years": [start_year, end_year],
            "locations": sorted(CANONICAL_LOCATIONS),
            "hourly_rows": len(hourly),
        },
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "object": f"{BUCKET_GOLD}/{parquet_key}",
        "object_bytes": len(data),
        **summarize(labels),
    }
    mbytes = json.dumps(manifest, indent=2).encode("utf-8")
    client.put_object(
        BUCKET_GOLD, manifest_key, io.BytesIO(mbytes), len(mbytes), content_type="application/json"
    )
    print(
        json.dumps(
            {k: manifest[k] for k in ("rows_total", "rows_usable", "excluded", "by_split")},
            indent=2,
        )
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Export RainNext24h labels to Gold ML.")
    parser.add_argument("--start-year", type=int, required=True, help="First year (inclusive)")
    parser.add_argument("--end-year", type=int, required=True, help="Last year (inclusive)")
    parser.add_argument("--threshold-mm", type=float, default=DEFAULT_THRESHOLD_MM)
    parser.add_argument("--version", default=DATASET_VERSION, help="Dataset version tag")
    parser.add_argument(
        "--overwrite", action="store_true", help="Replace an existing dataset of this version"
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Exit 0 without writing when the whole dataset of this version exists",
    )
    args = parser.parse_args()
    try:
        export_labels(
            args.start_year,
            args.end_year,
            args.threshold_mm,
            args.version,
            overwrite=args.overwrite,
            skip_existing=args.skip_existing,
        )
    except (RuntimeError, ValueError) as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
