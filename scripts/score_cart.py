#!/usr/bin/env python3
"""scripts/score_cart.py.

Score the frozen CART RainNext24h model for a window of UTC forecast origins and write the
predictions to Gold (workbook ID 32). dags/dag_rain_scoring.py runs it, then loads the
predictions into the DW with scripts/publish_predictions.py.

Checks before anything is written:
- Artifact: model.joblib must have the SHA-256 recorded by the test evaluation, and its
  report must list the same ordered features as the feature code (build_rain_features).
- Window: origins must lie in the model's test split. Only out-of-sample predictions are
  published (decided 2026-10-21); train and validation fitted or selected the model. Each
  target window (t, t+24h] must end inside the project scope (last hour 2025-12-31 23:00
  UTC), so the last origin day is 2025-12-30.
- Feature availability: features are computed from Silver Open-Meteo with
  build_rain_features.compute_features. Each feature at origin t uses only values stamped at
  or before t, and one day of history is read before the window. A missing Silver object
  fails the run. An origin with a missing feature fails the run too, unless
  --allow-missing (it is then reported and not scored).

Open-Meteo is reanalysis published with a delay, so the scores are a retrospective
experiment, not a real-time forecast (see the model's availability note).

Output (bucket gold), one object per UTC origin day, rewritten only when its content changes:
  predictions/year=YYYY/month=MM/rain_predictions_cart_<YYYYMMDD>.parquet
Report: quality_reports/scoring_cart_<version>_<start>_<end>_<timestamp>.json
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
from datetime import date, datetime, timezone
from typing import Any

import joblib
import pandas as pd
from build_rain_features import FEATURE_SOURCE, FEATURES, compute_features
from evaluate_cart_test import _exists, evaluation_keys
from export_rain_labels import LABEL_SOURCE
from load_gold_observations import BUCKET_GOLD, BUCKET_SILVER, get_minio_client
from minio import Minio
from rescore_cart_corrected_labels import corrected_evaluation_key
from station_config import CANONICAL_LOCATIONS
from train_cart import MODEL_VERSION, _get, model_keys, read_feature_manifest

SPLIT = "test"
HORIZON = pd.Timedelta(hours=24)
SCOPE_LAST_HOUR_UTC = pd.Timestamp("2025-12-31 23:00", tz="UTC")  # project scope 2016-2025
HISTORY = pd.Timedelta(hours=24)  # longest lag or trailing window of the features
OUTPUT_COLUMNS = [
    "location_code",
    "origin_utc",
    "target_start_utc",
    "target_end_utc",
    "probability",
    "predicted_rain",
    "probability_threshold",
    "model_name",
    "model_version",
    "model_sha256",
    "feature_source",
    "evaluation_split",
]


def prediction_key(day: date) -> str:
    return (
        f"predictions/year={day.year}/month={day.month:02d}/"
        f"rain_predictions_cart_{day:%Y%m%d}.parquet"
    )


def silver_key(location: str, year: int, month: int) -> str:
    return (
        f"weather_hourly/year={year}/month={month:02d}/"
        f"weather_hourly_{location}_{year}{month:02d}.parquet"
    )


def check_window(start: date, end: date, test_window: list[str]) -> None:
    """Refuse a window outside the test split or with targets beyond the project scope."""
    test_start, test_end = (pd.Timestamp(t) for t in test_window)
    first, last = (
        pd.Timestamp(start, tz="UTC"),
        pd.Timestamp(end, tz="UTC") + pd.Timedelta(hours=23),
    )
    if start > end:
        raise ValueError(f"window {start}..{end} is reversed")
    if first < test_start or last >= test_end:
        raise ValueError(
            f"window {start}..{end} is outside the test split {test_window[0]}..{test_window[1]}"
        )
    if last + HORIZON > SCOPE_LAST_HOUR_UTC:
        raise ValueError(
            f"origins on {end} have targets ending after {SCOPE_LAST_HOUR_UTC:%Y-%m-%d %H:%M} "
            "UTC, outside the 2016-2025 scope; the last origin day is "
            f"{(SCOPE_LAST_HOUR_UTC - HORIZON).date()}"
        )


def read_silver(client: Minio, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Silver Open-Meteo rows of every canonical location for the months covering start..end."""
    months = pd.period_range(start.tz_localize(None), end.tz_localize(None), freq="M")
    frames, missing = [], []
    for period in months:
        for loc in sorted(CANONICAL_LOCATIONS):
            key = silver_key(loc, period.year, period.month)
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
                raise ValueError(f"{key}: unexpected source in an Open-Meteo Silver object")
            frames.append(df)
    if missing:
        raise RuntimeError(f"{len(missing)} Silver Open-Meteo objects missing, e.g. {missing[:3]}")
    data = pd.concat(frames, ignore_index=True)
    data["timestamp_utc"] = pd.to_datetime(data["timestamp_utc"], utc=True)
    return data[(data["timestamp_utc"] >= start) & (data["timestamp_utc"] <= end)]


def window_features(
    hourly: pd.DataFrame, first: pd.Timestamp, last: pd.Timestamp
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Feature rows for every location x origin hour in first..last, plus missing-hour counts.

    An origin without a Silver row is reindexed in, so it shows up as missing features
    instead of disappearing from the counts.
    """
    feats = compute_features(hourly)
    origins = pd.date_range(first, last, freq="h", tz="UTC")
    grid = pd.MultiIndex.from_product(
        [sorted(CANONICAL_LOCATIONS), origins], names=["location_code", "origin_utc"]
    )
    feats = feats.set_index(["location_code", "origin_utc"]).reindex(grid).reset_index()
    no_row = (
        ~grid.isin(pd.MultiIndex.from_frame(hourly[["location_code", "timestamp_utc"]]))
    ).sum()
    return feats, {"origins_without_silver_row": int(no_row)}


def check_artifact(client: Minio, version: str) -> tuple[Any, dict[str, Any], str]:
    model_key, report_key = model_keys(version)
    report = json.loads(_get(client, report_key))
    model_bytes = _get(client, model_key)
    sha = hashlib.sha256(model_bytes).hexdigest()
    corrected, original = corrected_evaluation_key(version), evaluation_keys(version)[0]
    reference = json.loads(_get(client, corrected if _exists(client, corrected) else original))
    if sha != reference["model_sha256"]:
        raise ValueError(f"{model_key} differs from the artifact evaluated on test (SHA-256)")
    if [f["name"] for f in report["features"]] != list(FEATURES):
        raise ValueError("the model's features differ from the feature code")
    if report["feature_source"] != FEATURE_SOURCE:
        raise ValueError(f"the model was trained on {report['feature_source']} features")
    return joblib.load(io.BytesIO(model_bytes)), report, sha


def _same(existing: pd.DataFrame, new: pd.DataFrame) -> bool:
    return list(existing.columns) == list(new.columns) and existing.reset_index(drop=True).equals(
        new.reset_index(drop=True)
    )


def write_days(client: Minio, preds: pd.DataFrame) -> dict[str, list[str]]:
    """One object per UTC origin day; unchanged content is not rewritten."""
    out: dict[str, list[str]] = {"written": [], "replaced": [], "unchanged": []}
    for day, g in preds.groupby(preds["origin_utc"].dt.date, sort=True):
        key = prediction_key(day)
        g = g.reset_index(drop=True)
        state = "written"
        if _exists(client, key):
            existing = pd.read_parquet(io.BytesIO(_get(client, key)))
            if set(existing["model_version"]) != set(g["model_version"]):
                raise RuntimeError(
                    f"{key} holds predictions of model {sorted(set(existing['model_version']))}; "
                    "another model version needs its own path"
                )
            if _same(existing, g):
                out["unchanged"].append(key)
                continue
            state = "replaced"
        buf = io.BytesIO()
        g.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
        body = buf.getvalue()
        client.put_object(BUCKET_GOLD, key, io.BytesIO(body), len(body))
        out[state].append(key)
    return out


def score(
    start: date,
    end: date,
    version: str = MODEL_VERSION,
    allow_missing: bool = False,
    client: Minio | None = None,
) -> dict[str, Any]:
    client = client or get_minio_client()
    model, report, sha = check_artifact(client, version)
    manifest = read_feature_manifest(client, report["feature_version"])
    check_window(start, end, manifest["splits_utc"][SPLIT])
    threshold = float(report["probability_threshold"])

    first = pd.Timestamp(start, tz="UTC")
    last = pd.Timestamp(end, tz="UTC") + pd.Timedelta(hours=23)
    hourly = read_silver(client, first - HISTORY, last)
    feats, gaps = window_features(hourly, first, last)
    complete = feats[list(FEATURES)].notna().all(axis=1)
    missing = feats.loc[~complete]
    if len(missing) and not allow_missing:
        raise RuntimeError(
            f"{len(missing)} origins have missing features "
            f"(first {missing['location_code'].iloc[0]} {missing['origin_utc'].iloc[0]}); "
            "pass --allow-missing to score the rest"
        )
    feats = feats.loc[complete].reset_index(drop=True)
    proba = model.predict_proba(feats[[*FEATURES, "location_code"]])[:, 1]
    preds = pd.DataFrame(
        {
            "location_code": feats["location_code"],
            "origin_utc": feats["origin_utc"],
            "target_start_utc": feats["origin_utc"],
            "target_end_utc": feats["origin_utc"] + HORIZON,
            "probability": proba,
            "predicted_rain": proba >= threshold,
            "probability_threshold": threshold,
            "model_name": report["model_name"],
            "model_version": version,
            "model_sha256": sha,
            "feature_source": FEATURE_SOURCE,
            "evaluation_split": SPLIT,
        }
    )[OUTPUT_COLUMNS]
    objects = write_days(client, preds.sort_values(["origin_utc", "location_code"]))

    result = {
        "check": "CART RainNext24h scoring (ID 32)",
        "model_name": report["model_name"],
        "model_version": version,
        "model_sha256": sha,
        "probability_threshold": threshold,
        "feature_source": FEATURE_SOURCE,
        "feature_columns_match_model": True,
        "availability": report["availability"],
        "window_origin_utc": [first.isoformat(), last.isoformat()],
        "evaluation_split": SPLIT,
        "origins_in_window": int(len(complete)),
        "scored": int(len(preds)),
        "not_scored_missing_features": {
            "total": int(len(missing)),
            "by_location": {k: int(v) for k, v in missing["location_code"].value_counts().items()},
            **gaps,
        },
        "predicted_rain_share": round(float(preds["predicted_rain"].mean()), 4)
        if len(preds)
        else None,
        "objects": {k: len(v) for k, v in objects.items()},
        "object_keys": [f"{BUCKET_GOLD}/{k}" for v in objects.values() for k in v][:3],
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    key = (
        f"quality_reports/scoring_cart_{version}_{start:%Y%m%d}_{end:%Y%m%d}_"
        f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    )
    body = json.dumps(result, indent=2).encode("utf-8")
    client.put_object(BUCKET_GOLD, key, io.BytesIO(body), len(body), "application/json")
    result["report_key"] = f"{BUCKET_GOLD}/{key}"
    print(
        json.dumps({k: result[k] for k in ("origins_in_window", "scored", "objects", "report_key")})
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Score CART RainNext24h for a window (ID 32).")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True, help="UTC")
    parser.add_argument("--end-date", type=date.fromisoformat, required=True, help="UTC, inclusive")
    parser.add_argument("--version", default=MODEL_VERSION, help="Model version tag")
    parser.add_argument(
        "--allow-missing", action="store_true", help="Score the origins that have every feature"
    )
    args = parser.parse_args()
    try:
        score(args.start_date, args.end_date, args.version, args.allow_missing)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
