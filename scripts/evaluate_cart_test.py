#!/usr/bin/env python3
"""scripts/evaluate_cart_test.py.

Evaluate the frozen CART RainNext24h model once on test 2025 (workbook ID 29).

The model, features, and probability threshold come from the ID 28 artifacts and are not
changed here. Before reading the test file, the script reloads the model and checks that it
reproduces the validation metrics stored in report.json, so the evaluated artifact is the
one that was selected. An existing evaluation is never overwritten: test 2025 is used once.

Inputs (bucket gold):
  ml_models/cart_rain_next24h_<version>/model.joblib, report.json   (ID 28)
  ml_features/cart_rain_{train_2016_2023,val_2024,test_2025}_<feature version>.parquet
    (train only for the climatology baseline; validation only for the reproduction check)

Outputs (bucket gold):
  ml_models/cart_rain_next24h_<version>/test_evaluation.json   metrics, baselines, breakdowns
  ml_models/cart_rain_next24h_<version>/test_predictions.parquet
      one row per test origin: probability, predicted class, actual label (error analysis)
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
from datetime import datetime, timezone
from typing import Any

import joblib
import numpy as np
import pandas as pd
from build_rain_features import features_keys
from export_rain_labels import object_exists
from load_gold_observations import BUCKET_GOLD, get_minio_client
from minio import Minio
from train_cart import (
    MODEL_VERSION,
    _get,
    baselines,
    check_release,
    classification_metrics,
    drop_missing,
    model_keys,
    read_feature_manifest,
    read_split,
)

RAIN_AMOUNT_BINS = [1.0, 5.0, 20.0, float("inf")]
RAIN_AMOUNT_LABELS = ["1-5 mm", "5-20 mm", ">=20 mm"]
WET_MONTHS = range(5, 11)  # May-October


def evaluation_keys(version: str = MODEL_VERSION) -> tuple[str, str]:
    base = f"ml_models/cart_rain_next24h_{version}"
    return f"{base}/test_evaluation.json", f"{base}/test_predictions.parquet"


def _exists(client: Minio, key: str) -> bool:
    return object_exists(client, key)


def read_test(client: Minio, feature_version: str) -> pd.DataFrame:
    split_keys, _ = features_keys(feature_version)
    df = pd.read_parquet(io.BytesIO(_get(client, split_keys["test"])))
    if set(df["split"]) != {"test"}:
        raise ValueError(f"{split_keys['test']} holds rows of another split")
    return df


def _round(m: dict[str, Any]) -> dict[str, Any]:
    return {k: round(v, 4) if isinstance(v, float) else v for k, v in m.items()}


def breakdowns(df: pd.DataFrame) -> dict[str, Any]:
    """Metrics by location, season, and month, plus detection rate by observed rain amount."""
    out: dict[str, Any] = {"by_location": {}, "by_season": {}, "by_month_local": {}}
    for loc, g in df.groupby("location_code"):
        out["by_location"][loc] = _round(classification_metrics(g["rain_next24h"], g["pred"]))
    season = np.where(df["month_local"].isin(WET_MONTHS), "wet_may_oct", "dry_nov_apr")
    for s, g in df.groupby(season):
        out["by_season"][s] = _round(classification_metrics(g["rain_next24h"], g["pred"]))
    for m, g in df.groupby("month_local"):
        out["by_month_local"][str(int(m))] = _round(
            classification_metrics(g["rain_next24h"], g["pred"])
        )
    rain = df[df["rain_next24h"]]
    amount = pd.cut(
        rain["rain_next24h_mm"], RAIN_AMOUNT_BINS, right=False, labels=RAIN_AMOUNT_LABELS
    )
    out["rain_detected_by_amount"] = {
        str(k): {"rain_cases": int(len(g)), "detected_share": round(float(g["pred"].mean()), 4)}
        for k, g in rain.groupby(amount, observed=True)
    }
    return out


def evaluate_test(version: str = MODEL_VERSION, client: Minio | None = None) -> dict[str, Any]:
    client = client or get_minio_client()
    model_key, report_key = model_keys(version)
    eval_key, pred_key = evaluation_keys(version)
    if _exists(client, eval_key):
        raise RuntimeError(f"{eval_key} already exists; test 2025 is evaluated only once")

    report = json.loads(_get(client, report_key))
    model_bytes = _get(client, model_key)
    check_release(version, report["feature_version"], model_bytes)  # test only for the release
    model = joblib.load(io.BytesIO(model_bytes))
    features = [f["name"] for f in report["features"]]
    threshold = float(report["probability_threshold"])
    feature_version = report["feature_version"]
    cols = [*features, "location_code"]

    manifest = read_feature_manifest(client, feature_version)
    if [f["name"] for f in manifest["features"]] != features:
        raise ValueError("feature manifest does not match the features the model was trained on")

    # The artifact must reproduce its validation result before test is touched
    val, _ = drop_missing(read_split(client, "validation", feature_version), features)
    val_pred = model.predict_proba(val[cols])[:, 1] >= threshold
    reproduced = classification_metrics(val["rain_next24h"].to_numpy(bool), val_pred)
    expected = report["validation_metrics_at_threshold"]
    if any(reproduced[k] != expected[k] for k in ("tp", "fp", "tn", "fn")):
        raise ValueError("model does not reproduce its validation confusion matrix")

    test_all = read_test(client, feature_version)
    test, not_covered = drop_missing(test_all, features)
    proba = model.predict_proba(test[cols])[:, 1]
    test = test.assign(probability=proba, pred=proba >= threshold)
    y = test["rain_next24h"].to_numpy(bool)

    train, _ = drop_missing(read_split(client, "train", feature_version), features)
    evaluation = {
        "model_name": report["model_name"],
        "model_version": version,
        "model_sha256": hashlib.sha256(model_bytes).hexdigest(),
        "feature_version": feature_version,
        "label_version": report["label_version"],
        "probability_threshold": threshold,
        "threshold_source": "validation 2024 (ID 28), frozen before this evaluation",
        "test_window_utc": manifest["splits_utc"]["test"],
        "availability": report["availability"],
        "validation_reproduced": True,
        "coverage": {
            "test_usable_labels": len(test_all),
            "evaluated": len(test),
            "not_covered_missing_features": not_covered,
            "share_evaluated": round(len(test) / len(test_all), 6) if len(test_all) else None,
        },
        "rain_share": round(float(y.mean()), 4),
        "test_metrics_at_threshold": classification_metrics(y, test["pred"].to_numpy()),
        "test_metrics_at_0_5_for_reference": classification_metrics(y, proba >= 0.5),
        "test_baselines": baselines(train, test, float(report["label_threshold_mm"])),
        "breakdowns": breakdowns(test),
        "validation_metrics_at_threshold": expected,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "objects": {
            "evaluation": f"{BUCKET_GOLD}/{eval_key}",
            "predictions": f"{BUCKET_GOLD}/{pred_key}",
        },
    }

    preds = test[
        [
            "location_code",
            "origin_utc",
            "target_start_utc",
            "target_end_utc",
            "probability",
            "pred",
            "rain_next24h",
            "rain_next24h_mm",
        ]
    ].rename(columns={"pred": "predicted_rain"})
    preds = preds.assign(model_version=version, probability_threshold=threshold)
    buf = io.BytesIO()
    preds.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    pbytes = buf.getvalue()
    client.put_object(BUCKET_GOLD, pred_key, io.BytesIO(pbytes), len(pbytes))
    ebytes = json.dumps(evaluation, indent=2).encode("utf-8")
    client.put_object(
        BUCKET_GOLD, eval_key, io.BytesIO(ebytes), len(ebytes), content_type="application/json"
    )
    print(
        json.dumps(
            {k: evaluation[k] for k in ("coverage", "rain_share", "test_metrics_at_threshold")},
            indent=2,
        )
    )
    return evaluation


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate frozen CART on test 2025 (ID 29).")
    parser.add_argument("--version", default=MODEL_VERSION, help="Model version tag")
    args = parser.parse_args()
    try:
        evaluate_test(args.version)
    except (RuntimeError, ValueError) as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
