#!/usr/bin/env python3
"""scripts/rescore_cart_corrected_labels.py.

Re-score the frozen CART v1 test 2025 predictions against corrected labels (decided
2026-10-22 after the ID 31 reconciliation found the rain_next24h_v0.2 float-sum bug).

Nothing about the model changes: the same artifact, features, and probability threshold.
The script first requires that the model, applied to the corrected-label feature files,
reproduces the stored test predictions exactly, so only the ground truth differs. It then
recomputes test metrics, baselines, and breakdowns, and validation metrics at the frozen
threshold for reference (no reselection). The original test_evaluation.json is kept; the
result is written once to a separate object.

Output (bucket gold):
  ml_models/cart_rain_next24h_<version>/test_evaluation_corrected_labels.json
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from datetime import datetime, timezone
from typing import Any

import joblib
import numpy as np
import pandas as pd
from build_rain_features import FEATURE_VERSION
from evaluate_cart_test import _exists, breakdowns, evaluation_keys, read_test
from load_gold_observations import BUCKET_GOLD, get_minio_client
from minio import Minio
from rain_labels import LABEL_VERSION
from train_cart import (
    MODEL_VERSION,
    _get,
    baselines,
    classification_metrics,
    drop_missing,
    model_keys,
    read_split,
)


def corrected_evaluation_key(version: str = MODEL_VERSION) -> str:
    return f"ml_models/cart_rain_next24h_{version}/test_evaluation_corrected_labels.json"


def rescore(
    version: str = MODEL_VERSION,
    feature_version: str = FEATURE_VERSION,
    client: Minio | None = None,
) -> dict[str, Any]:
    client = client or get_minio_client()
    out_key = corrected_evaluation_key(version)
    if _exists(client, out_key):
        raise RuntimeError(f"{out_key} already exists")
    model_key, report_key = model_keys(version)
    eval_key, pred_key = evaluation_keys(version)
    report = json.loads(_get(client, report_key))
    original = json.loads(_get(client, eval_key))
    stored = pd.read_parquet(io.BytesIO(_get(client, pred_key)))
    model = joblib.load(io.BytesIO(_get(client, model_key)))
    features = [f["name"] for f in report["features"]]
    cols = [*features, "location_code"]
    threshold = float(report["probability_threshold"])

    test_all = read_test(client, feature_version)
    if (test_all["label_version"] != LABEL_VERSION).any():
        raise ValueError(f"test features do not carry {LABEL_VERSION} labels")
    test, not_covered = drop_missing(test_all, features)
    proba = model.predict_proba(test[cols])[:, 1]
    test = test.assign(probability=proba, pred=proba >= threshold)

    # Only the labels may differ from the original evaluation
    joined = test.merge(
        stored[["location_code", "origin_utc", "probability", "predicted_rain"]],
        on=["location_code", "origin_utc"],
        how="outer",
        suffixes=("", "_stored"),
        indicator=True,
        validate="one_to_one",
    )
    if (joined["_merge"] != "both").any():
        raise ValueError("corrected test rows differ from the stored prediction rows")
    if not np.allclose(joined["probability"], joined["probability_stored"], rtol=0, atol=1e-12):
        raise ValueError("model probabilities changed; features differ beyond rounding")
    if (joined["pred"] != joined["predicted_rain"]).any():
        raise ValueError("predicted classes changed")

    y = test["rain_next24h"].to_numpy(bool)
    labels_changed = int(
        (
            test.set_index(["location_code", "origin_utc"])["rain_next24h"]
            != stored.set_index(["location_code", "origin_utc"])["rain_next24h"]
        ).sum()
    )
    train, _ = drop_missing(read_split(client, "train", feature_version), features)
    val, _ = drop_missing(read_split(client, "validation", feature_version), features)
    val_pred = model.predict_proba(val[cols])[:, 1] >= threshold

    result = {
        "model_name": report["model_name"],
        "model_version": version,
        "model_sha256": original["model_sha256"],
        "probability_threshold": threshold,
        "reason": "ground-truth correction: rain_next24h_v0.2 labelled exact 1.0 mm windows No "
        "because of float sums; labels rebuilt as " + LABEL_VERSION,
        "model_or_threshold_changed": False,
        "predictions_identical_to_original": True,
        "feature_version": feature_version,
        "label_version": LABEL_VERSION,
        "test_labels_changed": labels_changed,
        "coverage": {
            "test_usable_labels": len(test_all),
            "evaluated": len(test),
            "not_covered_missing_features": not_covered,
        },
        "rain_share": round(float(y.mean()), 4),
        "test_metrics_at_threshold": classification_metrics(y, test["pred"].to_numpy()),
        "test_metrics_at_threshold_original_labels": original["test_metrics_at_threshold"],
        "test_baselines": baselines(train, test, float(report["label_threshold_mm"])),
        "breakdowns": breakdowns(test),
        "validation_metrics_at_threshold_corrected_labels": classification_metrics(
            val["rain_next24h"].to_numpy(bool), val_pred
        ),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    rbytes = json.dumps(result, indent=2).encode("utf-8")
    client.put_object(
        BUCKET_GOLD, out_key, io.BytesIO(rbytes), len(rbytes), content_type="application/json"
    )
    print(
        json.dumps(
            {
                k: result[k]
                for k in ("test_labels_changed", "rain_share", "test_metrics_at_threshold")
            },
            indent=2,
        )
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-score frozen CART on corrected labels.")
    parser.add_argument("--version", default=MODEL_VERSION, help="Model version tag")
    parser.add_argument("--feature-version", default=FEATURE_VERSION)
    args = parser.parse_args()
    try:
        rescore(args.version, args.feature_version)
    except (RuntimeError, ValueError) as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
