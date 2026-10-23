#!/usr/bin/env python3
"""scripts/analyze_cart_thresholds.py.

Threshold comparison and error analysis of the frozen CART RainNext24h v1 model (workbook
ID 36; rules in docs/rain-label.md).

- Validation 2024 (corrected labels rain_next24h_v0.2.1, features v2): metrics over the grid
  the model's threshold rule used (0.30-0.70, step 0.02), and the two decided rules applied to
  it: the model's own threshold rule, train_cart.choose_threshold (maximum F1; ties go to the
  value closest to 0.5; it froze 0.40 on the original labels), and the Recall operating point
  (highest precision with recall >= 0.85).
- Test 2025: metrics only at predeclared thresholds (0.40 official, the Recall operating point,
  0.50 for reference). Nothing is selected on test; the test figures are post-hoc and are
  reported as such. Error breakdowns at the official threshold and the operating point.

The model is checked against the corrected-label evaluation before anything is computed:
same SHA-256 and the same test confusion matrix at 0.40. Nothing in the DW changes.

Output (bucket gold), written once unless --overwrite:
  ml_models/cart_rain_next24h_<version>/threshold_error_analysis.json
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
from evaluate_cart_test import _exists, breakdowns, read_test
from load_gold_observations import BUCKET_GOLD, get_minio_client
from minio import Minio
from rescore_cart_corrected_labels import corrected_evaluation_key
from train_cart import (
    MODEL_VERSION,
    THRESHOLDS,
    _get,
    choose_threshold,
    classification_metrics,
    drop_missing,
    model_keys,
    read_split,
)

FEATURE_VERSION = "v2"  # corrected labels; same feature values as v1 up to rounding (ID 22)
GRID = THRESHOLDS  # the grid of the model's threshold rule (0.30-0.70, step 0.02)
RECALL_TARGET = 0.85  # decided 2026-10-22 (docs/rain-label.md)
REFERENCE = 0.50
MISSED_BINS = [1.0, 2.0, 5.0, 10.0, 20.0, float("inf")]
MISSED_LABELS = ["1-2 mm", "2-5 mm", "5-10 mm", "10-20 mm", ">=20 mm"]
NEAR_MISS_BINS = [-0.001, 0.0, 0.5, 1.0]
NEAR_MISS_LABELS = ["0 mm", "0-0.5 mm", "0.5-1 mm"]
PAST_RAIN_BINS = [-float("inf"), 0.0005, 1.0, float("inf")]  # sums are rounded to 0.001 mm
PAST_RAIN_LABELS = ["dry past 24 h (0 mm)", "trace past 24 h (<1 mm)", "rain past 24 h (>=1 mm)"]
PERIODS = {
    "00-05": range(0, 6),
    "06-11": range(6, 12),
    "12-17": range(12, 18),
    "18-23": range(18, 24),
}


def analysis_key(version: str = MODEL_VERSION) -> str:
    return f"ml_models/cart_rain_next24h_{version}/threshold_error_analysis.json"


def _round(m: dict[str, Any]) -> dict[str, Any]:
    return {k: round(v, 4) if isinstance(v, float) else v for k, v in m.items()}


def scan(y: np.ndarray, proba: np.ndarray, thresholds: list[float]) -> list[dict[str, Any]]:
    rows = []
    for t in thresholds:
        pred = proba >= t
        rows.append(
            {
                "threshold": t,
                **_round(classification_metrics(y, pred)),
                "predicted_rain_share": round(float(pred.mean()), 4),
            }
        )
    return rows


def recall_rule(rows: list[dict[str, Any]], target: float = RECALL_TARGET) -> float | None:
    """Highest precision among thresholds whose recall reaches the target."""
    ok = [r for r in rows if r["recall"] is not None and r["recall"] >= target]
    if not ok:
        return None
    best = max(r["precision"] for r in ok)
    return max(r["threshold"] for r in ok if r["precision"] == best)


def _share(g: pd.DataFrame, col: str) -> float | None:
    return round(float(g[col].mean()), 4) if len(g) else None


def errors(df: pd.DataFrame) -> dict[str, Any]:
    """Where the model errs; `df` has rain_next24h, rain_next24h_mm, pred, probability."""
    out = breakdowns(df)  # by location, season, local month; detection by rain amount
    rain, dry = df[df["rain_next24h"]], df[~df["rain_next24h"]]
    amount = pd.cut(rain["rain_next24h_mm"], MISSED_BINS, right=False, labels=MISSED_LABELS)
    out["missed_rain_by_amount"] = {
        str(k): {
            "rain_cases": int(len(g)),
            "missed": int((~g["pred"]).sum()),
            "missed_share": round(float((~g["pred"]).mean()), 4),
        }
        for k, g in rain.groupby(amount, observed=True)
    }
    near = pd.cut(dry["rain_next24h_mm"], NEAR_MISS_BINS, right=True, labels=NEAR_MISS_LABELS)
    out["false_alarms_by_observed_rain"] = {
        str(k): {
            "no_rain_cases": int(len(g)),
            "false_alarms": int(g["pred"].sum()),
            "false_alarm_share": _share(g, "pred"),
        }
        for k, g in dry.groupby(near, observed=True)
    }
    out["false_alarms_total"] = int(dry["pred"].sum())
    past = pd.cut(df["rain_past_24h_mm"], PAST_RAIN_BINS, right=False, labels=PAST_RAIN_LABELS)
    out["by_rain_in_past_24h"] = {
        str(k): {
            "cases": int(len(g)),
            **_round(classification_metrics(g["rain_next24h"], g["pred"])),
        }
        for k, g in df.groupby(past, observed=True)
    }
    out["by_origin_local_period"] = {
        p: _round(classification_metrics(g["rain_next24h"], g["pred"]))
        for p, hours in PERIODS.items()
        if len(g := df[df["hour_local"].isin(hours)])
    }
    bins = pd.cut(df["probability"], np.linspace(0, 1, 11), include_lowest=True)
    out["calibration"] = [
        {
            "probability_bin": str(k),
            "cases": int(len(g)),
            "mean_probability": round(float(g["probability"].mean()), 4),
            "observed_rain_share": _share(g, "rain_next24h"),
        }
        for k, g in df.groupby(bins, observed=True)
    ]
    return out


def analyze(
    version: str = MODEL_VERSION, overwrite: bool = False, client: Minio | None = None
) -> dict[str, Any]:
    client = client or get_minio_client()
    out_key = analysis_key(version)
    if _exists(client, out_key) and not overwrite:
        raise RuntimeError(f"{out_key} already exists; pass --overwrite to recompute")
    model_key, report_key = model_keys(version)
    report = json.loads(_get(client, report_key))
    reference = json.loads(_get(client, corrected_evaluation_key(version)))
    model_bytes = _get(client, model_key)
    sha = hashlib.sha256(model_bytes).hexdigest()
    if sha != reference["model_sha256"]:
        raise ValueError("model artifact differs from the one evaluated on test")
    model = joblib.load(io.BytesIO(model_bytes))
    features = [f["name"] for f in report["features"]]
    cols = [*features, "location_code"]
    official = float(report["probability_threshold"])

    val, _ = drop_missing(read_split(client, "validation", FEATURE_VERSION), features)
    val = val.assign(probability=model.predict_proba(val[cols])[:, 1])
    y_val = val["rain_next24h"].to_numpy(bool)
    val_scan = scan(y_val, val["probability"].to_numpy(), GRID)
    f1_threshold, _ = choose_threshold(y_val, val["probability"].to_numpy())
    recall_threshold = recall_rule(val_scan)
    if recall_threshold is None:
        raise ValueError(f"no threshold on the grid reaches validation recall {RECALL_TARGET}")

    test, not_covered = drop_missing(read_test(client, FEATURE_VERSION), features)
    test = test.assign(probability=model.predict_proba(test[cols])[:, 1])
    y_test = test["rain_next24h"].to_numpy(bool)
    at_official = classification_metrics(y_test, test["probability"].to_numpy() >= official)
    if any(
        at_official[k] != reference["test_metrics_at_threshold"][k]
        for k in ("tp", "fp", "tn", "fn")
    ):
        raise ValueError(
            "test confusion matrix at the official threshold differs from the reference evaluation"
        )

    predeclared = {
        "official_validation_f1": official,
        "recall_operating_point": recall_threshold,
        "reference_0_5": REFERENCE,
    }
    test_at = {
        name: {
            "threshold": t,
            **_round(classification_metrics(y_test, test["probability"].to_numpy() >= t)),
            "predicted_rain_share": round(float((test["probability"] >= t).mean()), 4),
        }
        for name, t in predeclared.items()
    }
    val_at = {
        name: next(r for r in val_scan if r["threshold"] == t) for name, t in predeclared.items()
    }
    result = {
        "model_name": report["model_name"],
        "model_version": version,
        "model_sha256": sha,
        "feature_version": FEATURE_VERSION,
        "label_version": str(test["label_version"].iloc[0]),
        "rules": {
            "grid": GRID,
            "f1_rule": "the model's rule train_cart.choose_threshold: maximum validation F1 "
            "on the grid, ties go to the value closest to 0.5",
            "recall_rule": f"highest validation precision among thresholds with validation recall >= {RECALL_TARGET}",
        },
        "validation": {
            "rows": len(val),
            "rain_share": round(float(y_val.mean()), 4),
            "scan": val_scan,
            "f1_rule_threshold_corrected_labels": f1_threshold,
            "recall_rule_threshold": recall_threshold,
            "at_predeclared": val_at,
            "note": "the official threshold 0.40 was frozen by the F1 rule on the original labels "
            "(v0.2) and is not reselected on the corrected labels",
            "errors_at_official": errors(val.assign(pred=val["probability"] >= official)),
        },
        "test": {
            "rows": len(test),
            "not_covered_missing_features": not_covered,
            "rain_share": round(float(y_test.mean()), 4),
            "disclosure": "post-hoc: test 2025 is evaluated only at thresholds fixed on validation; "
            "no threshold or model is chosen from these figures",
            "at_predeclared": test_at,
            "errors_at_official": errors(test.assign(pred=test["probability"] >= official)),
            "errors_at_recall_operating_point": errors(
                test.assign(pred=test["probability"] >= recall_threshold)
            ),
        },
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "object": f"{BUCKET_GOLD}/{out_key}",
    }
    body = json.dumps(result, indent=2, default=str).encode("utf-8")
    client.put_object(
        BUCKET_GOLD, out_key, io.BytesIO(body), len(body), content_type="application/json"
    )
    summary = {
        "validation_f1_rule": f1_threshold,
        "validation_recall_rule": recall_threshold,
        "test_at_predeclared": {
            k: {m: v[m] for m in ("threshold", "precision", "recall", "f1", "fp", "fn")}
            for k, v in test_at.items()
        },
        "object": result["object"],
    }
    print(json.dumps(summary, indent=2))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Threshold comparison and error analysis (ID 36).")
    parser.add_argument("--version", default=MODEL_VERSION, help="Model version")
    parser.add_argument("--overwrite", action="store_true", help="Recompute an existing analysis")
    args = parser.parse_args()
    try:
        analyze(args.version, args.overwrite)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
