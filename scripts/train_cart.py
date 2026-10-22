#!/usr/bin/env python3
"""scripts/train_cart.py.

Train and tune the CART RainNext24h classifier (workbook ID 28).

Inputs (bucket gold, from ID 23): the train and validation feature files and the feature
manifest. The test file is never read here; test evaluation is ID 29, after the model and
threshold are frozen.

Procedure (decided 2026-10-21, see docs/rain-label.md):
- Drop rows with any missing feature (only the first 24 origin hours of 2016).
- One-hot encode `location_code`; numeric features pass through unscaled.
- Fit every grid configuration on train 2016-2023 and score it on validation 2024.
- Select by validation F1 of the Rain class at probability 0.5; configurations within
  F1_TOLERANCE of the best are treated as tied and the shallowest, then smallest, tree wins.
- Choose the probability threshold on validation for the selected model, then freeze.
- Report always-No, always-Yes, persistence, and climatology baselines on validation.
- No refit on train + validation.

Outputs (bucket gold):
  ml_models/cart_rain_next24h_<version>/model.joblib   preprocessing + tree pipeline
  ml_models/cart_rain_next24h_<version>/report.json    parameters, features, threshold,
                                                       grid results, validation metrics
Load model.joblib only from this trusted bucket: joblib files can execute code on load.
"""

from __future__ import annotations

import argparse
import io
import itertools
import json
import sys
from datetime import datetime, timezone
from typing import Any

import joblib
import numpy as np
import pandas as pd
import sklearn
from build_rain_features import FEATURE_VERSION, features_keys
from export_rain_labels import object_exists, refuse_overwrite
from joblib import Parallel, delayed
from load_gold_observations import BUCKET_GOLD, get_minio_client
from minio import Minio
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.tree import DecisionTreeClassifier
from station_config import CANONICAL_LOCATIONS

MODEL_NAME = "DecisionTree_CART"
MODEL_VERSION = "v1"
RANDOM_STATE = 42
F1_TOLERANCE = 0.005
DEFAULT_GRID: dict[str, list[Any]] = {
    "max_depth": [4, 6, 8, 10, 12],
    "min_samples_leaf": [50, 200, 1000],
    "class_weight": [None, "balanced"],
    "ccp_alpha": [0.0, 1e-5, 1e-4, 1e-3],
}
THRESHOLDS = [round(x, 2) for x in np.arange(0.30, 0.701, 0.02)]
ALLOWED_SPLITS = ("train", "validation")  # test stays unread until ID 29


def model_keys(version: str = MODEL_VERSION) -> tuple[str, str]:
    base = f"ml_models/cart_rain_next24h_{version}"
    return f"{base}/model.joblib", f"{base}/report.json"


def _get(client: Minio, key: str) -> bytes:
    resp = client.get_object(BUCKET_GOLD, key)
    try:
        return resp.read()
    finally:
        resp.close()
        resp.release_conn()


def read_feature_manifest(client: Minio, version: str = FEATURE_VERSION) -> dict[str, Any]:
    _, manifest_key = features_keys(version)
    return json.loads(_get(client, manifest_key))


def read_split(client: Minio, split: str, version: str = FEATURE_VERSION) -> pd.DataFrame:
    if split not in ALLOWED_SPLITS:
        raise ValueError(f"split {split!r} must not be read during training")
    split_keys, _ = features_keys(version)
    df = pd.read_parquet(io.BytesIO(_get(client, split_keys[split])))
    if set(df["split"]) != {split}:
        raise ValueError(f"{split_keys[split]} holds rows of another split")
    return df


def drop_missing(df: pd.DataFrame, features: list[str]) -> tuple[pd.DataFrame, int]:
    keep = df[features].notna().all(axis=1)
    return df[keep].reset_index(drop=True), int((~keep).sum())


def make_pipeline(features: list[str], params: dict[str, Any]) -> Pipeline:
    pre = ColumnTransformer(
        [
            ("num", "passthrough", features),
            (
                "loc",
                OneHotEncoder(categories=[sorted(CANONICAL_LOCATIONS)], sparse_output=False),
                ["location_code"],
            ),
        ],
        verbose_feature_names_out=False,
    )
    tree = DecisionTreeClassifier(random_state=RANDOM_STATE, **params)
    return Pipeline([("pre", pre), ("tree", tree)])


def classification_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, Any]:
    """Confusion counts and metrics with Rain (True) as positive; undefined values are None."""
    y_true = np.asarray(y_true, dtype=bool)
    y_pred = np.asarray(y_pred, dtype=bool)
    tp = int((y_true & y_pred).sum())
    fp = int((~y_true & y_pred).sum())
    tn = int((~y_true & ~y_pred).sum())
    fn = int((y_true & ~y_pred).sum())
    n = tp + fp + tn + fn
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision is not None and recall is not None and precision + recall
        else None
    )
    return {
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "support_rain": tp + fn,
        "support_no_rain": tn + fp,
        "accuracy": (tp + tn) / n if n else None,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def baselines(train: pd.DataFrame, val: pd.DataFrame, threshold_mm: float) -> dict[str, Any]:
    y = val["rain_next24h"].to_numpy(bool)
    keys = ["location_code", "month_local"]
    climatology = train.groupby(keys)["rain_next24h"].mean().rename("rate").reset_index()
    clim_rate = val[keys].merge(climatology, on=keys, how="left")["rate"]  # unseen -> NaN -> No
    return {
        "always_no": classification_metrics(y, np.zeros(len(y), bool)),
        "always_yes": classification_metrics(y, np.ones(len(y), bool)),
        "persistence_rain_past_24h": classification_metrics(
            y, val["rain_past_24h_mm"].to_numpy() >= threshold_mm
        ),
        "climatology_location_month": classification_metrics(
            y, np.asarray(clim_rate, dtype=float) >= 0.5
        ),
    }


def _fit_and_score(
    params: dict[str, Any],
    features: list[str],
    x_train: pd.DataFrame,
    y_train: np.ndarray,
    x_val: pd.DataFrame,
    y_val: np.ndarray,
) -> dict[str, Any]:
    model = make_pipeline(features, params).fit(x_train, y_train)
    tree = model.named_steps["tree"]
    pred = model.predict_proba(x_val)[:, 1] >= 0.5
    return {
        **params,
        "depth": int(tree.get_depth()),
        "leaves": int(tree.get_n_leaves()),
        **classification_metrics(y_val, pred),
    }


def grid_configs(grid: dict[str, list[Any]]) -> list[dict[str, Any]]:
    keys = list(grid)
    return [dict(zip(keys, values)) for values in itertools.product(*(grid[k] for k in keys))]


def select_config(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Best validation F1; within F1_TOLERANCE prefer the shallowest, then smallest tree."""
    scored = [r for r in results if r["f1"] is not None]
    if not scored:
        raise ValueError("no configuration has a defined validation F1")
    best = max(r["f1"] for r in scored)
    tied = [r for r in scored if r["f1"] >= best - F1_TOLERANCE]
    return min(tied, key=lambda r: (r["depth"], r["leaves"], -r["f1"]))


def choose_threshold(y_val: np.ndarray, proba: np.ndarray) -> tuple[float, list[dict[str, Any]]]:
    """Threshold with the highest validation F1; ties go to the value closest to 0.5."""
    scan = [{"threshold": t, **classification_metrics(y_val, proba >= t)} for t in THRESHOLDS]
    defined = [s for s in scan if s["f1"] is not None]
    best = max(defined, key=lambda s: (round(s["f1"], 6), -abs(s["threshold"] - 0.5)))
    return best["threshold"], scan


def train_cart(
    feature_version: str = FEATURE_VERSION,
    version: str = MODEL_VERSION,
    grid: dict[str, list[Any]] | None = None,
    n_jobs: int = -1,
    client: Minio | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    client = client or get_minio_client()
    model_key, report_key = model_keys(version)
    # A model evaluated on test is frozen: its SHA-256 is recorded in the evaluation and DW
    evaluation_key = f"ml_models/cart_rain_next24h_{version}/test_evaluation.json"
    if object_exists(client, evaluation_key):
        raise RuntimeError(
            f"model {version} is frozen ({evaluation_key} exists); use a new --version"
        )
    refuse_overwrite(client, [model_key, report_key], overwrite)
    manifest = read_feature_manifest(client, feature_version)
    features = [f["name"] for f in manifest["features"]]
    train, train_dropped = drop_missing(read_split(client, "train", feature_version), features)
    val, val_dropped = drop_missing(read_split(client, "validation", feature_version), features)
    threshold_mm = float(train["threshold_mm"].iloc[0])

    cols = [*features, "location_code"]
    x_train, y_train = train[cols], train["rain_next24h"].to_numpy(bool)
    x_val, y_val = val[cols], val["rain_next24h"].to_numpy(bool)

    configs = grid_configs(grid or DEFAULT_GRID)
    print(f"Fitting {len(configs)} configurations on {len(train):,} train rows")
    results = Parallel(n_jobs=n_jobs)(
        delayed(_fit_and_score)(p, features, x_train, y_train, x_val, y_val) for p in configs
    )
    chosen = select_config(results)
    params = {k: chosen[k] for k in (grid or DEFAULT_GRID)}

    model = make_pipeline(features, params).fit(x_train, y_train)
    proba = model.predict_proba(x_val)[:, 1]
    threshold, scan = choose_threshold(y_val, proba)
    tree = model.named_steps["tree"]
    names = model.named_steps["pre"].get_feature_names_out()
    importances = sorted(
        ((str(n), float(v)) for n, v in zip(names, tree.feature_importances_) if v > 0),
        key=lambda x: -x[1],
    )

    buf = io.BytesIO()
    joblib.dump(model, buf)
    data = buf.getvalue()
    client.put_object(BUCKET_GOLD, model_key, io.BytesIO(data), len(data))

    report = {
        "model_name": MODEL_NAME,
        "model_version": version,
        "feature_version": feature_version,
        "label_version": manifest["label_version"],
        "feature_source": manifest["feature_source"],
        "availability": manifest["availability"],
        "features": manifest["features"],
        "categorical_features": manifest["categorical_features"],
        "encoded_feature_names": [str(n) for n in names],
        "label_threshold_mm": threshold_mm,
        "train_window_utc": manifest["splits_utc"]["train"],
        "validation_window_utc": manifest["splits_utc"]["validation"],
        "test_split_read": False,
        "rows": {
            "train_used": len(train),
            "train_dropped_missing_features": train_dropped,
            "validation_used": len(val),
            "validation_dropped_missing_features": val_dropped,
        },
        "selection_rule": (
            f"max validation F1 (Rain) at probability 0.5; configurations within {F1_TOLERANCE} "
            "of the best are tied and the shallowest, then smallest, tree is chosen"
        ),
        "random_state": RANDOM_STATE,
        "grid": grid or DEFAULT_GRID,
        "selected_params": params,
        "tree": {"depth": int(tree.get_depth()), "leaves": int(tree.get_n_leaves())},
        "probability_threshold": threshold,
        "threshold_rule": "max validation F1 over 0.30-0.70 in steps of 0.02",
        "validation_metrics_at_0_5": classification_metrics(y_val, proba >= 0.5),
        "validation_metrics_at_threshold": classification_metrics(y_val, proba >= threshold),
        "validation_baselines": baselines(train, val, threshold_mm),
        "threshold_scan": scan,
        "feature_importances": [{"feature": n, "importance": v} for n, v in importances],
        "grid_results": results,
        "dependencies": {
            "scikit-learn": sklearn.__version__,
            "pandas": pd.__version__,
            "numpy": np.__version__,
            "joblib": joblib.__version__,
            "python": sys.version.split()[0],
        },
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "objects": {
            "model": f"{BUCKET_GOLD}/{model_key}",
            "model_bytes": len(data),
            "report": f"{BUCKET_GOLD}/{report_key}",
        },
    }
    rbytes = json.dumps(report, indent=2).encode("utf-8")
    client.put_object(
        BUCKET_GOLD, report_key, io.BytesIO(rbytes), len(rbytes), content_type="application/json"
    )
    summary = {k: report[k] for k in ("selected_params", "tree", "probability_threshold", "rows")}
    summary["validation_metrics_at_threshold"] = report["validation_metrics_at_threshold"]
    print(json.dumps(summary, indent=2))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Train and tune CART RainNext24h (ID 28).")
    parser.add_argument("--feature-version", default=FEATURE_VERSION)
    parser.add_argument("--version", default=MODEL_VERSION, help="Model version tag")
    parser.add_argument("--n-jobs", type=int, default=-1, help="Parallel grid fits")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an untested model of this version (never allowed after test evaluation)",
    )
    args = parser.parse_args()
    try:
        train_cart(args.feature_version, args.version, n_jobs=args.n_jobs, overwrite=args.overwrite)
    except (RuntimeError, ValueError) as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
