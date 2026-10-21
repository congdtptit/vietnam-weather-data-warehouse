"""Decision-tree rules for the evaluated CART RainNext24h model (workbook ID 30).

Rules are read from the fitted tree of `gold/ml_models/cart_rain_next24h_<version>/model.joblib`
(the artifact evaluated on test 2025 in ID 29), using the real feature names and units from
its report.json. Thresholds are in original units because the pipeline does not scale
numeric features. The notebook `notebooks/cart_tree_rules.ipynb` only displays these results.

A rule describes the model, not a causal mechanism.
"""

from __future__ import annotations

import hashlib
import io
import json
from typing import Any

import joblib
import numpy as np
import pandas as pd
from train_cart import MODEL_VERSION, _get, model_keys

LOCATION_PREFIX = "location_code_"
INTEGER_FEATURES = {"hour_local", "month_local"}


def load_artifacts(client, version: str = MODEL_VERSION) -> tuple[Any, dict[str, Any], str]:
    """Return (model pipeline, report, SHA-256 of the model file)."""
    model_key, report_key = model_keys(version)
    model_bytes = _get(client, model_key)
    report = json.loads(_get(client, report_key))
    return joblib.load(io.BytesIO(model_bytes)), report, hashlib.sha256(model_bytes).hexdigest()


def short_units(report: dict[str, Any]) -> dict[str, str]:
    """Feature -> short unit ("mm", "%", "hPa", "°C", "m/s"); calendar features have none."""
    units = {}
    for f in report["features"]:
        unit = f["unit"].split(" (")[0].replace("degC", "°C")
        units[f["name"]] = "" if f["name"] in INTEGER_FEATURES else unit
    return units


def _fmt(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _numeric_text(name: str, lo: float | None, hi: float | None, unit: str) -> str:
    """Interval for `lo < x <= hi`; integer features use whole-number bounds."""
    u = f" {unit}" if unit else ""
    if name in INTEGER_FEATURES:
        lo_i = None if lo is None else int(np.floor(lo)) + 1  # x > 4.5 -> x >= 5
        hi_i = None if hi is None else int(np.floor(hi))  # x <= 4.5 -> x <= 4
        if lo_i is not None and hi_i is not None:
            return f"{lo_i} ≤ {name} ≤ {hi_i}"
        return f"{name} ≥ {lo_i}" if lo_i is not None else f"{name} ≤ {hi_i}"
    if lo is not None and hi is not None:
        return f"{_fmt(lo)} < {name} ≤ {_fmt(hi)}{u}"
    return f"{name} > {_fmt(lo)}{u}" if lo is not None else f"{name} ≤ {_fmt(hi)}{u}"


def simplify(path: list[tuple[str, str, float]], units: dict[str, str]) -> list[str]:
    """Merge the split conditions of one root-to-leaf path into one condition per feature.

    `path` holds (encoded feature name, "<=" or ">", threshold) in tree order.
    """
    order: list[str] = []
    bounds: dict[str, list[float | None]] = {}
    loc_is: str | None = None
    loc_not: list[str] = []
    for name, op, thr in path:
        if name.startswith(LOCATION_PREFIX):
            code = name[len(LOCATION_PREFIX) :]
            if "location" not in order:
                order.append("location")
            if op == ">":  # one-hot value 1
                loc_is = code
            else:
                loc_not.append(code)
            continue
        if name not in bounds:
            order.append(name)
            bounds[name] = [None, None]
        lo, hi = bounds[name]
        if op == ">":
            bounds[name][0] = thr if lo is None else max(lo, thr)
        else:
            bounds[name][1] = thr if hi is None else min(hi, thr)

    out = []
    for name in order:
        if name == "location":
            if loc_is is not None:
                out.append(f"location = {loc_is}")
            elif len(loc_not) == 1:
                out.append(f"location ≠ {loc_not[0]}")
            else:
                out.append(f"location not in {{{', '.join(sorted(loc_not))}}}")
        else:
            out.append(_numeric_text(name, *bounds[name], units.get(name, "")))
    return out


def _paths(tree, names: np.ndarray) -> dict[int, list[tuple[str, str, float]]]:
    """Leaf node id -> split conditions from the root."""
    t = tree.tree_
    paths: dict[int, list[tuple[str, str, float]]] = {}
    stack: list[tuple[int, list[tuple[str, str, float]]]] = [(0, [])]
    while stack:
        node, path = stack.pop()
        left, right = t.children_left[node], t.children_right[node]
        if left == right:  # leaf
            paths[node] = path
            continue
        name, thr = str(names[t.feature[node]]), float(t.threshold[node])
        stack.append((right, [*path, (name, ">", thr)]))
        stack.append((left, [*path, (name, "<=", thr)]))
    return paths


def leaf_rules(
    model, x: pd.DataFrame, y: np.ndarray, threshold: float, units: dict[str, str]
) -> pd.DataFrame:
    """One row per leaf: readable rule, rows of `x` reaching it, observed Rain share, model output.

    `model_probability` is the tree's leaf output (weighted when class_weight is used);
    `observed_rain_share` is the unweighted share of Rain among the rows of `x` in the leaf.
    """
    pre, tree = model.named_steps["pre"], model.named_steps["tree"]
    names = pre.get_feature_names_out()
    leaves = tree.apply(pre.transform(x))
    stats = (
        pd.DataFrame({"leaf": leaves, "rain": np.asarray(y, dtype=float)})
        .groupby("leaf")["rain"]
        .agg(rows="size", observed_rain_share="mean")
    )
    rows = []
    for leaf, path in _paths(tree, names).items():
        value = tree.tree_.value[leaf][0]
        proba = float(value[1] / value.sum())
        conditions = simplify(path, units)
        rows.append(
            {
                "leaf": int(leaf),
                "depth": len(path),
                "rule": " AND ".join(conditions),
                "n_conditions": len(conditions),
                "rows": int(stats["rows"].get(leaf, 0)),
                "observed_rain_share": float(stats["observed_rain_share"].get(leaf, np.nan)),
                "model_probability": proba,
                "predicted_rain": proba >= threshold,
            }
        )
    out = pd.DataFrame(rows)
    out["row_share"] = out["rows"] / len(x)
    return out.sort_values("rows", ascending=False).reset_index(drop=True)


def node_table(model, x: pd.DataFrame, y: np.ndarray, units: dict[str, str], max_depth: int):
    """Nodes down to `max_depth` with their split, rows of `x`, and observed Rain share.

    The left child of a node holds the rows that satisfy its `split` condition.
    """
    pre, tree = model.named_steps["pre"], model.named_steps["tree"]
    names = pre.get_feature_names_out()
    reach = tree.decision_path(pre.transform(x))  # rows x nodes indicator
    rows = np.asarray(reach.sum(axis=0)).ravel()
    rain = np.asarray(reach.T @ np.asarray(y, dtype=float)).ravel()
    t = tree.tree_
    out = []
    stack = [(0, None, 0, None)]
    while stack:
        node, parent, depth, branch = stack.pop()
        is_leaf = t.children_left[node] == t.children_right[node]
        split = None
        if not is_leaf:
            name, thr = str(names[t.feature[node]]), float(t.threshold[node])
            split = simplify([(name, "<=", thr)], units)[0]
            if depth < max_depth:
                stack.append((t.children_right[node], node, depth + 1, "no"))
                stack.append((t.children_left[node], node, depth + 1, "yes"))
        out.append(
            {
                "node": int(node),
                "parent": parent,
                "depth": depth,
                "branch": branch,
                "split": split,
                "is_leaf": bool(is_leaf),
                "rows": int(rows[node]),
                "observed_rain_share": float(rain[node] / rows[node]) if rows[node] else np.nan,
            }
        )
    return pd.DataFrame(out)


def decision_path(model, row: pd.DataFrame, units: dict[str, str]) -> pd.DataFrame:
    """Steps taken by one row (a one-row DataFrame) from the root to its leaf."""
    pre, tree = model.named_steps["pre"], model.named_steps["tree"]
    names = pre.get_feature_names_out()
    xt = pre.transform(row)
    nodes = tree.decision_path(xt).indices
    t = tree.tree_
    steps = []
    for node in nodes:
        if t.children_left[node] == t.children_right[node]:
            continue
        name, thr = str(names[t.feature[node]]), float(t.threshold[node])
        value = float(xt[0, t.feature[node]])
        op = "<=" if value <= thr else ">"
        steps.append(
            {
                "condition": simplify([(name, op, thr)], units)[0],
                "value_at_t": row["location_code"].iloc[0]
                if name.startswith(LOCATION_PREFIX)
                else value,
            }
        )
    return pd.DataFrame(steps)


def hours_to_first_rain(hourly: pd.DataFrame, horizon: int = 24) -> pd.DataFrame:
    """For each location and UTC hour t, the first k in 1..horizon with rain > 0 at t + k h.

    Computed per location on a gap-checked hourly grid; NaN when no rain or data is missing
    before the first rainy hour. Used only to describe errors after evaluation.
    """
    parts = []
    for loc, g in hourly.groupby("location_code", sort=True):
        s = g.set_index("timestamp_utc")["precipitation_mm"].sort_index()
        s = s.reindex(pd.date_range(s.index.min(), s.index.max(), freq="h", tz="UTC"))
        first = pd.Series(np.nan, index=s.index)
        blocked = pd.Series(False, index=s.index)  # a missing hour before any rain stops the search
        for k in range(1, horizon + 1):
            ahead = s.shift(-k)
            hit = first.isna() & ~blocked & (ahead > 0)
            first[hit] = k
            blocked |= first.isna() & ahead.isna()
        parts.append(
            pd.DataFrame({"location_code": loc, "origin_utc": s.index, "onset_hours": first})
        )
    return pd.concat(parts, ignore_index=True)


ONSET_BINS = [0, 3, 6, 12, 18, 24]
ONSET_LABELS = ["1-3 h", "4-6 h", "7-12 h", "13-18 h", "19-24 h"]


def missed_rain_profile(df: pd.DataFrame, threshold: float) -> dict[str, Any]:
    """Describe Rain cases and misses. `df` needs rain_next24h, probability, rain_past_24h_mm,
    rain_next24h_mm, month_local, and onset_hours."""
    rain = df[df["rain_next24h"].astype(bool)].copy()
    rain["hit"] = rain["probability"] >= threshold
    missed = rain[~rain["hit"]]
    dry = rain["rain_past_24h_mm"] <= 0.05
    dry_season = ~rain["month_local"].between(5, 10)
    onset = pd.cut(rain["onset_hours"], ONSET_BINS, labels=ONSET_LABELS)
    return {
        "rain_cases": len(rain),
        "missed": len(missed),
        "missed_share_of_rain": len(missed) / len(rain) if len(rain) else None,
        "missed_with_probability_0_30_to_threshold": float(
            missed["probability"].between(0.30, threshold, inclusive="left").mean()
        ),
        "missed_under_5_mm": float((missed["rain_next24h_mm"] < 5).mean()),
        "missed_median_mm": float(missed["rain_next24h_mm"].median()),
        "missed_dry_past_24h": float((missed["rain_past_24h_mm"] <= 0.05).mean()),
        "recall_after_wet_24h": float(rain.loc[~dry, "hit"].mean()),
        "recall_after_dry_24h": float(rain.loc[dry, "hit"].mean()),
        "recall_after_dry_24h_dry_season": float(rain.loc[dry & dry_season, "hit"].mean()),
        "recall_after_dry_24h_wet_season": float(rain.loc[dry & ~dry_season, "hit"].mean()),
        "recall_by_onset": rain.groupby(onset, observed=True)["hit"]
        .agg(cases="size", recall="mean")
        .reset_index(names="onset"),
    }
