"""Unit tests for scripts/cart_rules.py (workbook ID 30). Synthetic tree, no MinIO."""

import re

import numpy as np
import pandas as pd
from build_rain_features import FEATURES
from cart_rules import decision_path, leaf_rules, node_table, simplify
from train_cart import make_pipeline

UNITS = {"humidity_percent": "%", "rain_past_12h_mm": "mm", "month_local": "", "hour_local": ""}


def _data(n=4000, seed=0):
    """Rain when humidity > 80 % at any location except LCA, where it needs > 1 mm in 12 h."""
    rng = np.random.default_rng(seed)
    df = pd.DataFrame({name: rng.normal(size=n) for name in FEATURES})
    df["humidity_percent"] = rng.uniform(40, 100, n)
    df["rain_past_12h_mm"] = rng.uniform(0, 3, n)
    df["location_code"] = rng.choice(["HAN", "LCA"], n)
    y = np.where(
        df["location_code"] == "LCA", df["rain_past_12h_mm"] > 1, df["humidity_percent"] > 80
    )
    return df[[*FEATURES, "location_code"]], y


def _model():
    x, y = _data()
    return make_pipeline(list(FEATURES), {"max_depth": 3, "min_samples_leaf": 20}).fit(x, y), x, y


def test_simplify_merges_bounds_and_translates_locations():
    path = [
        ("humidity_percent", ">", 70.0),
        ("location_code_LCA", "<=", 0.5),
        ("humidity_percent", "<=", 92.456),
        ("humidity_percent", ">", 80.0),
        ("month_local", ">", 4.5),
        ("location_code_HAN", "<=", 0.5),
    ]
    assert simplify(path, UNITS) == [
        "80 < humidity_percent ≤ 92.46 %",
        "location not in {HAN, LCA}",
        "month_local ≥ 5",
    ]
    assert simplify([("location_code_LCA", ">", 0.5)], UNITS) == ["location = LCA"]
    assert simplify([("hour_local", "<=", 6.5)], UNITS) == ["hour_local ≤ 6"]
    assert simplify([("location_code_LCA", "<=", 0.5)], UNITS) == ["location ≠ LCA"]


def test_leaf_rules_cover_every_row_and_match_the_model():
    model, x, y = _model()
    rules = leaf_rules(model, x, y, threshold=0.5, units=UNITS)
    tree = model.named_steps["tree"]
    assert len(rules) == tree.get_n_leaves()
    assert rules["rows"].sum() == len(x) and np.isclose(rules["row_share"].sum(), 1.0)

    # The learned rule uses real units and the location split
    text = " | ".join(rules["rule"])
    assert "location = LCA" in text and "rain_past_12h_mm > 1" in text
    humidity = re.search(r"humidity_percent > ([\d.]+) %", text)
    assert humidity and abs(float(humidity.group(1)) - 80) < 1

    # Leaf output equals predict_proba for rows reaching that leaf
    leaves = tree.apply(model.named_steps["pre"].transform(x))
    proba = model.predict_proba(x)[:, 1]
    for _, r in rules.iterrows():
        assert np.allclose(proba[leaves == r["leaf"]], r["model_probability"])


def test_decision_path_ends_in_the_rows_leaf():
    model, x, y = _model()
    row = x.iloc[[0]]
    steps = decision_path(model, row, UNITS)
    rules = leaf_rules(model, x, y, threshold=0.5, units=UNITS)
    leaf = model.named_steps["tree"].apply(model.named_steps["pre"].transform(row))[0]
    rule = rules.set_index("leaf").loc[leaf, "rule"]
    # Every merged condition of the leaf rule appears among the row's raw steps' features
    for cond in rule.split(" AND "):
        feature = cond.split(" ")[2] if cond[0].isdigit() else cond.split(" ")[0]
        assert any(feature in s for s in steps["condition"])
    assert len(steps) == rules.set_index("leaf").loc[leaf, "depth"]


def test_node_table_counts_rows_down_the_tree():
    model, x, y = _model()
    nodes = node_table(model, x, y, UNITS, max_depth=2).set_index("node")
    assert nodes.loc[0, "rows"] == len(x) and nodes["depth"].max() == 2
    assert np.isclose(nodes.loc[0, "observed_rain_share"], np.mean(y))
    for node, r in nodes[nodes["depth"] == 1].iterrows():
        assert r["parent"] == 0 and r["branch"] in ("yes", "no")
    children = nodes[nodes["parent"] == 0]
    assert children["rows"].sum() == len(x)
    # The "yes" child holds the rows satisfying the root split, recomputed independently
    tree = model.named_steps["tree"]
    left = tree.tree_.children_left[0]
    xt = model.named_steps["pre"].transform(x)
    expected = int((xt[:, tree.tree_.feature[0]] <= tree.tree_.threshold[0]).sum())
    assert nodes.loc[left, "rows"] == expected and nodes.loc[left, "branch"] == "yes"


def test_hours_to_first_rain_uses_future_hours_and_stops_at_gaps():
    from cart_rules import hours_to_first_rain

    ts = pd.date_range("2024-01-01", periods=40, freq="h", tz="UTC")
    rain = np.zeros(40)
    rain[[5, 30]] = 1.0
    hourly = pd.DataFrame({"location_code": "HAN", "timestamp_utc": ts, "precipitation_mm": rain})
    on = hours_to_first_rain(hourly.drop(index=20)).set_index("origin_utc")["onset_hours"]
    assert on[ts[0]] == 5 and on[ts[4]] == 1
    assert np.isnan(on[ts[5]])  # rain at t itself is not "next"; next rain hour 30 is behind a gap
    assert on[ts[21]] == 9  # after the gap: 30 - 21
    assert np.isnan(on[ts[31]])  # no rain within 24 h


def test_missed_rain_profile_counts_misses():
    from cart_rules import missed_rain_profile

    df = pd.DataFrame(
        {
            "rain_next24h": [True, True, True, True, False],
            "probability": [0.9, 0.35, 0.1, 0.5, 0.6],
            "rain_past_24h_mm": [3.0, 0.0, 0.0, 0.0, 0.0],
            "rain_next24h_mm": [10.0, 2.0, 8.0, 3.0, 0.0],
            "month_local": [7, 1, 7, 7, 1],
            "onset_hours": [1, 20, 5, 2, np.nan],
        }
    )
    p = missed_rain_profile(df, threshold=0.4)
    assert p["rain_cases"] == 4 and p["missed"] == 2
    assert p["missed_with_probability_0_30_to_threshold"] == 0.5
    assert p["missed_under_5_mm"] == 0.5 and p["missed_dry_past_24h"] == 1.0
    assert p["recall_after_wet_24h"] == 1.0 and np.isclose(p["recall_after_dry_24h"], 1 / 3)
    assert p["recall_after_dry_24h_dry_season"] == 0.0
    onset = p["recall_by_onset"].set_index("onset")
    assert onset.loc["1-3 h", "cases"] == 2 and onset.loc["19-24 h", "recall"] == 0.0
