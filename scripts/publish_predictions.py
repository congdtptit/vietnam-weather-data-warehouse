#!/usr/bin/env python3
"""scripts/publish_predictions.py.

Publish the Gold predictions of a scoring window into the DW, or check that the DW matches
them (workbook ID 32; the predictions come from scripts/score_cart.py).

The model must already be registered in dim_model (scripts/load_predictions.py, ID 31) with
the same artifact SHA-256 and probability threshold as the predictions. This script never
inserts or changes dim_model.

Publication covers one scope, (model, origins in the window), in one transaction:
- rows of the scope that are absent from Gold are deleted;
- new rows are inserted;
- existing rows are updated only when a value differs.
An unchanged rerun therefore writes nothing, and predicted_at keeps its value.

Modes:
- --dry-run does the same work and rolls it back.
- --check compares the DW scope with Gold (keys, probability rounded to 4 decimals as stored,
  class, threshold, window, split) and fails on any difference. It writes
  quality_reports/prediction_reconcile_<version>_<start>_<end>_<timestamp>.json (bucket gold).
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
from evaluate_cart_test import _exists
from load_gold_observations import BUCKET_GOLD, get_db_connection, get_minio_client
from load_predictions import prediction_records
from minio import Minio
from psycopg2.extras import execute_values
from score_cart import SPLIT, prediction_key
from train_cart import MODEL_VERSION, _get

UPSERT_SQL = """
    INSERT INTO fact_weather_prediction AS p (
        prediction_date_key, prediction_time_key, target_date_key, location_key, model_name,
        model_version, rain_next_24h, rain_probability, model_key, origin_utc,
        target_start_utc, target_end_utc, horizon_hours, probability_threshold,
        evaluation_split
    ) VALUES %s
    ON CONFLICT (model_key, location_key, origin_utc) DO UPDATE SET
        prediction_date_key = EXCLUDED.prediction_date_key,
        prediction_time_key = EXCLUDED.prediction_time_key,
        target_date_key = EXCLUDED.target_date_key,
        model_name = EXCLUDED.model_name,
        model_version = EXCLUDED.model_version,
        rain_next_24h = EXCLUDED.rain_next_24h,
        rain_probability = EXCLUDED.rain_probability,
        target_start_utc = EXCLUDED.target_start_utc,
        target_end_utc = EXCLUDED.target_end_utc,
        horizon_hours = EXCLUDED.horizon_hours,
        probability_threshold = EXCLUDED.probability_threshold,
        evaluation_split = EXCLUDED.evaluation_split,
        predicted_at = CURRENT_TIMESTAMP
    WHERE (p.prediction_date_key, p.prediction_time_key, p.target_date_key, p.model_name,
           p.model_version, p.rain_next_24h, p.rain_probability, p.target_start_utc,
           p.target_end_utc, p.horizon_hours, p.probability_threshold, p.evaluation_split)
        IS DISTINCT FROM
          (EXCLUDED.prediction_date_key, EXCLUDED.prediction_time_key, EXCLUDED.target_date_key,
           EXCLUDED.model_name, EXCLUDED.model_version, EXCLUDED.rain_next_24h,
           EXCLUDED.rain_probability, EXCLUDED.target_start_utc, EXCLUDED.target_end_utc,
           EXCLUDED.horizon_hours, EXCLUDED.probability_threshold, EXCLUDED.evaluation_split)
    RETURNING (xmax = 0) AS inserted;
"""

RETRACT_SQL = """
    DELETE FROM fact_weather_prediction p
    WHERE p.model_key = %s AND p.origin_utc >= %s AND p.origin_utc < %s
      AND NOT EXISTS (
          SELECT 1 FROM unnest(%s::int[], %s::timestamptz[]) AS k(location_key, origin_utc)
          WHERE k.location_key = p.location_key AND k.origin_utc = p.origin_utc
      );
"""

SCOPE_SQL = """
    SELECT l.location_code, p.origin_utc, p.target_start_utc, p.target_end_utc,
           p.rain_probability, p.rain_next_24h, p.probability_threshold, p.evaluation_split,
           p.horizon_hours
    FROM fact_weather_prediction p
    JOIN dim_location l ON l.location_key = p.location_key
    WHERE p.model_key = %s AND p.origin_utc >= %s AND p.origin_utc < %s
"""


def window_bounds(start: date, end: date) -> tuple[datetime, datetime]:
    """[first origin, day after the last origin day) in UTC."""
    first = pd.Timestamp(start, tz="UTC")
    return first.to_pydatetime(), (
        pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)
    ).to_pydatetime()


def read_gold(client: Minio, start: date, end: date, version: str) -> pd.DataFrame:
    """Every day of the window must have a Gold object of `version`; score the window first."""
    days = pd.date_range(start, end, freq="D").date
    keys = [prediction_key(d) for d in days]
    missing = [k for k in keys if not _exists(client, k)]
    if missing:
        raise RuntimeError(
            f"{len(missing)} Gold prediction objects missing (e.g. {missing[0]}); "
            "run scripts/score_cart.py for the window first"
        )
    preds = pd.concat(
        [pd.read_parquet(io.BytesIO(_get(client, k))) for k in keys], ignore_index=True
    )
    if set(preds["model_version"]) != {version}:
        raise ValueError(f"Gold holds models {sorted(set(preds['model_version']))}, not {version}")
    if (preds["evaluation_split"] != SPLIT).any():
        raise ValueError(f"Gold predictions outside the {SPLIT} split")
    if preds.duplicated(["location_code", "origin_utc"]).any():
        raise ValueError("duplicate (location, origin) predictions in Gold")
    return preds


def registered_model(cur, preds: pd.DataFrame) -> dict[str, Any]:
    """The dim_model row the predictions belong to; it must match their artifact."""
    ids = preds[["model_name", "model_version", "model_sha256", "probability_threshold"]]
    ids = ids.drop_duplicates()
    if len(ids) != 1:
        raise ValueError(f"the window mixes models or thresholds: {ids.to_dict('records')}")
    name, version, sha, threshold = ids.iloc[0]
    cur.execute(
        "SELECT model_key, artifact_sha256, probability_threshold FROM dim_model "
        "WHERE model_name = %s AND model_version = %s",
        (name, version),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(
            f"{name} {version} is not in dim_model; register it with scripts/load_predictions.py"
        )
    model_key, dw_sha, dw_threshold = row
    if dw_sha != sha:
        raise ValueError(f"dim_model SHA-256 of {name} {version} differs from the scored artifact")
    if float(dw_threshold) != float(threshold):
        raise ValueError(f"dim_model threshold {dw_threshold} differs from the scored {threshold}")
    return {
        "model_key": model_key,
        "model_name": name,
        "model_version": version,
        "probability_threshold": float(threshold),
    }


def publish(conn, preds: pd.DataFrame, start: date, end: date, dry_run: bool = False):
    lower, upper = window_bounds(start, end)
    try:
        with conn.cursor() as cur:
            model = registered_model(cur, preds)
            cur.execute("SELECT location_code, location_key FROM dim_location")
            location_keys = dict(cur.fetchall())
            records = prediction_records(preds, model["model_key"], location_keys, model)
            cur.execute(
                RETRACT_SQL,
                (
                    model["model_key"],
                    lower,
                    upper,
                    [rec[3] for rec in records],
                    [rec[9] for rec in records],
                ),
            )
            retracted = cur.rowcount
            changed = execute_values(cur, UPSERT_SQL, records, page_size=2000, fetch=True)
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    inserted = sum(1 for (is_insert,) in changed if is_insert)
    return {
        "model_key": model["model_key"],
        "window_origin_utc": [lower.isoformat(), upper.isoformat()],
        "rows": len(records),
        "inserted": inserted,
        "updated": len(changed) - inserted,
        "unchanged": len(records) - len(changed),
        "retracted": retracted,
        "dry_run": dry_run,
    }


def compare(gold: pd.DataFrame, dw: pd.DataFrame) -> dict[str, Any]:
    """Differences between Gold predictions and the DW rows of the same scope."""
    key = ["location_code", "origin_utc"]
    # Same rounding as the loader (Python round), not numpy's, so stored values compare equal
    g = gold.assign(rain_probability=[round(float(p), 4) for p in gold["probability"]])
    merged = g.merge(dw, on=key, how="outer", suffixes=("_gold", "_dw"), indicator=True)
    both = merged[merged["_merge"] == "both"]
    mismatch = {
        "probability": (both["rain_probability_gold"] != both["rain_probability_dw"]).sum(),
        "class": (both["predicted_rain"] != both["rain_next_24h"]).sum(),
        "threshold": (both["probability_threshold_gold"] != both["probability_threshold_dw"]).sum(),
        "window": (
            (both["target_start_utc_gold"] != both["target_start_utc_dw"])
            | (both["target_end_utc_gold"] != both["target_end_utc_dw"])
        ).sum(),
        "split": (both["evaluation_split_gold"] != both["evaluation_split_dw"]).sum(),
    }
    return {
        "gold_rows": len(gold),
        "dw_rows": len(dw),
        "missing_in_dw": int((merged["_merge"] == "left_only").sum()),
        "extra_in_dw": int((merged["_merge"] == "right_only").sum()),
        "mismatches": {k: int(v) for k, v in mismatch.items()},
    }


def read_dw_scope(cur, model_key: int, start: date, end: date) -> pd.DataFrame:
    lower, upper = window_bounds(start, end)
    cur.execute(SCOPE_SQL, (model_key, lower, upper))
    cols = [d[0] for d in cur.description]
    dw = pd.DataFrame(cur.fetchall(), columns=cols)
    for col in ("origin_utc", "target_start_utc", "target_end_utc"):
        dw[col] = pd.to_datetime(dw[col], utc=True)
    dw["rain_probability"] = dw["rain_probability"].astype(float)
    dw["probability_threshold"] = dw["probability_threshold"].astype(float)
    return dw


def check(conn, client: Minio, preds: pd.DataFrame, start: date, end: date) -> dict[str, Any]:
    with conn.cursor() as cur:
        model = registered_model(cur, preds)
        dw = read_dw_scope(cur, model["model_key"], start, end)
    conn.rollback()  # read-only
    result = {
        "check": "DW predictions equal the Gold predictions of the window (ID 32)",
        "model_name": model["model_name"],
        "model_version": model["model_version"],
        "window_origin_dates_utc": [start.isoformat(), end.isoformat()],
        **compare(preds, dw),
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    result["passed"] = (
        result["gold_rows"] == result["dw_rows"]
        and not result["missing_in_dw"]
        and not result["extra_in_dw"]
        and not any(result["mismatches"].values())
    )
    key = (
        f"quality_reports/prediction_reconcile_{model['model_version']}_{start:%Y%m%d}_"
        f"{end:%Y%m%d}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    )
    body = json.dumps(result, indent=2).encode("utf-8")
    client.put_object(BUCKET_GOLD, key, io.BytesIO(body), len(body), "application/json")
    result["report_key"] = f"{BUCKET_GOLD}/{key}"
    return result


def run(
    start: date,
    end: date,
    mode: str = "publish",
    version: str = MODEL_VERSION,
    conn=None,
    client: Minio | None = None,
) -> dict[str, Any]:
    client = client or get_minio_client()
    preds = read_gold(client, start, end, version)
    own = conn is None
    conn = conn or get_db_connection()
    try:
        if mode == "check":
            result = check(conn, client, preds, start, end)
        else:
            result = publish(conn, preds, start, end, dry_run=mode == "dry-run")
    finally:
        if own:
            conn.close()
    print(json.dumps(result))
    if mode == "check" and not result["passed"]:
        raise RuntimeError("DW predictions differ from Gold; see " + result["report_key"])
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish or check scored predictions (ID 32).")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True, help="UTC")
    parser.add_argument("--end-date", type=date.fromisoformat, required=True, help="UTC, inclusive")
    parser.add_argument("--version", default=MODEL_VERSION, help="Model version tag")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Publish, then roll back")
    mode.add_argument("--check", action="store_true", help="Compare the DW with Gold")
    args = parser.parse_args()
    selected = "check" if args.check else "dry-run" if args.dry_run else "publish"
    try:
        run(args.start_date, args.end_date, selected, args.version)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
