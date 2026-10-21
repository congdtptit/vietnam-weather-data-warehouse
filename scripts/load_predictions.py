#!/usr/bin/env python3
"""scripts/load_predictions.py.

Load the CART RainNext24h model and its test 2025 predictions into the DW (workbook ID 31).
Requires sql/04_predictions.sql and seeded dim_date/dim_time/dim_location.

Inputs (bucket gold, from IDs 28-29):
  ml_models/cart_rain_next24h_<version>/report.json, test_evaluation.json,
  test_predictions.parquet

Only test 2025 predictions are loaded (decided 2026-10-21): they are the out-of-sample
predictions of the frozen model. The model row is upserted into dim_model on
(model_name, model_version). The (model, evaluation_split) scope is replaced in one
transaction: rows absent from the input are deleted, incoming rows are upserted on
(model_key, location_key, origin_utc). Reruns are therefore duplicate-free.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from typing import Any

import pandas as pd
from evaluate_cart_test import evaluation_keys
from load_gold_observations import get_db_connection, get_minio_client
from minio import Minio
from psycopg2.extras import Json, execute_values
from train_cart import MODEL_VERSION, _get, model_keys

LOCAL_OFFSET = pd.Timedelta(hours=7)  # Vietnam, UTC+7; matches dim_date/dim_time keys
LABEL_SOURCE = "Open-Meteo"
SPLIT = "test"

MODEL_UPSERT_SQL = """
    INSERT INTO dim_model (
        model_name, model_version, algorithm, label_version, label_source, feature_version,
        feature_source, label_threshold_mm, probability_threshold, horizon_hours,
        train_start_utc, train_end_utc, hyperparameters, artifact_uri, artifact_sha256,
        validation_f1, test_precision, test_recall, test_f1, availability_note
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (model_name, model_version) DO UPDATE SET
        algorithm = EXCLUDED.algorithm,
        label_version = EXCLUDED.label_version,
        label_source = EXCLUDED.label_source,
        feature_version = EXCLUDED.feature_version,
        feature_source = EXCLUDED.feature_source,
        label_threshold_mm = EXCLUDED.label_threshold_mm,
        probability_threshold = EXCLUDED.probability_threshold,
        horizon_hours = EXCLUDED.horizon_hours,
        train_start_utc = EXCLUDED.train_start_utc,
        train_end_utc = EXCLUDED.train_end_utc,
        hyperparameters = EXCLUDED.hyperparameters,
        artifact_uri = EXCLUDED.artifact_uri,
        artifact_sha256 = EXCLUDED.artifact_sha256,
        validation_f1 = EXCLUDED.validation_f1,
        test_precision = EXCLUDED.test_precision,
        test_recall = EXCLUDED.test_recall,
        test_f1 = EXCLUDED.test_f1,
        availability_note = EXCLUDED.availability_note
    RETURNING model_key;
"""

PREDICTION_UPSERT_SQL = """
    INSERT INTO fact_weather_prediction (
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
        predicted_at = CURRENT_TIMESTAMP;
"""

RETRACT_SQL = """
    DELETE FROM fact_weather_prediction p
    WHERE p.model_key = %s AND p.evaluation_split = %s
      AND NOT EXISTS (
          SELECT 1 FROM unnest(%s::int[], %s::timestamptz[]) AS k(location_key, origin_utc)
          WHERE k.location_key = p.location_key AND k.origin_utc = p.origin_utc
      );
"""


def model_row(report: dict[str, Any], evaluation: dict[str, Any]) -> tuple[Any, ...]:
    """dim_model values from the ID 28 report and the ID 29 evaluation."""
    if evaluation["model_version"] != report["model_version"]:
        raise ValueError("report and evaluation belong to different model versions")
    if float(evaluation["probability_threshold"]) != float(report["probability_threshold"]):
        raise ValueError("evaluation threshold differs from the frozen report threshold")
    train_start, train_end = report["train_window_utc"]
    test = evaluation["test_metrics_at_threshold"]
    return (
        report["model_name"],
        report["model_version"],
        "sklearn DecisionTreeClassifier (CART)",
        report["label_version"],
        LABEL_SOURCE,
        report["feature_version"],
        report["feature_source"],
        float(report["label_threshold_mm"]),
        float(report["probability_threshold"]),
        24,
        train_start,
        train_end,
        Json({**report["selected_params"], "random_state": report["random_state"]}),
        report["objects"]["model"],
        evaluation["model_sha256"],
        round(float(report["validation_metrics_at_threshold"]["f1"]), 4),
        round(float(test["precision"]), 4),
        round(float(test["recall"]), 4),
        round(float(test["f1"]), 4),
        report["availability"],
    )


def prediction_records(
    preds: pd.DataFrame, model_key: int, location_keys: dict[str, int], report: dict[str, Any]
) -> list[tuple[Any, ...]]:
    """Fact rows with Vietnam-local date/time keys derived from the UTC origin and target end."""
    threshold = float(report["probability_threshold"])
    if (preds["probability_threshold"] != threshold).any():
        raise ValueError("prediction file threshold differs from the report")
    if (preds["predicted_rain"] != (preds["probability"] >= threshold)).any():
        raise ValueError("predicted class does not follow probability >= threshold")
    if preds.duplicated(["location_code", "origin_utc"]).any():
        raise ValueError("duplicate (location, origin) predictions")
    unknown = sorted(set(preds["location_code"]) - set(location_keys))
    if unknown:
        raise ValueError(f"locations missing in dim_location: {unknown}")
    if not (preds["target_end_utc"] - preds["target_start_utc"] == pd.Timedelta(hours=24)).all():
        raise ValueError("target windows are not 24 hours")

    origin_local = preds["origin_utc"] + LOCAL_OFFSET
    target_local = preds["target_end_utc"] + LOCAL_OFFSET
    return [
        (
            int(o.strftime("%Y%m%d")),
            o.hour * 100,
            int(e.strftime("%Y%m%d")),
            location_keys[r.location_code],
            report["model_name"],
            report["model_version"],
            bool(r.predicted_rain),
            round(float(r.probability), 4),
            model_key,
            r.origin_utc.to_pydatetime(),
            r.target_start_utc.to_pydatetime(),
            r.target_end_utc.to_pydatetime(),
            24,
            threshold,
            SPLIT,
        )
        for r, o, e in zip(preds.itertuples(index=False), origin_local, target_local)
    ]


def load_predictions(conn, report, evaluation, preds: pd.DataFrame) -> dict[str, Any]:
    """Upsert the model and replace its test-split predictions in one transaction."""
    try:
        with conn.cursor() as cur:
            cur.execute(MODEL_UPSERT_SQL, model_row(report, evaluation))
            model_key = cur.fetchone()[0]
            cur.execute("SELECT location_code, location_key FROM dim_location")
            location_keys = dict(cur.fetchall())
            records = prediction_records(preds, model_key, location_keys, report)
            cur.execute(
                RETRACT_SQL,
                (
                    model_key,
                    SPLIT,
                    [rec[3] for rec in records],
                    [rec[9] for rec in records],
                ),
            )
            retracted = cur.rowcount
            execute_values(cur, PREDICTION_UPSERT_SQL, records, page_size=2000)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"model_key": model_key, "rows_upserted": len(records), "rows_retracted": retracted}


def read_inputs(client: Minio, version: str) -> tuple[dict, dict, pd.DataFrame]:
    _, report_key = model_keys(version)
    eval_key, pred_key = evaluation_keys(version)
    report = json.loads(_get(client, report_key))
    evaluation = json.loads(_get(client, eval_key))
    preds = pd.read_parquet(io.BytesIO(_get(client, pred_key)))
    return report, evaluation, preds


def run(version: str = MODEL_VERSION, conn=None, client: Minio | None = None) -> dict[str, Any]:
    client = client or get_minio_client()
    report, evaluation, preds = read_inputs(client, version)
    own_conn = conn is None
    conn = conn or get_db_connection()
    try:
        stats = load_predictions(conn, report, evaluation, preds)
    finally:
        if own_conn:
            conn.close()
    print(json.dumps(stats))
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Load CART test predictions into the DW.")
    parser.add_argument("--version", default=MODEL_VERSION, help="Model version tag")
    args = parser.parse_args()
    try:
        run(args.version)
    except (RuntimeError, ValueError) as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
