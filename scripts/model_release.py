#!/usr/bin/env python3
"""scripts/model_release.py.

Export or import the release bundle of a frozen CART model (found necessary by the
clean-machine rerun, workbook ID 34).

Model v1 was trained on labels rain_next24h_v0.2, whose float-sum bug was fixed on
2026-10-22, so today's code cannot rebuild its labels and features v1. A new machine gets
them, the model, and its one-time test evaluation from this bundle instead. Labels and
features v2, and everything else, are rebuilt from the sources by the pipeline.

Bundle: a directory holding the Gold objects under their keys, plus release.json (version,
model SHA-256 pin, and key, bytes, SHA-256 of every object).

  export --version v1 --out DIR   read the objects from Gold, check the model against the pin
  import --from DIR               check every file and the model pin, then upload; an object
                                  already in Gold with the same bytes is skipped, one with
                                  other bytes is refused (nothing is overwritten)
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import train_cart
from evaluate_cart_test import _exists
from load_gold_observations import BUCKET_GOLD, get_minio_client
from minio import Minio
from train_cart import _get, model_keys

RELEASE_OBJECTS: dict[str, list[str]] = {
    "v1": [
        "ml_labels/rain_next24h_labels_v1.json",
        "ml_labels/rain_next24h_labels_v1.parquet",
        "ml_features/cart_rain_features_v1.json",
        "ml_features/cart_rain_train_2016_2023_v1.parquet",
        "ml_features/cart_rain_val_2024_v1.parquet",
        "ml_features/cart_rain_test_2025_v1.parquet",
        "ml_models/cart_rain_next24h_v1/model.joblib",
        "ml_models/cart_rain_next24h_v1/report.json",
        "ml_models/cart_rain_next24h_v1/test_evaluation.json",
        "ml_models/cart_rain_next24h_v1/test_evaluation_corrected_labels.json",
        "ml_models/cart_rain_next24h_v1/test_predictions.parquet",
        "ml_models/cart_rain_next24h_v1/threshold_error_analysis.json",
    ],
}


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _pin(version: str) -> str:
    released = train_cart.RELEASED_MODELS  # read at call time
    if version not in RELEASE_OBJECTS or version not in released:
        raise ValueError(f"{version} is not a released model")
    return released[version]["model_sha256"]


def export_release(version: str, out: Path, client: Minio | None = None) -> dict[str, Any]:
    client = client or get_minio_client()
    pin, model_key = _pin(version), model_keys(version)[0]
    objects = []
    for key in RELEASE_OBJECTS[version]:
        body = _get(client, key)
        if key == model_key and _sha(body) != pin:
            raise ValueError(f"{key} in Gold differs from the released {version} (SHA-256)")
        path = out / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        objects.append({"key": key, "bytes": len(body), "sha256": _sha(body)})
    release = {
        "model_version": version,
        "model_sha256": pin,
        "bucket": BUCKET_GOLD,
        "objects": objects,
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    (out / "release.json").write_text(json.dumps(release, indent=2), encoding="utf-8")
    print(json.dumps({"exported": len(objects), "bytes": sum(o["bytes"] for o in objects)}))
    return release


def import_release(source: Path, client: Minio | None = None) -> dict[str, int]:
    client = client or get_minio_client()
    release = json.loads((source / "release.json").read_text(encoding="utf-8"))
    version = release["model_version"]
    if release["model_sha256"] != _pin(version):
        raise ValueError(f"bundle pins another model than the released {version}")
    if sorted(o["key"] for o in release["objects"]) != sorted(RELEASE_OBJECTS[version]):
        raise ValueError(f"bundle does not hold exactly the {version} release objects")
    bodies = {}
    for o in release["objects"]:  # check everything before uploading anything
        body = (source / o["key"]).read_bytes()
        if _sha(body) != o["sha256"]:
            raise ValueError(f"{o['key']} does not match release.json (SHA-256)")
        bodies[o["key"]] = body
    if _sha(bodies[model_keys(version)[0]]) != release["model_sha256"]:
        raise ValueError("bundle model differs from its pin")
    for key, body in bodies.items():
        if _exists(client, key) and _sha(_get(client, key)) != _sha(body):
            raise RuntimeError(f"{key} already exists in Gold with other bytes; nothing imported")
    counts = {"uploaded": 0, "identical_skipped": 0}
    for key, body in bodies.items():
        if _exists(client, key):
            counts["identical_skipped"] += 1
            continue
        client.put_object(BUCKET_GOLD, key, io.BytesIO(body), len(body))
        counts["uploaded"] += 1
    print(json.dumps({"model_version": version, **counts}))
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Export or import a model release (ID 34).")
    sub = parser.add_subparsers(dest="command", required=True)
    exp = sub.add_parser("export")
    exp.add_argument("--version", default="v1")
    exp.add_argument("--out", type=Path, required=True)
    imp = sub.add_parser("import")
    imp.add_argument("--from", dest="source", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "export":
            export_release(args.version, args.out)
        else:
            import_release(args.source)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
