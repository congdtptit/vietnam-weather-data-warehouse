#!/usr/bin/env python3
"""scripts/ingest_boundaries.py.

Pre-merger (63-province) province boundaries of Viet Nam into Bronze and a cleaned northern
subset into Silver (workbook ID 4; decision in docs/task-specs-weeks-2-5.md section 7).

Source: geoBoundaries gbOpen VNM ADM1, pinned to release commit 9469f09 so the bytes do not
change between runs. The source represents boundaries of 2008 (63 provinces; it splits Côn Đảo
from Bà Rịa-Vũng Tàu as a 64th unit) and lists its license as public domain (Wikimedia
Commons); HDX lists the same data under ODbL. Both are recorded in the manifest.

Bronze (bucket bronze, received bytes unchanged):
  spatial/boundaries/vnm_adm1_pre2025_geoboundaries_9469f09.geojson
  spatial/manifests/vnm_adm1_pre2025_geoboundaries_9469f09.json
Silver (bucket silver):
  spatial/boundaries/northern_provinces_pre2025_v1.geojson   25 northern provinces
  spatial/boundaries/northern_provinces_pre2025_v1.json      checks and lineage

Checks before anything is published: GeoJSON in CRS84, the expected unit count, unique
names, valid geometries, all 25 northern provinces present, and every research point inside
exactly one province equal to its dim_location.province.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any, Callable

import requests
from ingest_ghcnh import BUCKET_BRONZE, get_minio_client, is_s3_not_found_error
from ingest_open_meteo import NORTH_LOCATIONS
from load_gold_observations import BUCKET_SILVER
from minio import Minio
from minio.error import S3Error
from shapely.geometry import Point, shape

SOURCE_NAME = "geoBoundaries gbOpen VNM ADM1"
SOURCE_COMMIT = "9469f09592ced973a3448cf66b6100b741b64c0d"
SOURCE_URL = (
    "https://github.com/wmgeolab/geoBoundaries/raw/9469f09/releaseData/gbOpen/VNM/ADM1/"
    "geoBoundaries-VNM-ADM1.geojson"
)
METADATA_URL = "https://www.geoboundaries.org/api/current/gbOpen/VNM/ADM1/"
LICENSE_NOTE = (
    "geoBoundaries metadata: boundaryLicense 'Public Domain', licenseSource Wikimedia Commons. "
    "HDX dataset 'geoboundaries-admin-boundaries-for-viet-nam' lists ODbL. Treat as ODbL "
    "(attribute geoBoundaries and Wikimedia Commons) until clarified."
)

BRONZE_KEY = "spatial/boundaries/vnm_adm1_pre2025_geoboundaries_9469f09.geojson"
BRONZE_MANIFEST_KEY = "spatial/manifests/vnm_adm1_pre2025_geoboundaries_9469f09.json"
SILVER_VERSION = "v1"
SILVER_KEY = f"spatial/boundaries/northern_provinces_pre2025_{SILVER_VERSION}.geojson"
SILVER_REPORT_KEY = f"spatial/boundaries/northern_provinces_pre2025_{SILVER_VERSION}.json"

CRS84 = "urn:ogc:def:crs:OGC:1.3:CRS84"
EXPECTED_UNITS = 64  # 63 provinces + Côn Đảo, which the source splits from Bà Rịa-Vũng Tàu
NON_PROVINCE_UNITS = {"Côn Đảo"}
TRANSIENT_STATUS = (429, 500, 502, 503, 504)

# Northern provinces (pre-merger units) used for the map context layer: the Red River Delta
# region of Decision 368/QĐ-TTg and the Northern Midlands and Mountains region of Decision
# 369/QĐ-TTg (both 2024-05-04).
NORTHERN_REGIONS: dict[str, list[str]] = {
    "Red River Delta": [
        "Hà Nội",
        "Vĩnh Phúc",
        "Bắc Ninh",
        "Quảng Ninh",
        "Hải Dương",
        "Hải Phòng",
        "Hưng Yên",
        "Thái Bình",
        "Hà Nam",
        "Nam Định",
        "Ninh Bình",
    ],
    "Northern Midlands and Mountains": [
        "Hà Giang",
        "Cao Bằng",
        "Bắc Kạn",
        "Tuyên Quang",
        "Lào Cai",
        "Yên Bái",
        "Thái Nguyên",
        "Lạng Sơn",
        "Bắc Giang",
        "Phú Thọ",
        "Điện Biên",
        "Lai Châu",
        "Sơn La",
        "Hòa Bình",
    ],
}

# Province of each research point, as seeded in dim_location.province (sql/init_dwh_schema.sql)
RESEARCH_PROVINCES = {
    "HAN": "Hà Nội",
    "PTO": "Phú Thọ",
    "TNG": "Thái Nguyên",
    "TUQ": "Tuyên Quang",
    "LCA": "Lào Cai",
    "HBI": "Hòa Bình",
    "HPH": "Hải Phòng",
    "NBI": "Ninh Bình",
}


def clean_name(name: str) -> str:
    """Source names may carry stray whitespace (the source has 'Hà Nội' followed by a tab)."""
    return unicodedata.normalize("NFC", name).strip()


def fetch(url: str, max_retries: int = 3, timeout: int = 60) -> tuple[bytes, str]:
    """GET with bounded retries for transient failures; returns (body, final URL)."""
    delay = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
        except requests.RequestException as ex:
            if attempt == max_retries:
                raise RuntimeError(f"{url}: network error after {attempt} attempts: {ex}") from ex
        else:
            if resp.status_code == 200:
                return resp.content, resp.url
            if resp.status_code not in TRANSIENT_STATUS or attempt == max_retries:
                raise RuntimeError(f"{url}: HTTP {resp.status_code}")
        time.sleep(delay)
        delay *= 2
    raise RuntimeError(f"{url}: retries exhausted")


def check_metadata(metadata: dict[str, Any]) -> None:
    """The 'current' metadata must describe the pinned build; otherwise it is not evidence."""
    if SOURCE_COMMIT[:7] not in metadata.get("gjDownloadURL", ""):
        raise RuntimeError(
            "geoBoundaries current metadata no longer points to build "
            f"{SOURCE_COMMIT[:7]} ({metadata.get('gjDownloadURL')}); review the new build"
        )


def build_silver(raw: bytes) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate the source FeatureCollection and return (northern subset, check report).

    Geometry is copied unchanged; only names are cleaned. Raises ValueError on any failed
    check, so nothing is published from an unexpected file.
    """
    data = json.loads(raw)
    if data.get("type") != "FeatureCollection":
        raise ValueError("source is not a GeoJSON FeatureCollection")
    crs = (data.get("crs") or {}).get("properties", {}).get("name", CRS84)
    if crs != CRS84:
        raise ValueError(f"unexpected CRS {crs}")
    features = data["features"]
    if len(features) != EXPECTED_UNITS:
        raise ValueError(f"{len(features)} units, expected {EXPECTED_UNITS}")

    by_name: dict[str, dict[str, Any]] = {}
    cleaned_names = []
    invalid = []
    for f in features:
        raw_name = f["properties"]["shapeName"]
        name = clean_name(raw_name)
        if name != raw_name:
            cleaned_names.append(raw_name)
        if name in by_name:
            raise ValueError(f"duplicate province name {name}")
        by_name[name] = f
        if not shape(f["geometry"]).is_valid:
            invalid.append(name)
    if invalid:
        raise ValueError(f"invalid geometries: {invalid}")
    provinces = set(by_name) - NON_PROVINCE_UNITS
    if len(provinces) != EXPECTED_UNITS - len(NON_PROVINCE_UNITS):
        raise ValueError(f"{len(provinces)} provinces, expected 63")

    northern = {n: region for region, names in NORTHERN_REGIONS.items() for n in names}
    missing = sorted(set(northern) - set(by_name))
    if missing:
        raise ValueError(f"northern provinces missing in source: {missing}")

    geoms = {n: shape(f["geometry"]) for n, f in by_name.items()}
    point_checks = {}
    location_of: dict[str, str] = {}
    for code, province in RESEARCH_PROVINCES.items():
        loc = NORTH_LOCATIONS[code]
        point = Point(loc["longitude"], loc["latitude"])
        hits = sorted(n for n, g in geoms.items() if g.contains(point))
        if hits != [province]:
            raise ValueError(f"{code} point falls in {hits}, expected {province}")
        point_checks[code] = {
            "province": province,
            "distance_to_border_deg": round(geoms[province].boundary.distance(point), 4),
        }
        location_of[province] = code

    out_features = []
    for name in sorted(northern):
        f = by_name[name]
        props = f["properties"]
        out_features.append(
            {
                "type": "Feature",
                "properties": {
                    "province_name": name,
                    "region": northern[name],
                    "shape_iso": props.get("shapeISO"),
                    "source_shape_id": props.get("shapeID"),
                    "location_code": location_of.get(name),
                    "has_observations": name in location_of,
                },
                "geometry": f["geometry"],
            }
        )
    silver = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": CRS84}},
        "features": out_features,
    }
    report = {
        "source_units": len(features),
        "source_provinces": len(provinces),
        "non_province_units": sorted(NON_PROVINCE_UNITS),
        "names_cleaned": cleaned_names,
        "invalid_geometries": 0,
        "northern_provinces": len(out_features),
        "provinces_with_observations": len(location_of),
        "research_point_checks": point_checks,
        "crs": CRS84,
    }
    return silver, report


def _stored_sha256(client: Minio, key: str) -> str | None:
    try:
        resp = client.get_object(BUCKET_BRONZE, key)
    except S3Error as err:
        if is_s3_not_found_error(err):
            return None
        raise
    try:
        return hashlib.sha256(resp.read()).hexdigest()
    finally:
        resp.close()
        resp.release_conn()


def _put_json(client: Minio, bucket: str, key: str, obj: dict[str, Any]) -> None:
    body = json.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8")
    client.put_object(bucket, key, io.BytesIO(body), len(body), content_type="application/json")


def run(
    client: Minio | None = None,
    fetcher: Callable[[str], tuple[bytes, str]] = fetch,
    overwrite: bool = False,
) -> dict[str, Any]:
    client = client or get_minio_client()
    raw, final_url = fetcher(SOURCE_URL)
    meta_bytes, _ = fetcher(METADATA_URL)
    metadata = json.loads(meta_bytes)
    check_metadata(metadata)
    silver, report = build_silver(raw)  # validate before publishing anything
    sha = hashlib.sha256(raw).hexdigest()
    retrieved_at = datetime.now(timezone.utc).isoformat()

    stored = _stored_sha256(client, BRONZE_KEY)
    if stored is not None and stored != sha and not overwrite:
        raise RuntimeError(
            f"{BRONZE_KEY} exists with different content (sha256 {stored[:12]}); "
            "pass --overwrite only after reviewing the change"
        )
    bronze_written = stored != sha
    if bronze_written:
        client.put_object(
            BUCKET_BRONZE,
            BRONZE_KEY,
            io.BytesIO(raw),
            len(raw),
            content_type="application/geo+json",
        )
        manifest = {
            "source": SOURCE_NAME,
            "source_url": SOURCE_URL,
            "resolved_url": final_url,
            "source_commit": SOURCE_COMMIT,
            "metadata_url": METADATA_URL,
            "metadata": {
                k: metadata.get(k)
                for k in (
                    "boundaryID",
                    "boundaryYearRepresented",
                    "boundaryType",
                    "boundaryCanonical",
                    "boundarySource",
                    "boundaryLicense",
                    "licenseSource",
                    "boundarySourceURL",
                    "sourceDataUpdateDate",
                    "buildDate",
                    "admUnitCount",
                    "gjDownloadURL",
                )
            },
            "license_note": LICENSE_NOTE,
            "boundary_basis": "pre-merger provinces (before 2025-07-01); source year 2008",
            "crs": CRS84,
            "feature_count": report["source_units"],
            "bytes": len(raw),
            "sha256": sha,
            "retrieved_at_utc": retrieved_at,
            "object": f"{BUCKET_BRONZE}/{BRONZE_KEY}",
        }
        _put_json(client, BUCKET_BRONZE, BRONZE_MANIFEST_KEY, manifest)

    body = json.dumps(silver, ensure_ascii=False).encode("utf-8")
    client.put_object(
        BUCKET_SILVER, SILVER_KEY, io.BytesIO(body), len(body), content_type="application/geo+json"
    )
    silver_report = {
        "dataset": "northern_provinces_pre2025",
        "version": SILVER_VERSION,
        "input": f"{BUCKET_BRONZE}/{BRONZE_KEY}",
        "input_sha256": sha,
        "region_definition": "Decisions 368/QĐ-TTg and 369/QĐ-TTg (2024-05-04)",
        "transformations": "names NFC-normalized and stripped; geometry unchanged",
        "generated_at_utc": retrieved_at,
        "object": f"{BUCKET_SILVER}/{SILVER_KEY}",
        "bytes": len(body),
        **report,
    }
    _put_json(client, BUCKET_SILVER, SILVER_REPORT_KEY, silver_report)
    summary = {
        "bronze_written": bronze_written,
        "sha256": sha,
        "silver_provinces": report["northern_provinces"],
        "research_point_checks": report["research_point_checks"],
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return {"bronze_written": bronze_written, "sha256": sha, "report": silver_report}


def main() -> int:
    parser = argparse.ArgumentParser(description="Load pre-merger province boundaries (ID 4).")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace the Bronze object if the source bytes changed",
    )
    args = parser.parse_args()
    try:
        run(overwrite=args.overwrite)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except (RuntimeError, ValueError, S3Error) as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
