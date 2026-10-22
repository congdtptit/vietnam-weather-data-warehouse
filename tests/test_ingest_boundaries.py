"""Unit tests for scripts/ingest_boundaries.py (workbook ID 4). Synthetic squares, mock MinIO."""

import hashlib
import json
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import ingest_boundaries as ib
import pytest
from ingest_open_meteo import NORTH_LOCATIONS
from minio.error import S3Error

ROOT = Path(__file__).resolve().parents[1]


def _square(lon, lat, half=0.05):
    ring = [
        [lon - half, lat - half],
        [lon + half, lat - half],
        [lon + half, lat + half],
        [lon - half, lat + half],
        [lon - half, lat - half],
    ]
    return {"type": "Polygon", "coordinates": [ring]}


def _source(**changes):
    """64 units: the 25 northern provinces, 38 other provinces, and Côn Đảo."""
    at_point = {p: c for c, p in ib.RESEARCH_PROVINCES.items()}
    names = [n for region in ib.NORTHERN_REGIONS.values() for n in region]
    names += [f"Southern {i}" for i in range(38)] + ["Côn Đảo"]
    features = []
    for i, name in enumerate(names):
        if name in at_point:
            loc = NORTH_LOCATIONS[at_point[name]]
            geom = _square(loc["longitude"], loc["latitude"])
        else:
            geom = _square(100.0 + (i % 10) * 0.2, 10.0 + (i // 10) * 0.2)
        raw_name = "Hà Nội\t" if name == "Hà Nội" else name
        features.append(
            {
                "type": "Feature",
                "properties": {"shapeName": raw_name, "shapeISO": f"VN-{i}", "shapeID": f"id{i}"},
                "geometry": geom,
            }
        )
    data = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": ib.CRS84}},
        "features": features,
    }
    for k, v in changes.items():
        data[k] = v
    return data


def _bytes(data):
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


METADATA = {"gjDownloadURL": ib.SOURCE_URL, "boundaryLicense": "Public Domain"}


def _client():
    store: dict[tuple[str, str], bytes] = {}
    client = Mock()

    def get(bucket, key):
        if (bucket, key) not in store:
            raise S3Error("NoSuchKey", "missing", "/r", "req", "host", None)
        body = store[(bucket, key)]
        return SimpleNamespace(read=lambda: body, close=lambda: None, release_conn=lambda: None)

    def put(bucket, key, data, length, content_type=None):
        store[(bucket, key)] = data.read()

    client.get_object.side_effect = get
    client.put_object.side_effect = put
    return client, store


def _fetcher(source_bytes, metadata=METADATA):
    pages = {ib.SOURCE_URL: source_bytes, ib.METADATA_URL: _bytes(metadata)}
    return lambda url: (pages[url], url)


def test_northern_subset_keeps_geometry_and_maps_every_research_point():
    data = _source()
    silver, report = ib.build_silver(_bytes(data))
    assert len(silver["features"]) == 25 and report["source_provinces"] == 63
    assert report["names_cleaned"] == ["Hà Nội\t"]
    observed = {f["properties"]["province_name"]: f for f in silver["features"]}
    assert {f["properties"]["location_code"] for f in observed.values()} - {None} == set(
        ib.RESEARCH_PROVINCES
    )
    assert sum(f["properties"]["has_observations"] for f in observed.values()) == 8
    source_han = next(f for f in data["features"] if f["properties"]["shapeName"] == "Hà Nội\t")
    assert observed["Hà Nội"]["geometry"] == source_han["geometry"]  # unchanged
    assert set(report["research_point_checks"]) == set(ib.RESEARCH_PROVINCES)


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda d: d["features"].pop(), "63 units"),
        (lambda d: d.update(crs={"properties": {"name": "EPSG:3857"}}), "unexpected CRS"),
        (
            lambda d: d["features"][0]["geometry"].update(
                coordinates=[[[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]]
            ),
            "invalid geometries",
        ),
        (lambda d: d["features"][1]["properties"].update(shapeName="Somewhere"), "missing"),
    ],
)
def test_unexpected_source_is_rejected(mutate, message):
    data = _source()
    mutate(data)
    with pytest.raises(ValueError, match=message):
        ib.build_silver(_bytes(data))


def test_research_point_in_the_wrong_province_is_rejected():
    data = _source()
    by_name = {f["properties"]["shapeName"]: f for f in data["features"]}
    by_name["Phú Thọ"]["geometry"], by_name["Yên Bái"]["geometry"] = (
        by_name["Yên Bái"]["geometry"],
        by_name["Phú Thọ"]["geometry"],
    )
    with pytest.raises(ValueError, match="PTO point falls in"):
        ib.build_silver(_bytes(data))


def test_run_publishes_once_and_refuses_changed_source():
    client, store = _client()
    raw = _bytes(_source())
    first = ib.run(client=client, fetcher=_fetcher(raw))
    assert first["bronze_written"] is True
    assert store[("bronze", ib.BRONZE_KEY)] == raw
    manifest = json.loads(store[("bronze", ib.BRONZE_MANIFEST_KEY)])
    assert manifest["sha256"] == hashlib.sha256(raw).hexdigest()
    assert manifest["source_commit"] == ib.SOURCE_COMMIT and manifest["feature_count"] == 64
    silver = json.loads(store[("silver", ib.SILVER_KEY)])
    assert len(silver["features"]) == 25
    assert json.loads(store[("silver", ib.SILVER_REPORT_KEY)])["input_sha256"] == manifest["sha256"]

    again = ib.run(client=client, fetcher=_fetcher(raw))
    assert again["bronze_written"] is False

    changed = _source()
    changed["features"][-1]["properties"]["shapeISO"] = "VN-X"
    with pytest.raises(RuntimeError, match="different content"):
        ib.run(client=client, fetcher=_fetcher(_bytes(changed)))
    assert store[("bronze", ib.BRONZE_KEY)] == raw


def test_metadata_of_another_build_publishes_nothing():
    client, store = _client()
    other = {**METADATA, "gjDownloadURL": "https://github.com/x/raw/abcdef0/file.geojson"}
    with pytest.raises(RuntimeError, match="review the new build"):
        ib.run(client=client, fetcher=_fetcher(_bytes(_source()), other))
    assert store == {}


def test_research_provinces_and_points_match_the_dim_location_seed():
    sql = (ROOT / "sql" / "init_dwh_schema.sql").read_text(encoding="utf-8")
    rows = re.findall(r"\('(\w{3})', '[^']+', '([^']+)', ([\d.]+), ([\d.]+), [\d.]+\)", sql)
    seed = {code: (prov, float(lat), float(lon)) for code, prov, lat, lon in rows}
    assert set(seed) == set(ib.RESEARCH_PROVINCES)
    for code, province in ib.RESEARCH_PROVINCES.items():
        loc = NORTH_LOCATIONS[code]
        assert seed[code] == (province, loc["latitude"], loc["longitude"])
    northern = [n for names in ib.NORTHERN_REGIONS.values() for n in names]
    assert len(northern) == len(set(northern)) == 25
    assert set(ib.RESEARCH_PROVINCES.values()) <= set(northern)
