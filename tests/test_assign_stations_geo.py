"""Unit tests for scripts/assign_stations_geo.py (workbook ID 11). Synthetic geometry."""

from unittest.mock import MagicMock

import assign_stations_geo as geo
import geopandas as gpd
import pandas as pd
import pytest
from pyproj import Geod
from shapely.geometry import Point, box
from station_config import STATION_REGISTRY


def _frame(rows, key):
    return gpd.GeoDataFrame(
        [{key: k, "status": "X", "geometry": Point(lon, lat)} for k, lon, lat in rows],
        crs=geo.WGS84,
    )


def test_projected_distance_equals_the_geodesic_in_the_region():
    han, lca = (105.8542, 21.0285), (103.9707, 22.4856)
    noi_bai, lao_cai = (105.807, 21.221), (103.967, 22.5)
    points = _frame([("HAN", *han), ("LCA", *lca)], "location_code")
    stations = _frame([("48820", *noi_bai), ("48803", *lao_cai)], "station_id")
    dist = geo.distance_matrix(points, stations)
    geod = Geod(ellps="WGS84")

    def km(a, b):
        return geod.inv(*a, *b)[2] / 1000

    # Mapped pairs (tens of km): within 0.01 km; a 254 km pair: UTM scale error under 0.1 %
    assert dist.loc["HAN", "48820"] == pytest.approx(km(han, noi_bai), abs=0.01)
    assert dist.loc["LCA", "48803"] == pytest.approx(km(lca, lao_cai), abs=0.01)
    assert dist.loc["HAN", "48803"] == pytest.approx(km(han, lao_cai), rel=1e-3)


def test_nearest_station_within_the_radius_only():
    points = _frame([("A", 105.0, 21.0), ("B", 107.0, 21.0)], "location_code")
    stations = _frame([("near", 105.05, 21.0), ("far", 105.2, 21.0)], "station_id")
    near = geo.nearest(points, stations, max_km=30)
    assert near["A"]["station_id"] == "near" and near["A"]["distance_km"] == pytest.approx(
        5.2, abs=0.1
    )
    assert near["B"] is None  # more than 30 km from every station


def test_province_join():
    provinces = gpd.GeoDataFrame(
        {"province_name": ["West", "East"]},
        geometry=[box(104.0, 20.0, 105.0, 22.0), box(105.0, 20.0, 106.0, 22.0)],
        crs=geo.WGS84,
    )
    points = _frame([("A", 104.5, 21.0), ("B", 105.5, 21.0), ("C", 108.0, 21.0)], "location_code")
    assert geo.province_of(points, provinces, "location_code") == {
        "A": "West",
        "B": "East",
        "C": None,
    }


def _check(monkeypatch, stored_km, ghcnh_station, nearest_station):
    monkeypatch.setattr(
        geo,
        "mappings",
        lambda: {
            "Meteostat hourly": {
                "48820": {
                    "location_code": "HAN",
                    "role": "nearest_available",
                    "distance_km": stored_km,
                }
            },
            "GHCNh daily rain": {
                ghcnh_station: {
                    "location_code": "HAN",
                    "role": "nearest_available",
                    "distance_km": 11.4,
                }
            },
        },
    )
    dist = pd.DataFrame({"48820": [21.86], "48825": [11.38]}, index=["HAN"])
    near = {"HAN": {"station_id": nearest_station, "distance_km": 11.38, "tie_with": []}}
    stations = pd.DataFrame({"station_id": ["48820", "48825"], "status": ["CANONICAL", "OTHER"]})
    return geo.check_mappings(dist, near, stations, {"48820": "Vĩnh Phúc", "48825": "Hà Nội"})


def test_spherical_stored_distance_passes_and_a_wrong_one_fails(monkeypatch):
    out, failures = _check(monkeypatch, 22.0, "48825", "48825")
    assert failures == []  # 22.0 haversine vs 21.86 ellipsoidal is within 0.1 km + 0.6 %
    pair = out["Meteostat hourly"]["pairs"][0]
    assert pair["outside_point_province"] is True  # reported, not failed
    meteostat = out["Meteostat hourly"]["nearest_comparison"]["HAN"]
    assert meteostat["agrees"] is False and meteostat["nearest_status"] == "OTHER"

    _, failures = _check(monkeypatch, 23.0, "48825", "48825")
    assert failures == ["Meteostat hourly 48820->HAN: stored 23.0 km, GeoPandas 21.86 km"]


def test_ghcnh_mapping_must_be_the_nearest_station(monkeypatch):
    _, failures = _check(monkeypatch, 22.0, "48820", "48825")
    assert any(f.startswith("GHCNh HAN: mapped 48820, nearest 48825") for f in failures)


def test_dim_station_must_match_the_registry():
    rows = []
    for sid, s in STATION_REGISTRY.items():
        loc = s.get("location_code")
        rows.append(
            (sid, loc, s.get("role") if loc else None, s.get("distance_km") if loc else None)
        )
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchall.return_value = rows
    _, failures = geo.check_dim_station(conn)
    assert failures == []

    cur.fetchall.return_value = [(*r[:3], 21.9) if r[0] == "48820" else r for r in rows]
    _, failures = geo.check_dim_station(conn)
    assert failures == [
        "dim_station 48820: DW ('HAN', 'nearest_available', 21.9), registry ('HAN', 'nearest_available', 22.0)"
    ]
