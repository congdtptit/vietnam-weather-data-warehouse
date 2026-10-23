#!/usr/bin/env python3
"""scripts/assign_stations_geo.py.

Station-to-point distances and nearest-station assignment with GeoPandas (workbook ID 11).

A read-only check of the station mappings in scripts/station_config.py, which the loaders
and dim_station use. Nothing is written to the DW or to the registry.

- Distances: the research points (ingest_open_meteo.NORTH_LOCATIONS) and every station with
  verified coordinates are projected to WGS 84 / UTM zone 48N (EPSG:32648). That zone spans
  102-108°E and so covers the region. For the mapped pairs (under 30 km) the projected
  distances equal the WGS84 geodesic within 0.01 km; over a few hundred kilometres the UTM
  scale error reaches about 0.04%. The stored distance_km values are spherical great-circle
  (haversine) distances rounded to 0.1 km. A sphere and the ellipsoid differ by up to about
  0.6%, so a stored value must match within 0.1 km + 0.6%. The report keeps both values.
- Assignment: geopandas.sjoin_nearest gives each point its nearest station within --max-km.
  The default, 30 km, is a parameter of this check, not a decision; the largest decided
  mapping is 27.3 km.
  - The GHCNh daily-rain mapping must equal that result.
  - Differences from the Meteostat hourly mapping are reported with the registry status of
    the nearer station; the hourly mapping also depends on which stations Meteostat serves.
- Provinces: points and mapped stations are joined to the pre-merger province polygons in
  Silver (ID 4). Each point must fall in its dim_location province. A mapped station outside
  its point's province is reported, not failed: dim_station stores no province.
- DW: dim_station must hold the registry's location, role, and distance.

Report (bucket gold): quality_reports/station_assignment_geo_<timestamp>.json. Exit 1 when a
check fails.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from datetime import datetime, timezone
from typing import Any

import geopandas as gpd
import pandas as pd
from ingest_boundaries import RESEARCH_PROVINCES, SILVER_KEY
from ingest_open_meteo import NORTH_LOCATIONS
from load_gold_observations import BUCKET_GOLD, BUCKET_SILVER, get_db_connection, get_minio_client
from shapely.geometry import Point
from station_config import GHCNH_RAIN_STATIONS, STATION_REGISTRY, get_canonical_stations

UTM_48N = "EPSG:32648"
WGS84 = "EPSG:4326"
TOLERANCE_KM = 0.1  # rounding of the stored values
SPHERE_SHARE = 0.006  # spherical versus ellipsoidal distance
DEFAULT_MAX_KM = 30.0


def points_frame() -> gpd.GeoDataFrame:
    rows = [
        {"location_code": code, "geometry": Point(p["longitude"], p["latitude"])}
        for code, p in sorted(NORTH_LOCATIONS.items())
    ]
    return gpd.GeoDataFrame(rows, crs=WGS84)


def stations_frame() -> gpd.GeoDataFrame:
    """Every registry station with verified coordinates (48816 has none)."""
    rows = [
        {
            "station_id": sid,
            "station_name": s["station_name"],
            "status": s["status"],
            "geometry": Point(s["longitude"], s["latitude"]),
        }
        for sid, s in sorted(STATION_REGISTRY.items())
        if s.get("latitude") is not None and s.get("longitude") is not None
    ]
    return gpd.GeoDataFrame(rows, crs=WGS84)


def mappings() -> dict[str, dict[str, dict[str, Any]]]:
    """Decided mappings per source: station id -> location, role, stored distance."""
    meteostat = {
        sid: {
            "location_code": s["location_code"],
            "role": s["role"],
            "distance_km": s["distance_km"],
        }
        for sid, s in get_canonical_stations().items()
    }
    return {"Meteostat hourly": meteostat, "GHCNh daily rain": dict(GHCNH_RAIN_STATIONS)}


def distance_matrix(points: gpd.GeoDataFrame, stations: gpd.GeoDataFrame) -> pd.DataFrame:
    """Kilometres in UTM 48N; rows are locations, columns station ids."""
    p, s = points.to_crs(UTM_48N), stations.to_crs(UTM_48N)
    return pd.DataFrame(
        {
            sid: p.geometry.distance(geom).to_numpy() / 1000
            for sid, geom in zip(s["station_id"], s.geometry)
        },
        index=p["location_code"],
    )


def nearest(
    points: gpd.GeoDataFrame, stations: gpd.GeoDataFrame, max_km: float
) -> dict[str, dict[str, Any] | None]:
    """Nearest station of each point within max_km (sjoin_nearest); ties are flagged."""
    joined = gpd.sjoin_nearest(
        points.to_crs(UTM_48N),
        stations.to_crs(UTM_48N),
        how="left",
        max_distance=max_km * 1000,
        distance_col="distance_m",
    )
    out: dict[str, dict[str, Any] | None] = {}
    for loc, g in joined.groupby("location_code", sort=True):
        g = g.dropna(subset=["station_id"])
        out[loc] = (
            None
            if g.empty
            else {
                "station_id": g["station_id"].iloc[0],
                "distance_km": round(float(g["distance_m"].iloc[0]) / 1000, 2),
                "tie_with": sorted(g["station_id"].iloc[1:]),
            }
        )
    return out


def province_of(frame: gpd.GeoDataFrame, provinces: gpd.GeoDataFrame, key: str) -> dict[str, Any]:
    joined = gpd.sjoin(
        frame, provinces[["province_name", "geometry"]], how="left", predicate="within"
    )
    return {
        k: (None if pd.isna(p) else p)
        for k, p in joined.drop_duplicates(key).set_index(key)["province_name"].items()
    }


def check_mappings(
    dist: pd.DataFrame,
    near: dict[str, dict[str, Any] | None],
    stations: gpd.GeoDataFrame,
    station_province: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    status = dict(zip(stations["station_id"], stations["status"]))
    out: dict[str, Any] = {}
    for source, mapped in mappings().items():
        pairs = []
        for sid, m in sorted(mapped.items()):
            loc = m["location_code"]
            km = round(float(dist.loc[loc, sid]), 2)
            ok = abs(km - m["distance_km"]) <= TOLERANCE_KM + SPHERE_SHARE * km
            if not ok:
                failures.append(
                    f"{source} {sid}->{loc}: stored {m['distance_km']} km, GeoPandas {km} km"
                )
            pairs.append(
                {
                    "station_id": sid,
                    "location_code": loc,
                    "role": m["role"],
                    "stored_km": m["distance_km"],
                    "geopandas_km": km,
                    "distance_matches": ok,
                    "station_province": station_province.get(sid),
                    "outside_point_province": station_province.get(sid) != RESEARCH_PROVINCES[loc],
                }
            )
        by_location = {m["location_code"]: sid for sid, m in mapped.items()}
        comparison = {}
        for loc, hit in near.items():
            mapped_sid, nearest_sid = by_location.get(loc), hit and hit["station_id"]
            entry = {
                "mapped": mapped_sid,
                "nearest_within_radius": nearest_sid,
                "agrees": mapped_sid == nearest_sid,
            }
            if not entry["agrees"] and nearest_sid is not None:
                entry["nearest_status"] = status[nearest_sid]
                entry["nearest_km"] = hit["distance_km"]
            comparison[loc] = entry
        if source.startswith("GHCNh"):
            failures += [
                f"GHCNh {loc}: mapped {c['mapped']}, nearest {c['nearest_within_radius']}"
                for loc, c in comparison.items()
                if not c["agrees"]
            ]
        out[source] = {"pairs": pairs, "nearest_comparison": comparison}
    return out, failures


def check_dim_station(conn) -> tuple[list[dict[str, Any]], list[str]]:
    """dim_station rows must carry the registry's location, role, and distance."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.station_id, l.location_code, s.station_role, s.distance_km "
            "FROM dim_station s LEFT JOIN dim_location l ON l.location_key = s.location_key "
            "ORDER BY s.station_id"
        )
        rows = cur.fetchall()
    failures = []
    dw = {sid: (loc, role, None if km is None else float(km)) for sid, loc, role, km in rows}
    for sid, s in STATION_REGISTRY.items():
        loc = s.get("location_code")
        expected = (loc, s.get("role") if loc else None, s.get("distance_km") if loc else None)
        if dw.get(sid) != expected:
            failures.append(f"dim_station {sid}: DW {dw.get(sid)}, registry {expected}")
    return [
        {"station_id": sid, "location_code": v[0], "role": v[1], "distance_km": v[2]}
        for sid, v in dw.items()
    ], failures


def read_provinces(client) -> gpd.GeoDataFrame:
    resp = client.get_object(BUCKET_SILVER, SILVER_KEY)
    try:
        features = json.loads(resp.read())["features"]
    finally:
        resp.close()
        resp.release_conn()
    return gpd.GeoDataFrame.from_features(features, crs=WGS84)


def run(max_km: float = DEFAULT_MAX_KM, client=None, conn=None) -> dict[str, Any]:
    client = client or get_minio_client()
    points, stations = points_frame(), stations_frame()
    provinces = read_provinces(client)
    dist = distance_matrix(points, stations)
    near = nearest(points, stations, max_km)
    point_province = province_of(points, provinces, "location_code")
    station_province = province_of(stations, provinces, "station_id")
    failures = [
        f"{loc} falls in {point_province.get(loc)}, expected {prov}"
        for loc, prov in RESEARCH_PROVINCES.items()
        if point_province.get(loc) != prov
    ]
    mapped, mapping_failures = check_mappings(dist, near, stations, station_province)
    failures += mapping_failures

    own = conn is None
    conn = conn or get_db_connection()
    try:
        dim_station, dw_failures = check_dim_station(conn)
    finally:
        if own:
            conn.close()
    failures += dw_failures

    report = {
        "check": "station distances and nearest-station assignment with GeoPandas (ID 11)",
        "crs": f"{UTM_48N} (WGS 84 / UTM zone 48N)",
        "max_km": max_km,
        "tolerance": f"{TOLERANCE_KM} km + {SPHERE_SHARE:.1%} of the distance",
        "distance_matrix_km": dist.round(2).to_dict(orient="index"),
        "nearest_within_radius": near,
        "point_province": point_province,
        "station_province": station_province,
        "mappings": mapped,
        "dim_station": dim_station,
        "failures": failures,
        "passed": not failures,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    key = f"quality_reports/station_assignment_geo_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    body = json.dumps(report, indent=2, ensure_ascii=False, default=str).encode("utf-8")
    client.put_object(BUCKET_GOLD, key, io.BytesIO(body), len(body), "application/json")
    report["report_key"] = f"{BUCKET_GOLD}/{key}"
    for loc, hit in near.items():
        print(
            f"  {loc}: nearest within {max_km:g} km -> {hit['station_id'] + ' ' + str(hit['distance_km']) + ' km' if hit else 'none'}"
        )
    print(
        json.dumps({k: report[k] for k in ("passed", "failures", "report_key")}, ensure_ascii=False)
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Check station assignment with GeoPandas (ID 11).")
    parser.add_argument(
        "--max-km", type=float, default=DEFAULT_MAX_KM, help="Nearest-station radius"
    )
    args = parser.parse_args()
    try:
        report = run(args.max_km)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
