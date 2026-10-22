"""Station metadata and location mapping configuration for Northern Vietnam weather sources.

Canonical research locations are defined in docs/NAMING_CONVENTIONS.md Table 6 and docs/data-contract.md.
PTO, TUQ, HBI, and NBI currently have no verified station; they are covered by gridded sources only.
"""

from typing import Any

# Canonical research locations in northern Vietnam (docs/NAMING_CONVENTIONS.md Table 6)
CANONICAL_LOCATIONS: set[str] = {
    "HAN",  # Hà Nội
    "PTO",  # Phú Thọ (Việt Trì)
    "TNG",  # Thái Nguyên
    "TUQ",  # Tuyên Quang
    "LCA",  # Lào Cai
    "HBI",  # Hòa Bình
    "HPH",  # Hải Phòng (Phù Liễn)
    "NBI",  # Ninh Bình
}

# Unified station registry shared between ingestion and transformation. Canonical stations
# carry their role and great-circle distance to the research point, as GHCNH_RAIN_STATIONS does.
# Station identities, coordinates, and elevations were verified on 2026-10-21 against
# Meteostat station metadata (https://data.meteostat.net/stations/<id>.json) and the
# NOAA ISD station history (isd-history.csv, USAF = WMO id + "0"). Meteostat coordinates
# are used where Meteostat serves the station, because Meteostat is the ingested source.
# Table 6 originally assigned several WMO ids to the wrong locations; those ids are kept
# below with their verified identity and are no longer mapped to a research location.
STATION_REGISTRY: dict[str, dict[str, Any]] = {
    "48820": {
        "station_name": "Nội Bài (Hà Nội)",
        "location_code": "HAN",
        "latitude": 21.2210,
        "longitude": 105.8070,
        "elevation_m": 11.9,
        "status": "CANONICAL",
        "role": "nearest_available",
        "distance_km": 22.0,
        "notes": (
            "Meteostat 'Ha Noi' lists 21.0167, 105.8000, 6 m (the WMO 48820 position), but its "
            "hourly values come from NOAA ISD 488200 'NOIBAI INTL' (isd_lite) and METAR VVNB, "
            "i.e. Nội Bài airport at 21.221, 105.807, 11.9 m (isd-history.csv). Coordinates "
            "follow the observing site since 2026-10-22; about 22 km from the HAN point"
        ),
    },
    "48826": {
        "station_name": "Phù Liễn (Hải Phòng)",
        "location_code": "HPH",
        "latitude": 20.8000,
        "longitude": 106.6333,
        "elevation_m": 119.0,
        "status": "CANONICAL",
        "role": "representative",
        "distance_km": 7.6,
        "notes": "Meteostat 'Phu Lien'; NOAA ISD 488260 PHU LIEN. Table 6 listed it under TNG",
    },
    "48831": {
        "station_name": "Thái Nguyên",
        "location_code": "TNG",
        "latitude": 21.6000,
        "longitude": 105.8330,
        "elevation_m": 32.0,
        "status": "CANONICAL",
        "role": "representative",
        "distance_km": 1.7,
        "notes": "NOAA ISD 488310 THAI NGUYEN; not served by Meteostat (HTTP 404)",
    },
    "48803": {
        "station_name": "Lào Cai",
        "location_code": "LCA",
        "latitude": 22.5000,
        "longitude": 103.9670,
        "elevation_m": 97.0,
        "status": "CANONICAL",
        "role": "representative",
        "distance_km": 1.6,
        "notes": "NOAA ISD 488030 LAO CAI; not served by Meteostat (HTTP 404)",
    },
    # Ids previously assigned in Table 6 whose verified identity is a different station.
    "48825": {
        "station_name": "Hà Đông (Hà Nội)",
        "location_code": None,
        "latitude": 20.9667,
        "longitude": 105.7667,
        "elevation_m": 6.0,
        "status": "UNMAPPED_MISASSIGNED",
        "notes": (
            "Was mapped to PTO (Việt Trì) in Table 6. NOAA ISD 488250 HA DONG; Meteostat "
            "coordinates match Hà Đông. Pending team decision for Meteostat; used as the "
            "GHCNh daily-rain station for HAN (see GHCNH_RAIN_STATIONS)"
        ),
    },
    "48830": {
        "station_name": "Lạng Sơn",
        "location_code": None,
        "latitude": 21.8333,
        "longitude": 106.7667,
        "elevation_m": 258.0,
        "status": "UNMAPPED_MISASSIGNED",
        "notes": "Was mapped to HBI (Hòa Bình) in Table 6. Meteostat/NOAA ISD: LANG SON",
    },
    "48838": {
        "station_name": "Móng Cái",
        "location_code": None,
        "latitude": 21.5170,
        "longitude": 107.9670,
        "elevation_m": 7.0,
        "status": "UNMAPPED_MISASSIGNED",
        "notes": "Was mapped to HPH in Table 6. NOAA ISD 488380 MONG CAI; not served by Meteostat",
    },
    "48840": {
        "station_name": "Thanh Hóa",
        "location_code": None,
        "latitude": 19.8000,
        "longitude": 105.7833,
        "elevation_m": 7.0,
        "status": "UNMAPPED_MISASSIGNED",
        "notes": "Was mapped to NBI (Ninh Bình) in Table 6. Meteostat/NOAA ISD: THANH HOA",
    },
    "48816": {
        "station_name": "Unverified (Table 6 listed it for Tuyên Quang)",
        "location_code": None,
        "latitude": None,
        "longitude": None,
        "elevation_m": None,
        "status": "UNMAPPED_UNVERIFIED",
        "notes": "Not found in Meteostat or NOAA ISD; identity unverified. Pending team decision",
    },
    # Regional stations outside the research locations:
    "48806": {
        "station_name": "Sơn La",
        "location_code": None,
        "latitude": 21.3333,
        "longitude": 103.9000,
        "elevation_m": 676.0,
        "status": "UNMAPPED_REGIONAL_NW",
        "notes": (
            "Regional Northwest station; unmapped pending team decision. "
            "Elevation from NOAA ISD (Meteostat reports 0)"
        ),
    },
    "48808": {
        "station_name": "Cao Bằng",
        "location_code": None,
        "latitude": 22.6667,
        "longitude": 106.2500,
        "elevation_m": 260.0,
        "status": "UNMAPPED_REGIONAL_NE",
        "notes": "Regional Northeast station; unmapped pending team decision",
    },
    "48823": {
        "station_name": "Nam Định",
        "location_code": None,
        "latitude": 20.4330,
        "longitude": 106.1500,
        "elevation_m": 3.0,
        "status": "UNMAPPED_GHCNH_ONLY",
        "notes": (
            "NOAA ISD 488230 NAM DINH; not served by Meteostat. Used only as the GHCNh "
            "daily-rain substitute for NBI (see GHCNH_RAIN_STATIONS)"
        ),
    },
}

# Station-to-location mapping for NOAA GHCNh 24-hour station rainfall (workbook ID 9).
# This mapping is separate from the Meteostat mapping above: GHCNh rain feeds its own
# daily table, so a substitute station here never adds a second Meteostat station to a
# location. Decided 2026-10-21 (docs/data-contract.md). Distances are to the research point.
GHCNH_RAIN_STATIONS: dict[str, dict[str, Any]] = {
    "48826": {"location_code": "HPH", "role": "representative", "distance_km": 7.6},
    "48831": {"location_code": "TNG", "role": "representative", "distance_km": 1.7},
    "48803": {"location_code": "LCA", "role": "representative", "distance_km": 1.6},
    "48825": {"location_code": "HAN", "role": "nearest_available", "distance_km": 11.3},
    "48823": {
        "location_code": "NBI",
        "role": "substitute_neighbouring_province",
        "distance_km": 27.3,
    },
}


def get_canonical_stations() -> dict[str, dict[str, Any]]:
    """Return only stations mapped to canonical research locations."""
    return {sid: s for sid, s in STATION_REGISTRY.items() if s.get("status") == "CANONICAL"}


def get_station_info(station_id: str) -> dict[str, Any] | None:
    """Lookup station metadata by string station ID."""
    return STATION_REGISTRY.get(str(station_id).strip())


def validate_station_location_mapping(station_id: str) -> tuple[bool, str | None, str]:
    """Validate whether station has a confirmed canonical location mapping.

    Returns:
        (is_valid, location_code, message)
    """
    sid = str(station_id).strip()
    info = STATION_REGISTRY.get(sid)
    if not info:
        return (
            False,
            None,
            f"Station ID {sid} is not configured in STATION_REGISTRY.",
        )

    loc = info.get("location_code")
    if not loc:
        return (
            False,
            None,
            f"Station {sid} ({info.get('station_name')}) has status '{info.get('status')}' "
            f"and is not mapped to any canonical location {sorted(CANONICAL_LOCATIONS)}. "
            "Pending team decision before publication.",
        )

    if loc not in CANONICAL_LOCATIONS:
        return (
            False,
            None,
            f"Station {sid} maps to non-canonical location code '{loc}'. "
            f"Expected one of {sorted(CANONICAL_LOCATIONS)}.",
        )

    return True, loc, f"Mapped to canonical location {loc}"
