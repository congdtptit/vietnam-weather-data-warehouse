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

# Unified station registry shared between ingestion and transformation.
# Station identities, coordinates, and elevations were verified on 2026-10-21 against
# Meteostat station metadata (https://data.meteostat.net/stations/<id>.json) and the
# NOAA ISD station history (isd-history.csv, USAF = WMO id + "0"). Meteostat coordinates
# are used where Meteostat serves the station, because Meteostat is the ingested source.
# Table 6 originally assigned several WMO ids to the wrong locations; those ids are kept
# below with their verified identity and are no longer mapped to a research location.
STATION_REGISTRY: dict[str, dict[str, Any]] = {
    "48820": {
        "station_name": "Hà Nội",
        "location_code": "HAN",
        "latitude": 21.0167,
        "longitude": 105.8000,
        "elevation_m": 6.0,
        "status": "CANONICAL",
        "notes": (
            "Meteostat 'Ha Noi' (ICAO VVNB). NOAA ISD names 488200 'NOIBAI INTL' at "
            "21.221, 105.807; coordinates follow Meteostat, the ingested source"
        ),
    },
    "48826": {
        "station_name": "Phù Liễn (Hải Phòng)",
        "location_code": "HPH",
        "latitude": 20.8000,
        "longitude": 106.6333,
        "elevation_m": 119.0,
        "status": "CANONICAL",
        "notes": "Meteostat 'Phu Lien'; NOAA ISD 488260 PHU LIEN. Table 6 listed it under TNG",
    },
    "48831": {
        "station_name": "Thái Nguyên",
        "location_code": "TNG",
        "latitude": 21.6000,
        "longitude": 105.8330,
        "elevation_m": 32.0,
        "status": "CANONICAL",
        "notes": "NOAA ISD 488310 THAI NGUYEN; not served by Meteostat (HTTP 404)",
    },
    "48803": {
        "station_name": "Lào Cai",
        "location_code": "LCA",
        "latitude": 22.5000,
        "longitude": 103.9670,
        "elevation_m": 97.0,
        "status": "CANONICAL",
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
            "coordinates match Hà Đông. Pending team decision"
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
