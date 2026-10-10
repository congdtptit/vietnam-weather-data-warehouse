"""Station metadata and location mapping configuration for Northern Vietnam weather sources.

Canonical research locations are defined in docs/NAMING_CONVENTIONS.md Table 6 and docs/data-contract.md.
"""

from typing import Any

# Canonical research locations in northern Vietnam (docs/NAMING_CONVENTIONS.md Table 6)
CANONICAL_LOCATIONS: set[str] = {
    "HAN",  # Hà Nội (Láng)
    "PTO",  # Phú Thọ (Việt Trì)
    "TNG",  # Thái Nguyên
    "TUQ",  # Tuyên Quang
    "LCA",  # Lào Cai
    "HBI",  # Hòa Bình
    "HPH",  # Hải Phòng (Phù Liễn)
    "NBI",  # Ninh Bình
}

# Unified station registry shared between ingestion and transformation.
# Table 6 defines the 8 reference WMO stations for northern Vietnam research locations.
STATION_REGISTRY: dict[str, dict[str, Any]] = {
    "48820": {
        "station_name": "Hà Nội (Láng)",
        "location_code": "HAN",
        "latitude": 21.0285,
        "longitude": 105.8542,
        "elevation_m": 10.0,
        "status": "CANONICAL",
        "notes": "Reference WMO station for HAN in Table 6",
    },
    "48825": {
        "station_name": "Việt Trì (Phú Thọ)",
        "location_code": "PTO",
        "latitude": 21.3986,
        "longitude": 105.2280,
        "elevation_m": 25.0,
        "status": "CANONICAL",
        "notes": "Reference WMO station for PTO in Table 6",
    },
    "48826": {
        "station_name": "Thái Nguyên",
        "location_code": "TNG",
        "latitude": 21.5942,
        "longitude": 105.8482,
        "elevation_m": 35.0,
        "status": "CANONICAL",
        "notes": "Reference WMO station for TNG in Table 6",
    },
    "48816": {
        "station_name": "Tuyên Quang",
        "location_code": "TUQ",
        "latitude": 21.8236,
        "longitude": 105.2153,
        "elevation_m": 50.0,
        "status": "CANONICAL",
        "notes": "Reference WMO station for TUQ in Table 6",
    },
    "48803": {
        "station_name": "Lào Cai",
        "location_code": "LCA",
        "latitude": 22.4856,
        "longitude": 103.9707,
        "elevation_m": 80.0,
        "status": "CANONICAL",
        "notes": "Reference WMO station for LCA in Table 6",
    },
    "48830": {
        "station_name": "Hòa Bình",
        "location_code": "HBI",
        "latitude": 20.8172,
        "longitude": 105.3376,
        "elevation_m": 20.0,
        "status": "CANONICAL",
        "notes": "Reference WMO station for HBI in Table 6",
    },
    "48838": {
        "station_name": "Hải Phòng (Phù Liễn)",
        "location_code": "HPH",
        "latitude": 20.8449,
        "longitude": 106.6881,
        "elevation_m": 4.0,
        "status": "CANONICAL",
        "notes": "Reference WMO station for HPH in Table 6",
    },
    "48840": {
        "station_name": "Ninh Bình",
        "location_code": "NBI",
        "latitude": 20.2506,
        "longitude": 105.9745,
        "elevation_m": 5.0,
        "status": "CANONICAL",
        "notes": "Reference WMO station for NBI in Table 6",
    },
    # Secondary / unmapped regional stations previously in ingestion list:
    "48806": {
        "station_name": "Sơn La",
        "location_code": None,
        "latitude": 21.3333,
        "longitude": 103.9000,
        "elevation_m": 600.0,
        "status": "UNMAPPED_REGIONAL_NW",
        "notes": "Regional Northwest station; unmapped pending team decision",
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
