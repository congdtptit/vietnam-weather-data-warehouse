#!/usr/bin/env python3
"""scripts/refresh_olap_cubes.py.

Refresh the precomputed sub-cubes of sql/06_olap_cubes.sql and check each against its view.

`mv_obs_daily` and `mv_obs_monthly` store the contents of `vw_obs_daily` and `vw_obs_monthly`.
They are stale after a DW load until refreshed, so `weather_bronze_to_dw_dag` runs this script
after the Gold loaders. A cube passes when its rows equal the view's rows in both directions
(EXCEPT ALL, so duplicates count); double-precision columns (the wind vector sums) are
compared at 6 decimals because parallel aggregation may add them in a different order.
`--check-only` skips the refresh and only compares, which is read-only.

Exit status 1 when a cube is missing or differs from its view.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from load_gold_observations import get_db_connection

CUBES = {"mv_obs_daily": "vw_obs_daily", "mv_obs_monthly": "vw_obs_monthly"}

COLUMNS_SQL = """
    SELECT a.attname, format_type(a.atttypid, a.atttypmod)
    FROM pg_attribute a
    WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
"""


def comparable_columns(cur, cube: str) -> str:
    """Select list for the comparison: doubles rounded, everything else as stored."""
    cur.execute(COLUMNS_SQL, (cube,))
    parts = []
    for name, data_type in cur.fetchall():
        if data_type == "double precision":
            parts.append(f"round({name}::numeric, 6) AS {name}")
        else:
            parts.append(name)
    return ", ".join(parts)


def compare(cur, cube: str, view: str) -> dict[str, int]:
    cols = comparable_columns(cur, cube)
    counts = {}
    for label, left, right in (("only_in_view", view, cube), ("only_in_cube", cube, view)):
        cur.execute(
            f"SELECT count(*) FROM (SELECT {cols} FROM {left} "
            f"EXCEPT ALL SELECT {cols} FROM {right}) d"
        )
        counts[label] = cur.fetchone()[0]
    cur.execute(f"SELECT count(*) FROM {cube}")
    counts["cube_rows"] = cur.fetchone()[0]
    return counts


def missing_cubes(cur) -> list[str]:
    cur.execute("SELECT matviewname FROM pg_matviews WHERE matviewname = ANY(%s)", (list(CUBES),))
    present = {row[0] for row in cur.fetchall()}
    return [cube for cube in CUBES if cube not in present]


def run(check_only: bool = False, conn=None) -> dict[str, Any]:
    own = conn is None
    conn = conn or get_db_connection()
    report: dict[str, Any] = {"check_only": check_only, "cubes": {}}
    try:
        with conn.cursor() as cur:
            missing = missing_cubes(cur)
            if missing:
                report["missing"] = missing
                report["passed"] = False
                return report
            for cube, view in CUBES.items():
                entry: dict[str, Any] = {"view": view}
                if not check_only:
                    started = time.perf_counter()
                    cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {cube}")
                    conn.commit()
                    entry["refresh_s"] = round(time.perf_counter() - started, 2)
                entry.update(compare(cur, cube, view))
                entry["passed"] = entry["only_in_view"] == 0 and entry["only_in_cube"] == 0
                report["cubes"][cube] = entry
        conn.rollback()  # end the read transaction of the comparison
    finally:
        if own:
            conn.close()
    report["passed"] = all(e["passed"] for e in report["cubes"].values())
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[2])
    parser.add_argument(
        "--check-only", action="store_true", help="compare the cubes with their views, no refresh"
    )
    args = parser.parse_args(argv)
    report = run(check_only=args.check_only)
    print(json.dumps(report, indent=2))
    if report.get("missing"):
        print(f"Missing cubes {report['missing']}: apply sql/06_olap_cubes.sql", file=sys.stderr)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
