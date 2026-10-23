#!/usr/bin/env python3
"""scripts/benchmark_dw_queries.py.

Measure the DW query workload of the dashboard and the SQL reconciliation (workbook ID 26).

Each query runs under EXPLAIN (ANALYZE, BUFFERS) a few times; the report keeps the median
server execution time, the rows returned, the top plan node, whether a sort spilled to disk,
and which indexes the plan used. Power BI imports the views (docs/powerbi-dashboard-v1.md),
so the refresh queries read whole views; the check queries are the filtered SQL used to
reconcile dashboard numbers (ID 35). Read-only: nothing in the DW changes.

Report (bucket gold): quality_reports/dw_query_benchmark_<timestamp>.json
"""

from __future__ import annotations

import argparse
import io
import json
import statistics
import sys
from datetime import datetime, timezone
from typing import Any

from load_gold_observations import BUCKET_GOLD, get_db_connection, get_minio_client

WORKLOAD = {
    "refresh vw_obs_daily": "SELECT * FROM vw_obs_daily",
    "refresh vw_obs_hourly": "SELECT * FROM vw_obs_hourly",
    "refresh vw_obs_monthly": "SELECT * FROM vw_obs_monthly",
    "refresh vw_coverage": "SELECT * FROM vw_coverage",
    "refresh vw_prediction_vs_actual": "SELECT * FROM vw_prediction_vs_actual",
    "refresh vw_rain_station_vs_model": "SELECT * FROM vw_rain_station_vs_model",
    "check daily HAN Open-Meteo 2024": "SELECT sum(rain_mm), count(*) FILTER (WHERE is_rainy_day) "
    "FROM vw_obs_daily WHERE source_name = 'Open-Meteo' AND location_code = 'HAN' AND year = 2024",
    "check hourly HAN Open-Meteo 2024-07-15": "SELECT * FROM vw_obs_hourly WHERE source_name = "
    "'Open-Meteo' AND location_code = 'HAN' AND local_date = '2024-07-15'",
    "check monthly ERA5 2024": "SELECT * FROM vw_obs_monthly WHERE source_name = 'ERA5' AND year = 2024",
}


def _walk(node: dict[str, Any]):
    yield node
    for child in node.get("Plans", []):
        yield from _walk(child)


def measure(cur, sql: str, runs: int) -> dict[str, Any]:
    times, plan = [], None
    for _ in range(runs):
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}")
        plan = cur.fetchone()[0][0]
        times.append(plan["Execution Time"])
    nodes = list(_walk(plan["Plan"]))
    return {
        "median_ms": round(statistics.median(times), 1),
        "runs_ms": [round(t, 1) for t in times],
        "rows": plan["Plan"].get("Actual Rows"),
        "top_node": plan["Plan"]["Node Type"],
        "sort_spilled_to_disk": any(n.get("Sort Space Type") == "Disk" for n in nodes),
        "indexes_used": sorted({n["Index Name"] for n in nodes if "Index Name" in n}),
        "seq_scans": sorted({n["Relation Name"] for n in nodes if n["Node Type"] == "Seq Scan"}),
    }


def benchmark(runs: int = 3, conn=None, client=None) -> dict[str, Any]:
    own = conn is None
    conn = conn or get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM fact_weather_observation")
            fact_rows = cur.fetchone()[0]
            cur.execute("SHOW work_mem")
            work_mem = cur.fetchone()[0]
            cur.execute("SHOW jit")
            jit = cur.fetchone()[0]
            results = {name: measure(cur, sql, runs) for name, sql in WORKLOAD.items()}
    finally:
        if own:
            conn.close()
    refresh = sum(r["median_ms"] for n, r in results.items() if n.startswith("refresh"))
    report = {
        "check": "DW query workload (dashboard refresh and SQL reconciliation)",
        "fact_weather_observation_rows": fact_rows,
        "settings": {"work_mem": work_mem, "jit": jit},
        "runs_per_query": runs,
        "queries": {name: {"sql": WORKLOAD[name], **r} for name, r in results.items()},
        "refresh_total_ms": round(refresh, 1),
        "slowest": max(results, key=lambda n: results[n]["median_ms"]),
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    client = client or get_minio_client()
    key = f"quality_reports/dw_query_benchmark_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    body = json.dumps(report, indent=2).encode("utf-8")
    client.put_object(BUCKET_GOLD, key, io.BytesIO(body), len(body), "application/json")
    report["report_key"] = f"{BUCKET_GOLD}/{key}"
    for name, r in results.items():
        print(
            f"  {name:42s} {r['median_ms']:9.1f} ms  rows {r['rows']}  disk sort {r['sort_spilled_to_disk']}"
        )
    print(json.dumps({k: report[k] for k in ("refresh_total_ms", "slowest", "report_key")}))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure the DW query workload (ID 26).")
    parser.add_argument("--runs", type=int, default=3, help="Runs per query (median reported)")
    args = parser.parse_args()
    try:
        benchmark(args.runs)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
