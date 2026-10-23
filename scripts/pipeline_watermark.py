#!/usr/bin/env python3
"""scripts/pipeline_watermark.py.

Watermark of the incremental Bronze -> DW pipeline (workbook ID 25; used by
dags/dag_weather_incremental.py).

The watermark is the last UTC year that weather_bronze_to_dw_dag loaded into the DW and
reconciled for every source and location. It moves only after such a run succeeded (the
run's last task, reconcile, passes only when DW row counts equal Silver). It moves only from
the value the run was planned on, so two runs cannot both advance from the same base.

Window rule (the pipeline scripts work by whole UTC years):
- With nothing committed, start at 2016.
- Otherwise take the year after the watermark, plus `lookback_years` committed years before
  it. Those are re-processed for late corrections: preliminary ERA5T values are replaced by
  final ERA5 about 2-3 months later, and Open-Meteo's archive follows its sources.
- At most `max_years` new years per run, never after 2025 (project scope). When the
  watermark is 2025 there is nothing new and the run stops.

Overlapping windows do not duplicate data: the pipeline replaces each window in Silver and
in the DW, and DW keys are unique per source x location x UTC hour (x station day for daily
rain).

Object (bucket gold): pipeline_state/weather_bronze_to_dw_watermark.json

CLI:
  show
  plan [--lookback-years N] [--max-years N]
  set --year YYYY --reason TEXT   (initialise or correct by hand; records the reason)
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from datetime import datetime, timezone
from typing import Any

from load_gold_observations import BUCKET_GOLD, get_minio_client
from minio import Minio

SCOPE_START, SCOPE_END = 2016, 2025
PIPELINE_DAG = "weather_bronze_to_dw_dag"
WATERMARK_KEY = "pipeline_state/weather_bronze_to_dw_watermark.json"


def read_watermark(client: Minio) -> dict[str, Any] | None:
    try:
        resp = client.get_object(BUCKET_GOLD, WATERMARK_KEY)
    except Exception as ex:
        if getattr(ex, "code", None) == "NoSuchKey":
            return None
        raise
    try:
        return json.loads(resp.read())
    finally:
        resp.close()
        resp.release_conn()


def committed_year(watermark: dict[str, Any] | None) -> int | None:
    return None if watermark is None else int(watermark["committed_through_year"])


def plan_window(
    watermark: dict[str, Any] | None, lookback_years: int = 1, max_years: int = 1
) -> dict[str, Any] | None:
    """The next window of UTC years, or None when everything up to 2025 is committed."""
    if lookback_years < 0 or max_years < 1:
        raise ValueError("lookback_years must be >= 0 and max_years >= 1")
    base = committed_year(watermark)
    first_new = SCOPE_START if base is None else base + 1
    if first_new > SCOPE_END:
        return None
    start = first_new if base is None else max(SCOPE_START, first_new - lookback_years)
    end = min(SCOPE_END, first_new + max_years - 1)
    return {
        "start_year": start,
        "end_year": end,
        "new_years": [first_new, end],
        "base_committed_through_year": base,
    }


def _write(client: Minio, body: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(body, indent=2).encode("utf-8")
    client.put_object(BUCKET_GOLD, WATERMARK_KEY, io.BytesIO(data), len(data), "application/json")
    return body


def advance(
    client: Minio,
    base: int | None,
    window: list[int],
    pipeline_run_id: str,
    advanced_by: str,
) -> dict[str, Any]:
    """Move the watermark to the end of a window whose pipeline run succeeded."""
    current = committed_year(read_watermark(client))
    if current != base:
        raise RuntimeError(
            f"watermark is {current}, but the run was planned on {base}; not advanced"
        )
    start, end = window
    if not SCOPE_START <= start <= end <= SCOPE_END:
        raise ValueError(f"window {start}-{end} is outside {SCOPE_START}-{SCOPE_END}")
    if base is not None and end <= base:
        raise ValueError(f"window end {end} does not move the watermark past {base}")
    return _write(
        client,
        {
            "pipeline": PIPELINE_DAG,
            "committed_through_year": end,
            "window_utc_years": [start, end],
            "previous_committed_through_year": base,
            "pipeline_run_id": pipeline_run_id,
            "advanced_by": advanced_by,
            "committed_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )


def set_watermark(client: Minio, year: int, reason: str) -> dict[str, Any]:
    """Initialise or correct the watermark by hand; the reason is kept with it."""
    if not SCOPE_START <= year <= SCOPE_END:
        raise ValueError(f"year {year} is outside {SCOPE_START}-{SCOPE_END}")
    if not reason.strip():
        raise ValueError("a reason is required")
    return _write(
        client,
        {
            "pipeline": PIPELINE_DAG,
            "committed_through_year": year,
            "previous_committed_through_year": committed_year(read_watermark(client)),
            "set_by_hand": True,
            "reason": reason,
            "committed_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Watermark of the incremental pipeline (ID 25).")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("show", help="Print the watermark")
    plan = sub.add_parser("plan", help="Print the next window")
    plan.add_argument("--lookback-years", type=int, default=1)
    plan.add_argument("--max-years", type=int, default=1)
    setter = sub.add_parser("set", help="Initialise or correct the watermark by hand")
    setter.add_argument("--year", type=int, required=True)
    setter.add_argument("--reason", required=True)
    args = parser.parse_args()
    try:
        client = get_minio_client()
        if args.command == "show":
            out = read_watermark(client)
        elif args.command == "plan":
            out = plan_window(read_watermark(client), args.lookback_years, args.max_years)
        else:
            out = set_watermark(client, args.year, args.reason)
        print(json.dumps(out, indent=2))
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user.")
        return 130
    except Exception as ex:
        print(f"[!] {ex}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
