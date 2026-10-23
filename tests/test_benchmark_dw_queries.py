"""Unit tests for scripts/benchmark_dw_queries.py (workbook ID 26). Fake cursor, no database."""

from types import SimpleNamespace
from unittest.mock import Mock

import benchmark_dw_queries as bench


def _plan(ms):
    return [
        {
            "Execution Time": ms,
            "Plan": {
                "Node Type": "Hash Join",
                "Actual Rows": 24,
                "Plans": [
                    {"Node Type": "Index Scan", "Index Name": "idx_fwo_location_source_ts"},
                    {
                        "Node Type": "Sort",
                        "Sort Space Type": "Disk",
                        "Plans": [{"Node Type": "Seq Scan", "Relation Name": "dim_date"}],
                    },
                ],
            },
        }
    ]


def test_measure_keeps_the_median_and_reads_the_plan():
    cur = Mock()
    plans = iter([_plan(30.0), _plan(10.0), _plan(20.0)])
    cur.fetchone.side_effect = lambda: (next(plans),)
    r = bench.measure(cur, "SELECT 1", runs=3)
    assert r["median_ms"] == 20.0 and r["runs_ms"] == [30.0, 10.0, 20.0] and r["rows"] == 24
    assert r["sort_spilled_to_disk"] and r["indexes_used"] == ["idx_fwo_location_source_ts"]
    assert r["seq_scans"] == ["dim_date"]
    assert cur.execute.call_args.args[0].startswith(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT 1"
    )


def test_benchmark_writes_a_report_and_sums_refresh_queries():
    answers = iter([(1490237,), ("4MB",), ("on",)])
    cur = Mock()
    cur.fetchone.side_effect = lambda: next(answers, None) or (_plan(5.0),)
    conn = Mock()
    conn.cursor.return_value.__enter__ = lambda self: cur
    conn.cursor.return_value.__exit__ = lambda self, *a: None
    store = {}
    client = SimpleNamespace(put_object=lambda b, k, data, n, ct: store.update({k: data.read()}))
    report = bench.benchmark(runs=1, conn=conn, client=client)
    refresh = [n for n in bench.WORKLOAD if n.startswith("refresh")]
    assert report["refresh_total_ms"] == 5.0 * len(refresh)
    assert report["fact_weather_observation_rows"] == 1490237 and report["settings"]["jit"] == "on"
    (key,) = store
    assert key.startswith("quality_reports/dw_query_benchmark_")
