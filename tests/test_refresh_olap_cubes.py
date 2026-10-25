"""Unit tests for scripts/refresh_olap_cubes.py (no database: the cursor is mocked)."""

from unittest.mock import MagicMock

import refresh_olap_cubes as roc


def _conn(cur):
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    return conn


def test_double_columns_are_rounded_in_the_comparison():
    cur = MagicMock()
    cur.fetchall.return_value = [("rain_mm", "numeric"), ("wind_dir_sin_sum", "double precision")]
    cols = roc.comparable_columns(cur, "mv_obs_daily")
    assert cols == "rain_mm, round(wind_dir_sin_sum::numeric, 6) AS wind_dir_sin_sum"


def test_missing_cubes_fail_without_refreshing():
    cur = MagicMock()
    cur.fetchall.return_value = [("mv_obs_daily",)]
    report = roc.run(conn=_conn(cur))
    assert report["passed"] is False and report["missing"] == ["mv_obs_monthly"]
    assert not any("REFRESH" in str(c.args[0]) for c in cur.execute.call_args_list)


def test_check_only_compares_without_refresh(monkeypatch):
    cur = MagicMock()
    cur.fetchall.return_value = [("mv_obs_daily",), ("mv_obs_monthly",)]
    monkeypatch.setattr(
        roc, "compare", lambda c, cube, view: {"only_in_view": 0, "only_in_cube": 0, "cube_rows": 5}
    )
    report = roc.run(check_only=True, conn=_conn(cur))
    assert report["passed"] is True and set(report["cubes"]) == set(roc.CUBES)
    assert not any("REFRESH" in str(c.args[0]) for c in cur.execute.call_args_list)


def test_a_differing_cube_fails(monkeypatch):
    cur = MagicMock()
    cur.fetchall.return_value = [("mv_obs_daily",), ("mv_obs_monthly",)]
    monkeypatch.setattr(
        roc,
        "compare",
        lambda c, cube, view: {"only_in_view": int(cube == "mv_obs_daily"), "only_in_cube": 0},
    )
    report = roc.run(conn=_conn(cur))
    assert report["passed"] is False
    assert report["cubes"]["mv_obs_daily"]["passed"] is False
    assert "refresh_s" in report["cubes"]["mv_obs_monthly"]


def test_fetch_turns_numerics_into_numbers():
    from decimal import Decimal

    import olap_queries as oq

    cur = MagicMock()
    cur.description = [("location_code",), ("rain_mm",), ("rainy_days",)]
    cur.fetchall.return_value = [("HAN", Decimal("12.5"), 3), ("LCA", None, None)]
    df = oq.fetch(_conn(cur), "SELECT 1")
    assert df.loc[0, "rain_mm"] == 12.5 and df["rainy_days"].dtype == "Int64"
    assert df["rain_mm"].isna().iloc[1]
