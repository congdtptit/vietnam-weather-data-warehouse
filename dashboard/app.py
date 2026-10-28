"""dashboard/app.py.

Web dashboard over the weather warehouse. It replaces the Power BI report of
docs/powerbi-dashboard-v1.md (decided by the user on 2026-10-27) with the same eight pages plus
an OLAP page, and runs offline on any machine that runs the Docker stack.

Run: `make dashboard`, then open http://localhost:8501. Read-only: every page queries the
PostgreSQL warehouse through scripts/dashboard_queries.py and shows the SQL it ran. Province
outlines come from the Silver GeoJSON in MinIO and are drawn without map tiles.
"""

from __future__ import annotations

import json
import statistics
import sys
import textwrap
import threading
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import dashboard_queries as dq
from load_gold_observations import BUCKET_SILVER, get_db_connection, get_minio_client

st.set_page_config(page_title="Kho khí tượng miền Bắc", page_icon="🌧️", layout="wide")

# Colours of the Power BI specification (validated for colour-vision deficiency).
SOURCE_COLORS = {"Open-Meteo": "#2A78D6", "ERA5": "#EB6834", "Meteostat": "#1BAF7A"}
STATION = "#008300"
RAIN = "#2A78D6"
TEMP = "#E34948"
DRY = "#EDA100"
ACTUAL = "#EB6834"
PREDICTED = "#6250D6"
SEASON_COLORS = {"Mùa mưa (5–10)": RAIN, "Mùa khô (11–4)": DRY}
ALL_MONTHS = tuple(range(1, 13))
YEAR_LIST = list(range(dq.YEARS[0], dq.YEARS[1] + 1))
BASIS = (
    "Lịch Việt Nam (UTC+7) · ngày mưa: đủ 24 giờ số liệu và tổng ≥ 1 mm · ngày nóng: đủ 24 giờ "
    "và cao nhất ≥ 35 °C · không bao giờ cộng các nguồn với nhau"
)
STATION_BASIS = (
    "Ngày mưa của trạm là 24 giờ kết thúc lúc 12:00 UTC (19:00 giờ Việt Nam); năm và tháng "
    "theo ngày UTC đó · ngày mưa: tổng ≥ 1 mm · mưa Open-Meteo chỉ tính ngày đủ 24 giờ · "
    "không cộng mưa của các điểm với nhau"
)
FORECAST_BASIS = (
    "Thời điểm dự báo, tháng và mùa theo giờ Việt Nam (UTC+7) · nhãn: tổng mưa Open-Meteo của "
    "24 giờ sau thời điểm dự báo ≥ 1 mm, chỉ tính khi đủ 24 giờ số liệu"
)
QUALITY_BASIS = (
    "Năm theo giờ Việt Nam (UTC+7) · độ phủ = số giờ có giá trị / số giờ phải có của năm · "
    "thiếu là để trống, không ghi 0"
)

_LOCK = threading.Lock()


# --- Data access -------------------------------------------------------------------------------


@st.cache_resource(show_spinner=False)
def connection():
    conn = get_db_connection()
    conn.set_session(readonly=True, autocommit=True)
    return conn


def _call(fn, *args):
    for attempt in (1, 2):
        try:
            with _LOCK:
                return fn(connection(), *args)
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            connection.clear()
            if attempt == 2:
                raise


@st.cache_data(show_spinner="Đang truy vấn kho dữ liệu…", ttl=900)
def cached(name: str, *args):
    return _call(getattr(dq, name), *args)


@st.cache_data(show_spinner="Đang truy vấn kho dữ liệu…", ttl=900)
def cached_olap(name: str, *args):
    return _call(dq.olap, name, *args)


@st.cache_resource(show_spinner=False)
def provinces() -> dict:
    obj = get_minio_client().get_object(BUCKET_SILVER, dq.BOUNDARY_KEY)
    try:
        return json.loads(obj.read())
    finally:
        obj.close()
        obj.release_conn()


def location_table() -> pd.DataFrame:
    return cached("locations")[0]


def names() -> dict[str, str]:
    return dict(zip(location_table()["location_code"], location_table()["location_name"]))


# --- Display helpers ---------------------------------------------------------------------------


def header(title: str, question: str, olap: str | None = None, basis: str = BASIS) -> None:
    """Page title, its business question, and the counting basis (pages 7 and 8 state their
    own, as the specification asks)."""
    st.title(title)
    st.markdown(f"**{question}**" + (f" · Thao tác OLAP: *{olap}*" if olap else ""))
    st.caption(basis)


def sql_box(*queries: str) -> None:
    with st.expander("Truy vấn SQL đã chạy trên kho"):
        for query in queries:
            st.code(textwrap.dedent(query).strip(), language="sql")


def style(fig: go.Figure, title: str, y_title: str = "", height: int = 340) -> go.Figure:
    fig.update_layout(
        title=dict(text=title, font=dict(size=15), x=0, xanchor="left", y=0.98, yanchor="top"),
        template="plotly_white",
        height=height,
        separators=",.",
        margin=dict(l=10, r=10, t=78, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, x=0),
        yaxis_title=y_title,
        font=dict(size=13),
    )
    return fig


def show(fig: go.Figure, **kwargs):
    return st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False}, **kwargs)


def headroom(fig: go.Figure, values, axis: str = "y", factor: float = 1.15) -> None:
    """Leave room for value labels drawn outside the bars."""
    top = pd.Series(values, dtype="float64").max()
    if pd.notna(top) and top > 0:
        (fig.update_yaxes if axis == "y" else fig.update_xaxes)(range=[0, top * factor])


def num(value, decimals: int = 1, unit: str = "") -> str:
    return dq.vn_number(value, decimals, unit)


def pct(value, decimals: int = 1) -> str:
    return dq.vn_number(None if pd.isna(value) else 100 * value, decimals, "%")


def _total(values: pd.Series) -> float:
    """Sum that stays unknown when every value is unknown."""
    return values.sum(min_count=1)


def table(df: pd.DataFrame) -> None:
    st.dataframe(df, hide_index=True, use_container_width=True)


def month_axis(fig: go.Figure) -> None:
    fig.update_xaxes(
        tickmode="array", tickvals=list(ALL_MONTHS), ticktext=[f"T{m}" for m in ALL_MONTHS]
    )


# --- 1. Overview -------------------------------------------------------------------------------


def page_overview() -> None:
    header(
        "Tổng quan khí tượng",
        "Kỳ đã chọn mưa bao nhiêu, nóng thế nào, số liệu có đủ không?",
        "Slice",
    )
    codes = list(location_table()["location_code"])
    c1, c2, c3, c4 = st.columns([1, 3, 1, 1])
    source = c1.selectbox("Nguồn dữ liệu", dq.SOURCES, key="ov_source")
    chosen = c2.multiselect(
        "Điểm nghiên cứu", codes, default=codes, format_func=names().get, key="ov_locations"
    )
    year = c3.selectbox(
        "Năm",
        [0, *YEAR_LIST],
        index=len(YEAR_LIST) - 1,
        format_func=lambda y: "TB 2016–2025" if not y else str(y),
        key="ov_year",
    )
    month = c4.selectbox(
        "Tháng",
        [0, *ALL_MONTHS],
        format_func=lambda m: "Cả năm" if not m else f"Tháng {m}",
        key="ov_month",
    )
    if not chosen:
        st.info("Chọn ít nhất một điểm.")
        return
    years = dq.YEARS if not year else (year, year)
    months = ALL_MONTHS if not month else (month,)
    monthly, sql_monthly = cached("monthly_rows", source, tuple(chosen), years, months)
    hours, sql_hours = cached("calendar_hours", years, months)
    hours_per_location = float(hours["hours"].iloc[0])
    s = dq.summary(monthly, len(chosen), hours_per_location)
    per = " mỗi năm" if not year else ""
    st.caption(
        f"Kỳ đang xem: {'cả năm' if not month else f'tháng {month}'}, "
        f"{'trung bình mỗi năm 2016–2025' if not year else f'năm {year}'}, "
        f"{len(chosen)} điểm, nguồn {source}."
    )

    cards = st.columns(5)
    cards[0].metric(f"Lượng mưa TB/điểm{per}", num(s["rain_mm"], 1, "mm"))
    cards[1].metric("Nhiệt độ trung bình", num(s["temp_mean_c"], 2, "°C"))
    cards[2].metric(f"Ngày mưa TB/điểm{per}", num(s["rainy_days"], 1))
    cards[3].metric(f"Ngày nóng TB/điểm{per}", num(s["hot_days"], 1))
    cards[4].metric("Độ phủ số liệu mưa", pct(s["rain_coverage"], 2))
    if source == "Meteostat":
        st.info(
            "Meteostat chỉ có trạm 48820 (sân bay Nội Bài) cho Hà Nội và không có số liệu "
            "mưa: mưa của Meteostat đều do mô hình điền nên đã bị loại. Ô có dấu — là không "
            "có số liệu, không phải 0."
        )
    if monthly.empty:
        st.warning("Không có số liệu cho lựa chọn này.")
        sql_box(sql_monthly, sql_hours)
        return

    loc = dq.by_location(monthly, hours_per_location)
    loc["name"] = loc["location_code"].map(names())
    left, right = st.columns(2)
    by_rain = loc.sort_values("rain_mm")
    fig = go.Figure(
        go.Bar(
            x=by_rain["rain_mm"],
            y=by_rain["name"],
            orientation="h",
            marker_color=RAIN,
            texttemplate="%{x:,.1f}",
            textposition="outside",
            hovertemplate="%{y}: %{x:,.1f} mm<extra></extra>",
        )
    )
    headroom(fig, by_rain["rain_mm"], axis="x", factor=1.2)
    left.plotly_chart(style(fig, f"Lượng mưa theo điểm (mm{per})"), use_container_width=True)
    fig = go.Figure(
        [
            go.Bar(
                name="Ngày mưa",
                x=loc["name"],
                y=loc["rainy_days"],
                marker_color=RAIN,
                hovertemplate="%{x}: %{y:,.1f} ngày mưa<extra></extra>",
            ),
            go.Bar(
                name="Ngày nóng",
                x=loc["name"],
                y=loc["hot_days"],
                marker_color=TEMP,
                hovertemplate="%{x}: %{y:,.1f} ngày nóng<extra></extra>",
            ),
        ]
    )
    right.plotly_chart(
        style(fig, f"Ngày mưa và ngày nóng theo điểm (ngày{per})"), use_container_width=True
    )

    month = dq.by_month(monthly)
    left, right = st.columns(2)
    fig = go.Figure(
        go.Bar(
            x=month["month"],
            y=month["rain_mm"],
            marker_color=RAIN,
            texttemplate="%{y:,.0f}",
            textposition="outside",
            hovertemplate="T%{x}: %{y:,.1f} mm<extra></extra>",
        )
    )
    month_axis(fig)
    headroom(fig, month["rain_mm"])
    left.plotly_chart(
        style(fig, f"Lượng mưa theo tháng (mm, TB mỗi điểm{per})", "mm"),
        use_container_width=True,
    )
    fig = go.Figure(
        go.Scatter(
            x=month["month"],
            y=month["temp_mean_c"],
            mode="lines+markers",
            line=dict(color=TEMP, width=2),
            hovertemplate="T%{x}: %{y:,.2f} °C<extra></extra>",
        )
    )
    month_axis(fig)
    right.plotly_chart(
        style(fig, "Nhiệt độ trung bình theo tháng (°C)", "°C"), use_container_width=True
    )

    st.subheader("Chi tiết theo điểm")
    table(
        pd.DataFrame(
            {
                "Điểm": loc["name"],
                f"Mưa (mm{per})": loc["rain_mm"].map(num),
                "Nhiệt độ TB (°C)": loc["temp_mean_c"].map(lambda v: num(v, 2)),
                "Thấp nhất (°C)": loc["temp_min_c"].map(lambda v: num(v, 2)),
                "Cao nhất (°C)": loc["temp_max_c"].map(lambda v: num(v, 2)),
                f"Ngày mưa{per}": loc["rainy_days"].map(lambda v: num(v, 0 if year else 1)),
                f"Ngày nóng{per}": loc["hot_days"].map(lambda v: num(v, 0 if year else 1)),
                "Độ phủ mưa": loc["rain_coverage"].map(lambda v: pct(v, 2)),
            }
        )
    )
    sql_box(sql_monthly, sql_hours)


# --- 2. Trends: roll-up and drill-down ---------------------------------------------------------


def _reset_below(level: str) -> None:
    if level == "year":
        st.session_state.tr_month = 0
        st.session_state.tr_day = 0
    elif level == "month":
        st.session_state.tr_day = 0


def _drill(chart_key: str, level: str) -> None:
    event = st.session_state.get(chart_key)
    points = event["selection"]["points"] if event else []
    if not points:
        return
    value = int(points[0]["x"])
    if level == "year":
        st.session_state.tr_year = value
        _reset_below("year")
    elif level == "month":
        st.session_state.tr_month = value
        _reset_below("month")
    elif level == "day":
        st.session_state.tr_day = value


def _day_count(flag) -> float:
    """A day's rainy or hot flag as a day count: 1, 0, or unknown for an incomplete day."""
    return float("nan") if flag is None or pd.isna(flag) else float(bool(flag))


def _roll_up() -> None:
    if st.session_state.get("tr_day"):
        st.session_state.tr_day = 0
    elif st.session_state.get("tr_month"):
        st.session_state.tr_month = 0
    else:
        st.session_state.tr_year = 0


def page_trends() -> None:
    header(
        "Xu hướng thời tiết",
        "Mưa và nhiệt độ ở một điểm thay đổi thế nào theo năm, tháng, ngày, giờ?",
        "Roll-up · Drill-down",
    )
    for key in ("tr_year", "tr_month", "tr_day"):
        st.session_state.setdefault(key, 0)
    c1, c2, c3, c4, c5 = st.columns([1.2, 1.2, 1, 1, 1])
    source = c1.selectbox("Nguồn dữ liệu", dq.SOURCES, key="tr_source")
    location = c2.selectbox(
        "Điểm nghiên cứu",
        list(location_table()["location_code"]),
        format_func=names().get,
        key="tr_location",
    )
    year = c3.selectbox(
        "Năm",
        [0, *YEAR_LIST],
        key="tr_year",
        format_func=lambda v: "Tất cả" if not v else str(v),
        on_change=_reset_below,
        args=("year",),
    )
    month = c4.selectbox(
        "Tháng",
        [0, *ALL_MONTHS],
        key="tr_month",
        format_func=lambda v: "Cả năm" if not v else f"Tháng {v}",
        disabled=not year,
        on_change=_reset_below,
        args=("month",),
    )
    days_in_month = pd.Period(f"{year}-{month:02d}").days_in_month if year and month else 31
    day = c5.selectbox(
        "Ngày",
        [0, *range(1, days_in_month + 1)],
        key="tr_day",
        format_func=lambda v: "Cả tháng" if not v else f"Ngày {v}",
        disabled=not month,
    )

    queries = []
    years_df, sql = cached("trend_years", source, location)
    queries.append(sql)
    if not year:
        level, data, x, x_title = "year", years_df, "year", "Năm"
        rain_col, temp_col = "rain_mm", "temp_mean_c"
    elif not month:
        data, sql = cached("trend_months", source, location, year)
        queries.append(sql)
        level, x, x_title = "month", "month", f"Tháng của năm {year}"
        rain_col, temp_col = "rain_mm", "temp_mean_c"
    elif not day:
        data, sql = cached("trend_days", source, location, year, month)
        queries.append(sql)
        level, x, x_title = "day", "day_of_month", f"Ngày của tháng {month}/{year}"
        rain_col, temp_col = "rain_mm", "temp_mean_c"
    else:
        data, sql = cached("trend_hours", source, location, date(year, month, day))
        queries.append(sql)
        level, x, x_title = "hour", "local_hour", f"Giờ của ngày {day}/{month}/{year}"
        rain_col, temp_col = "precipitation_mm", "temperature_c"

    path = " › ".join(
        p
        for p in (
            f"{names()[location]} · {source}",
            "2016–2025" if not year else str(year),
            None if not month else f"tháng {month}",
            None if not day else f"ngày {day}",
        )
        if p
    )
    hint = ""
    if level != "hour" and not data.empty:
        hint = (
            " Bấm vào một cột mưa để khoan xuống mức dưới."
            if data[rain_col].notna().any()
            else " Nguồn này không có số liệu mưa; dùng các ô Năm, Tháng, Ngày để khoan xuống."
        )
    b1, b2 = st.columns([4, 1])
    b1.markdown(f"Đang xem: **{path}**.{hint}")
    b2.button("⤴ Gộp lên một mức", on_click=_roll_up, disabled=not year, use_container_width=True)

    if data.empty:
        st.warning("Không có số liệu cho lựa chọn này.")
        sql_box(*queries)
        return
    left, right = st.columns(2)
    unit = "mm/giờ" if level == "hour" else "mm"
    fig = go.Figure(
        go.Bar(
            x=data[x],
            y=data[rain_col],
            marker_color=RAIN,
            hovertemplate="%{x}: %{y:,.1f} mm<extra></extra>",
        )
    )
    if level == "month":
        month_axis(fig)
    fig.update_xaxes(title=x_title, type="category" if level != "month" else "linear")
    chart_key = f"tr_chart_{level}"
    with left:
        show(
            style(fig, f"Lượng mưa ({unit})", "mm"),
            key=chart_key,
            on_select=(lambda: _drill(chart_key, level)) if level != "hour" else "ignore",
            selection_mode="points",
        )
    fig = go.Figure(
        go.Scatter(
            x=data[x],
            y=data[temp_col],
            mode="lines+markers",
            line=dict(color=TEMP, width=2),
            hovertemplate="%{x}: %{y:,.2f} °C<extra></extra>",
        )
    )
    if level == "month":
        month_axis(fig)
    fig.update_xaxes(title=x_title, type="category" if level != "month" else "linear")
    with right:
        show(style(fig, "Nhiệt độ trung bình (°C)", "°C"))

    # Each row comes from its own level of the cube (day and month cubes, the year from the
    # monthly cube), so the check below compares two independent aggregates.
    rows = []
    if day:
        days_df, _ = cached("trend_days", source, location, year, month)
        d = days_df[days_df["day_of_month"] == day].iloc[0]
        rows.append(
            (
                "Ngày",
                f"{day}/{month}/{year}",
                d["rain_mm"],
                d["temp_mean_c"],
                d["temp_min_c"],
                d["temp_max_c"],
                _day_count(d["is_rainy_day"]),
                _day_count(d["is_hot_day"]),
            )
        )
    if month:
        months_df, _ = cached("trend_months", source, location, year)
        m = months_df[months_df["month"] == month].iloc[0]
        rows.append(
            (
                "Tháng",
                f"{month}/{year}",
                m["rain_mm"],
                m["temp_mean_c"],
                m["temp_min_c"],
                m["temp_max_c"],
                m["rainy_days"],
                m["hot_days"],
            )
        )
    if year:
        y = years_df[years_df["year"] == year].iloc[0]
        rows.append(
            (
                "Năm",
                str(year),
                y["rain_mm"],
                y["temp_mean_c"],
                y["temp_min_c"],
                y["temp_max_c"],
                y["rainy_days"],
                y["hot_days"],
            )
        )
    if rows:
        st.subheader("Roll-up: từ mức đang xem lên năm")
        table(
            pd.DataFrame(
                {
                    "Mức": [r[0] for r in rows],
                    "Thời gian": [r[1] for r in rows],
                    "Mưa (mm)": [num(r[2]) for r in rows],
                    "Nhiệt độ TB (°C)": [num(r[3], 2) for r in rows],
                    "Thấp nhất (°C)": [num(r[4], 2) for r in rows],
                    "Cao nhất (°C)": [num(r[5], 2) for r in rows],
                    "Ngày mưa": [num(r[6], 0) for r in rows],
                    "Ngày nóng": [num(r[7], 0) for r in rows],
                }
            )
        )
        parent = {"month": "Năm", "day": "Tháng", "hour": "Ngày"}.get(level)
        total = data[rain_col].sum(min_count=1)
        if parent and pd.notna(total):
            expected = next(r[2] for r in rows if r[0] == parent)
            same = pd.notna(expected) and abs(total - expected) < 1e-6
            st.caption(
                f"Kiểm tra roll-up: tổng các cột của biểu đồ = {num(total)} mm; dòng "
                f"{parent.lower()} ở bảng trên = {num(expected)} mm "
                f"({'khớp' if same else 'không khớp'})."
            )

    profile_years = dq.YEARS if not year else (year, year)
    profile, sql = cached("hour_profile", source, location, profile_years)
    queries.append(sql)
    n_years = profile_years[1] - profile_years[0] + 1
    scope = "TB mỗi năm 2016–2025" if not year else f"năm {year}"
    left, right = st.columns(2)
    fig = go.Figure(
        go.Bar(
            x=profile["local_hour"],
            y=profile["rain_mm"] / n_years,
            marker_color=RAIN,
            hovertemplate="%{x} giờ: %{y:,.1f} mm<extra></extra>",
        )
    )
    fig.update_xaxes(title="Giờ trong ngày (giờ Việt Nam)", dtick=2)
    left.plotly_chart(
        style(fig, f"Lượng mưa theo giờ trong ngày (mm, {scope})", "mm"), use_container_width=True
    )
    fig = go.Figure(
        go.Scatter(
            x=profile["local_hour"],
            y=profile["temp_mean_c"],
            mode="lines+markers",
            line=dict(color=TEMP, width=2),
            hovertemplate="%{x} giờ: %{y:,.2f} °C<extra></extra>",
        )
    )
    fig.update_xaxes(title="Giờ trong ngày (giờ Việt Nam)", dtick=2)
    right.plotly_chart(
        style(fig, f"Nhiệt độ theo giờ trong ngày (°C, {scope})", "°C"), use_container_width=True
    )
    st.caption(
        "Lượng mưa của một giờ gắn với mốc cuối khoảng: giá trị lúc 14 giờ là mưa từ 13 đến 14 giờ."
    )
    sql_box(*queries)


# --- 3. Seasons and locations: dice and pivot --------------------------------------------------


def page_seasons() -> None:
    header(
        "Mùa và khu vực",
        "Tháng, mùa, điểm nào mưa nhiều; ngày mưa, ngày nóng rơi vào mùa nào?",
        "Dice · Pivot",
    )
    codes = list(location_table()["location_code"])
    c1, c2 = st.columns([1, 3])
    source = c1.selectbox("Nguồn dữ liệu", dq.SOURCES[:2], key="se_source")
    chosen = c2.multiselect(
        "Điểm nghiên cứu", codes, default=codes, format_func=names().get, key="se_locations"
    )
    c1, c2, c3 = st.columns([2, 2, 1])
    years = c1.slider("Khoảng năm", dq.YEARS[0], dq.YEARS[1], dq.YEARS, key="se_years")
    period = c2.radio(
        "Tháng", ["Cả năm", "Mùa mưa (5–10)", "Mùa khô (11–4)"], horizontal=True, key="se_period"
    )
    pivot = c3.toggle("Xoay trục (Pivot)", key="se_pivot")
    months = {"Cả năm": ALL_MONTHS, "Mùa mưa (5–10)": dq.RAINY_SEASON_MONTHS}.get(
        period, (1, 2, 3, 4, 11, 12)
    )
    if not chosen:
        st.info("Chọn ít nhất một điểm.")
        return
    monthly, sql = cached("monthly_rows", source, tuple(chosen), tuple(years), tuple(months))
    st.markdown(
        f"Khối con đang chọn (dice): nguồn **{source}** × **{len(chosen)} điểm** × "
        f"tháng **{period.lower()}** × năm **{years[0]}–{years[1]}**."
    )

    grid = dq.month_pivot(monthly)
    grid.index = [names().get(i, i) for i in grid.index]
    cells = grid.drop(index="TB các điểm").drop(columns="Cả kỳ")
    z, x, y = cells.to_numpy(), list(cells.columns), list(cells.index)
    if pivot:
        z, x, y = z.T, y, x
    fig = go.Figure(
        go.Heatmap(
            z=z,
            x=x,
            y=y,
            colorscale="Blues",
            colorbar=dict(title="mm"),
            texttemplate="%{z:,.0f}",
            hovertemplate="%{y} · %{x}: %{z:,.1f} mm<extra></extra>",
        )
    )
    fig.update_yaxes(autorange="reversed")
    title = (
        "Lượng mưa trung bình mỗi năm (mm): tháng × điểm"
        if pivot
        else "Lượng mưa trung bình mỗi năm (mm): điểm × tháng"
    )
    show(style(fig, title, height=120 + 34 * len(y)))
    shown = grid.T if pivot else grid
    formatted = shown.map(lambda v: num(v))
    formatted.insert(0, "", shown.index)
    table(formatted)
    st.caption(
        "Mỗi ô là tổng mưa của tháng đó, lấy trung bình qua các năm đã chọn. "
        "'Cả kỳ' là tổng các tháng đã chọn; 'TB các điểm' là trung bình các điểm, "
        "không phải tổng."
    )

    days = dq.season_days(monthly)
    days["name"] = days["location_code"].map(names())
    left, right = st.columns(2)
    for col, title, target in (
        ("rainy_days", "Ngày mưa mỗi năm theo mùa", left),
        ("hot_days", "Ngày nóng mỗi năm theo mùa", right),
    ):
        fig = go.Figure(
            [
                go.Bar(
                    name=season,
                    x=part["name"],
                    y=part[col],
                    marker_color=SEASON_COLORS[season],
                    hovertemplate="%{x}: %{y:,.1f} ngày<extra></extra>",
                )
                for season, part in days.groupby("season")
            ]
        )
        target.plotly_chart(style(fig, title, "ngày"), use_container_width=True)
    sql_box(sql)


# --- 4. Source comparison ----------------------------------------------------------------------


def page_sources() -> None:
    header(
        "So sánh nguồn",
        "Ba nguồn khác nhau thế nào tại cùng một điểm, cùng thời gian?",
        "Slice · Dice",
    )
    c1, c2 = st.columns(2)
    location = c1.selectbox(
        "Điểm nghiên cứu",
        list(location_table()["location_code"]),
        format_func=names().get,
        key="so_location",
    )
    year = c2.selectbox("Năm", YEAR_LIST, index=len(YEAR_LIST) - 2, key="so_year")
    months_df, sql_months = cached("source_months", location, year)
    years_df, sql_years = cached("source_years", location)

    left, right = st.columns(2)
    fig = go.Figure()
    for source, part in months_df.groupby("source_name"):
        fig.add_trace(
            go.Scatter(
                x=part["month"],
                y=part["temp_mean_c"],
                name=source,
                mode="lines+markers",
                line=dict(color=SOURCE_COLORS[source]),
                hovertemplate="T%{x}: %{y:,.2f} °C<extra>" + source + "</extra>",
            )
        )
    month_axis(fig)
    left.plotly_chart(
        style(fig, f"Nhiệt độ trung bình theo tháng, {year} (°C)", "°C"), use_container_width=True
    )
    fig = go.Figure(
        [
            go.Bar(
                x=part["month"],
                y=part["rain_mm"],
                name=source,
                marker_color=SOURCE_COLORS[source],
                hovertemplate="T%{x}: %{y:,.1f} mm<extra>" + source + "</extra>",
            )
            for source, part in months_df.groupby("source_name")
            if part["rain_mm"].notna().any()
        ]
    )
    month_axis(fig)
    right.plotly_chart(
        style(fig, f"Lượng mưa theo tháng, {year} (mm)", "mm"), use_container_width=True
    )

    fig = go.Figure(
        [
            go.Bar(
                x=part["year"],
                y=part["rain_mm"],
                name=source,
                marker_color=SOURCE_COLORS[source],
                hovertemplate="%{x}: %{y:,.1f} mm<extra>" + source + "</extra>",
            )
            for source, part in years_df.groupby("source_name")
            if part["rain_mm"].notna().any()
        ]
    )
    fig.update_xaxes(dtick=1)
    show(style(fig, "Lượng mưa theo năm, 2016–2025 (mm)", "mm"))

    st.subheader(f"Đối chiếu từng nguồn: {names()[location]}, {year}")
    # Calendar hours of the whole local year, so a source missing a month is not inflated
    hours, sql_hours = cached("calendar_hours", (year, year), ALL_MONTHS)
    expected = float(hours["hours"].iloc[0])
    rows = []
    for source, part in months_df.groupby("source_name"):
        complete_rain, complete_temp = (
            part["complete_rain_days"].sum(),
            part["complete_temp_days"].sum(),
        )
        rows.append(
            {
                "Nguồn": source,
                "Mưa (mm)": num(part["rain_mm"].sum(min_count=1)),
                "Nhiệt độ TB (°C)": num(
                    part["temp_sum"].sum() / part["temp_valid_hours"].sum()
                    if part["temp_valid_hours"].sum()
                    else None,
                    2,
                ),
                "Ngày mưa": num(part["rainy_days"].sum() if complete_rain else None, 0),
                "Ngày nóng": num(part["hot_days"].sum() if complete_temp else None, 0),
                "Độ phủ mưa": pct(part["rain_valid_hours"].sum() / expected, 2),
                "Độ phủ nhiệt độ": pct(part["temp_valid_hours"].sum() / expected, 2),
            }
        )
    table(pd.DataFrame(rows))
    st.caption(
        "ERA5 là giá trị của ô lưới 0,25° gần điểm nhất. Ở Lào Cai và Hòa Bình ô lưới cao "
        "hơn điểm thật nên nhiệt độ thấp hơn và áp suất không so được giữa các nguồn. "
        "Meteostat chỉ có Hà Nội (trạm sân bay Nội Bài) và không có số liệu mưa."
    )
    sql_box(sql_months, sql_years, sql_hours)


# --- 5. Data quality ---------------------------------------------------------------------------


def page_quality() -> None:
    header(
        "Chất lượng dữ liệu",
        "Mỗi nguồn có đủ số liệu không; cột nào thiếu và vì sao?",
        basis=QUALITY_BASIS,
    )
    cov, sql_cov = cached("coverage")
    counts, sql_counts = cached("column_completeness")
    cov = dq.coverage_pct(cov)

    hours = cov.groupby("source_name")["record_hours"].sum()
    cards = st.columns(len(hours))
    for card, (source, value) in zip(cards, hours.items()):
        card.metric(f"Số giờ có bản ghi · {source}", num(value, 0))

    c1, _ = st.columns([1, 3])
    measure = c1.radio(
        "Độ phủ của", ["Mưa", "Nhiệt độ", "Bản ghi"], horizontal=True, key="qa_measure"
    )
    col = {"Mưa": "rain_pct", "Nhiệt độ": "temp_pct", "Bản ghi": "record_pct"}[measure]
    cov["row"] = cov["source_name"] + " · " + cov["location_code"].map(names())
    grid = cov.pivot(index="row", columns="year", values=col)
    fig = go.Figure(
        go.Heatmap(
            z=grid.to_numpy(),
            x=[str(c) for c in grid.columns],
            y=list(grid.index),
            colorscale="Blues",
            zmin=0,
            zmax=100,
            colorbar=dict(title="%"),
            texttemplate="%{z:,.1f}",
            hovertemplate="%{y} · %{x}: %{z:,.2f}%<extra></extra>",
        )
    )
    fig.update_yaxes(autorange="reversed")
    show(
        style(
            fig,
            f"Độ phủ số liệu {measure.lower()} theo năm (% số giờ phải có)",
            height=140 + 26 * len(grid),
        )
    )

    st.subheader("Tỉ lệ giá trị thiếu theo từng cột (mọi bản ghi trong kho, 2016–2025 theo UTC)")
    missing = dq.column_missing_pct(counts)
    labels = {
        "temperature_c": "Nhiệt độ",
        "humidity_percent": "Độ ẩm",
        "pressure_hpa": "Áp suất",
        "precipitation_mm": "Lượng mưa",
        "wind_speed_ms": "Tốc độ gió",
        "wind_direction_deg": "Hướng gió",
        "cloud_cover_percent": "Mây",
        "solar_radiation_wm2": "Bức xạ",
    }
    shown = pd.DataFrame(
        {"Nguồn": missing["source_name"], "Số bản ghi": missing["rows"].map(lambda v: num(v, 0))}
    )
    for column, label in labels.items():
        shown[label] = missing[column].map(lambda v: num(v, 2, "%"))
    table(shown)
    st.markdown(
        "- Thiếu là **để trống**, không ghi 0 và không nội suy.\n"
        "- Meteostat chỉ có trạm 48820 (sân bay Nội Bài) cho Hà Nội. Mọi giá trị mưa của "
        "Meteostat đều do mô hình điền nên đã bị loại (thiếu 100%). Mây: phần lớn giờ thiếu là "
        "trạm không báo, khoảng một phần ba là giá trị do mô hình điền đã bị loại.\n"
        "- Bức xạ: ERA5 không tải biến này, Meteostat không có.\n"
        "- Năm 2016 theo giờ Việt Nam thiếu 7 giờ đầu, vì số liệu bắt đầu từ 00:00 UTC.\n"
        "- Bảng này đếm mọi bản ghi theo giờ UTC 2016–2025, nên nhiều hơn số giờ ở các thẻ "
        "(đếm theo năm Việt Nam 2016–2025): 7 giờ đầu năm 2026 theo giờ Việt Nam ở mỗi điểm, "
        "tức 56 bản ghi của Open-Meteo, 56 của ERA5 và 7 của Meteostat."
    )
    sql_box(sql_cov, sql_counts)


# --- 6. Map ------------------------------------------------------------------------------------


def page_map() -> None:
    header("Bản đồ lượng mưa", "Mưa phân bố thế nào giữa 8 điểm nghiên cứu?", "Slice")
    c1, c2, _ = st.columns([1, 1, 2])
    source = c1.selectbox("Nguồn dữ liệu", dq.SOURCES[:2], key="mp_source")
    year = c2.selectbox(
        "Năm",
        [0, *YEAR_LIST],
        index=len(YEAR_LIST) - 1,
        format_func=lambda y: "TB 2016–2025" if not y else str(y),
        key="mp_year",
    )
    years = dq.YEARS if not year else (year, year)
    locs = location_table()
    monthly, sql = cached("monthly_rows", source, tuple(locs["location_code"]), years, ALL_MONTHS)
    hours, _ = cached("calendar_hours", years, ALL_MONTHS)
    per = dq.by_location(monthly, float(hours["hours"].iloc[0]))
    points = locs.merge(per, on="location_code", how="left")

    try:
        geo = provinces()
    except Exception as exc:  # MinIO stopped: the map still shows the points
        geo = None
        st.warning(f"Không đọc được ranh giới tỉnh từ MinIO ({exc}); bản đồ chỉ vẽ các điểm.")
    fig = go.Figure()
    if geo:
        lons, lats = dq.boundary_lines(geo)
        fig.add_trace(
            go.Scatter(
                x=lons,
                y=lats,
                mode="lines",
                fill="toself",
                fillcolor="#EEF2F6",
                line=dict(color="#9AA9B8", width=0.8),
                hoverinfo="skip",
                showlegend=False,
            )
        )
    rain = points["rain_mm"]
    size = 12 + 38 * (rain / rain.max()) ** 0.5 if rain.notna().any() else 16
    fig.add_trace(
        go.Scatter(
            x=points["longitude"],
            y=points["latitude"],
            mode="markers+text",
            marker=dict(size=size, color=RAIN, opacity=0.75, line=dict(color="white", width=1.5)),
            text=points["location_name"],
            textposition="top center",
            customdata=points[["province", "rain_mm", "temp_mean_c", "rainy_days"]].to_numpy(),
            hovertemplate=(
                "<b>%{text}</b> (%{customdata[0]})<br>Mưa: %{customdata[1]:,.1f} mm"
                "<br>Nhiệt độ TB: %{customdata[2]:,.2f} °C<br>Ngày mưa: "
                "%{customdata[3]:,.1f}<extra></extra>"
            ),
            showlegend=False,
        )
    )
    fig.update_xaxes(visible=False, range=[102.1, 108.2])
    fig.update_yaxes(visible=False, range=[19.6, 23.5], scaleanchor="x", scaleratio=1.07)
    left, right = st.columns([3, 2])
    with left:
        show(
            style(
                fig,
                f"Lượng mưa tại 8 điểm (kích thước theo mm{'' if year else ' mỗi năm'})",
                height=620,
            )
        )
        if geo:
            st.caption(
                "Ranh giới 25 tỉnh miền Bắc trước sáp nhập (geoBoundaries, tầng Silver), vẽ "
                "không cần nền bản đồ trực tuyến."
            )
    with right:
        st.subheader("Số liệu theo điểm")
        table(
            pd.DataFrame(
                {
                    "Điểm": points["location_name"],
                    "Tỉnh": points["province"],
                    "Mưa (mm)": points["rain_mm"].map(num),
                    "Nhiệt độ TB (°C)": points["temp_mean_c"].map(lambda v: num(v, 2)),
                    "Ngày mưa": points["rainy_days"].map(lambda v: num(v, 0 if year else 1)),
                    "Độ cao (m)": points["elevation_m"].map(lambda v: num(v, 0)),
                }
            )
        )
    sql_box(sql)


# --- 7. Station and model ----------------------------------------------------------------------


def page_stations() -> None:
    header(
        "Trạm đo và mô hình",
        "Mưa của Open-Meteo có khớp với trạm đo mặt đất không?",
        "Drill-down năm → tháng",
        basis=STATION_BASIS,
    )
    c1, c2 = st.columns([1, 3])
    year = c1.selectbox(
        "Năm",
        [0, *YEAR_LIST],
        index=len(YEAR_LIST) - 1,
        format_func=lambda y: "2016–2025" if not y else str(y),
        key="st_year",
    )
    years = dq.YEARS if not year else (year, year)
    rows, sql = cached("station_rows", years)
    if rows.empty:
        st.info("Không có số liệu trạm cho lựa chọn này.")
        sql_box(sql)
        return
    # One location for the cards and the line (rain is never added across locations); the
    # column chart and the table show every location with a station side by side.
    codes = sorted(rows["location_code"].unique())
    location = c2.selectbox(
        "Điểm có trạm (thẻ số và biểu đồ đường)",
        codes,
        index=codes.index("HAN") if "HAN" in codes else 0,
        format_func=names().get,
        key="st_location",
    )
    summary = dq.station_summary(rows)
    own = summary[summary["location_code"] == location].iloc[0]
    one = rows[rows["location_code"] == location]
    st.markdown(
        f"Thẻ số và biểu đồ đường: **{names()[location]}** (trạm {own['station_id']} "
        f"{own['station_name']}). Biểu đồ cột và bảng: cả {len(codes)} điểm có trạm, "
        "mỗi điểm một cột riêng."
    )
    cards = st.columns(3)
    cards[0].metric("Ngày mưa theo trạm", num(own["station_rainy_days"], 0))
    cards[1].metric("Ngày mưa theo Open-Meteo", num(own["model_rainy_days"], 0))
    cards[2].metric("Số ngày so sánh", num(own["compared_days"], 0))

    left, right = st.columns(2)
    fig = go.Figure(
        [
            go.Bar(
                name="Trạm đo",
                x=summary["location_name"],
                y=summary["station_rain_mm"],
                marker_color=STATION,
                hovertemplate="%{x}: %{y:,.1f} mm<extra>Trạm</extra>",
            ),
            go.Bar(
                name="Open-Meteo (ngày đủ 24 giờ)",
                x=summary["location_name"],
                y=summary["model_rain_mm"],
                marker_color=RAIN,
                hovertemplate="%{x}: %{y:,.1f} mm<extra>Open-Meteo</extra>",
            ),
        ]
    )
    period = "2016–2025" if not year else str(year)
    left.plotly_chart(
        style(fig, f"Tổng lượng mưa theo điểm (mm, {period})", "mm"), use_container_width=True
    )
    key = "month" if year else "year"
    series = (
        one.groupby(key)
        .agg(station=("station_rain_mm", _total), model=("model_rain_mm", _total))
        .reset_index()
    )
    fig = go.Figure(
        [
            go.Scatter(
                name="Trạm đo",
                x=series[key],
                y=series["station"],
                mode="lines+markers",
                line=dict(color=STATION, width=2),
            ),
            go.Scatter(
                name="Open-Meteo",
                x=series[key],
                y=series["model"],
                mode="lines+markers",
                line=dict(color=RAIN, width=2),
            ),
        ]
    )
    if year:
        month_axis(fig)
    else:
        fig.update_xaxes(dtick=1)
    right.plotly_chart(
        style(
            fig, f"Lượng mưa theo {'tháng' if year else 'năm'} tại {names()[location]} (mm)", "mm"
        ),
        use_container_width=True,
    )

    table(
        pd.DataFrame(
            {
                "Điểm": summary["location_name"],
                "Trạm (WMO)": summary["station_id"],
                "Tên trạm": summary["station_name"],
                "Vai trò": summary["station_role"],
                "Số ngày so sánh": summary["compared_days"].map(lambda v: num(v, 0)),
                "Mưa trạm (mm)": summary["station_rain_mm"].map(num),
                "Mưa Open-Meteo (mm)": summary["model_rain_mm"].map(num),
                "Ngày mưa trạm": summary["station_rainy_days"].map(lambda v: num(v, 0)),
                "Ngày mưa Open-Meteo": summary["model_rainy_days"].map(lambda v: num(v, 0)),
            }
        )
    )
    st.caption(
        "Hà Nội dùng trạm Hà Đông (11,3 km), Ninh Bình dùng trạm Nam Định (27,3 km); Phú Thọ, "
        "Tuyên Quang, Hòa Bình không có trạm. Ngày 01/01/2016 chỉ có 13 giờ Open-Meteo trong "
        "khung 24 giờ nên mưa mô hình để trống (cả 5 trạm ghi 0,0 mm)."
    )
    sql_box(sql)


# --- 8. Forecast and actual --------------------------------------------------------------------


def page_forecast() -> None:
    header(
        "Dự báo và thực tế",
        "Mô hình CART v1 dự báo mưa 24 giờ tới đúng đến đâu?",
        "Slice",
        basis=FORECAST_BASIS,
    )
    st.info(
        "CART v1, ngưỡng xác suất 0,40 chọn trên validation 2024. Nhãn: tổng mưa 24 giờ tới "
        "≥ 1 mm (Open-Meteo). Test 2025 được đánh giá đúng một lần; đây là thí nghiệm hồi "
        "cứu trên số liệu tái phân tích. Cách đoán giữ nguyên (persistence) đạt F1 0,729."
    )
    rows, sql = cached("prediction_rows")
    codes = list(location_table()["location_code"])
    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
    chosen = c1.multiselect(
        "Điểm nghiên cứu", codes, default=codes, format_func=names().get, key="fc_locations"
    )
    season = c2.selectbox("Mùa", ["Cả năm", "Mùa mưa (5–10)", "Mùa khô (11–4)"], key="fc_season")
    months = c3.slider("Tháng", 1, 12, (1, 12), key="fc_months")
    threshold = c4.select_slider(
        "Ngưỡng xác suất",
        options=[round(0.30 + 0.02 * i, 2) for i in range(21)],
        value=0.40,
        format_func=lambda v: num(v, 2),
        key="fc_threshold",
    )
    if threshold != 0.40:
        st.warning(
            f"Đang xem ngưỡng {num(threshold, 2)}: đây là phân tích thêm. Kết quả chính "
            "thức là ngưỡng 0,40; ngưỡng ưu tiên Recall 0,36 được chọn trên validation."
        )
    data = rows[rows["location_code"].isin(chosen) & rows["month"].between(*months)]
    if season != "Cả năm":
        data = data[data["month"].map(dq.season_of_month) == season]
    if data.empty:
        st.info("Không có dự báo cho lựa chọn này.")
        sql_box(sql)
        return
    data = data.assign(season=data["month"].map(dq.season_of_month))
    counts = dq.classification_counts(data, threshold)
    m = dq.classification_metrics(counts)
    cards = st.columns(5)
    cards[0].metric("Số dự báo", num(m["n"], 0))
    cards[1].metric("Accuracy", num(m["accuracy"], 3))
    cards[2].metric("Precision", num(m["precision"], 3))
    cards[3].metric("Recall", num(m["recall"], 3))
    cards[4].metric("F1", num(m["f1"], 3))
    rates = st.columns(5)
    rates[0].metric("Tỉ lệ có mưa thực tế", num(m["actual_rate"], 3))
    rates[1].metric("Tỉ lệ dự báo mưa", num(m["predicted_rate"], 3))
    rates[2].metric("Xác suất mưa trung bình", num(data["rain_probability"].mean(), 3))

    left, mid = st.columns([1, 1])
    n = max(m["n"], 1)
    z = [[counts["tp"], counts["fn"]], [counts["fp"], counts["tn"]]]
    text = [
        [
            f"TP<br>{num(counts['tp'], 0)}<br>({num(100 * counts['tp'] / n, 1)}%)",
            f"FN · bỏ sót<br>{num(counts['fn'], 0)}<br>({num(100 * counts['fn'] / n, 1)}%)",
        ],
        [
            f"FP · báo sai<br>{num(counts['fp'], 0)}<br>({num(100 * counts['fp'] / n, 1)}%)",
            f"TN<br>{num(counts['tn'], 0)}<br>({num(100 * counts['tn'] / n, 1)}%)",
        ],
    ]
    fig = go.Figure(
        go.Heatmap(
            z=z,
            x=["Dự báo mưa", "Dự báo không mưa"],
            y=["Thực tế mưa", "Thực tế không mưa"],
            text=text,
            texttemplate="%{text}",
            colorscale="Blues",
            showscale=False,
            hoverinfo="skip",
        )
    )
    fig.update_yaxes(autorange="reversed")
    with left:
        show(style(fig, "Ma trận nhầm lẫn", height=380))
    by_loc = dq.metrics_by(data, "location_code", threshold)
    by_loc["name"] = by_loc["location_code"].map(names())
    by_loc = by_loc.sort_values("f1")
    fig = go.Figure(
        go.Bar(
            x=by_loc["f1"],
            y=by_loc["name"],
            orientation="h",
            marker_color=PREDICTED,
            texttemplate="%{x:,.3f}",
            textposition="inside",
            hovertemplate="%{y}: F1 %{x:,.3f}<extra></extra>",
        )
    )
    fig.update_xaxes(range=[0, 1])
    with mid:
        show(style(fig, "F1 theo điểm", height=380))
    # As specified: the two rates on one percentage axis; F1 and the mean probability, which
    # are not percentages, only in the tooltip
    by_month = dq.metrics_by(data, "month", threshold)
    by_month["prob_mean"] = by_month["month"].map(data.groupby("month")["rain_probability"].mean())
    extra = by_month[["f1", "prob_mean"]].to_numpy()
    tip = "<br>F1 %{customdata[0]:,.3f} · xác suất TB %{customdata[1]:,.3f}"
    fig = go.Figure(
        [
            go.Scatter(
                name="Tỉ lệ có mưa thực tế",
                x=by_month["month"],
                y=by_month["actual_rate"],
                customdata=extra,
                mode="lines+markers",
                line=dict(color=ACTUAL, width=2),
                hovertemplate="T%{x}: %{y:.1%}" + tip + "<extra>Thực tế</extra>",
            ),
            go.Scatter(
                name="Tỉ lệ dự báo mưa",
                x=by_month["month"],
                y=by_month["predicted_rate"],
                customdata=extra,
                mode="lines+markers",
                line=dict(color=PREDICTED, width=2, dash="dot"),
                hovertemplate="T%{x}: %{y:.1%}" + tip + "<extra>Dự báo</extra>",
            ),
        ]
    )
    month_axis(fig)
    fig.update_yaxes(range=[0, 1], tickformat=".0%")
    show(
        style(
            fig,
            "Theo tháng: tỉ lệ có mưa thực tế và tỉ lệ dự báo mưa (F1 khi rê chuột)",
            height=360,
        )
    )

    by_season = dq.metrics_by(data, "season", threshold)
    table(
        pd.DataFrame(
            {
                "Mùa": by_season["season"],
                "Số dự báo": by_season["n"].map(lambda v: num(v, 0)),
                "Precision": by_season["precision"].map(lambda v: num(v, 3)),
                "Recall": by_season["recall"].map(lambda v: num(v, 3)),
                "F1": by_season["f1"].map(lambda v: num(v, 3)),
            }
        )
    )

    st.subheader("Xem dự báo của một ngày")
    c1, c2 = st.columns([1, 1])
    location = c1.selectbox("Điểm", codes, format_func=names().get, key="fc_day_location")
    day = c2.date_input(
        "Ngày (giờ Việt Nam)",
        value=date(2025, 7, 15),
        min_value=date(2025, 1, 1),
        max_value=date(2025, 12, 31),
        key="fc_day",
        format="DD/MM/YYYY",
    )
    one = rows[
        (rows["location_code"] == location) & (pd.to_datetime(rows["origin_local"]).dt.date == day)
    ]
    if one.empty:
        st.info(
            "Không có dự báo cho ngày này (dự báo có từ 07:00 01/01/2025 đến 06:00 31/12/2025 "
            "giờ Việt Nam)."
        )
    else:
        official = threshold == 0.40
        predicted = one["predicted_rain"] if official else one["rain_probability"] >= threshold
        if official:
            outcome = one["outcome"].fillna("—")
        else:
            outcome = pd.Series(
                [
                    "—"
                    if a is None or pd.isna(a)
                    else ("TP" if p and a else "FP" if p else "FN" if a else "TN")
                    for p, a in zip(predicted, one["actual_rain"])
                ],
                index=one.index,
            )
        table(
            pd.DataFrame(
                {
                    "Thời điểm dự báo": pd.to_datetime(one["origin_local"]).dt.strftime("%H:%M"),
                    "Xác suất mưa": one["rain_probability"].map(lambda v: num(v, 4)),
                    "Dự báo": predicted.map({True: "Có mưa", False: "Không mưa"}),
                    "Mưa thực tế 24 giờ tới (mm)": one["actual_rain_mm"].map(num),
                    "Kết quả" if official else f"Kết quả (ngưỡng {num(threshold, 2)})": outcome,
                }
            )
        )
    sql_box(sql)


# --- 9. OLAP cubes -----------------------------------------------------------------------------


def page_olap() -> None:
    header(
        "Khối OLAP",
        "Kho tính trước khối nào, nhanh hơn bao nhiêu, và GROUP BY CUBE cho gì?",
        "Roll-up · CUBE",
    )
    sizes, sql_sizes = cached("cube_sizes")
    left, right = st.columns([1, 1])
    with left:
        st.subheader("Hai khối con tính trước")
        table(
            pd.DataFrame(
                {
                    "Khối (materialized view)": sizes["cube"],
                    "Grain": sizes["grain"],
                    "Số dòng": sizes["rows"].map(lambda v: num(v, 0)),
                }
            )
        )
        st.markdown(
            "- **Ngày:** cần cho ngày mưa, ngày nóng; nhỏ hơn mức giờ 24 lần.\n"
            "- **Tháng:** dùng nhiều nhất cho xu hướng, mùa; mức năm tính tức thì từ đây.\n"
            "- Không có khối gộp qua các nguồn hay cộng mưa qua các điểm.\n"
            "- Bước `refresh_olap_cubes` của DAG làm mới hai khối sau mỗi lần nạp và so với view "
            "gốc theo cả hai chiều."
        )
    with right:
        st.subheader("Đo tốc độ trực tiếp")
        st.caption(
            "Cùng một câu hỏi (mưa theo tháng, Hà Nội, Open-Meteo, 2024), chạy 3 lần trên "
            "view gốc và trên khối tính trước."
        )
        if st.button("Chạy đo", key="olap_time"):
            timings = {}
            for relation in ("view", "cube"):
                runs = [
                    _call(dq.timed_monthly_query, relation, "Open-Meteo", "HAN", 2024)
                    for _ in range(3)
                ]
                timings[relation] = (statistics.median(r[2] for r in runs), runs[0][1])
            c1, c2, c3 = st.columns(3)
            c1.metric("Từ view (ms)", num(timings["view"][0], 1))
            c2.metric("Từ khối (ms)", num(timings["cube"][0], 1))
            c3.metric("Nhanh hơn", f"{num(timings['view'][0] / timings['cube'][0], 0)} lần")
            sql_box(timings["view"][1], timings["cube"][1])

    st.subheader("GROUP BY CUBE: mọi tổ hợp điểm × mùa và các dòng tổng")
    c1, c2, _ = st.columns([1, 2, 1])
    source = c1.selectbox("Nguồn dữ liệu", dq.SOURCES[:2], key="cu_source")
    years = c2.slider("Khoảng năm", dq.YEARS[0], dq.YEARS[1], (2016, 2025), key="cu_years")
    cube, sql_cube = cached_olap("cube_rainy_days", source, tuple(years))
    table(
        pd.DataFrame(
            {
                "Điểm": cube["location"].map(
                    lambda c: "Tất cả 8 điểm" if c == "(all 8)" else names().get(c, c)
                ),
                "Mùa": cube["season"].map(
                    {
                        "(all)": "Cả năm",
                        "rainy (5-10)": "Mùa mưa (5–10)",
                        "dry (11-4)": "Mùa khô (11–4)",
                    }
                ),
                "Ngày đủ số liệu": cube["complete_days"].map(lambda v: num(v, 0)),
                "Ngày mưa": cube["rainy_days"].map(lambda v: num(v, 0)),
                "Tỉ lệ ngày mưa": cube["rainy_day_pct"].map(lambda v: num(v, 1, "%")),
                "Nhiệt độ TB (°C)": cube["temp_mean_c"].map(lambda v: num(v, 2)),
            }
        )
    )
    st.caption(
        "Tỉ lệ ngày mưa = ngày mưa / ngày đủ số liệu, nhiệt độ = tổng / số giờ hợp lệ, "
        "nên các dòng tổng tính đúng, không phải trung bình của các trung bình."
    )

    st.subheader("ROLLUP: tháng → quý → năm")
    c1, c2, c3 = st.columns(3)
    r_source = c1.selectbox("Nguồn", dq.SOURCES[:2], key="ru_source")
    r_location = c2.selectbox(
        "Điểm", list(location_table()["location_code"]), format_func=names().get, key="ru_location"
    )
    r_year = c3.selectbox("Năm", YEAR_LIST, index=len(YEAR_LIST) - 2, key="ru_year")
    rollup, sql_rollup = cached_olap("roll_up", r_source, r_location, r_year)
    level_names = {"month": "Tháng", "quarter": "Quý", "year": "Năm"}
    table(
        pd.DataFrame(
            {
                "Mức": rollup["level"].map(level_names),
                "Quý": rollup["quarter"].map(lambda v: num(v, 0)),
                "Tháng": rollup["month"].map(lambda v: num(v, 0)),
                "Mưa (mm)": rollup["rain_mm"].map(num),
                # count(*) FILTER gives 0 for a period without a complete day: unknown, not 0
                "Ngày mưa": rollup["rainy_days"]
                .where(rollup["complete_days"] > 0)
                .map(lambda v: num(v, 0)),
                "Nhiệt độ TB (°C)": rollup["temp_mean_c"].map(lambda v: num(v, 2)),
            }
        )
    )
    st.markdown(
        "**Năm thao tác OLAP trong ứng dụng:** Roll-up và Drill-down ở trang *Xu hướng* (bấm vào "
        "cột để khoan xuống, nút *Gộp lên một mức*); Slice ở *Tổng quan* và *So sánh nguồn*; "
        "Dice và Pivot ở *Mùa và khu vực*; GROUP BY CUBE và ROLLUP ở trang này."
    )
    sql_box(sql_sizes, sql_cube, sql_rollup)


# --- App ---------------------------------------------------------------------------------------


def main() -> None:
    try:
        connection()
    except Exception as exc:
        st.error(
            "Không kết nối được kho dữ liệu PostgreSQL. Chạy `docker compose up -d` rồi tải "
            f"lại trang. Chi tiết: {exc}"
        )
        st.stop()
    pages = [
        st.Page(
            page_overview,
            title="Tổng quan",
            icon=":material/dashboard:",
            url_path="tong-quan",
            default=True,
        ),
        st.Page(page_trends, title="Xu hướng", icon=":material/trending_up:", url_path="xu-huong"),
        st.Page(
            page_seasons, title="Mùa và khu vực", icon=":material/grid_on:", url_path="mua-khu-vuc"
        ),
        st.Page(
            page_sources,
            title="So sánh nguồn",
            icon=":material/compare_arrows:",
            url_path="so-sanh-nguon",
        ),
        st.Page(
            page_quality,
            title="Chất lượng dữ liệu",
            icon=":material/fact_check:",
            url_path="chat-luong",
        ),
        st.Page(page_map, title="Bản đồ", icon=":material/map:", url_path="ban-do"),
        st.Page(
            page_stations,
            title="Trạm đo và mô hình",
            icon=":material/water_drop:",
            url_path="tram-do",
        ),
        st.Page(
            page_forecast,
            title="Dự báo và thực tế",
            icon=":material/online_prediction:",
            url_path="du-bao",
        ),
        st.Page(page_olap, title="Khối OLAP", icon=":material/view_in_ar:", url_path="khoi-olap"),
    ]
    with st.sidebar:
        st.markdown("### Kho khí tượng miền Bắc")
        st.caption("8 điểm · 2016–2025 · PostgreSQL `weather_dwh` · đọc trực tiếp kho, chỉ đọc")
    st.navigation(pages).run()


main()
