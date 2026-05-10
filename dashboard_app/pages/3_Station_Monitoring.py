import html
import textwrap

import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from utils.db import run_query
from utils.formatters import fmt_number
from utils.queries import (
    STATION_STATUS_QUERY,
    ALERT_MONITOR_QUERY,
    STATION_DAILY_QUERY,
    DATA_QUALITY_QUERY
)
from utils.ui import (
    init_page,
    page_header,
    kpi_card,
    style_plotly,
    status_badge
)


# ==========================================================
# Page Config
# ==========================================================
init_page("Station Monitoring")
st_autorefresh(interval=30000, key="station_refresh")


# ==========================================================
# Safe HTML Renderer
# ==========================================================
def render_html(raw_html: str):
    clean_html = html.unescape(textwrap.dedent(raw_html)).strip()
    clean_html = "\n".join(
        line.strip()
        for line in clean_html.splitlines()
        if line.strip()
    )
    st.markdown(clean_html, unsafe_allow_html=True)


# ==========================================================
# Typography Polish
# Text only. No card size or layout changes.
# ==========================================================
st.markdown(
    """
<style>
/* Main page title and subtitle */
.ewis-title {
    font-size: 50px !important;
    font-weight: 950 !important;
    letter-spacing: -0.9px !important;
}

.ewis-subtitle {
    font-size: 21px !important;
    font-weight: 750 !important;
    color: #CFEFFF !important;
    line-height: 1.55 !important;
}

/* Section titles */
.section-title {
    font-size: 34px !important;
    font-weight: 950 !important;
    color: #FFFFFF !important;
    margin-top: 17px !important;
    margin-bottom: 15px !important;
    letter-spacing: -0.5px !important;
}

/* KPI cards typography only */
.kpi-label {
    font-size: 18px !important;
    font-weight: 950 !important;
    color: #D7F3FF !important;
    letter-spacing: 0.2px !important;
    line-height: 1.15 !important;
}

.kpi-value {
    font-size: 42px !important;
    font-weight: 950 !important;
    color: #FFFFFF !important;
    line-height: 1 !important;
    margin-top: 8px !important;
}

.kpi-hint {
    font-size: 15.5px !important;
    font-weight: 800 !important;
    color: #C2DDEA !important;
    line-height: 1.3 !important;
    margin-top: 8px !important;
}

/* Station summary card */
.station-summary-title {
    font-size: 32px !important;
    font-weight: 950 !important;
    color: #FFFFFF !important;
    margin-bottom: 12px !important;
    letter-spacing: -0.4px !important;
}

.station-summary-text {
    font-size: 20px !important;
    font-weight: 750 !important;
    color: #D7F3FF !important;
    line-height: 1.55 !important;
    margin-bottom: 10px !important;
}

.station-summary-reason {
    font-size: 20px !important;
    font-weight: 800 !important;
    color: #E5F7FF !important;
    line-height: 1.55 !important;
    margin-top: 10px !important;
}

/* Streamlit metric text inside Data Trust */
[data-testid="stMetricLabel"] {
    font-size: 19px !important;
    font-weight: 900 !important;
    color: #D7F3FF !important;
}

[data-testid="stMetricValue"] {
    font-size: 40px !important;
    font-weight: 950 !important;
    color: #FFFFFF !important;
}

/* Dataframe readability */
[data-testid="stDataFrame"] {
    font-size: 17px !important;
}

/* Streamlit alert/info/success text readability */
.stAlert {
    font-size: 18px !important;
    font-weight: 800 !important;
}

/* Sidebar readability */
section[data-testid="stSidebar"] label {
    font-size: 18px !important;
    font-weight: 850 !important;
}

section[data-testid="stSidebar"] div[data-baseweb="select"] {
    font-size: 17px !important;
}

/* Plotly text smoothing */
.js-plotly-plot,
.plotly,
.svg-container {
    font-weight: 700 !important;
}
</style>
""",
    unsafe_allow_html=True
)


# ==========================================================
# Header
# ==========================================================
page_header(
    "🏭 Station Monitoring",
    "Drill down into station-level WQI, status reasons, open alerts, and historical performance."
)


# ==========================================================
# Load Data
# ==========================================================
stations = run_query(STATION_STATUS_QUERY)
alerts = run_query(ALERT_MONITOR_QUERY)
daily = run_query(STATION_DAILY_QUERY)
dq = run_query(DATA_QUALITY_QUERY)

if stations.empty:
    st.error("No station status data available.")
    st.stop()


# ==========================================================
# Sidebar Filters
# ==========================================================
govs = ["All"] + sorted(stations["governorate"].dropna().unique().tolist())
selected_gov = st.sidebar.selectbox("Governorate", govs)

filtered_stations = stations.copy()

if selected_gov != "All":
    filtered_stations = filtered_stations[
        filtered_stations["governorate"] == selected_gov
    ]

station_names = sorted(
    filtered_stations["station_name"].dropna().unique().tolist()
)

if not station_names:
    st.warning("No stations available for the selected governorate.")
    st.stop()

selected_station = st.sidebar.selectbox("Station", station_names)


# ==========================================================
# Selected Station
# ==========================================================
s = stations[stations["station_name"] == selected_station].iloc[0]
station_id = s["station_id"]


# ==========================================================
# Station Summary Card
# ==========================================================
render_html(
    f"""
    <div class="glass-card">
        <div class="station-summary-title">
            {selected_station}
        </div>

        <div class="station-summary-text">
            {s.get("governorate", "-")}
            | Source: {s.get("water_source_type", "-")}
            | Capacity: {fmt_number(s.get("plant_capacity_m3_day"))} m³/day
        </div>

        <div class="station-summary-text">
            {status_badge(s.get("overall_status_color"))}
        </div>

        <div class="station-summary-reason">
            <b>Reason:</b> {s.get("overall_status_reason", "-")}
        </div>
    </div>
    """
)


# ==========================================================
# KPI Cards
# ==========================================================
c1, c2, c3, c4, c5 = st.columns(5, gap="large")

with c1:
    kpi_card(
        "Latest WQI",
        fmt_number(s.get("latest_wqi_score"), 2),
        s.get("latest_wqi_class", ""),
        "💧"
    )

with c2:
    kpi_card(
        "Active Alerts",
        fmt_number(s.get("active_alert_count")),
        "Open issues",
        "🚨"
    )

with c3:
    kpi_card(
        "Water Alerts",
        fmt_number(s.get("active_water_alert_count")),
        "Quality domain",
        "🧪"
    )

with c4:
    kpi_card(
        "Process Alerts",
        fmt_number(s.get("active_process_alert_count")),
        "Operations domain",
        "⚙️"
    )

with c5:
    kpi_card(
        "Freshness",
        fmt_number(s.get("data_freshness_minutes")),
        "minutes since update",
        "⏱️"
    )


# ==========================================================
# Alerts + Data Trust
# ==========================================================
left, right = st.columns([1, 1], gap="large")


# ----------------------------------------------------------
# Open Alerts
# ----------------------------------------------------------
with left:
    st.markdown(
        '<div class="section-title">🚨 Open Alerts for Station</div>',
        unsafe_allow_html=True
    )

    station_alerts = alerts[
        (alerts["station_id"] == station_id)
        & (alerts["status"] == "open")
    ]

    if station_alerts.empty:
        st.success("No open alerts for this station.")
    else:
        cols = [
            "event_timestamp",
            "alert_type",
            "alert_domain",
            "severity_level",
            "measured_value",
            "alert_message"
        ]

        existing_cols = [
            col for col in cols
            if col in station_alerts.columns
        ]

        st.dataframe(
            station_alerts[existing_cols],
            use_container_width=True,
            hide_index=True
        )


# ----------------------------------------------------------
# Data Trust
# ----------------------------------------------------------
with right:
    st.markdown(
        '<div class="section-title">✅ Data Trust</div>',
        unsafe_allow_html=True
    )

    station_dq = (
        dq[dq["station_id"] == station_id]
        .sort_values("summary_date", ascending=False)
    )

    if station_dq.empty:
        st.info("No data quality records.")
    else:
        latest_dq = station_dq.iloc[0]

        k1, k2 = st.columns(2)

        with k1:
            st.metric(
                "Data Quality Score",
                fmt_number(latest_dq.get("data_quality_score"), 2)
            )

        with k2:
            st.metric(
                "Quality Band",
                latest_dq.get("data_quality_band", "-")
            )


# ==========================================================
# Station Daily WQI History
# ==========================================================
st.markdown(
    '<div class="section-title">📈 Station Daily WQI History</div>',
    unsafe_allow_html=True
)

station_daily = daily[daily["station_id"] == station_id].copy()

if station_daily.empty:
    st.info("No historical daily snapshot available.")
else:
    fig = px.line(
        station_daily.sort_values("summary_date"),
        x="summary_date",
        y=["avg_wqi", "min_wqi", "max_wqi"],
        markers=True,
        labels={
            "summary_date": "Summary Date",
            "value": "WQI",
            "variable": "Metric"
        }
    )

    fig.update_layout(
        font=dict(size=20, color="#E5F7FF"),
        xaxis=dict(
            title_font=dict(size=22),
            tickfont=dict(size=19)
        ),
        yaxis=dict(
            title_font=dict(size=22),
            tickfont=dict(size=19)
        ),
        legend=dict(
            font=dict(size=20),
            title=dict(font=dict(size=20))
        )
    )

    fig.update_traces(
        textfont=dict(size=20),
        hoverlabel=dict(
            font_size=20
        )
    )

    st.plotly_chart(
        style_plotly(fig, height=420),
        use_container_width=True
    )

    cols = [
        "summary_date",
        "avg_wqi",
        "min_wqi",
        "max_wqi",
        "alert_count",
        "critical_alert_count",
        "snapshot_status_color",
        "snapshot_status_reason"
    ]

    existing_cols = [
        col for col in cols
        if col in station_daily.columns
    ]

    st.dataframe(
        station_daily[existing_cols]
        .sort_values("summary_date", ascending=False),
        use_container_width=True,
        hide_index=True
    )