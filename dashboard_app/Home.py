import html
import textwrap

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from utils.db import run_query
from utils.formatters import fmt_number, fmt_percent
from utils.ui import init_page, page_header


# ==========================================================
# Page Config
# ==========================================================
init_page("EWIS | Water Intelligence")
st_autorefresh(interval=30000, key="home_refresh")


# ==========================================================
# Page-level CSS polish
# ==========================================================
st.markdown(
    """
<style>
.block-container {
    padding-top: 1.2rem !important;
}

p, li, span, div {
    -webkit-font-smoothing: antialiased;
}

[data-testid="stAppViewContainer"] {
    background:
        radial-gradient(circle at 18% 18%, rgba(34, 211, 238, 0.13), transparent 28%),
        radial-gradient(circle at 78% 8%, rgba(20, 184, 166, 0.10), transparent 30%),
        linear-gradient(135deg, #061827 0%, #08243A 48%, #063A4A 100%) !important;
}

.ewis-title {
    font-size: 52px !important;
    font-weight: 950 !important;
    letter-spacing: -0.9px !important;
}

.ewis-subtitle {
    font-size: 22px !important;
    font-weight: 750 !important;
    color: #CFEFFF !important;
    line-height: 1.55 !important;
}

.section-title {
    font-size: 36px !important;
    font-weight: 950 !important;
    color: #FFFFFF !important;
    margin-top: 24px !important;
    margin-bottom: 18px !important;
    letter-spacing: -0.5px !important;
}

.glass-card {
    background: linear-gradient(135deg, rgba(255,255,255,0.14), rgba(255,255,255,0.065)) !important;
    border: 1px solid rgba(255,255,255,0.18) !important;
    box-shadow: 0 18px 45px rgba(0,0,0,0.24) !important;
}

.system-key {
    color: #22D3EE;
    font-weight: 950;
}

.system-good {
    color: #22C55E;
    font-weight: 950;
}

.system-warn {
    color: #FACC15;
    font-weight: 950;
}
</style>
""",
    unsafe_allow_html=True
)


# ==========================================================
# Robust HTML Renderer
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
# Queries
# ==========================================================
SYSTEM_KPI_QUERY = """
SELECT TOP 1 *
FROM dbo.mart_system_readiness_summary
ORDER BY summary_date DESC;
"""

STATION_STATUS_QUERY = """
SELECT *
FROM dbo.mart_station_latest_status;
"""

OPEN_ALERTS_QUERY = """
SELECT *
FROM dbo.mart_alert_monitor
WHERE status = 'open';
"""

MART_HEALTH_QUERY = """
SELECT COUNT(*) AS ready_marts
FROM (
    SELECT 'mart_station_latest_status' AS mart_name, COUNT(*) AS row_count
    FROM dbo.mart_station_latest_status

    UNION ALL

    SELECT 'mart_system_readiness_summary', COUNT(*)
    FROM dbo.mart_system_readiness_summary

    UNION ALL

    SELECT 'mart_governorate_daily_summary', COUNT(*)
    FROM dbo.mart_governorate_daily_summary

    UNION ALL

    SELECT 'mart_parameter_trend', COUNT(*)
    FROM dbo.mart_parameter_trend

    UNION ALL

    SELECT 'mart_station_daily_snapshot', COUNT(*)
    FROM dbo.mart_station_daily_snapshot

    UNION ALL

    SELECT 'mart_alert_monitor', COUNT(*)
    FROM dbo.mart_alert_monitor

    UNION ALL

    SELECT 'mart_data_quality_monitor', COUNT(*)
    FROM dbo.mart_data_quality_monitor
) x
WHERE row_count > 0;
"""


# ==========================================================
# Helper Functions
# ==========================================================
def clamp(value, minimum=0, maximum=100):
    return max(minimum, min(maximum, float(value)))


def readiness_color(score):
    score = float(score or 0)

    if score >= 90:
        return "#22C55E", "rgba(34,197,94,0.18)", "Stable"
    elif score >= 75:
        return "#FACC15", "rgba(250,204,21,0.16)", "Controlled Monitoring"
    elif score >= 60:
        return "#FB923C", "rgba(251,146,60,0.18)", "Elevated Operational Risk"
    else:
        return "#EF4444", "rgba(239,68,68,0.18)", "Attention Required"


def premium_metric(title, value, subtitle, accent="#22D3EE"):
    render_html(
        f"""
        <div class="glass-card" style="
            position: relative;
            overflow: hidden;
            padding: 24px 26px;
            min-height: 136px;
            background:
                linear-gradient(135deg, rgba(255,255,255,0.15), rgba(255,255,255,0.065)),
                radial-gradient(circle at top right, {accent}44, transparent 38%),
                radial-gradient(circle at bottom left, {accent}22, transparent 42%) !important;
            box-shadow:
                inset 6px 0 0 {accent},
                0 18px 45px rgba(0,0,0,0.26),
                0 0 28px {accent}22 !important;
        ">

            <div style="
                position:absolute;
                top:-45px;
                right:-45px;
                width:120px;
                height:120px;
                border-radius:50%;
                background:{accent}33;
                filter: blur(10px);
            "></div>

            <div style="
                position:absolute;
                bottom:-55px;
                left:-55px;
                width:135px;
                height:135px;
                border-radius:50%;
                background:{accent}22;
                filter: blur(12px);
            "></div>

            <div style="position:relative; z-index:2;">
                <div style="
                    font-size:20px;
                    color:#D7F3FF;
                    font-weight:950;
                    letter-spacing:.55px;
                    text-transform:uppercase;
                    line-height:1.2;
                ">
                    {title}
                </div>

                <div style="
                    font-size:50px;
                    color:#FFFFFF;
                    font-weight:950;
                    margin-top:10px;
                    line-height:1;
                    text-shadow: 0 0 18px {accent}55;
                ">
                    {value}
                </div>

                <div style="
                    font-size:18.5px;
                    color:#D2EAF5;
                    margin-top:13px;
                    line-height:1.45;
                    font-weight:800;
                ">
                    {subtitle}
                </div>
            </div>
        </div>
        """
    )


def wide_system_card(body_html, accent="#22D3EE"):
    render_html(
        f"""
        <div class="glass-card" style="
            padding: 44px 48px;
            min-height: 365px;
            box-shadow:
                inset 0 6px 0 {accent},
                0 18px 45px rgba(0,0,0,0.24) !important;
        ">
            <div style="
                font-size:42px;
                font-weight:950;
                color:#FFFFFF;
                margin-bottom:34px;
                letter-spacing:-0.7px;
            ">
                EWIS System Overview
            </div>

            <div style="
                font-size:23px;
                color:#E3F7FF;
                line-height:1.85;
                font-weight:800;
            ">
                {body_html}
            </div>
        </div>
        """
    )


def route_card(title, description, destination, accent="#22D3EE"):
    render_html(
        f"""
        <div class="glass-card" style="
            padding: 26px 28px;
            min-height: 172px;
            box-shadow:
                inset 5px 0 0 {accent},
                0 18px 45px rgba(0,0,0,0.24) !important;
        ">
            <div style="
                font-size:29px;
                font-weight:950;
                color:#FFFFFF;
                margin-bottom:14px;
                letter-spacing:-0.3px;
            ">
                {title}
            </div>

            <div style="
                font-size:19px;
                color:#D2EAF5;
                line-height:1.65;
                font-weight:800;
            ">
                {description}
            </div>

            <div style="
                font-size:17px;
                color:#22D3EE;
                font-weight:950;
                margin-top:16px;
            ">
                Go to: {destination}
            </div>
        </div>
        """
    )


# ==========================================================
# Load Data
# ==========================================================
try:
    kpi_df = run_query(SYSTEM_KPI_QUERY)
    stations_df = run_query(STATION_STATUS_QUERY)
    open_alerts_df = run_query(OPEN_ALERTS_QUERY)
    mart_health_df = run_query(MART_HEALTH_QUERY)

except Exception as e:
    page_header(
        "EWIS Water Intelligence Platform",
        "National water quality intelligence and operational readiness."
    )
    st.error(f"Database connection/query error: {e}")
    st.stop()


# ==========================================================
# Header
# ==========================================================
page_header(
    "EWIS Water Intelligence Platform",
    "A national command layer for drinking-water quality, operational awareness, and trusted BI."
)

if kpi_df.empty or stations_df.empty:
    st.warning("No data available from EWIS Data Warehouse marts yet.")
    st.stop()


# ==========================================================
# Metrics
# ==========================================================
k = kpi_df.iloc[0]

total_stations = stations_df["station_id"].nunique()

green_stations = stations_df[
    stations_df["overall_status_color"] == "green"
]["station_id"].nunique()

yellow_stations = stations_df[
    stations_df["overall_status_color"] == "yellow"
]["station_id"].nunique()

orange_stations = stations_df[
    stations_df["overall_status_color"] == "orange"
]["station_id"].nunique()

red_stations = stations_df[
    stations_df["overall_status_color"] == "red"
]["station_id"].nunique()

open_critical_records = (
    len(open_alerts_df[open_alerts_df["severity_level"] == "critical"])
    if not open_alerts_df.empty
    else 0
)

stations_with_open_alerts = (
    open_alerts_df["station_id"].nunique()
    if not open_alerts_df.empty
    else 0
)

stable_pct = (green_stations / total_stations * 100) if total_stations else 0

valid_readings_pct = float(k.get("valid_readings_pct", 0) or 0)
avg_data_quality_score = float(k.get("avg_data_quality_score", 0) or 0)

avg_freshness = (
    stations_df["data_freshness_minutes"].dropna().mean()
    if "data_freshness_minutes" in stations_df.columns
    else 0
)
avg_freshness = float(avg_freshness or 0)

ready_marts = (
    int(mart_health_df.iloc[0]["ready_marts"])
    if not mart_health_df.empty
    else 0
)


# ==========================================================
# Platform Readiness Score
# ==========================================================
alert_control_score = clamp(
    100
    - ((stations_with_open_alerts / total_stations) * 65 if total_stations else 0)
    - (open_critical_records * 12)
)

freshness_score = clamp(100 - (avg_freshness * 3))

platform_readiness_score = clamp(
    stable_pct * 0.22
    + valid_readings_pct * 0.25
    + avg_data_quality_score * 0.25
    + alert_control_score * 0.18
    + freshness_score * 0.10
)

progress_width = clamp(platform_readiness_score)


# ==========================================================
# Status Logic
# ==========================================================
hero_accent, hero_bg, readiness_label = readiness_color(platform_readiness_score)

if red_stations > 0 or open_critical_records > 0:
    status_title = "Attention Required"
    status_message = (
        "A limited critical signal is currently active. "
        "Review operational pages for affected stations and alert details."
    )

elif orange_stations > 0:
    status_title = "Elevated Operational Risk"
    status_message = (
        "Some stations require attention, while the national system remains "
        "controlled and observable."
    )

elif yellow_stations > 0:
    status_title = "Controlled Monitoring"
    status_message = (
        "The platform is stable overall, with selected stations under observation "
        "for operational or data-quality signals."
    )

else:
    status_title = "Stable"
    status_message = (
        "All monitored water quality, operational, and data trust signals are stable."
    )


# ==========================================================
# Hero
# ==========================================================
render_html(
    f"""
    <div class="glass-card" style="
        position: relative;
        overflow: hidden;
        padding: 38px 42px;
        margin-bottom: 22px;
        background:
            linear-gradient(135deg, {hero_bg}, rgba(255,255,255,0.055)),
            radial-gradient(circle at top right, {hero_accent}33, transparent 36%),
            radial-gradient(circle at bottom left, {hero_accent}22, transparent 42%) !important;
        border: 1px solid rgba(255,255,255,0.16);
        box-shadow:
            inset 9px 0 0 {hero_accent},
            0 22px 55px rgba(0,0,0,0.28),
            0 0 36px {hero_accent}33 !important;
    ">

        <div style="
            position:absolute;
            top:-60px;
            right:-60px;
            width:170px;
            height:170px;
            border-radius:50%;
            background:{hero_accent}33;
            filter: blur(18px);
        "></div>

        <div style="
            position:absolute;
            bottom:-75px;
            left:-75px;
            width:190px;
            height:190px;
            border-radius:50%;
            background:{hero_accent}24;
            filter: blur(20px);
        "></div>

        <div style="
            position:relative;
            z-index:2;
            display:flex;
            align-items:center;
            justify-content:space-between;
            gap:36px;
        ">
            <div>
                <div style="
                    font-size:17px;
                    color:#B7D7E8;
                    font-weight:950;
                    letter-spacing:1px;
                    text-transform:uppercase;
                ">
                    EWIS Command Landing · Competition Presentation Mode
                </div>

                <div style="
                    font-size:55px;
                    font-weight:950;
                    color:{hero_accent};
                    margin-top:12px;
                    letter-spacing:-0.9px;
                    line-height:1.1;
                    text-shadow: 0 0 24px {hero_accent}66;
                ">
                    {status_title}
                </div>

                <div style="
                    font-size:22.5px;
                    color:#E3F7FF;
                    margin-top:16px;
                    max-width:980px;
                    line-height:1.6;
                    font-weight:800;
                ">
                    {status_message}
                </div>
            </div>

            <div style="text-align:right; min-width:240px;">
                <div style="
                    font-size:69px;
                    font-weight:950;
                    color:white;
                    line-height:1;
                    text-shadow: 0 0 24px {hero_accent}66;
                ">
                    {fmt_percent(platform_readiness_score, 1)}
                </div>

                <div style="
                    font-size:19px;
                    color:#B7D7E8;
                    margin-top:12px;
                    font-weight:950;
                ">
                    Platform Readiness
                </div>

                <div style="
                    margin-top:14px;
                    width:100%;
                    height:8px;
                    border-radius:999px;
                    background:rgba(255,255,255,0.14);
                    overflow:hidden;
                ">
                    <div style="
                        width:{progress_width}%;
                        height:100%;
                        border-radius:999px;
                        background:{hero_accent};
                        box-shadow:0 0 18px {hero_accent};
                    "></div>
                </div>

                <div style="
                    font-size:14px;
                    color:{hero_accent};
                    font-weight:950;
                    margin-top:10px;
                    text-transform:uppercase;
                    letter-spacing:.6px;
                ">
                    {readiness_label}
                </div>
            </div>
        </div>
    </div>
    """
)


# ==========================================================
# Essential Metrics
# ==========================================================
m1, m2, m3, m4 = st.columns(4, gap="large")

with m1:
    premium_metric(
        "Stable Coverage",
        f"{fmt_number(green_stations)} / {fmt_number(total_stations)}",
        "Stations currently in stable live state",
        "#22C55E"
    )

with m2:
    premium_metric(
        "Affected Stations",
        fmt_number(stations_with_open_alerts),
        "Stations with active open alerts",
        "#FACC15"
    )

with m3:
    premium_metric(
        "Data Validity",
        fmt_percent(valid_readings_pct, 2),
        "Validated readings in latest system snapshot",
        "#22D3EE"
    )

with m4:
    premium_metric(
        "BI Readiness",
        f"{ready_marts}/7",
        "Marts available for dashboard consumption",
        "#38BDF8"
    )


# ==========================================================
# System Overview
# ==========================================================
render_html(
    """
    <div class="section-title" style="font-size:36px; font-weight:950;">
        System Overview
    </div>
    """
)

system_overview_html = f"""
<div style="
    display:grid;
    grid-template-columns: 1.15fr 1fr 1fr;
    gap:48px;
    align-items:start;
">
    <div>
        <div style="
            font-size:32px;
            font-weight:950;
            color:#FFFFFF;
            margin-bottom:18px;
            letter-spacing:-0.4px;
        ">
            What EWIS Is
        </div>

        <div style="
            font-size:23px;
            color:#E5F7FF;
            line-height:1.85;
            font-weight:800;
            max-width:620px;
        ">
            EWIS converts real-time water-treatment telemetry into trusted operational intelligence
            for monitoring, risk awareness, and executive decision-making.
        </div>
    </div>

    <div>
        <div style="
            font-size:32px;
            font-weight:950;
            color:#FFFFFF;
            margin-bottom:18px;
            letter-spacing:-0.4px;
        ">
            How It Works
        </div>

        <div style="
            font-size:22px;
            color:#E5F7FF;
            line-height:2;
            font-weight:850;
        ">
            <div>1. Stream sensor readings</div>
            <div>2. Validate and score signals</div>
            <div>3. Detect alerts and risks</div>
            <div>4. Publish BI-ready marts</div>
        </div>
    </div>

    <div>
        <div style="
            font-size:32px;
            font-weight:950;
            color:#FFFFFF;
            margin-bottom:18px;
            letter-spacing:-0.4px;
        ">
            Dashboard Rules
        </div>

        <div style="
            font-size:22px;
            color:#E5F7FF;
            line-height:2;
            font-weight:850;
        ">
            <div><b>Live status:</b> <span class="system-key">overall_status_color</span></div>
            <div><b>Live alerts:</b> <span class="system-warn">status = open</span></div>
            <div><b>BI marts ready:</b> <span class="system-good">{ready_marts}/7</span></div>
            <div><b>Facts usage:</b> drill-through only</div>
        </div>
    </div>
</div>
"""

wide_system_card(
    system_overview_html,
    "#22D3EE"
)


# ==========================================================
# Recommended Navigation
# ==========================================================
render_html(
    """
    <div class="section-title" style="font-size:36px; font-weight:950;">
        Recommended Navigation
    </div>
    """
)

n1, n2, n3 = st.columns(3, gap="large")

with n1:
    route_card(
        "Executive View",
        "National monitoring, live station colors, alerts, and priority stations.",
        "Executive Overview",
        "#22C55E"
    )

with n2:
    route_card(
        "Station Diagnosis",
        "Station-level reasons, alert drill-down, freshness, and history.",
        "Station Monitoring",
        "#22D3EE"
    )

with n3:
    route_card(
        "Analysis & Trust",
        "Trends, parameter breaches, and data reliability validation.",
        "Water Quality Trends / Data Trust",
        "#38BDF8"
    )


# ==========================================================
# Footer
# ==========================================================
st.success(
    "Home page simplified for executive storytelling. Detailed live monitoring is available in the sidebar pages."
)