import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from utils.db import run_query
from utils.formatters import fmt_number, fmt_percent
from utils.queries import (
    NATIONAL_KPI_QUERY,
    STATUS_DISTRIBUTION_QUERY,
    STATION_STATUS_QUERY,
    OPEN_ALERTS_QUERY
)
from utils.ui import (
    init_page,
    page_header,
    kpi_card,
    style_plotly,
    STATUS_COLORS,
    STATUS_RGB
)


# ==========================================================
# Page Config
# ==========================================================
init_page("Executive Overview")
st_autorefresh(interval=30000, key="exec_refresh")


# ==========================================================
# Page-level Typography Polish
# Balanced readable text. No layout, card size, chart size, or map size changes.
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

/* KPI cards typography only - card size is unchanged */
.kpi-label {
    font-size: 22px !important;
    font-weight: 950 !important;
    color: #D7F3FF !important;
    letter-spacing: 0.2px !important;
    line-height: 1.15 !important;
}

.kpi-value {
    font-size: 54px !important;
    font-weight: 950 !important;
    color: #FFFFFF !important;
    line-height: 0.98 !important;
    margin-top: 9px !important;
}

.kpi-hint {
    font-size: 18px !important;
    font-weight: 800 !important;
    color: #C2DDEA !important;
    line-height: 1.3 !important;
    margin-top: 9px !important;
}

/* Dataframe readability */
[data-testid="stDataFrame"] {
    font-size: 17px !important;
}

/* Streamlit alert text readability */
.stAlert {
    font-size: 18px !important;
    font-weight: 800 !important;
}

/* Plotly container text smoothing */
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
    "🌍 Executive Overview",
    "Live national status, station health, open alerts, and operational readiness."
)


# ==========================================================
# Load Data
# ==========================================================
kpi_df = run_query(NATIONAL_KPI_QUERY)
status_df = run_query(STATUS_DISTRIBUTION_QUERY)
stations_df = run_query(STATION_STATUS_QUERY)
alerts_df = run_query(OPEN_ALERTS_QUERY)

if kpi_df.empty:
    st.error("No KPI data available.")
    st.stop()

if stations_df.empty:
    st.error("No station status data available.")
    st.stop()

k = kpi_df.iloc[0]


# ==========================================================
# Derived KPIs
# ==========================================================
green_count = (
    stations_df[stations_df["overall_status_color"] == "green"]["station_id"].nunique()
    if not stations_df.empty else 0
)

yellow_count = (
    stations_df[stations_df["overall_status_color"] == "yellow"]["station_id"].nunique()
    if not stations_df.empty else 0
)

orange_count = (
    stations_df[stations_df["overall_status_color"] == "orange"]["station_id"].nunique()
    if not stations_df.empty else 0
)

red_count = (
    stations_df[stations_df["overall_status_color"] == "red"]["station_id"].nunique()
    if not stations_df.empty else 0
)

open_critical = (
    alerts_df[alerts_df["severity_level"] == "critical"]["alert_id"].nunique()
    if not alerts_df.empty else 0
)

stations_with_open = (
    alerts_df["station_id"].nunique()
    if not alerts_df.empty else 0
)


# ==========================================================
# KPI Cards
# ==========================================================
c1, c2, c3, c4, c5, c6 = st.columns(6, gap="large")

with c1:
    kpi_card(
        "Stations Reporting",
        fmt_number(k.get("stations_reporting_count")),
        "Live reporting stations",
        "🏭"
    )

with c2:
    kpi_card(
        "Green Stations",
        fmt_number(green_count),
        "Stable current status",
        "🟢"
    )

with c3:
    kpi_card(
        "Yellow Stations",
        fmt_number(yellow_count),
        "Need monitoring",
        "🟡"
    )

with c4:
    kpi_card(
        "Open Alerts",
        fmt_number(len(alerts_df)),
        "Current open alert records",
        "🚨"
    )

with c5:
    kpi_card(
        "Critical Open",
        fmt_number(open_critical),
        "Critical live alerts",
        "🔴"
    )

with c6:
    kpi_card(
        "Valid Data",
        fmt_percent(k.get("valid_readings_pct")),
        "Warehouse trust signal",
        "✅"
    )


# ==========================================================
# Main Layout
# ==========================================================
left, right = st.columns([1.05, 1.45], gap="large")


# ==========================================================
# Left Column: Status Distribution + Alerts
# ==========================================================
with left:
    st.markdown(
        '<div class="section-title">📊 Current Station Status</div>',
        unsafe_allow_html=True
    )

    fig = px.pie(
        status_df,
        names="overall_status_color",
        values="station_count",
        color="overall_status_color",
        color_discrete_map=STATUS_COLORS,
        hole=0.62
    )

    fig.update_traces(
        textposition="inside",
        textinfo="percent+label",
        textfont=dict(size=20, color="#E5F7FF"),
        marker=dict(
            line=dict(
                color="rgba(255,255,255,0.18)",
                width=1
            )
        )
    )

    fig.update_layout(
        showlegend=True,
        legend_title_text="Status",
        font=dict(size=20, color="#E5F7FF"),
        legend=dict(
            font=dict(size=20),
            title=dict(font=dict(size=20))
        )
    )

    st.plotly_chart(
        style_plotly(fig, height=360),
        use_container_width=True
    )

    st.markdown(
        '<div class="section-title">🚨 Open Alert Types</div>',
        unsafe_allow_html=True
    )

    if alerts_df.empty:
        st.success("No open alerts currently.")
    else:
        alert_type_df = (
            alerts_df
            .groupby(["alert_type", "severity_level"], as_index=False)
            .size()
            .rename(columns={"size": "count"})
            .sort_values("count", ascending=True)
        )

        fig2 = px.bar(
            alert_type_df,
            x="count",
            y="alert_type",
            color="severity_level",
            orientation="h",
            color_discrete_map={
                "warning": "#FACC15",
                "critical": "#EF4444"
            },
            labels={
                "count": "Open Alerts",
                "alert_type": "Alert Type",
                "severity_level": "Severity"
            }
        )

        fig2.update_layout(
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

        fig2.update_traces(
            textfont=dict(size=20),
            hoverlabel=dict(
                font_size=20
            )
        )

        st.plotly_chart(
            style_plotly(fig2, height=330),
            use_container_width=True
        )


# ==========================================================
# Right Column: Map
# ==========================================================
with right:
    st.markdown(
        '<div class="section-title">🗺️ Live Station Map</div>',
        unsafe_allow_html=True
    )

    map_df = stations_df.dropna(subset=["latitude", "longitude"]).copy()

    map_df["color"] = map_df["overall_status_color"].map(STATUS_RGB)

    map_df["color"] = map_df["color"].apply(
        lambda x: x if isinstance(x, list) else [148, 163, 184]
    )

    map_df["radius"] = (
        map_df["status_priority"]
        .fillna(1)
        .astype(float) * 2500 + 3500
    )

    tooltip = {
        "html": """
        <b>{station_name}</b><br/>
        Governorate: {governorate}<br/>
        WQI: {latest_wqi_score}<br/>
        Alerts: {active_alert_count}<br/>
        Status: {overall_status_color}<br/>
        Reason: {overall_status_reason}
        """,
        "style": {
            "backgroundColor": "#061827",
            "color": "white",
            "fontSize": "19px"
        }
    }

    deck = pdk.Deck(
        initial_view_state=pdk.ViewState(
            latitude=26.8,
            longitude=30.8,
            zoom=5,
            pitch=0
        ),
        layers=[
            pdk.Layer(
                "ScatterplotLayer",
                data=map_df,
                get_position="[longitude, latitude]",
                get_fill_color="color",
                get_radius="radius",
                pickable=True,
                opacity=0.82,
            )
        ],
        tooltip=tooltip
    )

    st.pydeck_chart(
        deck,
        use_container_width=True
    )


# ==========================================================
# Priority Stations Table
# ==========================================================
st.markdown(
    '<div class="section-title">🎯 Priority Stations</div>',
    unsafe_allow_html=True
)

priority_cols = [
    "station_name",
    "governorate",
    "latest_wqi_score",
    "latest_wqi_class",
    "active_alert_count",
    "overall_status_color",
    "overall_status_reason"
]

existing_priority_cols = [
    col for col in priority_cols
    if col in stations_df.columns
]

priority_df = stations_df.sort_values(
    ["status_priority", "active_alert_count"],
    ascending=[False, False]
)

st.dataframe(
    priority_df[existing_priority_cols].head(15),
    use_container_width=True,
    hide_index=True
)