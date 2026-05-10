import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from utils.db import run_query
from utils.formatters import fmt_number
from utils.queries import GOVERNORATE_SUMMARY_QUERY
from utils.ui import init_page, page_header, kpi_card, style_plotly


# ==========================================================
# Page Config
# ==========================================================
init_page("Governorate Overview")
st_autorefresh(interval=30000, key="gov_refresh")


# ==========================================================
# Page-level Typography Polish
# Balanced readable text. No layout, card size, chart size, or table structure changes.
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
    "🏙️ Governorate Performance",
    "Regional water quality, alert load, station mix, and data quality comparison."
)


# ==========================================================
# Load Data
# ==========================================================
df = run_query(GOVERNORATE_SUMMARY_QUERY)

if df.empty:
    st.error("No governorate summary data available.")
    st.stop()


# ==========================================================
# Derived KPIs
# ==========================================================
avg_wqi = df["avg_wqi"].mean()
total_alerts = df["alert_count"].sum()
critical_alerts = df["critical_alert_count"].sum()
total_green = df["current_green_station_count"].sum()


# ==========================================================
# KPI Cards
# ==========================================================
c1, c2, c3, c4 = st.columns(4, gap="large")

with c1:
    kpi_card(
        "Avg Governorate WQI",
        fmt_number(avg_wqi, 2),
        "Mean regional quality",
        "💧"
    )

with c2:
    kpi_card(
        "Regional Alerts",
        fmt_number(total_alerts),
        "Daily alert volume",
        "🚨"
    )

with c3:
    kpi_card(
        "Critical Alerts",
        fmt_number(critical_alerts),
        "Critical incidents",
        "🔴"
    )

with c4:
    kpi_card(
        "Green Stations",
        fmt_number(total_green),
        "Stable stations",
        "🟢"
    )


# ==========================================================
# Main Layout
# ==========================================================
left, right = st.columns(2, gap="large")


# ==========================================================
# Left Chart: Avg WQI by Governorate
# ==========================================================
with left:
    st.markdown(
        '<div class="section-title">💧 Avg WQI by Governorate</div>',
        unsafe_allow_html=True
    )

    fig = px.bar(
        df.sort_values("avg_wqi", ascending=True),
        x="avg_wqi",
        y="governorate",
        orientation="h",
        color="avg_wqi",
        color_continuous_scale=["#EF4444", "#FACC15", "#22C55E"],
        labels={
            "avg_wqi": "Avg WQI",
            "governorate": "Governorate"
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
            tickfont=dict(size=18)
        ),
        coloraxis_colorbar=dict(
            title=dict(
                text="Avg WQI",
                font=dict(size=20)
            ),
            tickfont=dict(size=18)
        )
    )

    fig.update_traces(
        hoverlabel=dict(
            font_size=20
        )
    )

    st.plotly_chart(
        style_plotly(fig, height=520),
        use_container_width=True
    )


# ==========================================================
# Right Chart: Current Station Mix
# ==========================================================
with right:
    st.markdown(
        '<div class="section-title">🧭 Current Station Mix</div>',
        unsafe_allow_html=True
    )

    mix_cols = [
        "current_green_station_count",
        "current_yellow_station_count",
        "current_orange_station_count",
        "current_red_station_count",
        "current_gray_station_count"
    ]

    mix_df = df[["governorate"] + mix_cols].melt(
        id_vars="governorate",
        var_name="status",
        value_name="count"
    )

    mix_df["status"] = (
        mix_df["status"]
        .str.replace("current_", "")
        .str.replace("_station_count", "")
    )

    fig2 = px.bar(
        mix_df,
        x="governorate",
        y="count",
        color="status",
        color_discrete_map={
            "green": "#22C55E",
            "yellow": "#FACC15",
            "orange": "#FB923C",
            "red": "#EF4444",
            "gray": "#94A3B8",
        },
        labels={
            "governorate": "Governorate",
            "count": "Station Count",
            "status": "Status"
        }
    )

    fig2.update_layout(
        xaxis_tickangle=-45,
        font=dict(size=20, color="#E5F7FF"),
        xaxis=dict(
            title_font=dict(size=22),
            tickfont=dict(size=17)
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
        hoverlabel=dict(
            font_size=20
        )
    )

    st.plotly_chart(
        style_plotly(fig2, height=520),
        use_container_width=True
    )


# ==========================================================
# Governorate Ranking Table
# ==========================================================
st.markdown(
    '<div class="section-title">📋 Governorate Ranking</div>',
    unsafe_allow_html=True
)

display_cols = [
    "summary_date",
    "governorate",
    "stations_count",
    "avg_wqi",
    "alert_count",
    "critical_alert_count",
    "affected_stations_count",
    "avg_data_quality_score",
    "current_green_station_count",
    "current_yellow_station_count",
    "current_orange_station_count",
    "current_red_station_count",
    "current_gray_station_count"
]

existing_display_cols = [
    col for col in display_cols
    if col in df.columns
]

ranking_df = df[existing_display_cols].sort_values(
    ["critical_alert_count", "alert_count"],
    ascending=False
)

st.dataframe(
    ranking_df,
    use_container_width=True,
    hide_index=True
)