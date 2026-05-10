import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from utils.db import run_query
from utils.queries import DATA_QUALITY_QUERY, NATIONAL_KPI_QUERY
from utils.formatters import fmt_number, fmt_percent
from utils.ui import init_page, page_header, kpi_card, style_plotly


# ==========================================================
# Page Config
# ==========================================================
init_page("EWIS | Data Trust")
st_autorefresh(interval=30000, key="data_trust_refresh")


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

/* Streamlit alert/info/warning text readability */
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

section[data-testid="stSidebar"] .stMarkdown {
    font-size: 18px !important;
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
    "✅ Data Trust & Reliability",
    "Sensor data quality, suspicious readings, invalid records, drift risk, and telemetry reliability."
)


# ==========================================================
# Load Data
# ==========================================================
dq_df = run_query(DATA_QUALITY_QUERY)
kpi_df = run_query(NATIONAL_KPI_QUERY)

if dq_df.empty:
    st.error("No data quality records available.")
    st.stop()


# ==========================================================
# Sidebar Filters
# ==========================================================
st.sidebar.markdown("## 🔎 Filters")

available_governorates = ["All"] + sorted(
    dq_df["governorate"].dropna().unique().tolist()
)

selected_governorate = st.sidebar.selectbox(
    "Governorate",
    available_governorates
)

available_bands = ["All"] + sorted(
    dq_df["data_quality_band"].dropna().unique().tolist()
)

selected_band = st.sidebar.selectbox(
    "Data Quality Band",
    available_bands
)

filtered_df = dq_df.copy()

if selected_governorate != "All":
    filtered_df = filtered_df[
        filtered_df["governorate"] == selected_governorate
    ]

if selected_band != "All":
    filtered_df = filtered_df[
        filtered_df["data_quality_band"] == selected_band
    ]

if filtered_df.empty:
    st.warning("No records match the selected filters.")
    st.stop()


# ==========================================================
# KPI Calculations
# ==========================================================
avg_quality_score = filtered_df["data_quality_score"].mean()

trusted_sensors = filtered_df[
    filtered_df["data_quality_band"] == "Trusted"
]["sensor_id"].nunique()

monitor_sensors = filtered_df[
    filtered_df["data_quality_band"] == "Monitor"
]["sensor_id"].nunique()

at_risk_sensors = filtered_df[
    filtered_df["data_quality_band"] == "At Risk"
]["sensor_id"].nunique()

total_readings = filtered_df["total_readings_count"].sum()
valid_readings = filtered_df["valid_readings_count"].sum()
invalid_readings = filtered_df["invalid_readings_count"].sum()
partial_readings = filtered_df["partial_readings_count"].sum()
duplicate_readings = filtered_df["duplicate_readings_count"].sum()
suspicious_readings = filtered_df["suspicious_readings_count"].sum()
drift_flags = filtered_df["drift_risk_flag_count"].sum()

valid_pct = (valid_readings / total_readings * 100) if total_readings else 0
invalid_pct = (invalid_readings / total_readings * 100) if total_readings else 0
suspicious_pct = (suspicious_readings / total_readings * 100) if total_readings else 0


# ==========================================================
# KPI Cards
# ==========================================================
c1, c2, c3, c4, c5, c6 = st.columns(6, gap="large")

with c1:
    kpi_card(
        "Avg Quality Score",
        fmt_number(avg_quality_score, 2),
        "Overall sensor trust score",
        "✅"
    )

with c2:
    kpi_card(
        "Valid Readings",
        fmt_percent(valid_pct, 2),
        f"{fmt_number(valid_readings)} of {fmt_number(total_readings)}",
        "🟢"
    )

with c3:
    kpi_card(
        "Suspicious Readings",
        fmt_percent(suspicious_pct, 2),
        f"{fmt_number(suspicious_readings)} records",
        "🟡"
    )

with c4:
    kpi_card(
        "Invalid Readings",
        fmt_percent(invalid_pct, 2),
        f"{fmt_number(invalid_readings)} records",
        "🔴"
    )

with c5:
    kpi_card(
        "Duplicate Readings",
        fmt_number(duplicate_readings),
        "Duplicate records detected",
        "📌"
    )

with c6:
    kpi_card(
        "Drift Risk Flags",
        fmt_number(drift_flags),
        "Sensor drift indicators",
        "🧭"
    )


# ==========================================================
# Quality Band + Sensor Ranking
# ==========================================================
left, right = st.columns([1, 1.35], gap="large")


# ----------------------------------------------------------
# Data Quality Band Distribution
# ----------------------------------------------------------
with left:
    st.markdown(
        '<div class="section-title">🛡️ Data Quality Band Distribution</div>',
        unsafe_allow_html=True
    )

    band_df = (
        filtered_df
        .groupby("data_quality_band", as_index=False)["sensor_id"]
        .nunique()
        .rename(columns={"sensor_id": "sensor_count"})
    )

    fig_band = px.pie(
        band_df,
        names="data_quality_band",
        values="sensor_count",
        hole=0.58,
        color="data_quality_band",
        color_discrete_map={
            "Trusted": "#22C55E",
            "Monitor": "#FACC15",
            "At Risk": "#EF4444"
        }
    )

    fig_band.update_traces(
        textinfo="percent+label",
        textfont=dict(size=20, color="#E5F7FF"),
        marker=dict(
            line=dict(
                color="rgba(255,255,255,0.18)",
                width=1
            )
        ),
        hoverlabel=dict(
            font_size=20
        )
    )

    fig_band.update_layout(
        font=dict(size=20, color="#E5F7FF"),
        legend=dict(
            font=dict(size=20),
            title=dict(font=dict(size=20))
        )
    )

    st.plotly_chart(
        style_plotly(fig_band, height=430),
        use_container_width=True
    )


# ----------------------------------------------------------
# Lowest Data Quality Sensors
# ----------------------------------------------------------
with right:
    st.markdown(
        '<div class="section-title">📉 Lowest Data Quality Sensors</div>',
        unsafe_allow_html=True
    )

    worst_df = (
        filtered_df
        .sort_values(
            [
                "data_quality_score",
                "suspicious_readings_count",
                "drift_risk_flag_count"
            ],
            ascending=[True, False, False]
        )
        .head(20)
    )

    fig_worst = px.bar(
        worst_df,
        x="data_quality_score",
        y="station_name",
        color="data_quality_band",
        orientation="h",
        color_discrete_map={
            "Trusted": "#22C55E",
            "Monitor": "#FACC15",
            "At Risk": "#EF4444"
        },
        hover_data=[
            "governorate",
            "sensor_id",
            "suspicious_readings_count",
            "invalid_readings_count",
            "drift_risk_flag_count"
        ],
        labels={
            "data_quality_score": "Data Quality Score",
            "station_name": "Station",
            "data_quality_band": "Quality Band"
        }
    )

    fig_worst.update_layout(
        font=dict(size=20, color="#E5F7FF"),
        xaxis=dict(
            title_font=dict(size=22),
            tickfont=dict(size=19)
        ),
        yaxis=dict(
            title_font=dict(size=22),
            tickfont=dict(size=18)
        ),
        legend=dict(
            font=dict(size=20),
            title=dict(font=dict(size=20))
        )
    )

    fig_worst.update_traces(
        hoverlabel=dict(
            font_size=20
        )
    )

    st.plotly_chart(
        style_plotly(fig_worst, height=430),
        use_container_width=True
    )


# ==========================================================
# Reliability Breakdown
# ==========================================================
st.markdown(
    '<div class="section-title">📊 Reliability Signal Breakdown</div>',
    unsafe_allow_html=True
)

signal_df = pd.DataFrame({
    "Signal": [
        "Valid Readings",
        "Suspicious Readings",
        "Invalid Readings",
        "Partial Readings",
        "Duplicate Readings",
        "Drift Risk Flags"
    ],
    "Count": [
        valid_readings,
        suspicious_readings,
        invalid_readings,
        partial_readings,
        duplicate_readings,
        drift_flags
    ]
})

fig_signal = px.bar(
    signal_df,
    x="Signal",
    y="Count",
    color="Signal",
    color_discrete_map={
        "Valid Readings": "#22C55E",
        "Suspicious Readings": "#FACC15",
        "Invalid Readings": "#EF4444",
        "Partial Readings": "#38BDF8",
        "Duplicate Readings": "#FB923C",
        "Drift Risk Flags": "#A855F7"
    },
    labels={
        "Signal": "Signal",
        "Count": "Count"
    }
)

fig_signal.update_layout(
    xaxis_tickangle=-20,
    showlegend=False,
    font=dict(size=20, color="#E5F7FF"),
    xaxis=dict(
        title_font=dict(size=22),
        tickfont=dict(size=18)
    ),
    yaxis=dict(
        title_font=dict(size=22),
        tickfont=dict(size=19)
    )
)

fig_signal.update_traces(
    hoverlabel=dict(
        font_size=20
    )
)

st.plotly_chart(
    style_plotly(fig_signal, height=430),
    use_container_width=True
)


# ==========================================================
# Governorate Data Quality
# ==========================================================
st.markdown(
    '<div class="section-title">🏙️ Data Quality by Governorate</div>',
    unsafe_allow_html=True
)

gov_quality_df = (
    filtered_df
    .groupby("governorate", as_index=False)
    .agg(
        avg_data_quality_score=("data_quality_score", "mean"),
        suspicious_readings=("suspicious_readings_count", "sum"),
        invalid_readings=("invalid_readings_count", "sum"),
        drift_flags=("drift_risk_flag_count", "sum")
    )
    .sort_values("avg_data_quality_score", ascending=True)
)

fig_gov = px.bar(
    gov_quality_df,
    x="avg_data_quality_score",
    y="governorate",
    orientation="h",
    color="avg_data_quality_score",
    color_continuous_scale=["#EF4444", "#FACC15", "#22C55E"],
    hover_data=[
        "suspicious_readings",
        "invalid_readings",
        "drift_flags"
    ],
    labels={
        "avg_data_quality_score": "Avg Data Quality Score",
        "governorate": "Governorate"
    }
)

fig_gov.update_layout(
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
            text="Avg Score",
            font=dict(size=20)
        ),
        tickfont=dict(size=18)
    )
)

fig_gov.update_traces(
    hoverlabel=dict(
        font_size=20
    )
)

st.plotly_chart(
    style_plotly(fig_gov, height=520),
    use_container_width=True
)


# ==========================================================
# Detailed Table
# ==========================================================
st.markdown(
    '<div class="section-title">📋 Data Quality Details</div>',
    unsafe_allow_html=True
)

display_cols = [
    "summary_date",
    "station_name",
    "governorate",
    "sensor_id",
    "total_readings_count",
    "valid_readings_count",
    "invalid_readings_count",
    "partial_readings_count",
    "duplicate_readings_count",
    "suspicious_readings_count",
    "anomaly_count",
    "stale_signal_flag_count",
    "drift_risk_flag_count",
    "data_quality_score",
    "data_quality_band"
]

existing_cols = [
    c for c in display_cols
    if c in filtered_df.columns
]

st.dataframe(
    filtered_df[existing_cols]
    .sort_values(
        ["data_quality_score", "suspicious_readings_count"],
        ascending=[True, False]
    ),
    use_container_width=True,
    hide_index=True
)