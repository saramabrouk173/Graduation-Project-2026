import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from utils.db import run_query
from utils.queries import PARAMETER_TREND_QUERY, STATION_DAILY_QUERY
from utils.formatters import fmt_number
from utils.ui import init_page, page_header, kpi_card, style_plotly


# ==========================================================
# Page Config
# ==========================================================
init_page("EWIS | Water Quality Trends")
st_autorefresh(interval=30000, key="water_quality_trends_refresh")


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
    "🧪 Water Quality Trends",
    "Interactive analysis of turbidity, chlorine, pH, breaches, and station WQI history."
)


# ==========================================================
# Load Data
# ==========================================================
trend_df = run_query(PARAMETER_TREND_QUERY)
daily_df = run_query(STATION_DAILY_QUERY)

if trend_df.empty:
    st.error("No parameter trend data available.")
    st.stop()


# ==========================================================
# Sidebar Filters
# ==========================================================
st.sidebar.markdown("## 🔎 Filters")

available_parameters = sorted(
    trend_df["parameter_name"].dropna().unique().tolist()
)

available_governorates = ["All"] + sorted(
    trend_df["governorate"].dropna().unique().tolist()
)

default_parameter = (
    "Final Turbidity"
    if "Final Turbidity" in available_parameters
    else available_parameters[0]
)

selected_parameter = st.sidebar.selectbox(
    "Parameter",
    available_parameters,
    index=available_parameters.index(default_parameter)
)

selected_governorate = st.sidebar.selectbox(
    "Governorate",
    available_governorates
)

filtered_df = trend_df[
    trend_df["parameter_name"] == selected_parameter
].copy()

if selected_governorate != "All":
    filtered_df = filtered_df[
        filtered_df["governorate"] == selected_governorate
    ]

if filtered_df.empty:
    st.warning("No records match the selected filters.")
    st.stop()


# ==========================================================
# KPI Cards
# ==========================================================
avg_value = filtered_df["avg_value"].mean()
min_value = filtered_df["min_value"].min()
max_value = filtered_df["max_value"].max()
total_breaches = filtered_df["breach_count"].sum()

affected_governorates = filtered_df.loc[
    filtered_df["breach_count"] > 0,
    "governorate"
].nunique()

c1, c2, c3, c4, c5 = st.columns(5, gap="large")

with c1:
    kpi_card(
        "Average Value",
        fmt_number(avg_value, 3),
        selected_parameter,
        "📈"
    )

with c2:
    kpi_card(
        "Minimum Value",
        fmt_number(min_value, 3),
        "Lowest observed value",
        "🔽"
    )

with c3:
    kpi_card(
        "Maximum Value",
        fmt_number(max_value, 3),
        "Highest observed value",
        "🔼"
    )

with c4:
    kpi_card(
        "Total Breaches",
        fmt_number(total_breaches),
        "Across selected scope",
        "⚠️"
    )

with c5:
    kpi_card(
        "Affected Governorates",
        fmt_number(affected_governorates),
        "Governorates with breaches",
        "🏙️"
    )


# ==========================================================
# Main Trend Section
# ==========================================================
left, right = st.columns([1.45, 1], gap="large")


# ----------------------------------------------------------
# Parameter Trend by Governorate
# ----------------------------------------------------------
with left:
    st.markdown(
        '<div class="section-title">📈 Parameter Trend by Governorate</div>',
        unsafe_allow_html=True
    )

    trend_plot_df = filtered_df.sort_values(
        ["trend_date", "time_bucket_hour"]
    )

    fig = px.line(
        trend_plot_df,
        x="time_bucket_hour",
        y="avg_value",
        color="governorate",
        markers=True,
        hover_data=[
            "trend_date",
            "governorate",
            "parameter_name",
            "avg_value",
            "min_value",
            "max_value",
            "breach_count"
        ],
        labels={
            "time_bucket_hour": "Hour",
            "avg_value": "Average Value",
            "governorate": "Governorate"
        }
    )

    fig.update_traces(
        line=dict(width=3),
        marker=dict(size=7),
        hoverlabel=dict(
            font_size=20
        )
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

    st.plotly_chart(
        style_plotly(fig, height=470),
        use_container_width=True
    )


# ----------------------------------------------------------
# Breach Load by Governorate
# ----------------------------------------------------------
with right:
    st.markdown(
        '<div class="section-title">🚨 Breach Load by Governorate</div>',
        unsafe_allow_html=True
    )

    breach_by_gov = (
        filtered_df
        .groupby("governorate", as_index=False)["breach_count"]
        .sum()
        .sort_values("breach_count", ascending=True)
    )

    fig_breach = px.bar(
        breach_by_gov,
        x="breach_count",
        y="governorate",
        orientation="h",
        color="breach_count",
        color_continuous_scale=["#22C55E", "#FACC15", "#FB923C", "#EF4444"],
        labels={
            "breach_count": "Breach Count",
            "governorate": "Governorate"
        }
    )

    fig_breach.update_layout(
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
                text="Breaches",
                font=dict(size=20)
            ),
            tickfont=dict(size=18)
        )
    )

    fig_breach.update_traces(
        hoverlabel=dict(
            font_size=20
        )
    )

    st.plotly_chart(
        style_plotly(fig_breach, height=470),
        use_container_width=True
    )


# ==========================================================
# Heatmap Section
# ==========================================================
st.markdown(
    '<div class="section-title">🔥 Breach Heatmap Across Parameters</div>',
    unsafe_allow_html=True
)

heatmap_df = (
    trend_df
    .groupby(["governorate", "parameter_name"], as_index=False)["breach_count"]
    .sum()
)

fig_heatmap = px.density_heatmap(
    heatmap_df,
    x="parameter_name",
    y="governorate",
    z="breach_count",
    color_continuous_scale=["#061827", "#22D3EE", "#FACC15", "#FB923C", "#EF4444"],
    labels={
        "parameter_name": "Parameter",
        "governorate": "Governorate",
        "breach_count": "Breaches"
    }
)

fig_heatmap.update_layout(
    xaxis_tickangle=-35,
    font=dict(size=20, color="#E5F7FF"),
    xaxis=dict(
        title_font=dict(size=22),
        tickfont=dict(size=18)
    ),
    yaxis=dict(
        title_font=dict(size=22),
        tickfont=dict(size=19)
    ),
    coloraxis_colorbar=dict(
        title=dict(
            text="Breaches",
            font=dict(size=20)
        ),
        tickfont=dict(size=18)
    )
)

fig_heatmap.update_traces(
    hoverlabel=dict(
        font_size=20
    )
)

st.plotly_chart(
    style_plotly(fig_heatmap, height=560),
    use_container_width=True
)


# ==========================================================
# Station Daily WQI Section
# ==========================================================
st.markdown(
    '<div class="section-title">🏭 Station Daily WQI Distribution</div>',
    unsafe_allow_html=True
)

if daily_df.empty:
    st.info("No station daily snapshot data available.")
else:
    daily_filtered = daily_df.copy()

    if selected_governorate != "All":
        daily_filtered = daily_filtered[
            daily_filtered["governorate"] == selected_governorate
        ]

    # Robust WQI column detection
    if "avg_wqi" in daily_filtered.columns:
        wqi_column = "avg_wqi"
    elif "latest_wqi_score" in daily_filtered.columns:
        wqi_column = "latest_wqi_score"
    elif "daily_wqi" in daily_filtered.columns:
        wqi_column = "daily_wqi"
    else:
        st.warning(
            "No WQI column found in station daily snapshot. Expected one of: avg_wqi, latest_wqi_score, daily_wqi."
        )
        st.stop()

    fig_wqi = px.box(
        daily_filtered,
        x="governorate",
        y=wqi_column,
        color="snapshot_status_color",
        points="all",
        color_discrete_map={
            "green": "#22C55E",
            "yellow": "#FACC15",
            "orange": "#FB923C",
            "red": "#EF4444",
            "gray": "#94A3B8"
        },
        labels={
            "governorate": "Governorate",
            wqi_column: "Average WQI",
            "snapshot_status_color": "Daily Status"
        }
    )

    fig_wqi.update_layout(
        xaxis_tickangle=-45,
        font=dict(size=20, color="#E5F7FF"),
        xaxis=dict(
            title_font=dict(size=22),
            tickfont=dict(size=18)
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

    fig_wqi.update_traces(
        hoverlabel=dict(
            font_size=20
        )
    )

    st.plotly_chart(
        style_plotly(fig_wqi, height=520),
        use_container_width=True
    )