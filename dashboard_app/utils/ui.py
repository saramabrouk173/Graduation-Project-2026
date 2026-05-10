from pathlib import Path

import plotly.io as pio
import streamlit as st


STATUS_COLORS = {
    "green": "#22C55E",
    "yellow": "#FACC15",
    "orange": "#FB923C",
    "red": "#EF4444",
    "gray": "#94A3B8",
}

STATUS_RGB = {
    "green": [34, 197, 94],
    "yellow": [250, 204, 21],
    "orange": [251, 146, 60],
    "red": [239, 68, 68],
    "gray": [148, 163, 184],
}


def init_page(title="EWIS Dashboard"):
    st.set_page_config(
        page_title=title,
        page_icon="💧",
        layout="wide"
    )
    load_css()
    apply_plotly_theme()


def load_css():
    base_dir = Path(__file__).resolve().parents[1]
    css_path = base_dir / "assets" / "styles.css"
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


def apply_plotly_theme():
    pio.templates["ewis_dark"] = pio.templates["plotly_dark"]
    pio.templates.default = "ewis_dark"


def page_header(title, subtitle):
    st.markdown(f"""
    <div class="ewis-title">{title}</div>
    <div class="ewis-subtitle">{subtitle}</div>
    """, unsafe_allow_html=True)


def glass_container(html):
    import textwrap

    clean_html = textwrap.dedent(html).strip()

    st.markdown(f"""
<div class="glass-card">
    {clean_html}
</div>
""", unsafe_allow_html=True)


def kpi_card(label, value, hint="", icon="💧"):
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{icon} {label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-hint">{hint}</div>
    </div>
    """, unsafe_allow_html=True)


def status_badge(color):
    c = str(color).lower()
    labels = {
        "green": "🟢 Stable",
        "yellow": "🟡 Monitor",
        "orange": "🟠 Risk",
        "red": "🔴 Critical",
        "gray": "⚪ No Data",
    }
    return f'<span class="status-badge status-{c}">{labels.get(c, "Unknown")}</span>'


def style_plotly(fig, height=None):
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#E5F7FF"),
        margin=dict(l=20, r=20, t=45, b=25),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(color="#CFEFFF")
        )
    )
    if height:
        fig.update_layout(height=height)
    return fig