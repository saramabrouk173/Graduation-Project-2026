import pandas as pd


def fmt_number(x, decimals=0):
    if x is None or pd.isna(x):
        return "-"
    try:
        return f"{float(x):,.{decimals}f}"
    except Exception:
        return str(x)


def fmt_percent(x, decimals=1):
    if x is None or pd.isna(x):
        return "-"
    try:
        return f"{float(x):.{decimals}f}%"
    except Exception:
        return str(x)


def fmt_text(x):
    if x is None or pd.isna(x):
        return "-"
    return str(x)


def status_label(color):
    mapping = {
        "green": "Stable",
        "yellow": "Monitor",
        "orange": "Risk",
        "red": "Critical",
        "gray": "No Data"
    }
    return mapping.get(str(color).lower(), "Unknown")