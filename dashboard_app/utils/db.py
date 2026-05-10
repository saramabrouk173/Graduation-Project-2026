import os
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine


def get_connection_string():
    db_user = os.getenv("EWIS_DB_USER", "sa")
    db_password = os.getenv("EWIS_DB_PASSWORD", "YourStrong@Password123")
    db_host = os.getenv("EWIS_DB_HOST", "localhost")
    db_port = os.getenv("EWIS_DB_PORT", "1433")
    db_name = os.getenv("EWIS_DB_NAME", "EWIS_Warehouse")

    password = quote_plus(db_password)
    return f"mssql+pymssql://{db_user}:{password}@{db_host}:{db_port}/{db_name}"


@st.cache_resource
def get_engine():
    return create_engine(get_connection_string())


@st.cache_data(ttl=30, show_spinner=False)
def run_query(query: str):
    engine = get_engine()
    return pd.read_sql(query, engine)


@st.cache_data(ttl=30, show_spinner=False)
def run_query_params(query: str, params: dict):
    engine = get_engine()
    return pd.read_sql(query, engine, params=params)