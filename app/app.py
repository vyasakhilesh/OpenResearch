import streamlit as st
import pandas as pd
import plotly.express as px
import os
import requests

st.title("CSV Explorer")

uploaded = st.file_uploader("Upload CSV", type=["csv"])
if uploaded is not None:
    df = pd.read_csv(uploaded)
    st.dataframe(df.head())
    numeric_cols = df.select_dtypes("number").columns.tolist()
    if numeric_cols:
        col = st.selectbox("Y axis", numeric_cols)
        fig = px.histogram(df, x=col)
        st.plotly_chart(fig, use_container_width=True)

# Example: calling Prefect API
prefect_api = os.getenv("PREFECT_API_URL")
if prefect_api:
    st.markdown(f"Prefect API: {prefect_api}")
    try:
        r = requests.get(f"{prefect_api}/health", timeout=2)
        st.write("Prefect health:", r.status_code)
    except Exception as e:
        st.write("Prefect not reachable:", e)