import streamlit as st
import os
import requests
import event
import eventSeries


st.set_page_config(page_title="OpenResearch Explorer", layout="wide")


def event_page():
    st.title("Event CSV Explorer")
    st.markdown("Upload a CSV or use the sample dataset to explore events, filters, and visualizations.")
    page = st.sidebar.radio("Navigate", ["Preview", "Report", "Filters"], key="app_page")

    uploaded_file = st.file_uploader("Upload CSV file", type=["csv"], key="event_upload")
    if uploaded_file is not None:
        raw = uploaded_file.getvalue().decode("utf-8")
    else:
        raw = event.SAMPLE_CSV
        st.info("Using embedded sample CSV. Upload your CSV to analyze your own data.")

    df = event.load_df(raw)
    if page == "Preview":
        event.render_preview(df)
    elif page == "Report":
        event.render_report(df)
    else:
        event.render_filters(df)


def eventSeries_page():
    st.title("Event Series Explorer")
    st.markdown("Upload an event-series CSV or use the sample dataset to explore series, rankings, and metadata.")

    uploaded_file = st.file_uploader("Upload event-series CSV", type=["csv"], key="event_series_upload")
    if uploaded_file is not None:
        raw = uploaded_file.getvalue().decode("utf-8")
    else:
        raw = eventSeries.SAMPLE_EVENTSERIES_CSV
        st.info("Using embedded sample event-series CSV. Upload your CSV to analyze your own data.")
    
    df = eventSeries.load_df(raw)
    page = st.sidebar.radio("Navigate", ["Preview", "Report", "Filters"], key="event_series_page")

    if page == "Preview":
        eventSeries.render_preview(df)
    elif page == "Report":
        eventSeries.render_report(df)
    else:
        eventSeries.render_filters(df)


if __name__ == "__main__":
    dataset = st.sidebar.radio(
        "Dataset",
        ["Events", "Event Series"],
        key="dataset_page",
    )

    if dataset == "Events":
        event_page()
    else:
        eventSeries_page()

    st.markdown("**Notes**")
    st.markdown(
        "- For large CSVs, consider pre-aggregating or using DuckDB/Polars for performance."
    )
    prefect_api = os.getenv("PREFECT_API_URL")
    prefect_server_ui_api = os.getenv("PREFECT_SERVER_UI_API_URL")
    if prefect_api:
        st.markdown(f"Prefect API: {prefect_api}")
        # st.markdown(f"Prefect Server UI API: {prefect_server_ui_api}")
        try:
            r = requests.get(f"{prefect_api}/health", timeout=2)
            st.write("Prefect health:", r.status_code)
            # r1 = requests.get(f"{prefect_server_ui_api}/health", timeout=2)
            # st.write("Prefect Server UI health:", r1.status_code)
        except Exception as e:
            st.write("Prefect not reachable:", e)