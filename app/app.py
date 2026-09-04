import streamlit as st
import pandas as pd
import plotly.express as px
import os
import requests
from io import StringIO
from report import data_quality_report

st.set_page_config(page_title="Event CSV Explorer", layout="wide")
st.title("Event CSV Explorer")
st.markdown("Upload a CSV or use the sample dataset to explore events, filters, and visualizations.")

# --- Embedded sample CSV ---
SAMPLE_CSV = r"""Index,Acronym,Title,Type,Field,Start date,End date,Submission deadline,Homepage,Event mode,City,Country,Series,Ordinal,Abstract deadline,Notification,Camera ready,Has host organization,Has general chair,Has program chair,State,Submitted papers,Accepted papers,Submitting link,Attendance fee currency,Registration link,Accepted short papers,Has Proceedings Link,Logo,Paper deadline,Demo deadline,Workshop deadline,Tutorial deadline,Has coordinator,Has workshop chair,Has tutorial chair,Has demo chair,Has Keynote speaker,On site regular,Early bird regular,On site student,Early bird student,Twitter account,Has Proceedings DOI,General chair,Program chair,Has PC member,Poster deadline,Has OC member,Attendees,Tracks,Has Recording Link,general chair,program chair,Has Proceedings Bibliography,isA,hasAVPortalLink,has Twitter
0,ICSSH 2017,25th International Conference on Social Science and Humanities,Conference,humanities,2017-11-07,2017-11-08,2017-11-05,https://gahssr.org/25th-international-conference-on-social-science-and-humanities-icssh-07-08-nov-2017-singapore-about-46,InPerson,Singapore,Singapore,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
1,ICTEL 2017,"25th International Conference on Teaching, Education & Learning",Conference,education,2017-10-10,2017-10-11,2017-10-09,https://adtelweb.org/25th-international-conference-on-teaching-education-and-learning-ictel-10-11-oct-2017-dubai-uae-about-42,InPerson,Dubai,United Arab Emirates,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
2,ICSSH 2017,26th International Conference on Social Science & Humanities,Conference,humanities,2017-11-14,2017-11-15,2017-11-12,https://gahssr.org/26th-international-conference-on-social-science-and-humanities-icssh-14-15-nov-2017-kuala-lumpur-about-47,InPerson,Kuala Lumpur,Malaysia,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
3,ICTEL 2017,"26th International Conference on Teaching, Education and Learning",Conference,education,2017-11-08,2017-11-09,2017-11-06,https://adtelweb.org/26th-international-conference-on-teaching-education-and-learning-ictel-08-09-nov-2017-singapore-about-43,,Singapore,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
4,ICSSH 2017,27th International Conference on Social Science and Humanities,Conference,humanities,2017-12-19,2017-12-20,2017-12-17,https://gahssr.org/27th-international-conference-on-social-science-and-humanities-icssh-19-20-dec-2017-dubai-about-48,InPerson,Dubai,United Arab Emirates,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
5,ICTEL 2017,"27th ICTEL  2017 : 27th International Conference on Teaching, Education and Learning (ICTEL)",Workshop,education,2017-11-15,2017-11-16,2017-11-13,https://adtelweb.org/27th-international-conference-on-teaching-education-and-learning-ictel-15-16-nov-2017-kuala-lumpur-about-44,InPerson,Kuala Lumpur,Malaysia,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
6,3DUI 2016,IEEE Symposium on 3D User Interfaces,Conference,Computer Science,2016-03-19,2016-03-20,,,InPerson,Greenville,USA,3DUI,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
7,3DUI 2020,IEEE Symposium on 3D User Interfaces,Symposium,Computer Science,2020-03-22,2020-03-26,2019-09-03,http://ieeevr.org/2020/,InPerson,Atlanta,USA,3DUI,15,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
8,3DUI 2026,IEEE VR 2026: The 33rd IEEE Conference on Virtual Reality and 3D User Interfaces,Conference,Virtual Reality,2026-03-21,2026-03-25,2025-09-12,https://ieeevr.org/2026/,InPerson,Daegu,Republic of Korea,3DUI,33,2025-09-05,2026-01-23,2026-01-30,IEEE,"Gerard J. Kim, JungHyun Han, Soon Ki Jung","Lonni Besançon, Bobby Bodenheimer, Daisuke Iwai, Shohei Mori, Tabitha Peck, Rick Skarbez",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
9,3IA 2009,12th International Conference on Computer Graphics and Artificial Intelligence,Conference,Computer graphics,2009-05-29,2009-05-30,2009-03-27,http://3ia.teiath.gr,InPerson,Athens,Greece,3IA,12,,2009-03-20,2009-04-15,TEI of Athens,Dimitri PLEMENOS,International Program Committee,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
"""

uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])
if uploaded_file is not None:
    raw = uploaded_file.read().decode("utf-8")
else:
    raw = SAMPLE_CSV
    st.info("Using embedded sample CSV. Upload your CSV to analyze your own data.")

@st.cache_data
def load_df(csv_text: str) -> pd.DataFrame:
    df = pd.read_csv(StringIO(csv_text), index_col=0)
    # Normalize column names
    df.columns = [c.strip() for c in df.columns]
    # Parse date columns if present
    for col in ["Start date", "End date", "Submission deadline", "Abstract deadline", "Paper deadline"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    # Convert numeric columns
    for col in ["Submitted papers", "Accepted papers", "Attendees", "Ordinal"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# load the DataFrame from the uploaded CSV or sample CSV
df = load_df(raw)

page = st.sidebar.radio("Navigate", ["Preview", "Report", "Filters"], key="app_page")


def render_preview(data: pd.DataFrame):
    st.header("Data Preview")
    st.dataframe(data.head(10))
    col = st.selectbox("X axis", options=data.columns, index=0, key="preview_x_axis")
    fig = px.histogram(data, x=col)
    st.plotly_chart(fig, use_container_width=True)


def render_report(data: pd.DataFrame):
    st.header("Data Quality Report")
    report = data_quality_report(data)

    st.subheader("Column level summary")
    report_display = report.copy()
    for column in report_display.select_dtypes(include="object").columns:
        report_display[column] = report_display[column].map(str)
    st.dataframe(report_display)

    csv_bytes = report.reset_index().to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download data quality report CSV",
        data=csv_bytes,
        file_name="data_quality_report.csv",
        mime="text/csv",
        key="download_report",
    )

    st.subheader("Missingness heatmap (sample rows)")
    sample = data.sample(min(200, len(data)), random_state=1) if len(data) > 0 else data
    miss = sample.isna().astype(int)
    fig = px.imshow(
        miss.T,
        labels=dict(x="row index (sample)", y="column", color="missing"),
        color_continuous_scale=["#ffffff", "#d62728"],
        aspect="auto",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Columns with most missing values")
    top_missing = report.sort_values("missing_pct", ascending=False).head(10)
    st.table(top_missing[["missing_count", "missing_pct"]])

    st.subheader("Columns with highest cardinality")
    top_card = report.sort_values("unique_count", ascending=False).head(10)
    st.table(top_card[["unique_count", "unique_pct"]])


@st.cache_data
def to_csv_bytes(data: pd.DataFrame) -> bytes:
    return data.to_csv(index=False).encode("utf-8")


def render_filters(data: pd.DataFrame):
    st.header("Filters")
    types = data["Type"].dropna().unique().tolist() if "Type" in data.columns else []
    fields = data["Field"].dropna().unique().tolist() if "Field" in data.columns else []
    cities = data["City"].dropna().unique().tolist() if "City" in data.columns else []
    countries = data["Country"].dropna().unique().tolist() if "Country" in data.columns else []
    series = data["Series"].dropna().unique().tolist() if "Series" in data.columns else []

    sel_type = st.sidebar.multiselect("Type", options=types, default=types, key="filter_type")
    sel_field = st.sidebar.multiselect("Field", options=fields, default=fields, key="filter_field")
    sel_city = st.sidebar.multiselect("City", options=cities, default=cities, key="filter_city")
    sel_country = st.sidebar.multiselect("Country", options=countries, default=countries, key="filter_country")
    sel_series = st.sidebar.multiselect("Series", options=series, default=series, key="filter_series")

    if "Start date" in data.columns:
        years = data["Start date"].dt.year.dropna().astype(int)
        if not years.empty:
            min_year, max_year = int(years.min()), int(years.max())
            yr_range = st.sidebar.slider("Start year range", min_year, max_year, (min_year, max_year), key="filter_year")
        else:
            yr_range = (None, None)
    else:
        yr_range = (None, None)

    search_text = st.sidebar.text_input("Search Title", key="filter_title")
    filtered = data.copy()
    for column, values in {
        "Type": sel_type,
        "Field": sel_field,
        "City": sel_city,
        "Country": sel_country,
        "Series": sel_series,
    }.items():
        if values and column in filtered.columns:
            filtered = filtered[filtered[column].isin(values)]
    if yr_range[0] is not None and "Start date" in filtered.columns:
        filtered = filtered[
            (filtered["Start date"].dt.year >= yr_range[0])
            & (filtered["Start date"].dt.year <= yr_range[1])
        ]
    if search_text and "Title" in filtered.columns:
        filtered = filtered[filtered["Title"].str.contains(search_text, case=False, na=False)]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total events", len(filtered))
    col2.metric("Unique series", filtered["Series"].nunique() if "Series" in filtered.columns else 0)
    top_city = filtered["City"].mode().iloc[0] if "City" in filtered.columns and not filtered["City"].dropna().empty else "—"
    col3.metric("Top city", top_city)
    if "Submitted papers" in filtered.columns and "Accepted papers" in filtered.columns:
        total_sub = filtered["Submitted papers"].sum(min_count=1)
        total_acc = filtered["Accepted papers"].sum(min_count=1)
        acc_rate = f"{(total_acc / total_sub * 100):.1f}%" if pd.notna(total_sub) and total_sub > 0 else "—"
    else:
        acc_rate = "—"
    col4.metric("Acceptance rate", acc_rate)

    st.subheader("Timeline of events")
    if "Start date" in filtered.columns:
        timeline = filtered.sort_values("Start date")
        fig_t = px.scatter(
            timeline,
            x="Start date",
            y="Title",
            color="Field" if "Field" in timeline.columns else None,
            hover_data=[c for c in ["City", "Country", "Type"] if c in timeline.columns],
            height=400,
        )
        st.plotly_chart(fig_t, use_container_width=True)
    else:
        st.info("No Start date column available for timeline.")

    st.subheader("Events by Field")
    if "Field" in filtered.columns:
        fig_bar = px.histogram(filtered, x="Field", color="Type" if "Type" in filtered.columns else None, height=400)
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("No Field column available.")

    st.subheader("Submitted and Accepted Papers Distribution")
    paper_columns = [c for c in ["Submitted papers", "Accepted papers"] if c in filtered.columns]
    if paper_columns:
        df_long = filtered.melt(
            id_vars=["Title"] if "Title" in filtered.columns else None,
            value_vars=paper_columns,
            var_name="Metric",
            value_name="Count",
        ).dropna(subset=["Count"])
        if not df_long.empty:
            fig_hist = px.histogram(df_long, x="Count", color="Metric", barmode="overlay", height=350)
            st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.info("No numeric paper counts available.")
    else:
        st.info("Submitted/Accepted columns not present.")

    st.subheader("Filtered Data")
    st.dataframe(filtered.reset_index(drop=True))
    csv_bytes = to_csv_bytes(filtered.reset_index(drop=True))
    st.download_button(
        "Download filtered CSV",
        data=csv_bytes,
        file_name="filtered_events.csv",
        mime="text/csv",
        key="download_filtered",
    )


if page == "Preview":
    render_preview(df)
elif page == "Report":
    render_report(df)
else:
    render_filters(df)

st.markdown("**Notes**")
st.markdown(
    "- For large CSVs, consider pre-aggregating or using DuckDB/Polars for performance."
)

# Example: calling Prefect API
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