import streamlit as st
import pandas as pd
import plotly.express as px
import os
import requests
from io import StringIO
from report import data_quality_report

SAMPLE_EVENTSERIES_CSV = r"""Acronym,Title,Has CORE2018 Rank,Has CORE2017 Rank,Has CORE2014 Rank,Has CORE2013 Rank,Has CORE2010 Rank,Field,WikiDataId,DblpSeries,Homepage,Has CORE2008 Rank,Has CORE2026 Rank,Has CORE2023 Rank,Has CORE2021 Rank,Has CORE2020 Rank,Logo,Has Bibliography,Period,Unit,Has Proceedings Bibliography,Has CORE Rank,Has host organization,Series,Type,Has Twitter,Has SC member,has Corelation,DlpSeries,Organiser,has CORE2015 Rank,has CORE2016 Rank,Organizes,has publisher
3DIM,3-D Digital Imaging and Modelling,C,C,C,C,C,Artificial Intelligence and Image Processing,,,,,,,,,,,,,,,,,,,,,,,,,,
3DPVT,International Symposium on 3D Data Processing Visualization and Transmission,C,C,C,C,C,Artificial Intelligence and Image Processing,,,,,,,,,,,,,,,,,,,,,,,,,,
3DUI,IEEE Symposium on 3D User Interfaces,B,B,B,B,B,Artificial Intelligence and Image Processing,Q105456162,3dui,https://ieeevr.org/,,,,,,,,,,,,,,,,,,,,,,,
3G,International Conference on 3G Mobile Communication Technologies,C,C,C,C,C,Artificial Intelligence and Image Processing,,,,,,,,,,,,,,,,,,,,,,,,,,
3IA,International Conference on Computer Graphics and Artificial Intelligence,,,,,,Computer graphics,,,http://3ia.teiath.gr/,,,,,,,,,,,,,,,,,,,,,,,
3PGIC,"International Conference on P2P, Parallel, Grid, Cloud and Internet Computing",,,,,,Computer Science,,,,,,,,,,,,,,,,,,,,,,,,,,
4S4D,EAI Conference on SmartCities and Smart Solutions for Sustainable Development,,,,,,Smart Cities and Sustainable Development Technology,,,,,,,,,,,,,,,,,,,,,,,,,,
5GU,International Conference on 5G for Ubiquitous Connectivity,,,,,,Telecommunications,,,,,,,,,,,,,,,,,,,,,,,,,,
5GWN,5G for Future Wireless Networks,,,,,,Wireless Communications,,,,,,,,,,,,,,,,,,,,,,,,,,
AAA-IDEA,International Workshop on Advanced Architectures and Algorithms for Internet Delivery and Applications,C,C,C,C,C,Information Systems,,,,,,,,,,,,,,,,,,,,,,,,,,
"""


@st.cache_data
def load_df(csv_text: str) -> pd.DataFrame:
    df = pd.read_csv(StringIO(csv_text), index_col=False)
    df.columns = [column.strip() for column in df.columns]
    # Given columns must be first in order and then rest columns can follow
    first_place_columns = ["Acronym", "Title", "Field", "DblpSeries", "Has CORE Rank", "Has CORE2026 Rank", 
                           "Has CORE2023 Rank", "Has CORE2021 Rank", "Has CORE2020 Rank", "Has CORE2018 Rank", 
                           "Has CORE2017 Rank", "Has CORE2016 Rank", "Has CORE2015 Rank", "Has CORE2014 Rank", 
                           "Has CORE2013 Rank", "Has CORE2010 Rank", "Has CORE2008 Rank"]
    df = df[[col for col in first_place_columns if col in df.columns] + [col for col in df.columns if col not in first_place_columns]]
    for column in ["Period"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
   
    return df


@st.cache_data
def to_csv_bytes(data: pd.DataFrame) -> bytes:
	return data.reset_index().to_csv(index=False).encode("utf-8")


def render_preview(data: pd.DataFrame):
	st.header("Event Series Data Preview")
	st.dataframe(data.head(10))

	if data.empty or len(data.columns) == 0:
		st.info("The uploaded CSV does not contain previewable columns.")
		return

	column = st.selectbox("Column to visualize", options=data.columns, key="series_preview_column")
	figure = px.histogram(data, x=column, title=f"Event series by {column}")
	st.plotly_chart(figure, use_container_width=True)


def render_report(data: pd.DataFrame):
	st.header("Event Series Data Quality Report")
	report = data_quality_report(data)

	st.subheader("Column level summary")
	report_display = report.copy()
	for column in report_display.select_dtypes(include="object").columns:
		report_display[column] = report_display[column].map(str)
	st.dataframe(report_display)

	st.download_button(
		"Download data quality report CSV",
		data=to_csv_bytes(report),
		file_name="event_series_data_quality_report.csv",
		mime="text/csv",
		key="series_download_report",
	)

	st.subheader("Missingness heatmap (sample rows)")
	sample = data.sample(min(200, len(data)), random_state=1) if len(data) > 0 else data
	missing = sample.isna().astype(int)
	figure = px.imshow(
		missing.T,
		labels={"x": "row index (sample)", "y": "column", "color": "missing"},
		color_continuous_scale=["#ffffff", "#d62728"],
		aspect="auto",
	)
	st.plotly_chart(figure, use_container_width=True)

	col1, col2 = st.columns(2)
	with col1:
		st.subheader("Columns with most missing values")
		st.table(report.sort_values("missing_pct", ascending=False).head(10)[["missing_count", "missing_pct"]])
	with col2:
		st.subheader("Columns with highest cardinality")
		st.table(report.sort_values("unique_count", ascending=False).head(10)[["unique_count", "unique_pct"]])


def render_filters(data: pd.DataFrame):
	st.header("Event Series Filters")

	filter_columns = ["Field", "Type", "Organiser", 
                      "Has CORE Rank", "Has CORE2026 Rank", "Has CORE2023 Rank",
                      "Period", "Unit", "Series"
                     ]
	selections = {}
	for column in filter_columns:
		if column in data.columns:
			options = sorted(data[column].dropna().astype(str).unique().tolist())
			selections[column] = st.sidebar.multiselect(
				column,
				options=options,
				default=options,
				key=f"series_filter_{column.lower()}",
			)

	search_text = st.sidebar.text_input("Search title or acronym", key="series_filter_search")
	filtered = data.copy()
	for column, values in selections.items():
		if values:
			filtered = filtered[filtered[column].astype(str).isin(values)]

	if search_text:
		masks = []
		if "Title" in filtered.columns:
			masks.append(filtered["Title"].astype(str).str.contains(search_text, case=False, na=False))
		if filtered.index.name == "Acronym":
			masks.append(filtered.index.astype(str).str.contains(search_text, case=False, na=False))
		if masks:
			combined_mask = masks[0]
			for mask in masks[1:]:
				combined_mask = combined_mask | mask
			filtered = filtered[combined_mask]

	col1, col2, col3 = st.columns(3)
	col1.metric("Total series", len(filtered))
	col2.metric("Unique fields", filtered["Field"].nunique() if "Field" in filtered.columns else 0)
	ranked = filtered["Has CORE Rank"].notna().sum() if "Has CORE Rank" in filtered.columns else 0
	col3.metric("CORE ranked series", int(ranked))

	st.subheader("Series by Field")
	if "Field" in filtered.columns:
		figure = px.histogram(filtered, x="Field", color="Type" if "Type" in filtered.columns else None, height=400)
		st.plotly_chart(figure, use_container_width=True)
	else:
		st.info("No Field column available.")

	rank_columns = [column for column in filtered.columns if column.startswith("Has CORE")]
	if rank_columns:
		st.subheader("CORE ranking coverage")
		rank_counts = filtered[rank_columns].notna().sum().sort_values(ascending=False).rename("Series")
		figure = px.bar(rank_counts, x=rank_counts.index, y="Series", height=350)
		st.plotly_chart(figure, use_container_width=True)

	st.subheader("Filtered Event Series")
	st.dataframe(filtered.reset_index())
	st.download_button(
		"Download filtered event-series CSV",
		data=to_csv_bytes(filtered),
		file_name="filtered_event_series.csv",
		mime="text/csv",
		key="series_download_filtered",
	)
