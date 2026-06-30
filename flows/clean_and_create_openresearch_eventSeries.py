from time import time
from prefect import flow, get_run_logger
from tasks.mw_auth import login_and_get_csrf
from tasks.mw_api import (
    get_series_titles,
    get_page_wikitext,
    create_page,
    edit_page,
    delete_page,
)

from tasks.mw_helper import set_multiple_params_in_template
from tasks.utils import extract_event_fields_from_wikitext
from tasks.core import create_icore_conference_details_data
from typing import List, Dict, Optional
import os
import pandas as pd

# PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")

    
@flow(name="clean-openresearch-eventSeries", description="Clean and update OpenResearch event series pages using authoritative sources and optional LLM assistance.")
def clean_openresearch_eventSeries(
    api_url: str,
    username: str,
    password: str,
    df_core_details: pd.DataFrame,
    template_name: str = "Event series",
    dry_run: bool = True,
):
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    csrf_token, session = login_and_get_csrf(api_url, username, password)
    # 1. collect pages
    page_titles = get_series_titles(api_url, session, title="Category:Event series")
    series_acronyms = []
    logger.info("Found %s pages", len(page_titles))
    # 3. iterate series and create pages
    for page_title in page_titles:
        series_wikitext = get_page_wikitext(api_url, page_title, session)
        series_json = extract_event_fields_from_wikitext(series_wikitext)
        logger.debug(f"""Series Page {page_title}: Json: {series_json} series_wikitext: {series_wikitext}""")
        series_title = series_json.get('Title', None)
        series_acronym = series_json.get('Acronym', None)
        series_acronyms = series_acronyms.append(series_acronym)
        try:
            if series_acronym.lower() in df_core_details['Acronym'].str.lower().values:
                # extract the corresponding row from df_core_details
                row = df_core_details[df_core_details['Acronym'].str.lower() == series_acronym.lower()].iloc[0]
                # extract the fields from the row
                logger.debug(f"Found matching row in CORE_details for acronym {series_acronym}: {row}")
                title = row['Title']
                acronym = row['Acronym']
                field = row['Field']
                DblpSeries = row['DblpSeries']
                has_CORE2026_Rank = row['has CORE2026 Rank']
                parameter_to_set = {"Acronym": acronym, "Field": field, "Title": title, "DblpSeries": DblpSeries, "has CORE2026 Rank": has_CORE2026_Rank}
                new_wikitext, changed, old_tpl = set_multiple_params_in_template(series_wikitext, parameter_to_set, template_name, False)
                if series_acronym.strip() != page_title.strip():
                    logger.warning(f"series_title: {series_acronym} is not equal to page_title: {page_title}")
                    # delete page and recreate page with correct acronym
                    logger.debug(f"Deleting page {page_title} and recreating with correct acronym {series_acronym}")
                    delete_page(api_url, page_title, csrf_token, session)
                    # create page with new updated parameters
                    logger.debug(f"Creating page {series_acronym} with wikitext: {new_wikitext}")
                    create_page(api_url, acronym, new_wikitext, csrf_token, f"Create page with updated parameters{parameter_to_set}", session, dry_run)
                else:
                    # edit page with new updated parameters
                    logger.debug(f"Editing page {series_acronym} with wikitext: {new_wikitext}")
                    edit_page(api_url, series_acronym, new_wikitext, csrf_token, session, f"Edit page with updated parameters{parameter_to_set}", dry_run)
        except Exception as e:
            logger.error(f"Error processing series {series_title} with acronym {series_acronym}: {e}")
    
    # if  df_core_details['Acronym'].str.lower() is not in series_acronyms, 
    # create a new page with the details from df_core_details
    for index, row in df_core_details.iterrows():
        try:    
            acronym = row['Acronym']
            if acronym.lower() not in [s.lower() for s in series_acronyms]:
                logger.info(f"Creating new page for acronym {acronym} as it does not exist in OpenResearch")
                title = row['Title']
                field = row['Field']
                DblpSeries = row['DblpSeries']
                has_CORE2026_Rank = row['has CORE2026 Rank']
                parameter_to_set = {"Acronym": acronym, "Field": field, "Title": title, "DblpSeries": DblpSeries, "has CORE2026 Rank": has_CORE2026_Rank}
                new_wikitext, changed, old_tpl = set_multiple_params_in_template("", parameter_to_set, template_name, False)
                logger.debug(f"Creating page {acronym} with wikitext: {new_wikitext}")
                create_page(api_url, acronym, new_wikitext, csrf_token, f"Create page with parameters{parameter_to_set}", session, dry_run)
        except Exception as e:
            logger.error(f"Error creating new page for acronym {acronym}: {e}")

    return True

if __name__ == "__main__":
    # Example invocation using environment variables and local CSVs for core dicts
    import pandas as pd
    import numpy as np
    core_26_path = os.environ.get("CORE_2026_PATH", "CORE_2026.csv")
    df_core_26 = pd.read_csv(core_26_path, header=None)
    details_path = core_26_path.replace('.csv', '_details.csv')
    df_core_details = pd.read_csv(details_path)
    df_core_details = df_core_details.replace(np.nan, '', regex=True)  # Replace NaN with empty string

    API = os.environ.get("OR_API", "https://www.openresearch.org/mediawiki/api.php")
    USER = os.environ.get("OR_USER")
    PASS = os.environ.get("OR_PASS")
    clean_openresearch_eventSeries(API, USER, PASS,
                                      df_core_details,
                                      dry_run=False,
                                      template_name="Event series",
                                      llm_api_key=os.environ.get("OPENROUTER_API_KEY"))
    # create_openresearch_events_flow.serve(API, USER, PASS, core_26_dict, core_23_dict, TARGET_YEARS, dry_run=False, llm_api_key=os.environ.get("OPENROUTER_API_KEY"))