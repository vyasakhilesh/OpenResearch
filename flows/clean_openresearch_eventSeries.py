from time import time
from prefect import flow, get_run_logger
from tasks.mw_auth import login_and_get_csrf
from tasks.mw_api import (
    get_eventSeries_pages,
    get_series_titles,
    get_page_wikitext,
    get_eventSeries_pages,
    create_page,
    edit_page,
    delete_page,
)

from tasks.mw_helper import find_template_block, set_multiple_params_in_template, extract_fields_from_template
from tasks.utils import extract_event_fields_from_wikitext
from tasks.core import create_icore_conference_details_data
from typing import List, Dict, Optional
import os
import pandas as pd

PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")


def build_event_fields_extraction_prompt(
    title: str,
    acronym: Optional[str],
    target_year: Optional[int],
    fields: str
) -> str:
    """
    Build a single-string prompt for an LLM that requests extraction of the given fields.
    - title: primary name of the entity (e.g., conference title)
    - acronym: disambiguating identifiers
    - target_year: integer year or None
    - fields: comma-separated list of fields with types, e.g. "Year:int, City:str, State:str, Country:str, Confidence:str"
    """
    
    prompt = f"""You are an information-extraction agent. Find the requested fields for the target entity using authoritative sources and return a single JSON object containing only the reliably determined keys.

Task::
  - Title: "{title}"
  - Acronym: "{acronym}"
  - Target year: {target_year}
  - Fields to extract: "{fields}"

Scope requirements::
  1. Search sources in this order of preference: official site, primary publication/proceedings pages (publisher pages, DBLP), then community trackers and aggregators (e.g., OpenAccept, conference trackers).
  2. Prefer primary sources and authoritative records; use secondary sources only when primary sources are unavailable or ambiguous.
  3. If multiple sources disagree, choose the value with the best primary-source support and reflect that choice only via the Confidence field.

Output requirements::
  1. Produce exactly one valid JSON object and nothing else.
  2. The JSON object must contain only the fields listed in the Fields to extract string and no other keys. Include only those keys you can reliably determine; represent missing or indeterminate values as null.
  3. Type rules:
     - int -> integer or null
     - str -> string or null
     - bool -> boolean or null
     - Confidence (if present) -> string, one of "high", "medium", "low"
     - Sources (if included) -> array of strings (URLs) or null
  4. Do not include any explanatory text, metadata, comments, or surrounding code fences before, inside, or after the JSON object.

Formatting rules::
  1. The output must be valid JSON using double quotes for keys and string values.
  2. Use the exact key names provided in the Fields to extract string (case-sensitive).
  3. Represent missing or indeterminate values as null.
  4. If a field is numeric, do not quote the number.
  5. Confidence must reflect source reliability: "high" for primary-source confirmation, "medium" for consistent secondary-source support, "low" for single or conflicting secondary sources.
  6. Prefer primary sources (official site, proceedings, DBLP, OpenAccept) and include those URLs in a "Sources" array if possible.
  7. Produce only the JSON object and nothing else; do not include any additional text, explanation, or formatting outside the JSON.
Begin by locating the most authoritative sources for the entity and identifiers for the target year, then output exactly one JSON object with only the keys specified in the Fields to extract string and values typed as required.
"""
    return prompt


    
@flow(name="clean-openresearch-eventSeries", description="Clean and update OpenResearch event series pages using authoritative sources and optional LLM assistance.")
def clean_openresearch_eventSeries(
    api_url: str,
    username: str,
    password: str,
    df_core_23: pd.DataFrame,
    df_core_26: pd.DataFrame,
    df_core_26_details: pd.DataFrame,
    template_name: str = "Event series",
    dry_run: bool = True,
    llm_api_key: Optional[str] = None,
):
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    csrf_token, session = login_and_get_csrf(api_url, username, password)
    # 1. collect pages
    page_titles = get_series_titles(api_url, session, title="Category:Event series")
    logger.info("Found %s pages", len(page_titles))
    # 3. iterate series and create pages
    for page_title in page_titles:
        series_wikitext = get_page_wikitext(api_url, page_title, session)
        series_json = extract_event_fields_from_wikitext(series_wikitext)
        logger.debug(f"""Series Page {page_title}: Json: {series_json} series_wikitext: {series_wikitext}""")
        series_title = series_json.get('Title', None)
        series_acronym = series_json.get('Acronym', None)
        try:
            if series_acronym.lower() in df_core_26_details['Acronym'].str.lower().values:
                # extract the corresponding row from df_core_26_details
                row = df_core_26_details[df_core_26_details['Acronym'].str.lower() == series_acronym.lower()].iloc[0]
                # extract the fields from the row
                logger.debug(f"Found matching row in CORE_26_details for acronym {series_acronym}: {row}")
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
                    logger.info(f"Deleting page {page_title} and recreating with correct acronym {series_acronym}")
                    # delete_page(api_url, page_title, csrf_token, session)
                    # create page with new updated parameters
                    logger.info(f"Creating page {series_acronym} with wikitext: {new_wikitext}")
                    #create_page(api_url, acronym, new_wikitext, csrf_token, f"Create page with updated parameters{parameter_to_set}", session, dry_run)
                else:
                    # edit page with new updated parameters
                    logger.info(f"Editing page {series_acronym} with wikitext: {new_wikitext}")
                    # edit_page(api_url, series_acronym, new_wikitext, csrf_token, session, f"Edit page with updated parameters{parameter_to_set}", dry_run)
        except Exception as e:
            logger.error(f"Error processing series {series_title} with acronym {series_acronym}: {e}")
    return True

if __name__ == "__main__":
    # Example invocation using environment variables and local CSVs for core dicts
    import pandas as pd
    import numpy as np
    core_23_path = os.environ.get("CORE_23_PATH", "CORE_2023.csv")
    core_26_path = os.environ.get("CORE_26_PATH", "CORE_2026.csv")
    df_core_23 = pd.read_csv(core_23_path, header=None)
    df_core_26 = pd.read_csv(core_26_path, header=None)
    # core_23_dict = dict(zip(df_core_23[2], df_core_23[4]))
    # core_26_dict = dict(zip(df_core_26[2], df_core_26[4]))
    # file: core_26_path.replace('.csv', '_details.csv') exists, if not, create it using create_icore_conference_details_data
    details_path = core_26_path.replace('.csv', '_details.csv')
    if not os.path.exists(details_path):
        df_core_26_details = create_icore_conference_details_data(core_26_path)
    else:
        df_core_26_details = pd.read_csv(details_path)
    API = os.environ.get("OR_API", "https://www.openresearch.org/mediawiki/api.php")
    USER = os.environ.get("OR_USER")
    PASS = os.environ.get("OR_PASS")
    df_core_26_details = df_core_26_details.replace(np.nan, '', regex=True)  # Replace NaN with empty string
    clean_openresearch_eventSeries(API, USER, PASS,
                                      df_core_23,
                                      df_core_26,
                                      df_core_26_details,
                                      dry_run=False,
                                      template_name="Event series",
                                      llm_api_key=os.environ.get("OPENROUTER_API_KEY"))
    # create_openresearch_events_flow.serve(API, USER, PASS, core_26_dict, core_23_dict, TARGET_YEARS, dry_run=False, llm_api_key=os.environ.get("OPENROUTER_API_KEY"))