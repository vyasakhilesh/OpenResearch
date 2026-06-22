from time import time
from prefect import flow, get_run_logger
from tasks.mw_auth import login_and_get_csrf
from tasks.mw_api import (
    get_series_titles,
    get_page_wikitext,
    create_page,
    page_exists,
)
from tasks.llm import (
    get_event_template, 
    fix_and_validate_event_template
)
from tasks.utils import filter_rank_series, extract_event_fields_from_wikitext
from typing import List, Dict, Optional
import os

PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")

def build_create_event_prompt(
    series_title: str,
    acronym: Optional[str],
    target_year: Optional[int],
) -> str:
    
    prompt = f"""You are an information-extraction agent. Find the given information (Acronym, Title, Ordinal, Series, Type, Field, Start date, End date, Submission deadline, Homepage URL, City, Country, Abstract deadline, Notification, Camera ready, Has host organization, has general chair, has program chair, Submitted papers, Accepted papers, Accepted short papers) about the event {series_title} ({acronym} {target_year}) of year {target_year} with best-known values to fill keys of the event template.
You must search the official event website, the conference proceedings (publisher pages / DBLP / https://dblp.org/), and community trackers (OpenAccept, CS conference stats) to gather evidence. For each key you include, provide a single best-known value.

Output requirements::
  1. Produce the filled template exactly in wiki key format starting with two opening curly braces "{{Event" and ending with two closing curly braces "}}".
  2. Immediately after the closing "}}", on a new line, include a "Sources:" section that lists one evidence URL per line. The "Sources:" section must be separated from the template by two blank lines.
  3. Do not include any other text, explanation, or commentary.
Formatting rules:
  - Each template line must be of the form: |Key=Value
  - Acronym must be abbreviation of event, e.g. ICWE 2024
  - Title is full title of the given event, (e.g. 24th International Conference on Web Engineering)
  - Ordinal of the event e.g 24 for 24th event
  - Type must be one of Conference, Workshop, Tutorial, Symposium
  - Series must be abbreviation of event series, e.g. ICWE
  - Field must be a primary scientific field of the event
  - Dates keys (Start date, End date, Submission deadline, Abstract deadline, Notification, Camera ready) must use YYYY/MM/DD format.
  - Keys (Has host organization, has general chair, has program chair) must be names of organizations or persons.
  - Keys (Submitted papers, Accepted papers, Accepted short papers) must be positive integer.
  - If value of key is unknown or cannot be reliably determined with full confidence then remove that key entirely.
  - Prefer primary sources (official site, proceedings, DBLP, OpenAccept) and include those URLs in the Sources list.
Produce only the template followed by two blank lines and then the Sources list.
"""
    return prompt

@flow(name="create-openresearch-events", description="Flow to create upcoming event pages on OpenResearch based on existing series pages and LLM extraction")
def create_openresearch_events(
    api_url: str,
    username: str,
    password: str,
    core_26_dict: Dict[str, str],
    core_23_dict: Dict[str, str],
    target_years: List[int],
    dry_run: bool = True,
    llm_api_key: Optional[str] = None,
):
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    csrf_token, session = login_and_get_csrf(api_url, username, password)
    # 1. collect pages
    titles = get_series_titles(api_url, session)
    logger.info("Found %s pages", len(titles))
    # 2. filter by ranking
    filtered_series_list = filter_rank_series(titles, core_26_dict, core_23_dict)
    logger.info("Candidate series needing new edition: %s", len(filtered_series_list))
    # 3. iterate target years and create pages
    for year in target_years:
        for series in filtered_series_list[0:5]:
            # fetch page wikitext and template
            page_title = f"{series} {year}"
            wikitext = get_page_wikitext(api_url, series, session)
            text_json = extract_event_fields_from_wikitext(wikitext)
            series_title = text_json.get('Title', None)
            
            if page_exists(api_url, page_title, session):
                logger.info("Event Page %s already exists, skipping", page_title)
                continue
            
            try:
                if llm_api_key:
                    prompt = build_create_event_prompt(series_title, series, year)
                    llm_output = get_event_template(llm_api_key, prompt)
                    logger.debug("LLM output for %s: \n %s", page_title, llm_output)
                    if llm_output:
                        fixed, err = fix_and_validate_event_template(llm_output)
                        logger.debug("LLM template validation for page_title: %s: \n fixed: %s, \n error: %s", page_title, fixed, err)
                        if err:
                            logger.error("LLM template validation failed for series: %s: \n error: %s \n llm_output: %s", series, err, llm_output)
                            continue
                summary = f"Added upcoming edition for {series} {year} (automated(LLM-assisted) edit)"
                res = create_page(api_url, page_title, fixed, csrf_token, summary, session, dry_run)
                logger.info("Create result for page_title %s: result: %s", page_title, res['error']['code'] if res.get('error') else res)
                if res.get('error'):
                    logger.error("Create result for page_title %s: result: %s", page_title, res['error']['code'])
                # time.sleep(1)
            except Exception as e:
                logger.error("Exception for %s: %s", page_title, e)
                # time.sleep(1)
    return True

if __name__ == "__main__":
    # Example invocation using environment variables and local CSVs for core dicts
    import pandas as pd
    core_23_path = os.environ.get("CORE_23_PATH", "CORE_23.csv")
    core_26_path = os.environ.get("CORE_26_PATH", "CORE_26.csv")
    df_core_23 = pd.read_csv(core_23_path, header=None)
    df_core_26 = pd.read_csv(core_26_path, header=None)
    core_23_dict = dict(zip(df_core_23[2], df_core_23[4]))
    core_26_dict = dict(zip(df_core_26[2], df_core_26[4]))
    API = os.environ.get("OR_API", "https://www.openresearch.org/mediawiki/api.php")
    USER = os.environ.get("OR_USER")
    PASS = os.environ.get("OR_PASS")
    TARGET_YEARS = [2021, 2022, 2023, 2024, 2025, 2026, 2027]
    create_openresearch_events(API, USER, PASS, core_26_dict, core_23_dict, 
                                    TARGET_YEARS, dry_run=False, 
                                    llm_api_key=os.environ.get("OPENROUTER_API_KEY"))
    # create_openresearch_events_flow.serve(API, USER, PASS, core_26_dict, core_23_dict, TARGET_YEARS, dry_run=False, llm_api_key=os.environ.get("OPENROUTER_API_KEY"))