from time import time
from prefect import flow, get_run_logger
from tasks.mw_auth import login_and_get_csrf
from tasks.mw_api import (
    get_series_titles,
    get_page_wikitext,
    edit_page,
)
from tasks.mw_helper import find_template_block, set_multiple_params_in_template, extract_fields_from_template
from tasks.llm import (
    get_event_fields,
)
from tasks.utils import filter_rank_series, extract_event_fields_from_wikitext, update_dict
from typing import List, Dict, Optional
import os

PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")

LLM_PROMPT_OLD = """
Task:: Find the following information (City, State, Country) about event {TITLE} ({ACRONYM}) of year {TARGET_YEAR} with best-known values to fill keys of the event.
Scope requirements::
  1. Search the official event website first, then the conference proceedings (publisher pages / DBLP), then community trackers (OpenAccept, CS Conference Stats) to gather information.
  2. Prefer primary sources (official site, proceedings, DBLP, OpenAccept).

Output requirements::
  1. Produce exactly one valid JSON object and nothing else.
  2. The JSON object must contain only the event keys (only those that could be reliably determined):
     a. "Year": integer or None
     b. "City": string or None
     c. "State": string or None
     d. "Country": string or None
     e. "Confidence": string, one of "high", "medium", or "low"
  3. Do not include any other explanatory text, commentary, metadata, surrounding code fences or additional keys before, inside, or after the JSON object.

Formatting rules::
   1. The JSON object must be valid JSON (use double quotes for keys and string values).
   2. Use the exact key names listed above.
"""

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


@flow(name="update-openresearch-events-fields", description="Flow to update OpenResearch event pages with new edition fields using LLM extraction")
def update_openresearch_events_fields(
    api_url: str,
    username: str,
    password: str,
    core_26_dict: Dict[str, str],
    core_23_dict: Dict[str, str],
    target_years: List[int],
    template_name: str = "Event",
    fields: str= "City:str, State:str, Country:str, Confidence:str, Sources:list",
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
            # for series
            series_wikitext = get_page_wikitext(api_url, series, session)
            series_json = extract_event_fields_from_wikitext(series_wikitext)
            series_title = series_json.get('Title', None)
            logger.debug(f"Series Page {series}: Title: {series_json} series_wikitext: {series_wikitext}")
            
            # For event
            page_title = f"{series} {year}"
            event_wikitext = get_page_wikitext(api_url, page_title, session)
            event_json = extract_event_fields_from_wikitext(event_wikitext)
            event_title = event_json.get('Title', None)
            logger.debug(f"Event Page {page_title}: Title: {event_json} event_wikitext: {event_wikitext}")
            
            if event_title:
                title = event_title
            elif series_title:
                title = series_title
            else:
                logger.info(f"{page_title}: [SKIP] No title found in event or series")
                continue
            
            event_wikitext = get_page_wikitext(api_url, page_title, session)
            tpl = find_template_block(event_wikitext, template_name)
            if not tpl:
                logger.info(f"{page_title}: [SKIP] No {template_name} template")
                continue
            
            tpl_text, _, _ = tpl
            
            extracted_fields = extract_fields_from_template(tpl_text, fields)
            logger.debug(f"{page_title}: Extracted fields: {extracted_fields}")
            
            if None in extracted_fields.values():
                logger.info(f"{page_title}: [INFO] Missing fields detected, attempting LLM extraction")
                try:
                    if llm_api_key:
                        llm_prompt = build_event_fields_extraction_prompt(
                            title=title,
                            acronym=page_title,
                            target_year=year,
                            fields=fields
                        )
                        logger.debug("LLM prompt for %s (%s): \n %s", title, page_title, llm_prompt)
                        parsed_response = get_event_fields(llm_api_key, llm_prompt)
                        logger.debug("LLM output for %s (%s): \n %s", title, page_title, parsed_response)
                        if parsed_response:
                            update_dict(extracted_fields, parsed_response)
                            logger.debug("Updated fields after LLM extraction for %s (%s): \n %s", title, page_title, extracted_fields)
                            parameter_to_set = {k:v for k, v in extracted_fields.items() if v is not None and k!="Sources" and k!="Confidence"}
                            new_wikitext, changed, old_tpl = set_multiple_params_in_template(event_wikitext, parameter_to_set, template_name)
                            # Append Confidence and Sources after new_wikitext for provenance
                            if extracted_fields.get("Sources") and extracted_fields.get("Confidence"):
                                sources_list = extracted_fields['Sources']
                                if isinstance(sources_list, list):
                                    sources_str = ", ".join(sources_list)
                                    new_wikitext += f"\n\n\n=== Sources (Confidence: {extracted_fields['Confidence']}) ===\n"
                                    for source in sources_list:
                                        new_wikitext += f"* {source}\n"
                            if not changed:
                                logger.info(f"[UNCHANGED] {title} already has desired params.")
                                continue
                            summary = f"Set {parameter_to_set} (acronym={title})"
                            logger.debug(f"New wikitext for {title}:\n{new_wikitext}")
                            logger.info(f"Editing page {title} with text: {new_wikitext}")
                            res = edit_page(api_url, page_title, new_wikitext, csrf_token, session, summary, dry_run)
                            logger.info("Edit result for title %s: result: %s", title, res['error']['code'] if res.get('error') else res)
                            if res.get('error'):
                                logger.error("Edit result for title %s: result: %s", title, res['error']['code'])
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
    update_openresearch_events_fields(API, USER, PASS, 
                                      core_26_dict, core_23_dict,
                                      TARGET_YEARS,
                                      fields="City:str, Country:str, Confidence:str, Sources:list", 
                                      dry_run=False, 
                                      llm_api_key=os.environ.get("OPENROUTER_API_KEY"))
    # create_openresearch_events_flow.serve(API, USER, PASS, core_26_dict, core_23_dict, TARGET_YEARS, dry_run=False, llm_api_key=os.environ.get("OPENROUTER_API_KEY"))