from prefect import flow, get_run_logger
from tasks.mw_auth import login_and_get_csrf
from tasks.mw_api import (
    get_event_pages,
    get_page_wikitext,
    find_template_block,
    extract_acronym_from_template,
    extract_series_strict_two_tokens,
    set_multiple_params_in_template,
    edit_page,
)
from tasks.llm import get_event_template, fix_and_validate_event_template
from tasks.utils import filter_rank_series
from typing import List, Dict, Optional
import os

LLM_PROMPT = """
Task:: Find the given information (Acronym, Title, Ordinal, Series, Type, Field, Start date, End date, Submission deadline, Homepage URL, City, Country, Abstract deadline, Notification, Camera ready, Has host organization, has general chair, has program chair, Submitted papers, Accepted papers, Accepted short papers) about the event {TITLE} ({SERIES_NAME} {TARGET_YEAR}) of year {TARGET_YEAR} with best-known values to fill keys of the event template.
You must search the official event website, the conference proceedings (publisher pages / DBLP / https://dblp.org/), and community trackers (OpenAccept, CS conference stats) to gather evidence. For each key you include, provide a single best-known value.
Output requirements::
  1. Produce the filled template exactly in wiki key format starting with two opening curly braces "{{Event" and ending with two closing curly braces "}}".
  2. Immediately after the closing "}}", on a new line, include a "Sources:" section that lists one evidence URL per line. The "Sources:" section must be separated from the template by two blank lines.
  3. Do not include any other text, explanation, or commentary.
Formatting rules:
  - Each template line must be of the form: |Key=Value
  - Acronym must be abbreviation of event, e.g. ICWE 2024
  - Ordinal is positive integer value
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

@flow(name="create-events-flow")
def create_events_flow(
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
    csrf_token, session_info = login_and_get_csrf(api_url, username, password)
    # 1. collect pages
    titles = get_event_pages(api_url, session_info)
    logger.info("Found %s pages", len(titles))
    # 2. filter by ranking
    candidates = filter_rank_series(titles, core_26_dict, core_23_dict)
    logger.info("Candidate series needing new edition: %s", len(candidates))
    # 3. iterate target years and create pages
    for year in target_years:
        for s in candidates:
            # fetch page wikitext and template
            wikitext = get_page_wikitext(api_url, s, session_info)
            tpl = find_template_block(wikitext)
            if not tpl:
                logger.info("No Event template on %s, skipping", s)
                continue
            tpl_text, _, _ = tpl
            acronym = extract_acronym_from_template(tpl_text) or s.split(":")[-1].replace(" ", "_")
            series = extract_series_strict_two_tokens(acronym)
            if not series:
                logger.info("Could not infer series for %s (acronym=%s), skipping", s, acronym)
                continue
            # Build params and set in template
            params_map = {"Series": series}
            new_wikitext, changed, old_tpl = set_multiple_params_in_template(wikitext, params_map)
            summary = f"Set Series to {series} (acronym={acronym})"
            if changed:
                # Optionally call LLM to generate full Event template for the specific year
                if llm_api_key:
                    llm_output = get_event_template(s, series, year, llm_api_key, LLM_PROMPT)
                    if llm_output:
                        fixed, err = fix_and_validate_event_template(llm_output)
                        if fixed and not err:
                            new_wikitext = fixed
                        else:
                            logger.info("LLM template validation failed for %s: %s", s, err)
                res = edit_page(api_url, s, new_wikitext, csrf_token, session_info, summary=summary, dry_run=dry_run)
                logger.info("Edit result for %s: %s", s, res)
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
    API = os.environ.get("MW_API", "https://www.openresearch.org/mediawiki/api.php")
    USER = os.environ.get("OR_USER")
    PASS = os.environ.get("OR_PASS")
    TARGET_YEARS = [2021, 2022, 2023, 2024, 2025, 2026]
    create_events_flow(API, USER, PASS, core_26_dict, core_23_dict, TARGET_YEARS, dry_run=True, llm_api_key=os.environ.get("OPENROUTER_API_KEY"))