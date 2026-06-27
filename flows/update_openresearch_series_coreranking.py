from time import time
from prefect import flow, get_run_logger
from tasks.mw_auth import login_and_get_csrf
from tasks.mw_api import (
    get_series_titles,
    get_page_wikitext,
    edit_page,
)
from tasks.mw_helper import find_template_block, extract_acronym_from_template, set_multiple_params_in_template
from tasks.utils import ci_get
from typing import Dict
import os
import re


@flow(name="update-openresearch-series-coreranking-flow")
def update_openresearch_series_coreranking(
    api_url: str,
    username: str,
    password: str,
    core_26_dict: Dict[str, str],
    core_23_dict: Dict[str, str],
    template_name: str = "Event series",
    PARAMS_TO_SET: list = ["has CORE2023 Rank", "has CORE2026 Rank"],
    SMW_PROPERTY_LINE:bool = False,
    dry_run: bool = True
):
    logger = get_run_logger()
    csrf_token, session = login_and_get_csrf(api_url, username, password)
    # 1. collect pages
    series_titles = get_series_titles(api_url, session)
    logger.info("Found %s pages", len(series_titles))
    # 3. iterate all series
    for series_title in series_titles:
        logger.info(f"Processing series: {series_title}")
        wikitext = get_page_wikitext(api_url, series_title, session)
        tpl = find_template_block(wikitext, template_name=template_name)
        logger.debug(f"Template block for {series_title}: {'FOUND' if tpl else 'NOT FOUND'}")
        if not tpl:
            logger.warning(f"[SKIP] {template_name} template on {series_title}")
            continue
        
        tpl_text, _, _ = tpl
        acronym = extract_acronym_from_template(tpl_text) or series_title.strip()
        acronym_key = acronym.strip() if acronym else None
        logger.debug(f"Extracted acronym for {series_title}: {acronym_key}")
        if not acronym_key:
            logger.warning(f"[SKIP] No acronym found on {series_title}")
            continue
        # Lookup rank: prefer CORE_26 then CORE_23
        rank23 = 'NA'
        rank26= 'NA'
        val26 = ci_get(core_26_dict, acronym_key)
        if val26 is not None and str(val26).strip():
            rank26 = str(val26).strip()

        val23 = ci_get(core_23_dict, acronym_key)
        if val23 is not None and str(val23).strip():
            rank23 = str(val23).strip()
        logger.info(f"Ranks for {series_title} (acronym: {acronym_key}): CORE2023 Rank={rank23}, CORE2026 Rank={rank26}")
        if rank23!='NA' or rank26!='NA':
            # Prepare params to set for both years
            params_map = {PARAMS_TO_SET[0]: rank23, PARAMS_TO_SET[1]: rank26}
            new_wikitext, changed, old_tpl = set_multiple_params_in_template(wikitext, params_map)
            # Optionally add SMW property after template if not present
            if SMW_PROPERTY_LINE:
                prop_pat = re.compile(r"""

                \[

                \[\s*Has core ranking::.*?\]

                \]

                """, re.IGNORECASE)
                if not prop_pat.search(new_wikitext):
                    tpl_block = find_template_block(new_wikitext)
                    if tpl_block:
                        _, s, e = tpl_block
                        insertion = f"\n\n[[Has core ranking::{rank26}]]\n"
                        new_wikitext = new_wikitext[:e] + insertion + new_wikitext[e:]
                        changed = True
            if not changed:
                logger.info(f"[UNCHANGED] {series_title} already has desired params.")
                continue
            summary = f"Set {PARAMS_TO_SET[0]} to {rank23} and {PARAMS_TO_SET[1]} to {rank26} (acronym={acronym_key})"
            logger.debug(f"New wikitext for {series_title}:\n{new_wikitext}")
            logger.info(f"Editing page {series_title} with text: {new_wikitext}")
            res = edit_page(api_url, series_title, new_wikitext, csrf_token, session, summary, dry_run)
            logger.info("Edit result for series_title %s: result: %s", series_title, res['error']['code'] if res.get('error') else res)
            if res.get('error'):
                logger.error("Edit result for series_title %s: result: %s", series_title, res['error']['code'])
    return True

if __name__ == "__main__":
    # Example invocation using environment variables and local CSVs for core dicts
    import pandas as pd
    core_23_path = os.environ.get("CORE_23_PATH", "CORE_2023.csv")
    core_26_path = os.environ.get("CORE_26_PATH", "CORE_2026.csv")
    df_core_23 = pd.read_csv(core_23_path, header=None)
    df_core_26 = pd.read_csv(core_26_path, header=None)
    core_23_dict = dict(zip(df_core_23[2], df_core_23[4]))
    core_26_dict = dict(zip(df_core_26[2], df_core_26[4]))
    API = os.environ.get("OR_API", "https://www.openresearch.org/mediawiki/api.php")
    USER = os.environ.get("OR_USER")
    PASS = os.environ.get("OR_PASS")
    update_openresearch_series_coreranking(API, USER, PASS, core_26_dict, core_23_dict, dry_run=False)