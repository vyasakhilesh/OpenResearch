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
    years: list,
    core_dict_year_map: Dict[str, Dict[str, str]],
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
    for series_title in series_titles[0:5]:
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
       
       
        def get_rank_for_year(year: str, acronym_key: str) -> str:
            rank = ci_get(core_dict_year_map.get(year, {}), acronym_key)
            return str(rank).strip() if rank is not None and str(rank).strip() else ''
        
        ranks_dict = {f"rank_{year}": get_rank_for_year(year, acronym_key) for year in years}


        logger.info(f"Ranks for {series_title} (acronym: {acronym_key}): CORE Ranks={ranks_dict}")
        if not any(ranks_dict.values()):
            logger.info(f"[SKIP] {series_title} has no CORE ranks for years {years}.")
            continue
        else:
            logger.info(f"Updating {series_title} with ranks: {ranks_dict}")
            # Prepare params to set for both years  
            params_map = {PARAMS_TO_SET[i]: ranks_dict[f"rank_{year}"] for i, year in enumerate(years) if ranks_dict[f"rank_{year}"]}
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
                        insertion = f"\n\n[[Has core ranking::{ranks_dict[f'rank_{years[-1]}']}]]\n"
                        new_wikitext = new_wikitext[:e] + insertion + new_wikitext[e:]
                        changed = True
            if not changed:
                logger.info(f"[UNCHANGED] {series_title} already has desired params.")
                continue
            summary = f"Set {params_map} (acronym={acronym_key})"
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
    
    years = ["2013", "2014", "2017", "2018", "2020", "2021", "2023", "2026"]
    
    def get_df_core_rankings_dict(year: str) -> Dict[str, str]:
        path = os.environ.get(f"CORE_{year}_PATH", f"CORE_{year}.csv")
        df = pd.read_csv(path, header=None)
        # Drop rows with missing acronym/rank and normalize to non-empty strings.
        cleaned = (
            df[[2, 4]]
            .dropna(subset=[2, 4])
            .astype(str)
            .apply(lambda s: s.str.strip())
        )
        cleaned = cleaned[(cleaned[2] != "") & (cleaned[4] != "")]
        return dict(zip(cleaned[2], cleaned[4]))  # column 2: acronym, column 4: rank

    core_dict_year_map = { year: get_df_core_rankings_dict(year) for year in years }
    parameters_to_set = [f"has CORE{year} Rank" for year in years]
    API = os.environ.get("OR_API", "https://www.openresearch.org/mediawiki/api.php")
    USER = os.environ.get("OR_USER")
    PASS = os.environ.get("OR_PASS")
    template_name = "Event series"
    update_openresearch_series_coreranking(API, USER, PASS, years, core_dict_year_map, template_name, parameters_to_set, dry_run=False)