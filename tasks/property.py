from time import time
from prefect import flow, get_run_logger
from tasks.mw_auth import login_and_get_csrf
from prefect import task
from typing import List, Dict, Optional
import os
import pandas as pd
import numpy as np
import re
from datetime import datetime
from tasks.mw_api import (
    get_property_members,
    get_page_wikitext,
    create_page,
    edit_page,
    delete_page,
    page_exists,
)
 
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")
# PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")


def fix_city_property_wikitext(api_url: str, page_titles: List[str], session, csrf_token: str, llm_api_key: Optional[str], dry_run: bool, logger):
    for idx, page_title in enumerate(page_titles):  # limit to first 10 for testing
        logger.info(f"Processing page {idx}:{page_title}")
        # fix duplicates and clean template using LLM if needed
        try:
            property_wikitext = get_page_wikitext(api_url, page_title, session)
            property_wikitext_org = property_wikitext
            logger.debug(f"Property Wikitext: {property_wikitext}")
            marker = "{{city_data}}"
            marker2 = "{{city data}}"
            if (marker not in property_wikitext) and (marker2 not in property_wikitext):
                # add {{city_data}} in property wikitext in the begining of text
                property_wikitext = f"{marker}\n{property_wikitext}"
            if property_wikitext != property_wikitext_org:
                summary = f"Edited {page_title} with new text {property_wikitext}"
                res = edit_page(api_url, page_title, property_wikitext, csrf_token, session, summary, dry_run)
                if res.get('error'):
                    logger.error("Edit result for page_title %s: result: %s", page_title, res['error']['code'])
                else:
                    logger.info(f"successfully edit result for page_title: {page_title}")
        except Exception as e:
            logger.error("Get property Wikitext Exception for %s: %s", page_title, e)
            continue            
  
@task(name="preprocessing-openresearch-properties", description="preprocessing OpenResearch property pages using core data details")
def preprocessing_openresearch_properties(
    api_url: str,
    username: str,
    password: str,
    core_all_details_path: str,
    template_name: str = "property",
    llm_api_key: Optional[str] = None,
    dry_run: bool = True,
):
    
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    
    csrf_token, session = login_and_get_csrf(api_url, username, password)
    property_name = 'Has location city'
    members = get_property_members(api_url, session, property_name, printouts=[property_name])
    logger.info(f"Fetched {len(members)} pages:\n, sample: {members[0:10]}")
    page_titles = []
    for p in members:
        values = p["printouts"].get(property_name, [])
        values = [v.get("fulltext", v) if isinstance(v, dict) else v for v in values]
        # logger.debug(f"- {p['title']}  ->  {', '.join(map(str, values))}")
        page_titles.append(', '.join(map(str, values)))
    fix_city_property_wikitext(api_url, page_titles, session, csrf_token, llm_api_key, dry_run, logger)
    return True