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
    get_category_pages,
    get_page_wikitext,
    create_page,
    edit_page,
    delete_page,
    page_exists,
)

 
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")
# PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")


def fix_category_wikitext(api_url: str, page_titles: List[str], session, csrf_token: str, llm_api_key: Optional[str], dry_run: bool, logger):
    for idx, page_title in enumerate(page_titles):  # limit to first 10 for testing
        logger.info(f"Processing page {idx}:{page_title}")
        # fix duplicates and clean template using LLM if needed
        try:
            category_wikitext = get_page_wikitext(api_url, page_title, session)
            category_wikitext_org = category_wikitext
            summary = f"Cleaned {page_title} with new text {category_wikitext}"
            if category_wikitext != category_wikitext_org:
                res = edit_page(api_url, page_title, category_wikitext, csrf_token, session, summary, dry_run)
                if res.get('error'):
                    logger.error("Edit result for page_title %s: result: %s", page_title, res['error']['code'])
                else:
                    logger.info(f"successfully edit result for page_title: {page_title}")
        except Exception as e:
            logger.error("Get category Wikitext Exception for %s: %s", page_title, e)
            continue
            
def collect_all_categories_iterative(api_url, session, root_category, max_depth=None):
    visited = set()
    result = []
    # stack holds tuples (category, depth)
    stack = [(root_category, 0)]

    while stack:
        category, depth = stack.pop()
        if category in visited:
            continue
        visited.add(category)
        result.append(category)

        if max_depth is not None and depth >= max_depth:
            continue

        try:
            subcats = get_category_pages(api_url, session, category)
        except Exception:
            # optionally log or handle transient errors; skip on failure
            subcats = []

        # push children onto stack; you can reverse to control order
        for sub in subcats:
            if sub not in visited:
                stack.append((sub, depth + 1))

    return result
  
@task(name="preprocessing-openresearch-categories", description="preprocessing OpenResearch category pages using core data details")
def preprocessing_openresearch_categories(
    api_url: str,
    username: str,
    password: str,
    core_all_details_path: str,
    template_name: str = "category",
    llm_api_key: Optional[str] = None,
    dry_run: bool = True,
):
    
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    
    csrf_token, session = login_and_get_csrf(api_url, username, password)
        
    # 1. collect pages
    page_titles = collect_all_categories_iterative(api_url, session, "Category:Science")
    logger.info(f"Found {len(page_titles)} pages, e.g., {page_titles[0:50]}")
    
    
    # fix category wikitext
    fix_category_wikitext(api_url, page_titles, session, csrf_token, llm_api_key, dry_run, logger)
    
    return True