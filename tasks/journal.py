from ast import pattern
import re
from click import prompt
from prefect import get_run_logger, task
from tasks.llm import clean_scientific_fields, build_scientific_fields_prompt
from tasks.mw_auth import login_and_get_csrf
from tasks.mw_api import (
    get_journal_members,
    get_series_titles,
    get_page_wikitext,
    create_page,
    edit_page,
    delete_page,
)
from tasks.mw_helper import set_multiple_params_in_template
from tasks.utils import (extract_journal_fields_from_wikitext, 
                         string_similarity_levenshtein, 
                         string_similarity_rapidfuzz,
                         reorder_journal_template,
                         normalize_homepage,
                         append_journal_template_to_df,
                         transform_journal_values,
                         transform_core_values,
                         convert_wiki_categories
                         )
import os
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime
from time import sleep

# PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")  # Set default logging level to ERROR if not specified


@task(name="deduplicate_openresearch_journal", description="deduplicate OpenResearch Journal pages")
def deduplicate_openresearch_journal(
    api_url: str,
    username: str,
    password: str,
    core_all_details_path: str,
    template_name: str = "Journal",
    dry_run: bool = True,
):  
    from itertools import combinations
    from collections import deque
    from concurrent.futures import ThreadPoolExecutor
    import logging
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    
    # get core_all_details dataframe
    df_core_all_details = pd.read_csv(core_all_details_path)
    df_core_all_details = df_core_all_details.replace(np.nan, '', regex=True)  # Replace NaN with empty string
    csrf_token, session = login_and_get_csrf(api_url, username, password)
    
    # 1. collect pages
    page_titles = get_series_titles(api_url, session, title=f"Category:{template_name}")
    logger.info(f"Found {len(page_titles)} pages, e.g., {page_titles[0:50]}")
    # iterate through all page_titles and compare the title of page_title with all other titles in page_titles, if the title is similar to another title, log a warning
    logger.info(f"Checking for similar titles in existing series pages")
    
    def comparing_titles(api_url, session, page_title, other_page_title):
        wikitext1 = get_page_wikitext(api_url, page_title, session)
        series_json = extract_journal_fields_from_wikitext(wikitext1)
        title1 = series_json.get('Title', page_title)
        
        wikitext2 = get_page_wikitext(api_url, other_page_title, session)
        series_json = extract_journal_fields_from_wikitext(wikitext2)
        title2 = series_json.get('Title', other_page_title)
        
        similarity = string_similarity_levenshtein(title1, title2)
        ratio = string_similarity_rapidfuzz(title1, title2)
        if ratio > 0.8 or similarity > 0.8:
            logger.warning(f"{page_title} is similar to {other_page_title} with similarity {similarity} and ratio {ratio}")
            logger.warning(f"{title1}\n{title2}")
            logger.warning(f"Wikitext of {page_title}:\n{wikitext1}")
            logger.warning(f"Wikitext of {other_page_title}:\n{wikitext2}")
        
        logger.info(f"Successfully compared page {page_title} with page {other_page_title}")
            

    def compare_all_threaded(page_titles, api_url, session, max_workers=20):
        """
        Submit pairwise comparisons concurrently while keeping at most max_workers futures in memory.
        """
        pending = deque()
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for i, (a, b) in enumerate(combinations(page_titles, 2)):
                if i > 2000000 and i < 2500000:
                    logger.info(f"Submitting comparison {i}: {a} vs {b}")
                    pending.append(ex.submit(comparing_titles, api_url, session, a, b))

                    # keep only up to max_workers futures queued to limit memory and in-flight requests
                    if len(pending) >= max_workers:
                        fut = pending.popleft()
                        try:
                            fut.result()
                        except Exception:
                            logger.exception("Error while comparing pages")

            # wait for remaining futures
            while pending:
                fut = pending.popleft()
                try:
                    fut.result()
                except Exception:
                    logger.exception("Error while comparing pages")
              
    """
    for i, page_title in enumerate(page_titles[0:-1]):
        for j, other_page_title in enumerate(page_titles[i+1:]):
            logger.info(f"Comparing page {i}:{page_title} with page {j}:{other_page_title}")
            comparing_titles(api_url, session, page_title, other_page_title)
    """
    compare_all_threaded(page_titles, api_url, session, max_workers=20)
                
    return True


def fix_journal_wikitext(api_url: str, page_titles: List[str], session, csrf_token: str, llm_api_key: Optional[str], dry_run: bool, logger):
    for idx, page_title in enumerate(page_titles):  # limit to first 10 for testing
        logger.info(f"Processing page {idx}:{page_title}")
        # fix duplicates and clean template using LLM if needed
        try:
            journal_wikitext = get_page_wikitext(api_url, page_title, session)
            journal_wikitext_org = journal_wikitext
            journal_json = extract_journal_fields_from_wikitext(journal_wikitext)
            logger.debug(f"""Journal Page {page_title}: Json: {journal_json} journal_wikitext: {journal_wikitext}""")
            acronym = journal_json.get("Acronym", None)
            # fix acronym
            if acronym is None:
                acronym = page_title
                journal_wikitext, changed, old_tpl = set_multiple_params_in_template(journal_wikitext, {"Acronym": acronym}, "Journal", False)
                        
            journal_wikitext = normalize_homepage(journal_wikitext)
            # transfrom values for keys
            journal_wikitext = transform_journal_values(journal_wikitext)
            # transform wiki categories to title case and remove quotes from category names
            journal_wikitext = convert_wiki_categories(journal_wikitext)
            # fix journal_wikitext template order
            journal_wikitext = reorder_journal_template(journal_wikitext, "Journal")
            summary = f"Cleaned {page_title} with new text {journal_wikitext}"
            if journal_wikitext != journal_wikitext_org:
                res = edit_page(api_url, page_title, journal_wikitext, csrf_token, session, summary, dry_run)
                if res.get('error'):
                    logger.error("Edit result for page_title %s: result: %s", page_title, res['error']['code'])
                else:
                    logger.info(f"successfully edit result for page_title: {page_title}")
        except Exception as e:
            logger.error("Get journal Wikitext Exception for %s: %s", page_title, e)
            continue

def fix_journal_wikitext_using_llm(api_url: str, page_titles: List[str], session, csrf_token: str, llm_api_key: Optional[str], dry_run: bool, logger):
    for idx, page_title in enumerate(page_titles):  # limit to first 10 for testing
        logger.info(f"Processing page {idx}:{page_title}")
        # fix duplicates and clean template using LLM if needed
        try:
            journal_wikitext = get_page_wikitext(api_url, page_title, session)
            journal_wikitext_org = journal_wikitext
            journal_json = extract_journal_fields_from_wikitext(journal_wikitext)
            logger.debug(f"""Journal Page {page_title}: Json: {journal_json} journal_wikitext: {journal_wikitext}""")
            field = journal_json.get("Field", None)
            # use LLM to fix separate the fields using semicolons
            if llm_api_key and field and ';' not in field:
                prompt = build_scientific_fields_prompt(field)
                llm_output = clean_scientific_fields(llm_api_key, prompt)
                llm_output = eval(llm_output) if llm_output else None
                logger.debug(f"LLM output for {page_title}: {llm_output}")
                if llm_output:
                    field = '; '.join(llm_output)  # convert the string representation of list to actual list and join with semicolon
                    sleep(5)  # Add a small delay to avoid overwhelming the LLM service
                    journal_wikitext, changed, old_tpl = set_multiple_params_in_template(journal_wikitext, {"Field": field}, "Journal", False)
            summary = f"Cleaned {page_title} with new field {field}"
            if journal_wikitext != journal_wikitext_org:
                res = edit_page(api_url, page_title, journal_wikitext, csrf_token, session, summary, dry_run)
                if res.get('error'):
                    logger.error("Edit result for page_title %s: result: %s", page_title, res['error']['code'])
                else:
                    logger.info(f"successfully edit result for page_title: {page_title}")
        except Exception as e:
            logger.error("Get journal Wikitext Exception for %s: %s", page_title, e)
            continue
            

@task(name="preprocessing-openresearch-journal", description="preprocessing OpenResearch Journal pages")
def preprocessing_openresearch_journal(
    api_url: str,
    username: str,
    password: str,
    core_all_details_path: str,
    template_name: str = "journal",
    llm_api_key: Optional[str] = None,
    dry_run: bool = True,
):
    
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    
    # get core_all_details dataframe
    df_core_all_details = pd.read_csv(core_all_details_path)
    df_core_all_details = df_core_all_details.replace(np.nan, '', regex=True)  # Replace NaN with empty string
    csrf_token, session = login_and_get_csrf(api_url, username, password)
        
    # 1. collect pages
    page_titles = get_journal_members(api_url, session, f"Category:{template_name}")
    logger.info(f"Found {len(page_titles)} pages, e.g., {page_titles[0:60]}")
    
    
    # fix journal wikitext
    fix_journal_wikitext(api_url, page_titles, session, csrf_token, llm_api_key, dry_run, logger)
    
    # fix journal wikitext using LLM if llm_api_key is provided
    # fix_journal_wikitext_using_llm(api_url, page_titles, session, csrf_token, llm_api_key, dry_run, logger)
        
    return True

@task(name="create-stats-openresearch-journal", description="collecting statistics of OpenResearch Journal pages in a dataframe")
def create_stats_openresearch_journal(
    api_url: str,
    username: str,
    password: str,
    core_all_details_path: str,
    template_name: str = "journal",
    llm_api_key: Optional[str] = None,
    dry_run: bool = True,
):
    
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    
    # get core_all_details dataframe
    df_core_all_details = pd.read_csv(core_all_details_path)
    df_core_all_details = df_core_all_details.replace(np.nan, '', regex=True)  # Replace NaN with empty string
    csrf_token, session = login_and_get_csrf(api_url, username, password)
        
    # 1. collect pages
    page_titles = get_journal_members(api_url, session, f"Category:{template_name}")
    logger.info(f"Found {len(page_titles)} pages, e.g., {page_titles[0:50]}")
    
    # 2. collect statistics of open research journal pages in a dataframe
    df_stats_openresearch_journal = pd.DataFrame()
    for idx, page_title in enumerate(page_titles):  # limit to first 10 for testing
        logger.info(f"Processing page {idx}:{page_title}")
        try:
            journal_wikitext = get_page_wikitext(api_url, page_title, session)
            df_stats_openresearch_journal = append_journal_template_to_df(journal_wikitext, df_stats_openresearch_journal)
        except Exception as e:
            logger.error("Get journal Wikitext Exception for %s: %s", page_title, e)
            continue
    
    # save to csv
    stats_output_path = os.environ.get("STATS_OUTPUT_PATH", "stats_openresearch_journal.csv")
    df_stats_openresearch_journal.to_csv(stats_output_path + f"stats_openresearch_journal_{datetime.now().strftime('%Y-%m-%d')}.csv", index=False)
    return True