from prefect import get_run_logger, task
from tasks.mw_auth import login_and_get_csrf
from tasks.mw_api import (
    get_series_titles,
    get_page_wikitext,
    create_page,
    edit_page,
    delete_page,
)
from tasks.mw_helper import set_multiple_params_in_template
from tasks.utils import extract_event_fields_from_wikitext, string_similarity_levenshtein, string_similarity_rapidfuzz
import os
import pandas as pd
import numpy as np

# PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")  # Set default logging level to ERROR if not specified

    
@task(name="preprocessing_core_openresearch_eventSeries", description="preprocessing OpenResearch event series pages using core data details")
def preprocessing_core_openresearch_eventSeries(
    api_url: str,
    username: str,
    password: str,
    core_all_details_path: str,
    template_name: str = "Event series",
    dry_run: bool = True,
):
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    
    # get core_all_details dataframe
    df_core_all_details = pd.read_csv(core_all_details_path)
    df_core_all_details = df_core_all_details.replace(np.nan, '', regex=True)  # Replace NaN with empty string
    csrf_token, session = login_and_get_csrf(api_url, username, password)
    
    # 1. collect pages
    page_titles = get_series_titles(api_url, session, title=f"Category:{template_name}")
    series_acronyms = []
    logger.info(f"Found {len(page_titles)} pages, e.g., {page_titles[0:50]}")
    # 3. iterate series and create pages
    for page_title in page_titles:  # limit to first 10 for testing
        logger.info(f"Processing page {page_title}")
        try:
            series_wikitext = get_page_wikitext(api_url, page_title, session)
            series_json = extract_event_fields_from_wikitext(series_wikitext)
            logger.debug(f"""Series Page {page_title}: Json: {series_json} series_wikitext: {series_wikitext}""")
            series_title = series_json.get('Title', None)
            series_acronym = series_json.get('Acronym', page_title)  # default to page_title if Acronym not found
            if series_acronym is None:
                logger.warning(f"Series {series_title} has no acronym")
                series_acronym = page_title  # default to page_title if Acronym not found
                # edit page to add the acronym parameter with the page_title as value
                new_wikitext, changed, old_tpl = set_multiple_params_in_template(series_wikitext, {"Acronym": series_acronym}, template_name, False)
                logger.debug(f"Editing page {series_acronym} to add Acronym parameter with value {series_acronym}")
                edit_page(api_url, series_acronym, new_wikitext, csrf_token, session, f"Edit page to add Acronym parameter with value {series_acronym}", dry_run)
            
            series_acronyms.append(series_acronym)
            
            if series_acronym.lower() in df_core_all_details['Acronym'].str.lower().values:
                # extract the corresponding row from df_core_all_details
                row = df_core_all_details[df_core_all_details['Acronym'].str.lower().str.replace('_', ' ') == series_acronym.lower().replace('_', ' ')].iloc[0]
                # extract the fields from the row
                logger.debug(f"Found matching row in CORE_details for acronym {series_acronym}: {row}")
                title = row['Title'].strip()
                acronym = row['Acronym'].strip()
                field = row['Field'].strip()
                DblpSeries = row['DblpSeries'].strip()
                parameter_to_set = {"Acronym": acronym, 
                                    "Field": field, 
                                    "Title": title, 
                                    "DblpSeries": DblpSeries, 
                                    }
                # create rank dictionary from the row if has CORE string exist in columns
                                # create rank dictionary from the row if has CORE string exist in columns
                for col in row.index:
                    if 'has CORE' in col and pd.notna(row[col]):
                        parameter_to_set[col] = row[col]
                
                # remove parameter which has empty value
                parameter_to_set = {k: v for k, v in parameter_to_set.items() if v != ''}
                new_wikitext, changed, old_tpl = set_multiple_params_in_template(series_wikitext, parameter_to_set, template_name, False)
                if acronym != page_title:
                    logger.warning(f"Acronym: {acronym} is not equal to page_title: {page_title}")
                    # delete page and recreate page with correct acronym
                    # logger.debug(f"Deleting page {page_title} and recreating with correct acronym {acronym}")
                    delete_page(api_url, page_title, csrf_token, session)
                    # create page with new updated parameters
                    logger.debug(f"Creating page {acronym} with wikitext: {new_wikitext}")
                    # if create_page fails, log the error and edit the page with new updated parameters
                    res = create_page(api_url, acronym, new_wikitext, csrf_token, session, f"If acronym and title not matched recreate page with updated parameters{parameter_to_set}", dry_run)
                    if res.get('error'):
                        logger.error("Create result for page_title %s: result: %s", page_title, res['error']['code'])
                        edit_page(api_url, acronym, new_wikitext, csrf_token, session, f"If article exists edit page with updated parameters{parameter_to_set}", dry_run)
                else:
                    # edit page with new updated parameters
                    logger.debug(f"Editing page {acronym} with wikitext: {new_wikitext}")
                    edit_page(api_url, acronym, new_wikitext, csrf_token, session, f"Edit page with updated parameters{parameter_to_set}", dry_run)
        except Exception as e:
            logger.error(f"Error processing page {page_title} series {series_title} with acronym {series_acronym}: {e}")
                
    return True

    
@task(name="create_core_openresearch_eventSeries", description="create OpenResearch event series pages using core data details")
def create_core_openresearch_eventSeries(
    api_url: str,
    username: str,
    password: str,
    core_all_details_path: str,
    template_name: str = "Event series",
    dry_run: bool = True,
):
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    
    # get core_all_details dataframe
    df_core_all_details = pd.read_csv(core_all_details_path)
    df_core_all_details = df_core_all_details.replace(np.nan, '', regex=True)  # Replace NaN with empty string
    csrf_token, session = login_and_get_csrf(api_url, username, password)
    
    # 1. collect pages
    page_titles = get_series_titles(api_url, session, title=f"Category:{template_name}")
    series_acronyms = []
    logger.info(f"Found {len(page_titles)} pages, e.g., {page_titles[0:50]}")
    # 3. iterate series and create pages
    for page_title in page_titles:  # limit to first 10 for testing
        try:
            series_wikitext = get_page_wikitext(api_url, page_title, session)
            series_json = extract_event_fields_from_wikitext(series_wikitext)
            logger.debug(f"""Series Page {page_title}: Json: {series_json} series_wikitext: {series_wikitext}""")
            series_title = series_json.get('Title', None)
            series_acronym = series_json.get('Acronym', page_title)  # default to page_title if Acronym not found
            if series_acronym is None:
                logger.warning(f"Series {series_title} has no acronym")
                series_acronym = page_title  # default to page_title if Acronym not found
            series_acronyms.append(series_acronym)
        except Exception as e:
            logger.error(f"Error processing page {page_title} series {series_title} with acronym {series_acronym}: {e}")
            
    # if  df_core_details['Acronym'].str.lower() is not in series_acronyms, 
    # create a new page with the details from df_core_details
    # limit to first 5 for testing df_core_all_details.head(5).iterrows()
    logger.info(f"Creating new pages for acronyms in CORE_details that do not exist in OpenResearch")
    logger.info(f"Existing series acronyms in OpenResearch: {len(series_acronyms)}, e.g., {series_acronyms[0:50]}")
    for index, row in df_core_all_details.iterrows():
        try:
            acronym = row['Acronym']
            if acronym not in series_acronyms and acronym.strip() != '':
                logger.info(f"Creating new page for acronym {acronym} as it does not exist in OpenResearch")
                title = row['Title'].strip()
                field = row['Field'].strip()
                DblpSeries = row['DblpSeries'].strip()
                parameter_to_set = {"Acronym": acronym, 
                                    "Field": field,
                                    "Title": title, 
                                    "DblpSeries": DblpSeries, 
                }
                # create rank dictionary from the row if has CORE string exist in columns
                for col in row.index:
                    if 'has CORE' in col and pd.notna(row[col]):
                        parameter_to_set[col] = row[col]
                
                # remove parameter which has empty value
                parameter_to_set = {k: v for k, v in parameter_to_set.items() if v != ''}
                wikitext = f"{{{{{template_name}\n}}}}"  # create a new wikitext with the template
                new_wikitext, changed, old_tpl = set_multiple_params_in_template(wikitext, parameter_to_set, template_name, False)
                logger.debug(f"Creating page {acronym} with wikitext: {new_wikitext}")
                res = create_page(api_url, acronym, new_wikitext, csrf_token, session, f"If page does not exists create page with parameters{parameter_to_set}", dry_run)
                if res.get('error'):
                        logger.error("Create core result for acronym %s: result: %s", acronym, res['error']['code'])
        except Exception as e:
            logger.error(f"Error creating new page for acronym {acronym}: {e}")
                
    return True


@task(name="deduplicate_openresearch_eventSeries", description="deduplicate OpenResearch event series pages")
def deduplicate_openresearch_eventSeries(
    api_url: str,
    username: str,
    password: str,
    core_all_details_path: str,
    template_name: str = "Event series",
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
        series_json = extract_event_fields_from_wikitext(wikitext1)
        title1 = series_json.get('Title', page_title)
        
        wikitext2 = get_page_wikitext(api_url, other_page_title, session)
        series_json = extract_event_fields_from_wikitext(wikitext2)
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
