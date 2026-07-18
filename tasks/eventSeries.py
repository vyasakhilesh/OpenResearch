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
                row = df_core_all_details[df_core_all_details['Acronym'].str.lower() == series_acronym.lower()].iloc[0]
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
                if acronym.strip() != page_title.strip():
                    logger.warning(f"series_title: {acronym} is not equal to page_title: {page_title}")
                    # delete page and recreate page with correct acronym
                    # logger.debug(f"Deleting page {page_title} and recreating with correct acronym {acronym}")
                    delete_page(api_url, page_title, csrf_token, session)
                    # create page with new updated parameters
                    logger.debug(f"Creating page {acronym} with wikitext: {new_wikitext}")
                    # if create_page fails, log the error and edit the page with new updated parameters
                    res = create_page(api_url, acronym, new_wikitext, csrf_token, f"Create page with updated parameters{parameter_to_set}", session, dry_run)
                    if res.get('error'):
                        logger.error("Create result for page_title %s: result: %s", page_title, res['error']['code'])
                        edit_page(api_url, acronym, new_wikitext, csrf_token, session, f"Edit page with updated parameters{parameter_to_set}", dry_run)
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
            acronym = row['Acronym'].strip()
            if acronym.lower() not in [s.lower().strip() for s in series_acronyms] and acronym.strip() != '':
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
                create_page(api_url, acronym, new_wikitext, csrf_token, f"Create page with parameters{parameter_to_set}", session, dry_run)
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
    for i, page_title in enumerate(page_titles):
        for j, other_page_title in enumerate(page_titles[i+1:]):
            similarity = string_similarity_levenshtein(page_title, other_page_title)
            ratio = string_similarity_rapidfuzz(page_title, other_page_title)
            if ratio > 0.8 and similarity > 0.8:
                logger.warning(f"Series page title {page_title} is similar to {other_page_title} with similarity ratio {ratio}")
                # show the wikitext of both pages
                wikitext1 = get_page_wikitext(api_url, page_title, session)
                wikitext2 = get_page_wikitext(api_url, other_page_title, session)
                # log the wikitext of both pages
                logger.info(f"Wikitext of {page_title}: {wikitext1}")
                logger.info(f"Wikitext of {other_page_title}: {wikitext2}")
                # Ask human to review the pages and decide if they should be merged or one should be deleted.
                # wait for human input to continue
                input("Press Enter to continue...")
                
    return True
