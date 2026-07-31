# Remove quotes from the acronym e.g. " DBKDA 2021", "DBKDA 2021"
# Remove year from front the acronym e.g. 2022 SGSE 2022, 2023 CCIOT if year missing in end then add it
# get Aconym from this e.g. 2024 15th International Conference on Environmental Science and Development (ICESD 2024)

# fix acronyms => acronym is missing or less than 2 words or (more than 3 words not match with event series acronym)
# if acronym is changed then delete page and recreate page with new acronym
# fix title
# fix page_title is matched with acronym

from time import time
from prefect import flow, get_run_logger
from tasks.mw_auth import login_and_get_csrf
from tasks.mw_api import (
    get_event_pages,
    get_page_wikitext,
    create_page,
    edit_page,
    delete_page,
    page_exists,
)
from prefect import task
from tasks.llm import (
    get_event_template, 
    fix_event_template
)
from tasks.mw_helper import find_template_block, set_multiple_params_in_template
from tasks.utils import (
    extract_event_fields_from_wikitext, 
    clean_submitted_papers,
    clean_accepted_papers,
    clean_accepted_short_papers,
)
from typing import List, Dict, Optional
import os
import pandas as pd
import numpy as np
import re

PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")
# PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")

def build_clean_event_prompt(
    text: str,
) -> str:
  
  prompt = f"""You are an information-extraction agent. Find the given information (Acronym, Title, Ordinal, Series, Type, Field, Start date, End date, Submission deadline, Homepage URL, City, Country, Abstract deadline, Notification, Camera ready, Has host organization, has general chair, has program chair, Submitted papers, Accepted papers, Accepted short papers) about the event from the given text with best-known values to fill keys of the event template.

Input text:: {text}

Output requirements::
  1. Produce the filled template exactly in wiki key format starting with two opening curly braces "{{Event" and ending with two closing curly braces "}}".
  2. Do not include any keys with unknown or missing values in the output.
  3. Do not include any other text, explanation, or commentary.
  
  Formatting rules:
  - Each template line must be of the form: |Key=Value
  - Acronym must be abbreviation of event with year, e.g. ICWE 2024
  - Title is full title of the given event, e.g. 24th International Conference on Web Engineering
  - Ordinal of the event and it must be between 1 to 200 and must not be a year. e.g 24 for 24th event, 1 for 1st
  - Type must be one of Conference, Workshop, Tutorial, Symposium
  - Series must be abbreviation of event series, e.g. ICWE
  - Field must be a primary scientific field of the event
  - Dates keys (Start date, End date, Submission deadline, Abstract deadline, Notification, Camera ready) must use YYYY/MM/DD format.
  - Keys (Has host organization, has general chair, has program chair) must be names of organizations or persons.
  - Keys (Submitted papers, Accepted papers, Accepted short papers) must be positive integer and cannot be zero or unknown.
  Produce only the template.
"""
  return prompt



def fix_event_wikitext(api_url: str, page_titles: List[str], session, csrf_token: str, llm_api_key: Optional[str], dry_run: bool, logger):
    for idx, page_title in enumerate(page_titles):  # limit to first 10 for testing
        logger.info(f"Processing page {idx}:{page_title}")
        # fix duplicates and clean template using LLM if needed
        try:
            event_wikitext = get_page_wikitext(api_url, page_title, session)
            pre_src_pattern = r"=== Sources ===\s*\}}\s*"
            src_pattern = r"=== Sources ===\s*\}\s*"
            ordinal_re = re.compile(r'\|\s*ordinal\s*=\s*["\']?\s*\d{4}\s*["\']?', re.IGNORECASE)
            event_wikitext = re.sub(pre_src_pattern, '', event_wikitext)
            event_wikitext = re.sub(src_pattern, '', event_wikitext)
            event_wikitext = ordinal_re.sub('', event_wikitext)
            # clean papers count lines
            event_wikitext = clean_submitted_papers(event_wikitext)
            event_wikitext = clean_accepted_papers(event_wikitext)
            event_wikitext = clean_accepted_short_papers(event_wikitext)
            # summary
            summary = f"Cleaned {page_title} with new text {event_wikitext}"
            res = edit_page(api_url, page_title, event_wikitext, csrf_token, session, summary, dry_run)
            if res.get('error'):
                logger.error("Edit result for page_title %s: result: %s", page_title, res['error']['code'])
        except Exception as e:
            logger.error("Get Event Wikitext Exception for %s: %s", page_title, e)
            continue
            
    
def fix_event_duplicates(api_url: str, page_titles: List[str], session, csrf_token: str, llm_api_key: Optional[str], dry_run: bool, logger):
    for idx, page_title in enumerate(page_titles):  # limit to first 10 for testing
        logger.info(f"Processing page {idx}:{page_title}")
        # fix duplicates and clean template using LLM if needed
        try:
            event_wikitext = get_page_wikitext(api_url, page_title, session)
            event_json = extract_event_fields_from_wikitext(event_wikitext)
            logger.debug(f"""Event Page {page_title}: Json: {event_json} event_wikitext: {event_wikitext}""")
            tpl, start, end = find_template_block(event_wikitext, "Event")
            acronym = event_json.get("Acronym", page_title)
            title = event_json.get("Title", page_title)
            logger.debug(f"Template block for {page_title}: {tpl}")
            if llm_api_key \
                and tpl \
                or (((len(acronym.strip().split(' ')) != 2 or (len(acronym.strip().split(' ')) == 2 and acronym.strip().split(' ')[0].isnumeric())) \
                or (len(title.strip().split(' ')) < 4)
                or (len(page_title.strip().split(' ')) != 2 or (len(page_title.strip().split(' ')) == 2 and page_title.strip().split(' ')[0].isnumeric())))):
                logger.info("LLM-assisted cleaning for page_title: %s, acronym: %s, title: %s", page_title, acronym, title)
                prompt = build_clean_event_prompt(event_wikitext)
                llm_output = get_event_template(llm_api_key, prompt)
                logger.debug("LLM output for %s: \n %s", page_title, llm_output)
                if llm_output:
                    fixed, err = fix_event_template(llm_output)
                    logger.debug("LLM template validation for page_title: %s: \n fixed: %s, \n error: %s", page_title, fixed, err)
                    if err:
                        logger.error("LLM template validation failed for page_title: %s: \n error: %s \n llm_output: %s", page_title, err, llm_output)
                        continue
                    new_wikitext = event_wikitext[:start] + fixed + event_wikitext[end:]
                    summary = f"Cleaned {page_title} with new text {new_wikitext} (automated(LLM-assisted))"
                    new_event_json = extract_event_fields_from_wikitext(new_wikitext)
                    logger.debug(f"New Event Page {page_title}: Json: {new_event_json}")
                    new_acronym = new_event_json.get("Acronym", None)
                    new_title = new_event_json.get("Title", None)
                    if new_acronym and new_title:
                      delete_page(api_url, page_title, csrf_token, session)
                      logger.info("Deleted page %s for recreation with cleaned template", page_title)
                      res = create_page(api_url, new_acronym, new_wikitext, csrf_token, session, summary, dry_run)
                      # existing page template block for new_acronym
                      existing_event_wikitext = get_page_wikitext(api_url, new_acronym, session)
                      existing_tpl, _, _ = find_template_block(existing_event_wikitext, "Event")
                      if res.get('error') and existing_tpl!=fixed:
                        logger.error("Edit result for page_title %s: result: %s", page_title, res['error']['code'])
                        summary = f"Recreated cleaned duplicated {page_title} with new text {new_wikitext} (automated(LLM-assisted))"
                        create_page(api_url, new_acronym + ' (Duplicate)', new_wikitext, csrf_token, session, summary, dry_run)
                        continue
                      logger.info("Create result for page_title %s: result: %s", page_title, res['error']['code'] if res.get('error') else res)
        except Exception as e:
          logger.error("Fix Event Duplicate Exception for %s: %s", page_title, e)
          create_page(api_url, page_title, event_wikitext, csrf_token, session, f"Recreated {page_title} after exception {e} (automated)", dry_run)
          logger.info("Recreated page %s after exception %s", page_title, e)
          
def fix_event_ordinal(api_url: str, page_titles: List[str], session, csrf_token: str, llm_api_key: Optional[str], dry_run: bool, logger):
    for idx, page_title in enumerate(page_titles):  # limit to first 10 for testing
        logger.info(f"Processing page {idx}:{page_title}")
        # fix duplicates and clean template using LLM if needed
        try:
            event_wikitext = get_page_wikitext(api_url, page_title, session)
            event_json = extract_event_fields_from_wikitext(event_wikitext)
            logger.debug(f"""Event Page {page_title}: Json: {event_json} event_wikitext: {event_wikitext}""")
            tpl, start, end = find_template_block(event_wikitext, "Event")
            ordinal = event_json.get("Ordinal", None)
            logger.debug(f"Template block for {page_title}: {tpl}")
            if ordinal:
               # extract number from ordinal string e.g. 15th => 15, 1st => 1, 2nd => 2, 3rd => 3, 4th => 4
               ordinal_number = int(''.join(filter(str.isdigit, ordinal)))
               if str(ordinal_number) != str(ordinal) and ordinal_number > 0:
                   logger.info("Fixing ordinal for page_title: %s, ordinal: %s", page_title, ordinal)
                   new_wikitext = set_multiple_params_in_template(event_wikitext, {"Ordinal": ordinal_number}, "Event")[0]
                   summary = f"Fixed ordinal {ordinal} to {ordinal_number} for {page_title} (automated)"
                   res = edit_page(api_url, page_title, new_wikitext, csrf_token, session, summary, dry_run)
                   logger.info("Edit result for page_title %s: result: %s", page_title, res['error']['code'] if res.get('error') else res)
        except Exception as e:
            logger.error("Fix Event Ordinal Exception for %s: %s", page_title, e)
  
@task(name="preprocessing-openresearch-events", description="preprocessing OpenResearch event pages using core data details")
def preprocessing_openresearch_events(
    api_url: str,
    username: str,
    password: str,
    core_all_details_path: str,
    template_name: str = "Stand-alone event",
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
    page_titles = get_event_pages(api_url, session, f"Category:{template_name}")
    logger.info(f"Found {len(page_titles)} pages, e.g., {page_titles[0:50]}")
    
    # fix event duplicates and clean template using LLM if needed
    # fix_event_duplicates(api_url, page_titles, session, csrf_token, llm_api_key, dry_run, logger)
    
    # fix event ordinal
    # fix_event_ordinal(api_url, page_titles, session, csrf_token, llm_api_key, dry_run, logger)
    
    # fix event wikitext
    fix_event_wikitext(api_url, page_titles, session, csrf_token, llm_api_key, dry_run, logger)



          # time.sleep(1)
    return True