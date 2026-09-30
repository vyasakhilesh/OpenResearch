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


PROPERTY_TEMPLATES = {
    # "Has location city": ("{{city_data}}", "{{city data}}", "{{Event", "{{event"),
    "Has location state": ("{{state_data}}", "{{state data}}", "{{Event", "{{event"),
    # "Has location country": ("{{country_data}}", "{{country data}}", "{{Event", "{{event"),
    "Field": ("{{research_field}}", "{{research_field", "{{research field", "{{Event", "{{event")
}


def _normalize_property_values(values):
    normalized = []
    for value in values or []:
        if isinstance(value, dict):
            for key in ("fulltext", "text", "displaytitle", "title", "value"):
                if key in value:
                    normalized.append(value[key])
                    break
            else:
                normalized.append(value)
        else:
            normalized.append(value)
    return [str(v) for v in normalized]


def fix_property_wikitext(
    api_url: str,
    property_name: str,
    members: List[Dict],
    session,
    csrf_token: str,
    llm_api_key: Optional[str],
    dry_run: bool,
    logger,
    template_markers: Optional[tuple] = None,
):
    template_markers = template_markers or PROPERTY_TEMPLATES.get(property_name, ("{{property_data}}", "{{property data}}"))
    page_titles = []
    for page in members:
        raw_values = page.get("printouts", {}).get(property_name, [])
        values = _normalize_property_values(raw_values)
        if values:
            logger.debug(f"Values:{values}")
            # page_titles.append(", ".join(values))
            page_titles.extend(values)
        else:
            logger.warning("No values found for property %s on page %s", property_name, page.get("title", "unknown"))
    
    page_titles = list(set(page_titles))
    total_page_titles = len(page_titles)
    for idx, page_title in enumerate(page_titles):
        logger.info("Processing page %s:%s out of %s", page_title, idx, total_page_titles)
        try:
            property_wikitext = get_page_wikitext(api_url, page_title, session)
            property_wikitext_org = property_wikitext
            logger.debug("Property Wikitext: %s", property_wikitext)
            
            if any(marker in property_wikitext for marker in ("{{Event", "{{event")):
               property_wikitext = property_wikitext.replace(f"{template_markers[0]}\n", "")

            if not any(marker in property_wikitext for marker in template_markers):
                # add the expected template to the beginning of the page text
                property_wikitext = f"{template_markers[0]}\n{property_wikitext}"
                

            if property_wikitext != property_wikitext_org:
                summary = f"Edited {page_title} with new text {property_wikitext}"
                res = edit_page(api_url, page_title, property_wikitext, csrf_token, session, summary, dry_run)
                if res.get("error"):
                    logger.error("Edit result for page_title %s: result: %s", page_title, res["error"]["code"])
                else:
                    logger.info("successfully edited page_title: %s", page_title)
        except Exception as exc:
            logger.error("Get property Wikitext Exception for %s: %s", page_title, exc)
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

    for property_name, template_markers in PROPERTY_TEMPLATES.items():
        members = get_property_members(api_url, session, property_name, printouts=[property_name])
        logger.info("Fetched %s pages for property %s; sample: %s", len(members), property_name, members[:10])
        fix_property_wikitext(
            api_url,
            property_name,
            members,
            session,
            csrf_token,
            llm_api_key,
            dry_run,
            logger,
            template_markers=template_markers,
        )

    return True