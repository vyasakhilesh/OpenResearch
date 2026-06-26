from prefect import task, get_run_logger
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
from typing import List, Tuple, Optional, Dict

DEFAULT_USER_AGENT = "openresearch-core-ranker/1.0 (contact: you@example.org)"

@task
def get_event_pages(api_url: str, session, category_title: str = "Category:Stand-alone event") -> List[str]:
    params = {"action":"query","list":"categorymembers","cmtitle":category_title, "cmlimit":"max","format":"json"}
    titles: List[str] = []
    while True:
        r = session.get(api_url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        members = data.get("query", {}).get("categorymembers", [])
        for m in members:
            titles.append(m["title"])
        if "continue" in data:
            params.update(data["continue"])
        else:
            break
    return titles

@task
def get_eventSeries_pages(api_url: str, session, category_title: str = "Category:Event series") -> List[str]:
    params = {"action":"query","list":"categorymembers","cmtitle":category_title, "cmlimit":"max","format":"json"}
    titles: List[str] = []
    while True:
        r = session.get(api_url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        members = data.get("query", {}).get("categorymembers", [])
        for m in members:
            titles.append(m["title"])
        if "continue" in data:
            params.update(data["continue"])
        else:
            break
    return titles

@task
def get_page_wikitext(api_url: str, title: str, session) -> str:
    r = session.get(api_url, params={"action":"query","prop":"revisions","rvprop":"content","titles":title,"format":"json"}, timeout=30)
    r.raise_for_status()
    pages = r.json().get("query", {}).get("pages", {})
    page = next(iter(pages.values()))
    revs = page.get("revisions", [])
    return revs[0]["*"] if revs else ""

# 5. Create page
# https://www.mediawiki.org/wiki/API:Edit
@task
def create_page(api_url: str, title: str, content: str, csrf_token: str, summary: str,  session, dry_run: bool = True) -> dict:
    if dry_run:
        print("DRY RUN: would create", title)
        return {"result":"dryrun"}
    payload = {
        "action":"edit",
        "title": title,
        "text": content,
        "token": csrf_token,
        "createonly": True,
        "format": "json",
        "summary": summary,
    }
    r = session.post(api_url, data=payload)
    return r.json()

# Edit Page
@task
def edit_page(api_url: str, title: str, new_text: str, csrf_token: str, session, summary: str = "Update core ranking", dry_run: bool = True) -> dict:
    logger = get_run_logger()
    if dry_run:
        logger.info(f"DRY RUN: would edit {title}")
        return {"result": "dry-run", "title": title, "summary": summary}
    payload = {
        "action": "edit",
        "title": title,
        "text": new_text,
        "token": csrf_token,
        "format": "json",
        "summary": summary,
        "bot": True
    }
    # logger.info(f"Editing page {title} with summary: {summary} and text: {new_text}...")
    r = session.post(api_url, data=payload, timeout=60)
    r.raise_for_status()
    return r.json()


# 6. Delete page
@task
def delete_page(api_url: str, title: str, csrf_token: str, session):
    # Delete page if already exist
    r = session.post(api_url, data={'action':"delete", 'title':title, 'token':csrf_token, 'format':"json"})
    return r.json()

# get series titles
@task
def get_series_titles(api_url: str, session, title:str="Category:Event series") -> List[str]:
    params = {"action":"query","list":"categorymembers","cmtitle":title,"cmlimit":"max","format":"json"}
    titles = []
    while True:
        r = session.get(api_url, params=params)
        r.raise_for_status()
        data = r.json()
        members = data.get("query", {}).get("categorymembers", [])
        for m in members:
            titles.append(m["title"])
        if "continue" in data:
            params.update(data["continue"])
        else:
            break
    return titles

# Existing page
@task
def page_exists(api_url: str, title: str, session):
    params = {
        "action": "query",
        "titles": title,
        "format": "json",
        "prop": "info",
        "inprop": "url",
    }
    r = session.get(api_url, params=params)
    r.raise_for_status()
    data = r.json()
    # print(data)
    pages = data.get("query", {}).get("pages", {})
    # print(pages)
    if not pages:
        return False
    page_info = next(iter(pages.values()))
    # non-existing pages with pageid = -1
    return "missing" not in page_info