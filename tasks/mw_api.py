from prefect import task, get_run_logger
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
from typing import List, Tuple, Optional, Dict

DEFAULT_USER_AGENT = "openresearch-core-ranker/1.0 (contact: you@example.org)"

@task
def get_event_members(api_url: str, session, category_title: str = "Category:Event") -> List[str]:
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
def get_eventSeries_members(api_url: str, session, category_title: str = "Category:Event series") -> List[str]:
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
def get_category_members(api_url: str, session, category_title: str) -> List[str]:
    params = {"action":"query","list":"categorymembers","cmtitle":category_title, "cmtype":"subcat", "cmlimit":"max","format":"json"}
    titles: List[str] = []
    while True:
        r = session.get(api_url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
<<<<<<< HEAD
        print(f"get_category_members: data: {data}")
=======
        # print(f"get_category_members: data: {data}")
>>>>>>> 0aa267f (added property and catefgory function)
        members = data.get("query", {}).get("categorymembers", [])
        for m in members:
            titles.append(m["title"])
        if "continue" in data:
            params.update(data["continue"])
        else:
            break
    return titles

@task
def get_property_usage_count(api_url: str, session, property_name):
    params = {"action": "browsebyproperty", "property": property_name, "format": "json",}
    r = session.get(api_url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if "error" in data:
        raise RuntimeError(f"API error: {data['error']}")

    for info in data.get("query", {}).values():
        if isinstance(info, dict) and "usageCount" in info:
            return info["usageCount"]
    return None


def get_property_members(api_url: str, session, property_name, printouts=None, page_size=500, max_results=None):
    session.headers.update({"User-Agent": "SMW-Property-Usage-Fetcher/1.0 (you@example.org)"})
    printouts = printouts or [property_name]
    print_part = "|".join(f"?{p}" for p in printouts)
    results = []
    offset = 0

    while True:
        query = f"[[{property_name}::+]]|{print_part}|limit={page_size}|offset={offset}"
        params = {
            "action": "ask",
            "query": query,
            "format": "json",
        }

        resp = session.get(api_url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise RuntimeError(f"API error: {data['error']}")

        batch = data.get("query", {}).get("results", {})
        if isinstance(batch, dict):
            for title, info in batch.items():
                results.append({
                    "title": info.get("fulltext", title),
                    # "url": info.get("fullurl"),
                    # "namespace": info.get("namespace"),
                    "printouts": info.get("printouts", {}),
                })

        if max_results and len(results) >= max_results:
            return results[:max_results]

        next_offset = data.get("query-continue-offset")
        if next_offset is None:
            break
        offset = next_offset

    return results

@task
def get_all_categories(api_url: str, session, category_title: str = "") -> List[str]:
    params = {"action":"query","list":"allcategories", "aclimit":"max", "format":"json"}
    if category_title:
        params["acprefix"] = category_title

    titles: List[str] = []

    while True:
        response = session.get(url=api_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        categories = data.get("query", {}).get("allcategories", [])
        for category in categories:
            titles.append(category["*"])

        continuation = data.get("continue")
        if not continuation:
            break

        params.update(continuation)

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
def create_page(api_url: str, title: str, content: str, csrf_token: str, session, summary: str, dry_run: bool = True) -> dict:
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