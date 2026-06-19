from prefect import task, get_run_logger
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
from typing import List, Tuple, Optional, Dict

DEFAULT_USER_AGENT = "openresearch-core-ranker/1.0 (contact: you@example.org)"

def _make_session_from_info(session_info: Optional[dict]) -> requests.Session:
    s = requests.Session()
    ua = (session_info or {}).get("User-Agent", DEFAULT_USER_AGENT)
    s.headers.update({"User-Agent": ua})
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429,500,502,503,504], allowed_methods=["POST","GET"])
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

@task
def get_event_pages(api_url: str, session_info: Optional[dict] = None, category_title: str = "Category:Stand-alone event") -> List[str]:
    s = _make_session_from_info(session_info)
    params = {"action":"query","list":"categorymembers","cmtitle":category_title, "cmlimit":"max","format":"json"}
    titles: List[str] = []
    while True:
        r = s.get(api_url, params=params, timeout=30)
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
def get_page_wikitext(api_url: str, title: str, session_info: Optional[dict] = None) -> str:
    s = _make_session_from_info(session_info)
    r = s.get(api_url, params={"action":"query","prop":"revisions","rvprop":"content","titles":title,"format":"json"}, timeout=30)
    r.raise_for_status()
    pages = r.json().get("query", {}).get("pages", {})
    page = next(iter(pages.values()))
    revs = page.get("revisions", [])
    return revs[0]["*"] if revs else ""

@task
def edit_page(api_url: str, title: str, new_text: str, csrf_token: str, session_info: Optional[dict] = None, summary: str = "Update core ranking", dry_run: bool = True) -> dict:
    s = _make_session_from_info(session_info)
    if dry_run:
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
    r = s.post(api_url, data=payload, timeout=60)
    r.raise_for_status()
    return r.json()