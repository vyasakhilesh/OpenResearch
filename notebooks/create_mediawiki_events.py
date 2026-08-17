# Required libraries
import os
import requests
import time
import json
import re
import datetime
from dateutil.relativedelta import relativedelta
from google.colab import userdata
from bs4 import BeautifulSoup
from google.colab import drive
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Dict, Any, List
from datetime import timedelta, timezone, date
from typing import Dict, Iterable, Optional
import pandas as pd
drive.mount('/content/drive')


# Environment variables
OR_API = "https://www.openresearch.org/mediawiki/api.php"
MW_USER = userdata.get('OR_USER') # os.environ.get("OR_USER")
MW_PASS = userdata.get('OR_PASS') # os.environ.get("OR_PASS")
DRY_RUN = False  # set False to actually edit
LLM_API_KEY = userdata.get('OPENROUTER_API_KEY') # os.environ.get("OPENROUTER_API_KEY")  # or other LLM
OPENROUTER_URL = "https://openrouter.ai/api/v1/responses"
OPENROUTER_API_KEY = LLM_API_KEY

# Session
session = requests.Session()
session.headers.update({"User-Agent": "openresearch-core-ranker/1.0 (contact: you@example.org)"})
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429,500,502,503,504], allowed_methods=["POST","GET"])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Get Core Ranking Data
# Define file paths
core_23_path = '/content/drive/MyDrive/Work/MediaWiki/CORE_2023.csv'
core_26_path = '/content/drive/MyDrive/Work/MediaWiki/CORE_2026.csv'

# Load the datasets
df_core_23 = pd.read_csv(core_23_path, header=None)
df_core_26 = pd.read_csv(core_26_path, header=None)

# create dict from acronym and ranking
core_23_dict = dict(zip(df_core_23[2], df_core_23[4]))
core_26_dict = dict(zip(df_core_26[2], df_core_26[4]))


# 1. Login to MediaWiki
# https://www.mediawiki.org/wiki/API:Login
def mw_login():
    r = session.get(OR_API, params={"action":"query","meta":"tokens","type":"login","format":"json"})
    token = r.json()["query"]["tokens"]["logintoken"]
    print("Token: ", token)
    payload = {"action":"login","lgname":MW_USER,"lgpassword":MW_PASS,"lgtoken":token,"format":"json"}
    r = session.post(OR_API, data=payload)
    # print("Data: ", r.json())
    if r.json().get("login",{}).get("result") not in ("Success","NeedToken"):
        raise Exception("Login failed: " + str(r.json()))
    r = session.get(OR_API, params={"action":"query","meta":"tokens","format":"json"})
    return r.json()["query"]["tokens"]["csrftoken"]

# 2. Query series and check for upcoming events
# API: https://www.mediawiki.org/wiki/API:Categorymembers
def get_series_without_upcoming():
    today = datetime.date.today().isoformat()
    # get all pages in Category:Series
    # Limit: "cmlimit":"max"
    params = {"action":"query","list":"categorymembers","cmtitle":"Category:Event series","cmlimit":"max","format":"json"}
    pages = []
    r = session.get(OR_API, params=params).json()
    # print("Page Data:", r)
    pages.extend(r["query"]["categorymembers"])
    series_without = []
    for p in pages:
        title = p["title"]
        ask_query = f'[[Series::{title}]] [[Start date::>={today}]]'
        ask_params = {"action":"ask","query":ask_query,"format":"json"}
        r = session.get(OR_API, params=ask_params).json()
        if not r.get("query",{}).get("results"):
            # print("Ask Data:", r)
            series_without.append(title)
    return series_without


def ci_get(d: dict, key: str):
    """Return value for a key in d matching case-insensitively, or None if not found."""
    if key is None:
        return None
    k_norm = key.casefold()
    for dk, dv in d.items():
        if isinstance(dk, str) and dk.casefold() == k_norm:
            return dv
    return None

def filter_rank_series(series_list, rank=['A*', 'A', 'B', 'C']):
    candidates = []
    for series in series_list:
      rank_26 = ci_get(core_26_dict, series)
      rank_23 = ci_get(core_23_dict, series)
      if rank_26:
        if rank_26 in rank:
           candidates.append(series)
      elif rank_23:
           if rank_26 in rank:
              candidates.append(series)
    return candidates



# Extract Title, Series and Field
FIELD_KEYS = {"series", "title", "field", "acronym"}

def _normalize_key(k: str) -> str:
    return k.strip().lower()

def _strip_quotes_and_ws(s: str) -> str:
    return s.strip().strip('"').strip("'").strip()

def _infer_series_from_acronym(acronym: str) -> Optional[str]:
    if not acronym:
        return None
    a = _strip_quotes_and_ws(acronym)
    # remove common year patterns at end like 2021, '21, (2021), -2021
    a = re.sub(r"[\s\-_]*\(?\b(19|20)\d{2}\b\)?$", "", a)
    a = re.sub(r"[\s\-_]*\b'\d{2}\b$", "", a)
    a = a.strip()
    return a or None

def extract_event_fields(text: str) -> Dict[str, Optional[str]]:
    """
    Parse a MediaWiki Event template block and return a dict with keys:
    'Series', 'Title', 'Field'. Values are strings or None if missing.
    """
    # Find the first {{Event ... }} block (non-greedy)
    m = re.search(r"\{\{\s*Event\b(.*?)\}\}", text, re.DOTALL | re.IGNORECASE)
    if not m:
        return {"Series": None, "Title": None, "Field": None}

    body = m.group(1)

    # Parse lines like |Key=Value
    fields = {}
    for line in body.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        # split at first '='
        parts = line[1:].split("=", 1)
        if len(parts) != 2:
            continue
        key, val = parts
        key_n = _normalize_key(key)
        val_s = _strip_quotes_and_ws(val)
        fields[key_n] = val_s

    # Extract Title and Field directly
    title = fields.get("title") or None
    field = fields.get("field") or None

    # Series resolution order: explicit Series field, then Acronym inference
    series = fields.get("series") or None
    acronym = fields.get("acronym", "") or None

    # Normalize empty strings to None
    def _none_if_empty(x):
        return x if x and x.strip() else None

    return {
        "Title": _none_if_empty(title),
    }

def get_page_wikitext(title):
    r = session.get(OR_API, params={"action":"query","prop":"revisions","rvprop":"content","titles":title,"format":"json"})
    r.raise_for_status()
    pages = r.json().get("query", {}).get("pages", {})
    page = next(iter(pages.values()))
    revs = page.get("revisions", [])
    return revs[0]["*"] if revs else ""

llm_prompt1 ="""
Search on the Web of information about {SERIES_NAME} {TARGET_YEAR}.
Update semantic mediawiki page in OpenResearch.org for the next edition of the {SERIES_NAME} conference in {TARGET_YEAR} according to the following template:

{{Event
|Acronym=ICWE 2024
|Title=The 24th International Conference on Web Engineering
|Ordinal=24
|Series=ICWE
|Type=Conference
|Field=Web Engineering
|Start date=2024/06/17
|End date=2024/06/20
|Submission deadline=2024/02/09
|Homepage=https://icwe2024.webengineering.org/
|City=Tampere
|Country=Finland
|Abstract deadline=2024/02/02
|Notification=2024/03/12
|Camera ready=2024/03/26
|Has host organization=International Society for Web Engineering
|has general chair=Kostas Stefanidis
|has program chair=Kari Systä, Maristella Matera, Sebastian Heil, Haridimos Kondylakis, Elisa Quintarelli
|Submitted papers=66
|Accepted papers=16
|Accepted short papers=8
}}
"""


llm_prompt2 = """Search the web for information about the next edition of the conference series {SERIES_NAME} in {TARGET_YEAR}.
Produce only the Semantic MediaWiki page code using the following Event template. The output must start with two opening curly braces "{{Event" and end with two closing curly braces "}}". Do not add any text before or after the template.
Fill fields with best-known values (Acronym, Title, Ordinal, Series, Type, Field, Start date YYYY-MM-DD, End date YYYY-MM-DD, Submission deadline YYYY-MM-DD, Homepage, City, Country, Abstract deadline, Notification, Camera ready, Has host organization, has general chair, has program chair, Submitted papers, Accepted papers, Accepted short papers).
If a field is unknown, leave it blank but keep the field present.

Use the example for conference series ICWE 2024 below as the exact format to produce:
{{Event
|Acronym=ICWE 2024
|Title=The 24th International Conference on Web Engineering
|Ordinal=24
|Series=ICWE
|Type=Conference
|Field=Web Engineering
|Start date=2024/06/17
|End date=2024/06/20
|Submission deadline=2024/02/09
|Homepage=https://icwe2024.webengineering.org/
|City=Tampere
|Country=Finland
|Abstract deadline=2024/02/02
|Notification=2024/03/12
|Camera ready=2024/03/26
|Has host organization=International Society for Web Engineering
|has general chair=Kostas Stefanidis
|has program chair=Kari Systä, Maristella Matera, Sebastian Heil, Haridimos Kondylakis, Elisa Quintarelli
|Submitted papers=66
|Accepted papers=16
|Accepted short papers=8
}}

"""

llm_prompt3 = """
Task:: Find the given information (Acronym, Title, Ordinal, Series, Type, Field, Start date YYYY-MM-DD, End date YYYY-MM-DD, Submission deadline YYYY-MM-DD, Homepage, City, Country, Abstract deadline, Notification, Camera ready, Has host organization, has general chair, has program chair, Submitted papers, Accepted papers, Accepted short papers) about the event {TITLE} ({SERIES_NAME} {TARGET_YEAR}) of year {TARGET_YEAR} with best-known values to fill fields of the event template.
       You can search the official event website, the conference or event proceedings (publisher page / DBLP), and community trackers (OpenAccept, CS conference stats) to get event general information and submission statistics.
       If a field of template is unknown or not determined, then remove the field completely. The output template must start with two opening curly braces "{{Event" and end with two closing curly braces "}}". Do not add any reasoning, explanatory text before, after and inside the template, surrounding code fences, and additional keys.

Example:: below is the example template for the event International Conference on Web Engineering (ICWE 2024) of year 2024:
          {{Event
          |Acronym=ICWE 2024
          |Title=The 24th International Conference on Web Engineering
          |Ordinal=24
          |Series=ICWE
          |Type=Conference
          |Field=Web Engineering
          |Start date=2024/06/17
          |End date=2024/06/20
          |Submission deadline=2024/02/09
          |Homepage=https://icwe2024.webengineering.org/
          |City=Tampere
          |Country=Finland
          |Abstract deadline=2024/02/02
          |Notification=2024/03/12
          |Camera ready=2024/03/26
          |Has host organization=International Society for Web Engineering
          |has general chair=Kostas Stefanidis
          |has program chair=Kari Systä, Maristella Matera, Sebastian Heil, Haridimos Kondylakis, Elisa Quintarelli
          |Submitted papers=66
          |Accepted papers=16
          |Accepted short papers=8
          }}

"""

llm_prompt3.format(TITLE="The 24th International Conference on Web Engineering", SERIES_NAME="ICWE", TARGET_YEAR=2024)

llm_prompt4 = """Search the web or use the web tool to get information about the next edition of the conference series {SERIES_NAME} {TARGET_YEAR}.
First, read the field descriptions below and confirm you understand them. Then fill only the Semantic MediaWiki page code using the example template for conference series ICWE 2024:

Field descriptions:
Acronym: Acronym of the event such as "ESWC 2009".
Title: Full title of the given event, e.g. "5th European Semantic Web Conference"
Ordinal: Ordinal of the event e.g. 1 for 1st.
Series: Abbreviation of event series, in case the event belongs to a continuing series
Type: Type of event e.g. "Conference", "Workshop", "Symposium", "Tutorial" etc.
Field: Primary scientific field of the event
Start date: Start date of the event in YYYY-MM-DD format
End date: End date of the event in YYYY/MM/
Submission deadline: General deadline for (most relevant kinds of) submissions in YYYY-MM-DD format
Homepage: Official homepage URL of the event
City: City of the event
Country: Country of the event
Abstract deadline: Abstract deadline of the event in YYYY-MM-DD format
Notification: Notification date of the event
Camera ready: Camera ready date of the event
Has host organization: Host organization of the event
has general chair: General chair of the event
has program chair: Program chair of the event
Submitted papers: Number of submitted papers
Accepted papers: Number of accepted papers
Accepted short papers: Number of accepted short papers


Example Template:
{{Event
|Acronym=ICWE 2024
|Title=The 24th International Conference on Web Engineering
|Ordinal=24
|Series=ICWE
|Type=Conference
|Field=Web Engineering
|Start date=2024/06/17
|End date=2024/06/20
|Submission deadline=2024/02/09
|Homepage=https://icwe2024.webengineering.org/
|City=Tampere
|Country=Finland
|Abstract deadline=2024/02/02
|Notification=2024/03/12
|Camera ready=2024/03/26
|Has host organization=International Society for Web Engineering
|has general chair=Kostas Stefanidis
|has program chair=Kari Systä, Maristella Matera, Sebastian Heil, Haridimos Kondylakis, Elisa Quintarelli
|Submitted papers=66
|Accepted papers=16
|Accepted short papers=8
}}


The output must start with two opening curly braces "{{Event" and end with two closing curly braces "}}". Do not add any text before or after the template.
Fill fields ((Acronym, Title, Ordinal, Series, Type, Field, Start date YYYY-MM-DD, End date YYYY-MM-DD, Submission deadline YYYY-MM-DD, Homepage, City, Country, Abstract deadline, Notification, Camera ready, Has host organization, has general chair, has program chair, Submitted papers, Accepted papers, Accepted short papers)) with best-known values.
Search the official conference website, the conference proceedings (publisher page / DBLP), and community trackers (OpenAccept, CS conference stats) to get total submitted papers and total accepted papers.
If a field is unknown or not determined, then remove the field completely.

"""

llm_prompt5 = """
Task:: Find the given information (Acronym, Title, Ordinal, Series, Type, Field, Start date, End date, Submission deadline, Homepage, City, Country, Abstract deadline, Notification, Camera ready, Has host organization, has general chair, has program chair, Submitted papers, Accepted papers, Accepted short papers) about the event {TITLE} ({SERIES_NAME} {TARGET_YEAR}) of year {TARGET_YEAR} with best-known values to fill keys of the event template.
You must search the official event website, the conference proceedings (publisher pages / DBLP / https://dblp.org/), and community trackers (OpenAccept, CS conference stats) to gather evidence. For each key you include, provide a single best-known value.
Output requirements::
  1. Produce the filled template exactly in wiki key format starting with two opening curly braces "{{Event" and ending with two closing curly braces "}}".
  2. Immediately after the closing "}}", on a new line, include a "Sources:" section that lists one evidence URL per line. The "Sources:" section must be separated from the template by two blank lines.
  3. Do not include any other text, explanation, or commentary.
Formatting rules:
  - Each template line must be of the form: |Key=Value
  - Acronym must be abbreviation of event with year, e.g. ICWE 2024
  - Ordinal is positive integer value
  - Type must be one of Conference, Workshop, Tutorial, Symposium
  - Series must be abbreviation of event series, e.g. ICWE
  - Field must be a primary scientific field of the event
  - Dates keys (Start date, End date, Submission deadline, Abstract deadline, Notification, Camera ready) must use YYYY-MM-DD format.
  - Keys (Has host organization, has general chair, has program chair) must be names of organizations or persons.
  - Keys (Submitted papers, Accepted papers, Accepted short papers) must be positive integer.
  - If value of key is unknown or cannot be reliably determined with full confidence then remove that key entirely.
  - Prefer primary sources (official site, proceedings, DBLP, OpenAccept) and include those URLs in the Sources list.
Produce only the template followed by two blank lines and then the Sources list.
"""

llm_prompt5.format(TITLE="The 24th International Conference on Web Engineering", SERIES_NAME="ICWE", TARGET_YEAR=2024)


def call_openrouter(prompt: str,
                    model: str = "openai/gpt-5.4-mini", # "deepseek/deepseek-v3.2"
                    temperature: float = 0.0,
                    max_output_tokens: int = 2000) -> dict:
    if not OPENROUTER_API_KEY or OPENROUTER_API_KEY.startswith("YOUR_"):
        raise RuntimeError("OPENROUTER_API_KEY not set in environment.")
    payload = {
        "model": model,
        "input": prompt,
        "temperature": temperature,
        "max_output_tokens": max_output_tokens,
        "tools":[{ "type": "openrouter:web_search", "search_context_size": "medium"}],
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENROUTER_API_KEY}"
    }
    resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()

def extract_text_from_response(resp_json: dict) -> Optional[str]:
    # Common OpenRouter shapes: 'output' -> list -> dict with 'content' -> list of dicts with 'text'
    out = resp_json.get("output")
    if isinstance(out, list) and out:
        first = out[0]
        if isinstance(first, dict):
            content = first.get("content")
            if isinstance(content, list):
                texts = []
                for c in content:
                    if isinstance(c, dict) and "text" in c and isinstance(c["text"], str):
                        texts.append(c["text"])
                if texts:
                    return "\n".join(texts).strip()
    # Fallback: 'choices' -> list -> 'message' -> 'content' or 'text'
    choices = resp_json.get("choices")
    if isinstance(choices, list) and choices:
        choice = choices[0]
        if isinstance(choice, dict):
            msg = choice.get("message") or choice.get("delta") or {}
            if isinstance(msg, dict):
                content = msg.get("content") or msg.get("text")
                if isinstance(content, str):
                    return content.strip()
    # Top-level text keys
    for key in ("text", "response", "result"):
        val = resp_json.get(key)
        if isinstance(val, str):
            return val.strip()
    return None

def split_template_and_sources(text: str) -> (Optional[str], List[str]):
    """
    Extract the {{Event ... }} template and the Sources list that follows.
    Returns (template_text, list_of_urls). If extraction fails, returns (None, []).
    """
    # Find the template block starting with {{Event and ending with }}
    template_match = re.search(r"(\{Event[\s\S]*?\})", text) # re.search(r"(\{\{Event[\s\S]*?\}\})", text) or
    if not template_match:
        return None, []
    template = template_match.group(1).strip()

    # The remainder after the template
    remainder = text[template_match.end():].strip()

    # Look for a "Sources:" header (case-insensitive) and capture following lines
    sources = []
    if remainder:
        # Accept "Sources:" or "References:" as header
        m = re.search(r"(?i)^(Sources|References)\s*:\s*\n?(.*)$", remainder, re.S)
        if m:
            body = m.group(2).strip()
        else:
            # If no explicit header, treat remainder as potential list of URLs
            body = remainder

        # Extract URLs from the body
        urls = re.findall(r"https?://[^\s\]\)]+", body)
        # Deduplicate while preserving order
        seen = set()
        for u in urls:
            if u not in seen:
                seen.add(u)
                sources.append(u)
    return template, sources


def get_event_template(title, series_name, target_year=2026):
    try:
        prompt = llm_prompt5.format(TITLE=title, SERIES_NAME=series_name, TARGET_YEAR=target_year)
        resp_json = call_openrouter(prompt)
        # print (resp_json)
    except Exception as e:
        print("OpenRouter request failed:", e)
        return None

    text = extract_text_from_response(resp_json)
    if not text:
        # If no single text block found, print full JSON for debugging
        print(json.dumps(resp_json, indent=2, ensure_ascii=False))
        return None

    template, sources = split_template_and_sources(text)

    if template:
        if sources:
            return template + "\n\n" + "\n=== Sources ===\n" + "\n\n".join(sources)
        else:
            # If the model did not provide explicit URLs, attempt to print any remaining text after template
            remainder = text.split(template, 1)[1].strip()
            if remainder:
                return template + "\n\n" + "\n=== Sources ===\n" + remainder
            else:
                return template
    else:
        # If template extraction failed, print the raw model output for inspection
        print(f"{title} ({series_name} {target_year}): Could not locate a {{Event ... }} template in the model output. Full output below:\n")
        print(text)
        return None

def fix_and_validate_event_template(text):
    """
    Find an Event template that may use single or double braces, fix it so it
    starts with '{{Event' and ends with '}}', validate required fields, and
    return the fixed template plus the remainder of the text.

    Returns: (fixed_text, error) where error is None on success or a string.
    """
    t = text or ""
    # Try to find a double-brace template first, non-greedy, DOTALL so '.' matches newlines
    m = re.search(r"\{\{Event\b(.*?)\}\}", t, flags=re.S)
    if not m:
        # Fallback: single-brace template {Event ... }
        m = re.search(r"\{Event\b(.*?)\}", t, flags=re.S)

    if not m:
        return None, "No Event template found"

    body = m.group(1)  # content after Event up to the matched closing brace(s)
    start, end = m.span()

    # Normalize whitespace: ensure body starts with a newline and does not have leading/trailing spaces
    body = body.strip("\n\r")
    # Rebuild template with exact delimiters
    fixed_template = "{{Event\n" + body + "\n}}"

    # Basic validation: required fields present inside the fixed template
    required = ['|Acronym=', '|Title=', '|Start date=', '|End date=', '|City=', '|Country=']
    missing = [f for f in required if f not in fixed_template]
    if missing:
        return None, f"Missing required fields: {missing}"

    # Check balanced double braces count
    if fixed_template.count("{{") != fixed_template.count("}}"):
        return None, "Unbalanced double braces after fix"

    # Reconstruct full text: replace the original matched span with the fixed template
    fixed_text = t[:start] + fixed_template + t[end:]

    return fixed_text, None

# 5. Create page
# https://www.mediawiki.org/wiki/API:Edit
def create_page(title, content, summary, csrf_token):
    if DRY_RUN:
        print("DRY RUN: would create", title)
        return {"result":"dryrun"}
    payload = {
        "action":"edit",
        "title": title,
        "text": content,
        "token": csrf_token,
        # "createonly": True,
        "format": "json"
    }
    r = session.post(OR_API, data=payload)
    return r.json()

# 6. Delete page
def delete_page(title, csrf_token):
    # Delete page if already exist
    r = session.post(OR_API, data={'action':"delete", 'title':title, 'token':csrf_token, 'format':"json"})
    return r.json()


def page_exists(title, csrf_token):
    params = {
        "action": "query",
        "titles": title,
        "format": "json",
        "prop": "info",
        "inprop": "url",
    }
    r = session.get(OR_API, params=params)
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


# Redit the pages which are done by a user
def _to_utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def get_pages_by_user_on_date(
    session,
    date: datetime,                     # date (any tz) — only the date portion is used
    namespaces: Iterable[int] = (0,),   # namespaces to search
    rc_limit: int = 500,
    username: str = 'Akhilesh'
) -> Dict[str, dict]:
    """
    Find pages edited by `username` on `date`.
    """

    # Normalize date to UTC midnight and compute next day
    # date_midnight = datetime(date.year, date.month, date.day, tzinfo=date.tzinfo or timezone.utc)
    # date_midnight = date_midnight.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    next_day = date + timedelta(days=2)

    rcstart = _to_utc_iso(date)
    rcend = _to_utc_iso(next_day)

    # 1) Collect titles edited by the user on that date
    titles = set()
    params = {
        "action": "query",
        "format": "json",
        "list": "recentchanges",
        "rcprop": "title|timestamp|ids",
        "rclimit": str(rc_limit),
        "rctype": "edit|new",
        "rcuser": username,
        "rcstart": rcstart,
        "rcend": rcend,
        "rcdir": "newer",
        "rcnamespace": "|".join(str(n) for n in namespaces),
    }

    cont = {}
    while True:
        # merge continuation if present
        req_params = params.copy()
        req_params.update(cont)
        r = session.get(OR_API, params=req_params)
        r.raise_for_status()
        data = r.json()

        for rc in data.get("query", {}).get("recentchanges", []):
            title = rc.get("title")
            if title:
                titles.add(title)

        if "continue" in data:
            cont = data["continue"]
        else:
            break

    if not titles:
        return {}

    return titles

page_edit_list = get_pages_by_user_on_date(session, datetime.datetime.fromisoformat("2026-04-15T16:32:00+00:00"))
print(page_edit_list)

def create_page_by_year(candidates, target_year, csrf):
    for s in candidates:
        page_title = f"{s} {target_year}"
        summary = f"Added upcoming edition for {s} {target_year} (automated)"
        wikitext = get_page_wikitext(s)
        text_json = extract_event_fields(wikitext)
        title = text_json.get('Title', None)

        if page_exists(page_title, csrf): # and page_title not in page_edit_list:
          # print(f"{page_title} already exists — skipping.")
          continue
        try:
          llm_output = get_event_template(title, s, target_year)
          print ("LLM Output: ", llm_output)
          fixed, err = fix_and_validate_event_template(llm_output)
          print ("Fixed Output: ", fixed)
          if err:
            print(f"{page_title} validation error: {err}")
            continue
          # print (delete_page(page_title, csrf))
          res = create_page(page_title, fixed, summary, csrf)
          print(page_title, "result:", res['error']['code'] if res.get('error') else res)
          time.sleep(1)
        except Exception as e:
          print(f"Exception for {page_title}: {e}")
          time.sleep(1)



csrf = mw_login()
series_list = get_series_without_upcoming()
print("Series without upcoming:", len(series_list), series_list)
candidates = filter_rank_series(series_list, ['A*', 'A', 'B'])
print("Candidate series needing new edition:", len(candidates), candidates)
# target year (e.g., next calendar year)
# target_year = datetime.date.today().year + 1
# nine_months_from_today = date.today() + relativedelta(months=+9)
# target_year = nine_months_from_today.year
# print ("Target year type: ", type(target_year))
target_year_list = [2021, 2022, 2023, 2024, 2025, 2026]
for target_year in target_year_list:
  create_page_by_year(candidates, target_year, csrf)


