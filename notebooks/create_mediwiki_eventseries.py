import os
import re
import requests
import pandas as pd
from google.colab import userdata
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sys import exception
from typing import Optional, Dict, Any
import json
from traitlets.config.application import T

# Configuration
OR_API = "https://www.openresearch.org/mediawiki/api.php"
MW_USER = os.environ.get("OR_USER") or userdata.get("OR_USER")
MW_PASS = os.environ.get("OR_PASS") or userdata.get("OR_PASS")
LLM_API_KEY = userdata.get('OPENROUTER_API_KEY') # os.environ.get("OPENROUTER_API_KEY")  # or other LLM
DRY_RUN = False  # Set to False to perform real edits
TEMPLATE_NAME = "Event"
PARAMS_TO_SET = ["Series"]
SMW_PROPERTY_LINE = False  # also add [[Has core ranking::...]] after template if True


session = requests.Session()
session.headers.update({"User-Agent": "openresearch-core-ranker/1.0 (contact: you@example.org)"})
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429,500,502,503,504], allowed_methods=["POST","GET"])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

def mw_login():
    r = session.get(OR_API, params={"action":"query","meta":"tokens","type":"login","format":"json"})
    r.raise_for_status()
    token = r.json()["query"]["tokens"]["logintoken"]
    payload = {"action":"login","lgname":MW_USER,"lgpassword":MW_PASS,"lgtoken":token,"format":"json"}
    r = session.post(OR_API, data=payload)
    r.raise_for_status()
    result = r.json().get("login", {}).get("result")
    if result not in ("Success", "NeedToken"):
        raise RuntimeError(f"Login failed: {r.json()}")
    r = session.get(OR_API, params={"action":"query","meta":"tokens","format":"json"})
    r.raise_for_status()
    return r.json()["query"]["tokens"]["csrftoken"]

def get_event_pages(title="Category:Stand-alone event"):
    params = {"action":"query","list":"categorymembers","cmtitle":title, "cmlimit":"max","format":"json"}
    titles = []
    while True:
        r = session.get(OR_API, params=params)
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

def get_event_series_pages(title="Category:Event series"):
    params = {"action":"query","list":"categorymembers","cmtitle":title, "cmlimit":"max","format":"json"}
    titles = []
    while True:
        r = session.get(OR_API, params=params)
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

def get_page_wikitext(title):
    r = session.get(OR_API, params={"action":"query","prop":"revisions","rvprop":"content","titles":title,"format":"json"})
    r.raise_for_status()
    pages = r.json().get("query", {}).get("pages", {})
    page = next(iter(pages.values()))
    revs = page.get("revisions", [])
    return revs[0]["*"] if revs else ""

def edit_page(title, new_text, token, summary="Update core ranking"):
    if DRY_RUN:
        print(f"[DRY RUN] Would edit: {title}  summary: {summary}")
        return {"result": "dry-run"}
    payload = {
        "action": "edit",
        "title": title,
        "text": new_text,
        "token": token,
        "format": "json",
        "bot": True
    }
    r = session.post(OR_API, data=payload)
    r.raise_for_status()
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



def extract_series_strict_two_tokens(acronym: str) -> str:
    """
    Return series name only for strict two-token inputs and three allowed patterns.
    Examples:
      "ICSLT 2026"     -> "ICSLT"
      "IEEE-CVPR 2023" -> "IEEE-CVPR"
      "2026 ICSLT"     -> "ICSLT"
      "ICSLT--Ei 2018" -> ""   (mixed-case -> ignore)
      "ICSLT 2026 Extra" -> "" (more than two tokens -> ignore)
    """
    _YEAR_RE = re.compile(r'^(?:19|20)\d{2}$')
    _ALLCAPS_TOKEN_RE = re.compile(r'^[A-Z0-9]+(?:(?:--|—|–|-|/|&)[A-Z0-9]+)*$')
    if not acronym:
        return None

    tokens = acronym.strip().split()
    if len(tokens) != 2:
        return None

    a, b = tokens[0].strip(), tokens[1].strip()

    # Case: ALLCAPS <YEAR>
    if _ALLCAPS_TOKEN_RE.match(a) and _YEAR_RE.match(b):
        return a

    # Case: <YEAR> ALLCAPS
    if _YEAR_RE.match(a) and _ALLCAPS_TOKEN_RE.match(b):
        return b

    return None

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
        "Series": _none_if_empty(series),
        "Acronym": _none_if_empty(acronym),
        "Title": _none_if_empty(title),
        "Field": _none_if_empty(field),
    }

llm_prompt6="""
Task: Extract the full Title without edition or ordinal and primary scientific Field of the event from Title: {TITLE}, Acronym: {ACRONYM} and Field: {FIELD} and return structured output as mentioned below.
      If a value is not available, use empty string. Provide no explanatory text, no surrounding code fences, and no additional keys.
Output: Output exactly one python JSON object with these keys and value types only:
        "Title": string or null
        "Field": string or null """

# 3. LLM to generate Event template

def extract_assistant_text(resp):
    """
    Extracts the assistant's final text from a Responses API JSON object.
    """
    # ["choices"][0]["message"]["content"])
    if "choices" not in resp:
        return None

    for item in resp["choices"]:
        if item.get("message", None):
            content = item['message'].get("content", None)
            return content
    return None


def call_llm(prompt, model="anthropic/claude-opus-4.6", temperature=1.0):
    headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type":"application/json"}
    data = {
    "model": model,
    "messages": [
        {"role": "user", "content": prompt}
    ],
    # web search and limit how many searches the model may perform
    "tools":[
                        {
                          "type": "web_search",
                          "user_location": {
                            "type": "approximate"
                          },
                          "search_context_size": "medium"
                        }
                      ],
            "include": [
                        "reasoning.encrypted_content",
                        "web_search_call.action.sources"
                      ],
    # Optional: control max tokens and temperature
    # "max_tokens": 2000,
    "temperature": temperature
}
    try:
      r = requests.post("https://openrouter.ai/api/v1/responses", headers=headers, json=data, timeout=(5, 60)) # https://openrouter.ai/api/v1/chat/completions
      # print("LLM Response: ", r.text)
      # print(r.json()["choices"][0]["message"]["content"])
      # print (r.json())
      text = extract_assistant_text(r.json())
      # print(text)
      return text
    except Exception as e:
      print("LLM Exception: ", e)
      return None

def get_title(title, acronym, field):
    prompt = llm_prompt6.format(TITLE=title, ACRONYM=acronym, FIELD=field)
    response_json = call_llm(prompt, model='deepseek/deepseek-v3.2') #openai/gpt-5.4-mini #anthropic/claude-sonnet-4.6 #deepseek/deepseek-v3.2 #google/gemini-2.5-flash-lite
    return response_json

# Extract JSON object

def _find_code_fence_content(text: str) -> Optional[str]:
    """
    Return the content inside the first Markdown code fence (``` or ```json) if present.
    """
    m = re.search(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.S | re.I)
    if m:
        return m.group(1)
    return None

def _extract_balanced_braces(text: str) -> Optional[str]:
    """
    Find the first balanced JSON object substring starting at the first '{'.
    Uses a simple stack-based brace matcher to handle nested braces.
    """
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    for i, ch in enumerate(text[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i+1]
    return None

def _clean_json_like(text: str) -> str:
    """
    Make conservative cleanups to improve chances of json.loads succeeding:
    - Unescape common escape sequences (turns literal '\n' into newline).
    - Remove trailing commas before } or ].
    - Strip surrounding quotes if the JSON is wrapped in a string literal.
    """
    # Unescape common escape sequences (handles strings like "\\n" -> newline)
    try:
        text = bytes(text, "utf-8").decode("unicode_escape")
    except Exception:
        pass

    # If the JSON is wrapped in quotes (e.g., "\"{...}\""), strip them
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        text = text[1:-1]

    # Remove trailing commas before } or ]
    text = re.sub(r",\s*(\}|])", r"\1", text)

    return text.strip()

def extract_json_object_from_llm(text: str) -> Optional[Any]:
    """
    Attempt to extract and parse a JSON object from an LLM response string.
    Returns the parsed Python object (usually a dict) or None if parsing fails.
    """
    # 1) If there's a code fence, prefer its content
    candidate = _find_code_fence_content(text)
    if candidate is None:
        # 2) Otherwise try to extract the first balanced {...} substring
        candidate = _extract_balanced_braces(text)

    if not candidate:
        return None

    candidate = _clean_json_like(candidate)

    # 3) Try json.loads
    try:
        return json.loads(candidate)
    except Exception:
        pass

    # 4) If json.loads fails, try ast.literal_eval as a fallback (handles Python-style None/True/False and single quotes)
    try:
        return ast.literal_eval(candidate)
    except Exception:
        pass

    # 5) Last-resort: try to coerce single quotes to double quotes and parse again
    coerced = candidate.replace("'", '"')
    try:
        return json.loads(coerced)
    except Exception:
        pass

    return None


# parsed = extract_json_object_from_llm(generate_submission_report("The 20th International Conference on Autonomous Agents and Multi-Agent Systems", "AAMAS 2021", 2021))
# print("Parsed object:", parsed)


csrf_token = mw_login()
titles = get_event_pages()
title_event_sereis = set(get_event_series_pages())
print(f"Found {len(titles)} event pages. e.g. {titles[0:20]}")
print(f"Found {len(title_event_sereis)} event series pages. e.g. {list(title_event_sereis)[0:20]}")

for title in titles:
    wikitext = get_page_wikitext(title)
    text_json = extract_event_fields(wikitext)
    series = text_json['Series']

    if page_exists(series, csrf_token):
          continue
    try:
      if series and series not in title_event_sereis:
          title_event_sereis.add(series)
          print('\n')
          print(extract_event_fields(wikitext))
          event_json = extract_json_object_from_llm(get_title(text_json['Title'], text_json['Acronym'],  text_json['Field']))
          ACRONYM = series
          TITLE = event_json.get('Title', None)
          FIELD = event_json.get('Field', None)
          print(f"ACRONYM: {ACRONYM}, TITLE: {TITLE}, FIELD: {FIELD}")
          new_wikitext= (
    "{{Event series\n"
    f"|Acronym={ACRONYM}\n"
    f"|Title={TITLE}\n"
    f"|Field={FIELD}\n"
    "}}"
)
          summary = f"Created event series {series} page"
          if ACRONYM and TITLE:
              summary = f"Created event series {series} page"
              res = edit_page(series, new_wikitext, csrf_token, summary=summary)
              print(f"[EDITED] {TITLE}: {res}\n")
    except Exception as e:
        print(f"Error processing {title}: {e}")
        print(f"text_json: {text_json}")
        continue
