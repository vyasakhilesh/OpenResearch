# Required libraries
import os
import requests
import datetime
import time
import json
import re
import ast
from datetime import date
from dateutil.relativedelta import relativedelta
from google.colab import userdata
from bs4 import BeautifulSoup
from google.colab import drive
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Dict, Any, List
from sys import exception
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
TEMPLATE_NAME = "Event"
PARAMS_TO_SET = ["City", "State", "Country"]
global csrf_token

# Session
session = requests.Session()
session.headers.update({"User-Agent": "openresearch-core-ranker/1.0 (contact: you@example.org)"})
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429,500,502,503,504], allowed_methods=["POST","GET"])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Get Core Ranking Data
# Define file paths
core_23_path = '/content/drive/MyDrive/Work/MediaWiki/CORE_23.csv'
core_26_path = '/content/drive/MyDrive/Work/MediaWiki/CORE_26.csv'

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

def mw_logout(csrf):
  r = session.post(OR_API, data={"action": "logout", "token": csrf, "format": "json"}, timeout=(5, 60))
  r.raise_for_status()
  return r.json()

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



def get_page_wikitext(title):
    r = session.get(OR_API, params={"action":"query","prop":"revisions","rvprop":"content","titles":title,"format":"json"})
    r.raise_for_status()
    pages = r.json().get("query", {}).get("pages", {})
    page = next(iter(pages.values()))
    revs = page.get("revisions", [])
    return revs[0]["*"] if revs else ""

llm_prompt6 = """
Task:: Find the following information (Acronym, Title, Ordinal, Series, Type, Field, Start date YYYY/MM/DD, End date YYYY/MM/DD, Submission deadline YYYY/MM/DD, Homepage URL, City, State, Country, Abstract deadline YYYY/MM/DD, Notification YYYY/MM/DD, Camera ready YYYY/MM/DD, Has host organization, Has general chair, Has program chair, Submitted papers, Accepted papers, Accepted short papers) about the event {TITLE} ({SERIES_NAME} {TARGET_YEAR}) of year {TARGET_YEAR}.

Evidence and sources requirements::
  1. Search the official event website first, then the conference proceedings (publisher pages / DBLP), then community trackers (OpenAccept, CS Conference Stats) to gather evidence.
  2. Prefer primary sources (official site, proceedings, DBLP, OpenAccept).
  3. For every key you include in the JSON object, attach one or more evidence URLs that directly support that value.
  4. The JSON object must include both a per-key evidence mapping and a consolidated list of source URLs.

 Output requirements::
    1. Produce exactly one valid JSON object and nothing else.
    2. The JSON object must contain only the event keys (only those that could be reliably determined), plus two additional fields:
       a. "evidence": an object mapping each included event key to a list of one or more evidence URLs used to determine that key's value.
       b. "sources": a list of unique evidence URLs (one per entry) that together support the JSON values; this list must equal the union of all URLs appearing in the "evidence" lists.
    3. Dates must use YYYY/MM/DD format.
    4. If a key value is unknown or cannot be reliably determined, omit that key entirely from the JSON object (do not include it with null or empty values).
    5. If a key is present, its entry in the "evidence" object must be a non-empty list of full URLs (each starting with http:// or https://) that directly support that key's value.
    6. The "sources" list must contain only full URLs (one per list item) and must include every URL referenced in "evidence".
    7. Do not repeat URLs inside the "sources" list; each URL must appear only once.
    8. Name fields (Has host organization, Has general chair, Has program chair) must be string.
    9. Numeric fields (Submitted papers, Accepted papers, Accepted short papers, Ordinal) must be integers.
    10. URL fields (e.g., Homepage URL) must be full URLs starting with http:// or https://.
    11. Provide only one best-known value per key.
    12. Do not include any other text, commentary, or metadata before, inside, or after the JSON object.


Formatting rules::
    1. The JSON object must be valid JSON (use double quotes for keys and string values).
    2. Use the exact key names listed above.
    3. The "evidence" field must be an object whose keys are the same event keys included in the JSON and whose values are arrays of URL strings.
    4. The "sources" field must be an array of URL strings.
Final delivery:
    1. Produce only the single JSON object described above and nothing else."""

llm_prompt7="""
Task:: Find the following information (City, State, Country) about event {TITLE} ({ACRONYM}) of year {TARGET_YEAR} with best-known values to fill keys of the event.
Scope requirements::
  1. Search the official event website first, then the conference proceedings (publisher pages / DBLP), then community trackers (OpenAccept, CS Conference Stats) to gather information.
  2. Prefer primary sources (official site, proceedings, DBLP, OpenAccept).

Output requirements::
  1. Produce exactly one valid JSON object and nothing else.
  2. The JSON object must contain only the event keys (only those that could be reliably determined):
     a. "Year": integer or None
     b. "City": string or None
     c. "State": string or None
     d. "Country": string or None
     e. "Confidence": string, one of "high", "medium", or "low"
  3. Do not include any other explanatory text, commentary, metadata, surrounding code fences or additional keys before, inside, or after the JSON object.

Formatting rules::
   1. The JSON object must be valid JSON (use double quotes for keys and string values).
   2. Use the exact key names listed above.
"""

llm_prompt7.format(TITLE="The 24th International Conference on Web Engineering", ACRONYM="ICWE", TARGET_YEAR=2024)


def call_openrouter(prompt: str,
                    model: str = "openai/gpt-5.4-mini", # "deepseek/deepseek-v3.2",
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


def generate_submission_report(title, acronym, year):
  try:
    prompt = llm_prompt7.format(TITLE=title, ACRONYM=acronym, TARGET_YEAR=year)
    response_json = call_openrouter(prompt, model='deepseek/deepseek-v3.2') #openai/gpt-5.4-mini #anthropic/claude-sonnet-4.6 #deepseek/deepseek-v3.2 #google/gemini-2.5-flash-lite
    text = extract_text_from_response(response_json)
    if not text:
        # If no single text block found, print full JSON for debugging
        print(json.dumps(response_json, indent=2, ensure_ascii=False))
        return None
    return text

    return response_json
  except Exception as e:
      print("OpenRouter request failed:", e)
      return None

def get_paper_report_by_year(response_json, year):
    if response_json.get('Year')==year and response_json.get('Confidence')!='low':
        return response_json.get('City', None), response_json.get('State', None), response_json.get('Country', None)
    return None, None, None

generate_submission_report("The International Conference on Web Engineering", "ICWE", 2024)

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


def get_page_wikitext(title):
    # print(title)
    r = session.get(OR_API, params={"action":"query","prop":"revisions","rvprop":"content","titles":title,"format":"json"}, timeout=(5, 60))
    r.raise_for_status()
    pages = r.json().get("query", {}).get("pages", {})
    page = next(iter(pages.values()))
    revs = page.get("revisions", [])
    return revs[0]["*"] if revs else ""

# 5. Edit page
# https://www.mediawiki.org/wiki/API:Edit
def edit_page(title, new_text, token, summary="Update event information"):
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
    r = session.post(OR_API, data=payload, timeout=(5, 60))
    r.raise_for_status()
    return r.json()


# Template editing
def find_template_block(wikitext, template_name=TEMPLATE_NAME):
    open_pat = re.compile(r'\{\{\s*' + re.escape(template_name) + r'\b', re.IGNORECASE)
    m = open_pat.search(wikitext)
    if not m:
        return None
    start = m.start()
    idx = m.end()
    depth = 2
    while idx < len(wikitext) - 1:
        pair = wikitext[idx:idx+2]
        if pair == '{{':
            depth += 2
            idx += 2
            continue
        if pair == '}}':
            depth -= 2
            idx += 2
            if depth <= 0:
                end = idx
                return wikitext[start:end], start, end
            continue
        idx += 1
    fallback = re.search(r'\}\}', wikitext[m.end():])
    if fallback:
        end = m.end() + fallback.end()
        return wikitext[start:end], start, end
    return None

def parse_template_lines(template_text):
    lines = template_text.splitlines()
    header = lines[0] if lines else ""
    footer = lines[-1] if len(lines) > 1 else ""
    body_lines = lines[1:-1] if len(lines) > 2 else []
    parsed = []
    for raw in body_lines:
        m = re.match(r'^\s*\|\s*([^=]+?)\s*=\s*(.*)$', raw)
        if m:
            name = m.group(1).strip()
            value = m.group(2).strip()
            parsed.append((raw, name, value))
        else:
            parsed.append((raw, None, None))
    return header, parsed, footer

def render_template(header, parsed, footer):
  out = [header]
  for raw, name, value in parsed:
      if name is None:
          out.append(raw)
      else:
          out.append(f"|{name}={value}")
  out.append(footer)
  return "\n".join(out)

def extract_title_text_from_template(tpl_text):
    header, parsed, footer = parse_template_lines(tpl_text)
    for _, name, value in parsed:
        if name and name.strip().lower() in ("title", "title"):
            return value.strip()
    # fallback: try to find an inline |Acronym= or link in header/body
    m = re.search(r'\|\s*Title\s*=\s*([^\n\|]+)', tpl_text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None

def set_multiple_params_in_template(wikitext, params_to_set):
    tpl = find_template_block(wikitext)
    if not tpl:
        return wikitext, False, None
    tpl_text, start, end = tpl
    header, parsed, footer = parse_template_lines(tpl_text)
    def norm(s): return re.sub(r'\s+', '', s).lower()
    # Build dict of existing params for quick lookup
    existing = {norm(name): (i, name, value) for i, (_, name, value) in enumerate(parsed) if name}
    changed = False
    # Update or insert each param
    for pname, pvalue in params_to_set.items():
        key = norm(pname)
        if key in existing:
            idx, orig_name, _ = existing[key]
            parsed[idx] = (parsed[idx][0], orig_name, str(pvalue))
            changed = True
        else:
            parsed.append((f"|{pname}={pvalue}", pname, str(pvalue)))
            changed = True
    new_tpl = render_template(header, parsed, footer)
    new_wikitext = wikitext[:start] + new_tpl + wikitext[end:]
    return new_wikitext, changed, tpl_text

def edit_page_by_year(candidates, target_year, csrf_token):
  for i, series in enumerate(candidates):
      print(f"\n{i}: {series}")
      try:
        acronym = f"{series} {target_year}"
        wikitext = get_page_wikitext(acronym)
        tpl = find_template_block(wikitext)
        # print(wikitext)
        if not tpl:
            print(f"{acronym}: [SKIP] No {TEMPLATE_NAME} template")
            continue
        tpl_text, _, _ = tpl
        title_text = extract_title_text_from_template(tpl_text).strip()

        # print(f"Title Text: {title_text}, Acronym: {acronym}, Target Year: {target_year}")
        if not title_text:
            print(f"{acronym}: [SKIP] No title text ")
            continue

        # Patterns to match lines like: |Submitted papers=9020  or  | Submitted papers = 9020
        city_pattern = re.compile(r"\|\s*City\s*=\s*([^|\n\r]+)", flags=re.I)
        state_pattern = re.compile(r"\|\s*State\s*=\s*([^|\n\r]+)", flags=re.I)
        country_pattern = re.compile(r"\|\s*Country\s*=\s*([^|\n\r]+)", flags=re.I)

        city_match = city_pattern.search(tpl_text)
        state_match = state_pattern.search(tpl_text)
        country_match = country_pattern.search(tpl_text)
        # print (f"{city_match} {state_match} {country_match}")

        if city_match and country_match:
          print(f"{acronym}: [SKIP] Having event information")
          continue

        # Update paper field
        ## LLM Code to generate submission report
        print (title_text, acronym, target_year)
        response_json = generate_submission_report(title_text, acronym, target_year)
        if not response_json:
            print(f"{acronym}: [SKIP] No event information (response)")
            continue
        parsed_response = extract_json_object_from_llm(response_json)
        print(f"{acronym} parsed response: {parsed_response}")
        if not parsed_response:
            print(f"{acronym}: [SKIP] No event information (parsed)")
            continue
        city, state, country = get_paper_report_by_year(parsed_response, target_year)
        # print(f"{acronym} event information: {city}, {state}, {country}")
        # print("wikitext: ", wikitext)
        if city!=None and country!=None and state!=None:
            # Prepare params to set for both years
            params_map = {PARAMS_TO_SET[0]: city, PARAMS_TO_SET[1]: state, PARAMS_TO_SET[2]: country}
            new_wikitext, changed, old_tpl = set_multiple_params_in_template(wikitext, params_map)
            # print ("New Wikitext: ", new_wikitext, changed, old_tpl)
            res = edit_page(acronym, new_wikitext, csrf_token)
            print(acronym, "result:", res['error']['code'] if res.get('error') else res)
        elif city!=None and country!=None:
            params_map = {PARAMS_TO_SET[0]: city, PARAMS_TO_SET[2]: country}
            new_wikitext, changed, old_tpl = set_multiple_params_in_template(wikitext, params_map)
            # print ("New Wikitext: ", new_wikitext, changed, old_tpl)
            res = edit_page(acronym, new_wikitext, csrf_token)
            print(acronym, "result:", res['error']['code'] if res.get('error') else res)
        elif state!=None and country!=None:
            params_map = {PARAMS_TO_SET[1]: state, PARAMS_TO_SET[2]: country}
            new_wikitext, changed, old_tpl = set_multiple_params_in_template(wikitext, params_map)
            # print ("New Wikitext: ", new_wikitext, changed, old_tpl)
            res = edit_page(acronym, new_wikitext, csrf_token)
            print(acronym, "result:", res['error']['code'] if res.get('error') else res)
        else:
          print(f"{acronym}: [SKIP] No complete event information city:{city} state:{state} country:{country}")
          continue

      except requests.exceptions.ConnectionError as e:
          print("Request failed:", e)
          # print ("################ Reconnection successfully ###################")
      except Exception as e:
          print("All Exception:", e)
          # print ("Logout: ", mw_logout(csrf_token))

csrf = mw_login()
series_list = get_series_without_upcoming()
print("Series without upcoming:", len(series_list), series_list)
candidates = filter_rank_series(series_list, ['A*', 'A', 'B'])
print("Candidate series needing new edition:", len(candidates), candidates)
target_year_list = [2021, 2022, 2023, 2024, 2025, 2026]
for target_year in target_year_list:
  edit_page_by_year(candidates, target_year, csrf)


