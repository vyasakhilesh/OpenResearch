# Required libraries
import os
import requests
import datetime
import time
import json
import re
import ast
from typing import Any, Optional
from datetime import date
from dateutil.relativedelta import relativedelta
from google.colab import userdata
from bs4 import BeautifulSoup
from google.colab import drive
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sys import exception
# drive.mount('/content/drive')


# Configuration
OR_API = "https://www.openresearch.org/mediawiki/api.php"
MW_USER = os.environ.get("OR_USER") or userdata.get("OR_USER")
MW_PASS = os.environ.get("OR_PASS") or userdata.get("OR_PASS")
LLM_API_KEY = userdata.get('OPENROUTER_API_KEY') # os.environ.get("OPENROUTER_API_KEY")  # or other LLM
DRY_RUN = False  # Set to False to perform real edits
TEMPLATE_NAME = "Event"
PARAMS_TO_SET = ["Submitted papers", "Accepted papers"]
SMW_PROPERTY_LINE = False
global csrf_token

session = requests.Session()
session.headers.update({"User-Agent": "openresearch-core-ranker/1.0 (contact: you@example.org)"})
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429,500,502,503,504], allowed_methods=["POST","GET"])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

# 1. Login to MediaWiki
# https://www.mediawiki.org/wiki/API:Login
def mw_login():
    r = session.get(OR_API, params={"action":"query","meta":"tokens","type":"login","format":"json"}, timeout=(5, 60))
    r.raise_for_status()
    token = r.json()["query"]["tokens"]["logintoken"]
    payload = {"action":"login","lgname":MW_USER,"lgpassword":MW_PASS,"lgtoken":token,"format":"json"}
    r = session.post(OR_API, data=payload, timeout=(5, 60))
    r.raise_for_status()
    result = r.json().get("login", {}).get("result")
    if result not in ("Success", "NeedToken"):
        raise RuntimeError(f"Login failed: {r.json()}")
    r = session.get(OR_API, params={"action":"query","meta":"tokens","format":"json"}, timeout=(5, 60))
    r.raise_for_status()
    return r.json()["query"]["tokens"]["csrftoken"]

def mw_logout(csrf):
    r = session.post(OR_API, params={"action": "logout", "token": csrf, "format": "json"}, timeout=(5, 60))
    r.raise_for_status()
    return r.json()

# 2. Query series and check for upcoming events
# API: https://www.mediawiki.org/wiki/API:Categorymembers
def get_series_titles():
    params = {"action":"query","list":"categorymembers","cmtitle":"Category:Event series","cmlimit":"max","format":"json"}
    titles = []
    while True:
        r = session.get(OR_API, params=params, timeout=(5, 60))
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


llm_prompt1 ="""
Return only a single JSON object and nothing else ( (no explanatory text)).
Search the official publisher proceedings pages for {TITLE} ({ACRONYM}) from year {TARGET_YEAR} through the most recent year available. For {TARGET_YEAR}, open the proceedings front matter (preface/foreword) and extract the number of submitted papers and the number of accepted papers for the main research track as stated by the conference chairs. If the preface does not state a number, use null.
Output exactly one JSON object with these keys and value types only:
"YEAR": integer (e.g., 2021)
"No. of Submitted Papers": integer or null
"No. of Accepted Papers": integer or null
Provide nothing except the JSON object.

"""

llm_prompt2="""
Task: Find the number of submitted and accepted papers for {TITLE} ({ACRONYM}) in {TARGET_YEAR} for the main research track.
Scope: Search the official conference website, the conference proceedings (publisher page / DBLP), and community trackers (OpenAccept, CS conference stats). If official submission counts are not published, find any post‑conference slides or program‑chair statements that report submission totals. Report: (1) total submissions, (2) total accepted papers, (3) acceptance rate"""

llm_prompt3 = """
Task: Output exactly one python JSON object with these keys and value types only:
      "Year": integer (e.g. 2021)
      "No. of Submitted Papers": integer or null
      "No. of Accepted Papers": integer or null
      "Acceptance Rate": float or null
      "Confidence: string, one of "high", "medium", or "low"

      If a value is not available, use null. Provide no explanatory text, no surrounding code fences, and no additional keys.

Scope: {MODEL_RESPONSE}"""

llm_prompt4="""
Task: Find the number of submitted and accepted papers for {TITLE} ({ACRONYM}) from {TARGET_YEAR} onwards for the main research track.
Scope: Search the official conference website, the conference proceedings (publisher page / DBLP), and community trackers (OpenAccept, CS conference stats). If official submission counts are not published, find any post‑conference slides or program‑chair statements that report submission totals. Report: (1) total submissions, (2) total accepted papers, (3) acceptance rate"""

llm_prompt5 = """
Task: Output exactly a Python list (JSON array) of JSON objects and nothing else. Each object must contain only these keys with the specified value types:
      "Year": integer (e.g., 2021)
      "No. of Submitted Papers": integer or null
      "No. of Accepted Papers": integer or null
      "Acceptance Rate": float or null (use a decimal between 0 and 100, e.g., 23.5)
      "Confidence": string, one of "high", "medium", or "low"

      If a value is not available, use null. Provide no explanatory text, no surrounding code fences, and no additional keys.

Scope: {MODEL_RESPONSE}"""

llm_prompt6="""
Task: Find the number of submitted and accepted papers for {TITLE} ({ACRONYM}) in {TARGET_YEAR} for the main research track and return structured output as mentioned below.
Scope: Search the official conference website, the conference proceedings (publisher page / DBLP), and community trackers (OpenAccept, CS conference stats). If official submission counts are not published, find any post‑conference slides or program‑chair statements that report submission totals. Report: (1) total submissions, (2) total accepted papers, (3) acceptance rate
Output: Output exactly one python JSON object with these keys and value types only:
        "Year": integer (e.g. 2021)
        "No. of Submitted Papers": integer or null
        "No. of Accepted Papers": integer or null
        "Confidence: string, one of "high", "medium", or "low"
        If a value is not available, use null. Provide no explanatory text, no surrounding code fences, and no additional keys."""

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
      r = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=data, timeout=(5, 60)) # https://openrouter.ai/api/v1/chat/completions
      # print("LLM Response: ", r.text)
      # print(r.json()["choices"][0]["message"]["content"])
      # print (r.json())
      text = extract_assistant_text(r.json())
      # print(text)
      return text
    except Exception as e:
      print("LLM Exception: ", e)
      return None

"""
def generate_submission_report(title, acronym, year):
    prompt = llm_prompt2.format(TITLE=title, ACRONYM=acronym, TARGET_YEAR=year)
    report_txt = call_llm(prompt)
    prompt = llm_prompt3.format(MODEL_RESPONSE=report_txt)
    response_json = call_llm(prompt)
    return response_json """

def generate_submission_report(title, acronym, year):
    prompt = llm_prompt6.format(TITLE=title, ACRONYM=acronym, TARGET_YEAR=year)
    response_json = call_llm(prompt, model='deepseek/deepseek-v3.2') #openai/gpt-5.4-mini #anthropic/claude-sonnet-4.6 #deepseek/deepseek-v3.2 #google/gemini-2.5-flash-lite
    return response_json

def generate_submission_report_list(title, acronym, year):
    prompt = llm_prompt4.format(TITLE=title, ACRONYM=acronym, TARGET_YEAR=year)
    report_txt = call_llm(prompt)
    prompt = llm_prompt5.format(MODEL_RESPONSE=report_txt)
    response_json = call_llm(prompt)
    return response_json

def get_paper_report_by_year(response_json, year):
    if response_json.get('Year')==year and response_json.get('Confidence')!='low':
        return response_json.get('No. of Submitted Papers', None), response_json.get('No. of Accepted Papers', None)
    return None, None

# TEST LLM
# print (generate_submission_report("The 20th International Conference on Autonomous Agents and Multi-Agent Systems", "AAMAS 2021", 2021))
# print(generate_submission_report_list("The International Conference on Autonomous Agents and Multi-Agent Systems", "AAMAS", 2021))

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

def edit_page(title, new_text, token, summary="Update paper report"):
    if DRY_RUN:
        print(f"[DRY RUN] Would edit: {title}  summary: {summary}")
        return {"result": "dry-run"}
    payload = {
        "action": "edit",
        "title": title,
        "text": new_text,
        "token": token,
        "format": "json",
        "summary": summary,
        "bot": True
    }
    r = session.post(OR_API, data=payload, timeout=(5, 60))
    r.raise_for_status()
    return r.json()


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

#
csrf_token = mw_login()
series_titles = get_series_titles()
start_year = 2021
end_year = 2026
print(f"Found {len(series_titles)} series pages.")

from sys import exception
for i, series in enumerate(series_titles):
    print(f"\n\n{i}: {series}")
    try:
        for target_year in range(start_year, end_year):
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
            submitted_pattern = re.compile(r"\|\s*Submitted\s+papers\s*=\s*([0-9]+)", flags=re.I)
            accepted_pattern = re.compile(r"\|\s*Accepted\s+papers\s*=\s*([0-9]+)", flags=re.I)

            submitted_match = submitted_pattern.search(tpl_text)
            accepted_match = accepted_pattern.search(tpl_text)
            if submitted_match and accepted_match:
              print(f"{acronym}: [SKIP] Having paper submission report")
              continue

            # Update paper field
            ## LLM Code to generate submission report
            response_json = generate_submission_report(title_text, acronym, target_year)
            if not response_json:
                print(f"{acronym}: [SKIP] No submission report (response)")
                continue
            parsed_response = extract_json_object_from_llm(response_json)
            print(f"{acronym} parsed response: {parsed_response}")
            if not parsed_response:
                print(f"{acronym}: [SKIP] No submission report (parsed)")
                continue
            submitted_papers, accepted_papers = get_paper_report_by_year(parsed_response, target_year)
            # print(f"{acronym} paper report: {submitted_papers}, {accepted_papers}")
            if submitted_papers!=None and accepted_papers!=None:
                # Prepare params to set for both years
                params_map = {PARAMS_TO_SET[0]: submitted_papers, PARAMS_TO_SET[1]: accepted_papers}
                new_wikitext, changed, old_tpl = set_multiple_params_in_template(wikitext, params_map)
                summary = f"Set {PARAMS_TO_SET[0]} to {submitted_papers} and {PARAMS_TO_SET[1]} to {accepted_papers} (acronym={acronym})"
                # print (new_wikitext)
                res = edit_page(acronym, new_wikitext, csrf_token, summary=summary)
                print(f"{acronym}: [EDITED] {res}")

    except requests.exceptions.ConnectionError as e:
        print("Request failed:", e)
        print ("Logout: ", mw_logout(csrf_token))
        csrf_token = mw_login()
        print ("################ Reconnection successfully ###################")
    except Exception as e:
        print("All Exception:", e)
        print ("Logout: ", mw_logout(csrf_token))

print ("Logout: ", mw_logout(csrf_token))
