import os
import re
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.colab import userdata
from google.colab import drive
drive.mount('/content/drive')

# Configuration
OR_API = "https://www.openresearch.org/mediawiki/api.php"
MW_USER = os.environ.get("OR_USER") or userdata.get("OR_USER")
MW_PASS = os.environ.get("OR_PASS") or userdata.get("OR_PASS")
DRY_RUN = False  # Set to False to perform real edits
TEMPLATE_NAME = "Event series"
PARAMS_TO_SET = ["has CORE2023 Rank", "has CORE2026 Rank"]
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

def get_series_titles():
    params = {"action":"query","list":"categorymembers","cmtitle":"Category:Event series","cmlimit":"max","format":"json"}
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
        "summary": summary,
        "bot": True
    }
    r = session.post(OR_API, data=payload)
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

# Load CSVs and build mapping: column 2 -> column 4
core_23_path = '/content/drive/MyDrive/Work/MediaWiki/CORE_2023.csv'
core_26_path = '/content/drive/MyDrive/Work/MediaWiki/CORE_2026.csv'
df_core_23 = pd.read_csv(core_23_path, header=None, dtype=str)
df_core_26 = pd.read_csv(core_26_path, header=None, dtype=str)
core_23_dict = dict(zip(df_core_23[2].str.strip(), df_core_23[4].str.strip()))
core_26_dict = dict(zip(df_core_26[2].str.strip(), df_core_26[4].str.strip()))

def extract_acronym_from_template(tpl_text):
    header, parsed, footer = parse_template_lines(tpl_text)
    for _, name, value in parsed:
        if name and name.strip().lower() in ("acronym", "acr"):
            return value.strip()
    # fallback: try to find an inline |Acronym= or link in header/body
    m = re.search(r'\|\s*Acronym\s*=\s*([^\n\|]+)', tpl_text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None

def ci_get(d: dict, key: str):
    """Return value for a key in d matching case-insensitively, or None if not found."""
    if key is None:
        return None
    k_norm = key.casefold()
    for dk, dv in d.items():
        if isinstance(dk, str) and dk.casefold() == k_norm:
            return dv
    return None

csrf_token = mw_login()
titles = get_series_titles()
print(f"Found {len(titles)} series pages.")
for title in titles:
    wikitext = get_page_wikitext(title)
    tpl = find_template_block(wikitext)
    if not tpl:
        print(f"[SKIP] No {TEMPLATE_NAME} template on {title}")
        continue
    tpl_text, _, _ = tpl
    acronym = extract_acronym_from_template(tpl_text) or title.strip()
    acronym_key = acronym.strip() if acronym else None
    if not acronym_key:
        print(f"[SKIP] No acronym found on {title}")
        continue
    # Lookup rank: prefer CORE_26 then CORE_23
    rank23 = 'NA'
    rank26= 'NA'
    val26 = ci_get(core_26_dict, acronym_key)
    if val26 is not None and str(val26).strip():
        rank26 = str(val26).strip()

    val23 = ci_get(core_23_dict, acronym_key)
    if val23 is not None and str(val23).strip():
        rank23 = str(val23).strip()
    """
    if not rank or rank.lower() in ("nan", ""):
        print(f"[NO RANK] {title} ({acronym_key}) - skipping")
        continue
    """
    if rank23!='NA' or rank26!='NA':
        # Prepare params to set for both years
        params_map = {PARAMS_TO_SET[0]: rank23, PARAMS_TO_SET[1]: rank26}
        new_wikitext, changed, old_tpl = set_multiple_params_in_template(wikitext, params_map)
        # Optionally add SMW property after template if not present
        if SMW_PROPERTY_LINE:
            prop_pat = re.compile(r"""

            \[

            \[\s*Has core ranking::.*?\]

            \]

            """, re.IGNORECASE)
            if not prop_pat.search(new_wikitext):
                tpl_block = find_template_block(new_wikitext)
                if tpl_block:
                    _, s, e = tpl_block
                    insertion = f"\n\n[[Has core ranking::{rank26}]]\n"
                    new_wikitext = new_wikitext[:e] + insertion + new_wikitext[e:]
                    changed = True
        if not changed:
            print(f"[UNCHANGED] {title} already has desired params.")
            continue
        summary = f"Set {PARAMS_TO_SET[0]} to {rank23} and {PARAMS_TO_SET[1]} to {rank26} (acronym={acronym_key})"
        res = edit_page(title, new_wikitext, csrf_token, summary=summary)
        print(f"[EDITED] {title}: {res}")

"""x = set(df_core_23[4].unique().tolist())
y = set(df_core_26[4].unique().tolist())
x.union(y)"""


