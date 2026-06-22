from typing import Dict, Optional, List, Any
from prefect import task
import re
from datetime import datetime, timezone
import ast
import json

@task
def filter_rank_series(series_list: List[str], core_26_dict: Dict[str, str], core_23_dict: Dict[str, str], rank: List[str] = ['A*', 'A', 'B', 'C']) -> List[str]:
    candidates = []
    for series in series_list:
        rank_26 = ci_get(core_26_dict, series)
        rank_23 = ci_get(core_23_dict, series)
        if rank_26:
            if rank_26 in rank:
                candidates.append(series)
        elif rank_23:
            if rank_23 in rank:
                candidates.append(series)
    return candidates


def ci_get(d: Dict[str, str], key: Optional[str]) -> Optional[str]:
    if key is None:
        return None
    k_norm = key.casefold()
    for dk, dv in d.items():
        if isinstance(dk, str) and dk.casefold() == k_norm:
            return dv
    return None

def extract_series_strict_two_tokens(acronym: Optional[str]) -> Optional[str]:
    if not acronym:
        return None
    _YEAR_RE = re.compile(r'^(?:19|20)\d{2}$')
    _ALLCAPS_TOKEN_RE = re.compile(r'^[A-Z0-9]+(?:(?:--|—|–|-|/|&)[A-Z0-9]+)*$')
    tokens = acronym.strip().split()
    if len(tokens) > 2:
        return None
    if len(tokens) == 1 and _ALLCAPS_TOKEN_RE.match(tokens[0].strip()) :
        # If there's only one token, we can't be sure which part is the series and which is the year, so we return None
        return tokens[0].strip()
    a, b = tokens[0].strip(), tokens[1].strip()
    if _ALLCAPS_TOKEN_RE.match(a) and _YEAR_RE.match(b):
        return a
    if _YEAR_RE.match(a) and _ALLCAPS_TOKEN_RE.match(b):
        return b
    return None

def extract_event_fields_from_wikitext(text: str) -> Dict[str, Optional[str]]:
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


def _to_utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


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