from typing import Dict, Optional, List, Any, Tuple
from prefect import task
import re
from datetime import datetime, timezone
import ast
import json
from collections.abc import Mapping
import requests
from html import unescape
from typing import Iterable
import Levenshtein
from rapidfuzz import fuzz
import re
from typing import List, Dict, Union
from collections import OrderedDict

Number = Union[int, float]

try:
    from dateutil import parser as _dateutil_parser
except Exception:
    _dateutil_parser = None

COMMON_FORMATS = [
    "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d",
    "%d-%m-%Y", "%d/%m/%Y", "%m-%d-%Y", "%m/%d/%Y",
    "%d %b %Y", "%d %B %Y", "%b %d, %Y", "%B %d, %Y",
    "%d.%m.%Y", "%Y%m%d", "%d-%b-%Y", "%d-%B-%Y",
    "%d-%m-%y", "%m/%d/%y", "%y-%m-%d",
]

EVENT_TEMPLATE_ORDER = [
    "Acronym", "Title", "Ordinal", "Type", "Field", "Series", "Superevent", "Homepage", "Logo",
    "Start date", "End date", "Event mode", "City", "State", "Country",
    "Abstract deadline", "Paper deadline", "Submission deadline", "Poster deadline",
    "Demo deadline", "Workshop deadline", "Tutorial deadline", "Notification",
    "Camera ready", "Attendance fee", "Reduced attendance fee", "Attendance fee currency",
    "Submitted papers", "Accepted papers", "Accepted short papers", "Has host organization", 
    "Has coordinator", "Has general chair",
    "Has program chair", "Has workshop chair", "Has OC member",
    "Has tutorial chair","Has demo chair", "Has PC member", "Has Keynote speaker"
]


def extract_numbers(text: str) -> List[Dict[str, Number]]:
    """
    Extract numbers from a string.
    """
    pattern = (
        r'(?P<prefix>[\$₹€£])?'                                  # optional currency symbol
        r'(?P<number>'
        r'[-+]?\d{1,3}(?:,\d{3})+(?:\.\d+)?(?:[eE][-+]?\d+)?'    # numbers with commas
        r'|[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?'            # plain ints/floats/scientific
        r')'
        r'(?P<percent>%)?'                                       # optional percent sign
    )
    matches = re.finditer(pattern, text)
    for m in matches:
        raw = m.group(0)
        num_str = m.group('number').replace(',', '')
        is_percent = bool(m.group('percent'))

        # convert to int when appropriate, otherwise float
        if re.fullmatch(r'[-+]?\d+', num_str):
            value: Number = int(num_str)
        else:
            value = float(num_str)

        if is_percent:
            value = value / 100.0

    return value


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
        return {"Series": None, "Title": None, "Field": None, 'Acronym': None, 'Ordinal': None}

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
    acronym = fields.get("acronym") or None
    ordinal = fields.get("ordinal") or None

    # Normalize empty strings to None
    def _none_if_empty(x):
        return x if x and x.strip() else None

    return {
        "Title": _none_if_empty(title),
        "Acronym": _none_if_empty(acronym),
        "Field":  _none_if_empty(field),
        "Ordinal": _none_if_empty(ordinal)
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



def update_dict(target, source,
                   treat_empty_strings=False,
                   treat_empty_containers=False):

    def is_missing(value):
        if value is None:
            return True
        if treat_empty_strings and value == "":
            return True
        if treat_empty_containers and (value == [] or value == {}):
            return True
        return False

    for key, src_val in source.items():
        if key not in target:
            # key absent in target -> copy entire value
            target[key] = src_val
        else:
            tgt_val = target[key]
            # If both are dict-like, recurse
            if isinstance(tgt_val, Mapping) and isinstance(src_val, Mapping):
                update_dict(tgt_val, src_val,
                            treat_empty_strings=treat_empty_strings,
                            treat_empty_containers=treat_empty_containers)
            else:
                # If target value is considered missing, replace it
                if is_missing(tgt_val):
                    target[key] = src_val
                    
# normalize string lowercase, remove stop words strip punctuation, whitespace, and replace multiple spaces with single space
def normalize_string(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.lower()
    # remove punctuation
    s = re.sub(r'[^\w\s]', '', s)
    # remove stop words
    stop_words = set([
        "the", "and", "of", "in", "on", "for", "with", "to", "a", "an",
        "at", "by", "from", "is", "are", "as", "that", "this", "these",
        "those", "it", "its", "be", "was", "were"
    ])
    tokens = s.split()
    tokens = [t for t in tokens if t not in stop_words]
    s = ' '.join(tokens)
    # replace multiple spaces with single space
    s = re.sub(r'\s+', ' ', s)
    return s.strip()

# calculate the similarity between two strings using normalized Levenshtein distance
def string_similarity_levenshtein(s1: Optional[str], s2: Optional[str]) -> float:
    if s1 is None or s2 is None:
        return 0.0
    s1 = normalize_string(s1)
    s2 = normalize_string(s2)
    if not s1 or not s2:
        return 0.0
    # calculate Levenshtein distance
    distance = Levenshtein.distance(s1, s2)
    max_len = max(len(s1), len(s2))
    if max_len > 0:
        similarity = 1 - (distance / max_len)
        return similarity
    return 0.0

# calculate the similarity between two strings using normalized rapidfuzz ratio
def string_similarity_rapidfuzz(s1: Optional[str], s2: Optional[str]) -> float:
    if s1 is None or s2 is None:
        return 0.0
    s1 = normalize_string(s1)
    s2 = normalize_string(s2)
    if not s1 or not s2:
        return 0.0
    # calculate rapidfuzz ratio
    similarity = fuzz.ratio(s1, s2) / 100.0
    return similarity

# calculate the similarity between two string using normalized cosine similarity
def string_similarity_cosine(s1: Optional[str], s2: Optional[str]):
    if s1 is None or s2 is None:
        return 0.0
    s1 = normalize_string(s1)
    s2 = normalize_string(s2)
    if not s1 or not s2:
        return 0.0
    # calculate cosine similarity
    similarity = 0.0
    return similarity

def clean_submitted_papers(text: str) -> str:
    PATTERN = re.compile(r'\|\s*submitted\s*papers\s*=\s*([^\|\n]*)', re.IGNORECASE)
    out_lines = []
    for line in text.splitlines():
        matches = list(PATTERN.finditer(line))
        if not matches:
            out_lines.append(line)
            continue

        # If any match on the line is invalid, drop the whole line
        keep_line = True
        for m in matches:
            val = m.group(1).strip()
            # Treat empty value as invalid
            if val == '':
                keep_line = False
                break
            # Accept only integer tokens
            if not re.fullmatch(r'[+-]?\d+', val):
                keep_line = False
                break
            try:
                n = int(val)
            except ValueError:
                keep_line = False
                break
            if n <= 0:
                keep_line = False
                break

        if keep_line:
            out_lines.append(line)
        # else: skip the line entirely

    return '\n'.join(out_lines)

def clean_accepted_papers(text: str) -> str:
    PATTERN = re.compile(r'\|\s*accepted\s*papers\s*=\s*([^\|\n]*)', re.IGNORECASE)
    out_lines = []
    for line in text.splitlines():
        matches = list(PATTERN.finditer(line))
        if not matches:
            out_lines.append(line)
            continue

        # If any match on the line is invalid, drop the whole line
        keep_line = True
        for m in matches:
            val = m.group(1).strip()
            # Treat empty value as invalid
            if val == '':
                keep_line = False
                break
            # Accept only integer tokens
            if not re.fullmatch(r'[+-]?\d+', val):
                keep_line = False
                break
            try:
                n = int(val)
            except ValueError:
                keep_line = False
                break
            if n <= 0:
                keep_line = False
                break

        if keep_line:
            out_lines.append(line)
        # else: skip the line entirely

    return '\n'.join(out_lines)

def clean_accepted_short_papers(text: str) -> str:
    PATTERN = re.compile(r'\|\s*accepted\s*short\s*papers\s*=\s*([^\|\n]*)', re.IGNORECASE)
    out_lines = []
    for line in text.splitlines():
        matches = list(PATTERN.finditer(line))
        if not matches:
            out_lines.append(line)
            continue

        # If any match on the line is invalid, drop the whole line
        keep_line = True
        for m in matches:
            val = m.group(1).strip()
            # Treat empty value as invalid
            if val == '':
                keep_line = False
                break
            # Accept only integer tokens
            if not re.fullmatch(r'[+-]?\d+', val):
                keep_line = False
                break
            try:
                n = int(val)
            except ValueError:
                keep_line = False
                break
            if n <= 0:
                keep_line = False
                break

        if keep_line:
            out_lines.append(line)
        # else: skip the line entirely

    return '\n'.join(out_lines)


def try_parse_date(s: str) -> Optional[datetime.date]:
    # Patterns that we consider invalid and will remove
    _YEAR_ONLY_RE = re.compile(r'^\s*\d{4}\s*$')
    _YEAR_MONTH_NUMERIC_RE = re.compile(r'^\s*\d{4}[-/]\d{1,2}\s*$')
    # Month name + year like "March 2020" or "Mar 2020" or "March, 2020"
    _MONTH_NAME_YEAR_RE = re.compile(
    r'^\s*(?:Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|'
    r'Jul|July|Aug|August|Sep|Sept|September|Oct|October|Nov|November|'
    r'Dec|December)\b[ ,\-]*\d{4}\s*$',
    re.IGNORECASE
    )
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None

    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        s = s[1:-1].strip()
        if s == "":
            return None

    if _YEAR_ONLY_RE.match(s):
        return None

    if _YEAR_MONTH_NUMERIC_RE.match(s):
        return None

    if _MONTH_NAME_YEAR_RE.match(s):
        return None

    for fmt in COMMON_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date()
        except Exception:
            continue

    if _dateutil_parser is not None:
        try:
            dt = _dateutil_parser.parse(s, dayfirst=False, yearfirst=True)
            return dt.date()
        except Exception:
            try:
                dt = _dateutil_parser.parse(s, dayfirst=True, yearfirst=False)
                return dt.date()
            except Exception:
                return None

    return None


def normalize_event_dates(text: str) -> str:
    FIELD_PATTERN = re.compile(
        r'^(?P<prefix>\s*\|\s*(?P<field>Start date|End date|Submission deadline|Notification|Abstract deadline|Camera ready)\s*=\s*)(?P<value>.*)$',
        re.IGNORECASE | re.MULTILINE,
    )

    out_lines = []
    for line in text.splitlines():
        m = FIELD_PATTERN.match(line)
        if not m:
            out_lines.append(line)
            continue

        prefix = m.group("prefix")
        raw_val = m.group("value").strip()

        if raw_val == "":
            continue

        parsed = try_parse_date(raw_val)

        if parsed is None:
            continue

        normalized = parsed.strftime("%Y-%m-%d")
        out_lines.append(f"{prefix}{normalized}")

    return "\n".join(out_lines)

def extract_acronym_year(s: str) -> Optional[Tuple[str, str]]:
    s = s.strip()
    # Accept letters for acronym, optional separator (- or _), then exactly 4 digits for year
    m = re.match(r'^([A-Za-z0-9-_\s]+)[\-_]?(\d{4})$', s)
    if not m:
        return None
    acronym, year = m.group(1).strip().strip('-').strip('_'), m.group(2)
    return ' '.join([acronym, year])

def normalize_url(value: str) -> str:
    v = value.strip()
    if not v:
        return ''
    if re.match(r'^[a-zA-Z][a-zA-Z0-9+\-.]*://', v):
        return v
    # protocol-relative
    if v.startswith('//'):
        return 'http:' + v
    return 'http://' + v

def normalize_homepage(text: str) -> str:
    line_re = re.compile(r'^\s*\|\s*(homepage(?:\s+url)?)\s*=\s*(.*)$', re.IGNORECASE)
    out_lines = []
    for line in text.splitlines():
        m = line_re.match(line)
        if not m:
            out_lines.append(line)
            continue

        raw_value = m.group(2)
        if raw_value.strip() == '':
            continue

        fixed = normalize_url(raw_value)
        out_lines.append(f"|Homepage={fixed}")

    return "\n".join(out_lines)

def _normalize_key_case(k: str) -> str:
    return re.sub(r'\s+', ' ', k.strip()).casefold()

def _find_event_block_bounds(text: str):
    start = text.find("{{Event")
    if start == -1:
        return None, None
    i = start
    stack = []
    while i < len(text) - 1:
        pair = text[i:i+2]
        if pair == "{{":
            stack.append(i)
            i += 2
            continue
        if pair == "}}":
            if not stack:
                i += 2
                continue
            stack.pop()
            i += 2
            if not stack:
                return start, i
            continue
        i += 1
    return start, None

def reorder_event_template(text: str) -> str:
    # canonical map
    CANONICAL_MAP = { _normalize_key_case(k): k for k in EVENT_TEMPLATE_ORDER }
    KV_LINE_RE = re.compile(r'^\s*\|\s*(.+)$')
    
    start, end = _find_event_block_bounds(text)
    if start is None or end is None:
        return text

    before = text[:start]
    block = text[start:end]  # opening {{Event ... and closing }}
    after = text[end:]

    inner = block[len("{{Event"):].rstrip("}").strip()
    lines = inner.splitlines()

    parsed = OrderedDict()
    unknown_original_keys = {}

    for raw in lines:
        m = KV_LINE_RE.match(raw)
        if not m:
            continue
        kv = m.group(1)
        # split at the first '='
        if '=' in kv:
            key_part, val_part = kv.split('=', 1)
            key = key_part.strip()
            val = val_part.strip()
        else:
            key = kv.strip()
            val = ""
        if val == "":
            continue
        norm = _normalize_key_case(key)
        if norm in CANONICAL_MAP:
            canonical = CANONICAL_MAP[norm]
            parsed[canonical] = val
        else:
            clean_key = re.sub(r'\s+', ' ', key.strip())
            unknown_original_keys[clean_key] = val

    # build ordered list
    ordered_items = []
    for key in EVENT_TEMPLATE_ORDER:
        if key in parsed:
            ordered_items.append((key, parsed.pop(key)))

    for key in list(parsed.keys()):
        ordered_items.append((key, parsed[key]))

    for key in sorted(unknown_original_keys.keys(), key=lambda s: s.casefold()):
        ordered_items.append((key, unknown_original_keys[key]))

    lines_out = ["{{Event"]
    for k, v in ordered_items:
        lines_out.append(f"|{k}={v}")
    lines_out.append("}}")
    new_block = "\n".join(lines_out)

    return before + new_block + after
