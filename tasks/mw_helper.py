from prefect import task, get_run_logger
import requests
import re
from typing import List, Tuple, Optional, Dict
from typing import Dict, Callable, Any

# Template helpers (kept as tasks so they are testable and visible)
@task
def find_template_block(wikitext: str, template_name: str = "Event") -> Optional[Tuple[str, int, int]]:
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

@task
def parse_template_lines(template_text: str) -> Tuple[str, List[Tuple[str, Optional[str], Optional[str]]], str]:
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

@task
def render_template(header: str, parsed: List[Tuple[str, Optional[str], Optional[str]]], footer: str) -> str:
    out = [header]
    for raw, name, value in parsed:
        if name is None:
            out.append(raw)
        else:
            out.append(f"|{name}={value}")
    out.append(footer)
    return "\n".join(out)

@task
def set_multiple_params_in_template(wikitext: str, params_to_set: Dict[str, str], template_name: str = "Event") -> Tuple[str, bool, Optional[str]]:
    tpl = find_template_block(wikitext, template_name)  # call task synchronously inside task
    if not tpl:
        return wikitext, False, None
    tpl_text, start, end = tpl
    header, parsed, footer = parse_template_lines(tpl_text)
    def norm(s): return re.sub(r'\s+', '', s).lower()
    existing = {norm(name): (i, name, value) for i, (_, name, value) in enumerate(parsed) if name}
    changed = False
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

@task
def extract_acronym_from_template(tpl_text: str) -> Optional[str]:
    header, parsed, footer = parse_template_lines(tpl_text)
    for _, name, value in parsed:
        if name and name.strip().lower() in ("acronym", "acr"):
            return value.strip()
    m = re.search(r'\|\s*Acronym\s*=\s*([^\n\|]+)', tpl_text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


def _parse_fields_spec(fields_str: str):
    parts = [p.strip() for p in fields_str.split(',') if p.strip()]
    specs = []
    for part in parts:
        if ':' in part:
            name, typ = [x.strip() for x in part.split(':', 1)]
        else:
            name, typ = part.strip(), 'str'
        specs.append((name, typ.lower()))
    return specs

# Default converters by type
_default_type_converters: Dict[str, Callable[[str], Any]] = {
    'int': lambda s: int(s.strip()),
    'float': lambda s: float(s.strip()),
    'str': lambda s: s.strip(),
    'bool': lambda s: s.strip().lower() in ('1', 'true', 'yes', 'y'),
}

def extract_fields_from_template(
    tpl_text: str,
    fields: str,
    *,
    type_converters: Optional[Dict[str, Callable[[str], Any]]] = None,
    field_converters: Optional[Dict[str, Callable[[str], Any]]] = None,
    first_only: bool = True
) -> Dict[str, Any]:

    specs = _parse_fields_spec(fields)
    converters_by_type = dict(_default_type_converters)
    if type_converters:
        converters_by_type.update(type_converters)

    result: Dict[str, Any] = {}

    for name, typ in specs:
        # Build a flexible regex for the field name allowing variable whitespace
        # e.g. "City" or "City Name" -> allow spaces and case-insensitive
        name_escaped = re.escape(name)
        name_pattern = re.sub(r'\\\s+', r'\\s*', name_escaped)  # allow flexible whitespace
        pattern = re.compile(r"\|\s*" + name_pattern + r"\s*=\s*([^|\n\r]+)", flags=re.I)

        if first_only:
            m = pattern.search(tpl_text)
            raw_values = [m.group(1).strip()] if m else []
        else:
            raw_values = [m.group(1).strip() for m in pattern.finditer(tpl_text)]

        # choose converter: field-specific overrides type-based
        conv = None
        if field_converters and name in field_converters:
            conv = field_converters[name]
        else:
            conv = converters_by_type.get(typ, converters_by_type['str'])

        def _convert(raw: str):
            try:
                return conv(raw)
            except Exception:
                return None

        if first_only:
            result[name] = _convert(raw_values[0]) if raw_values else None
        else:
            result[name] = [_convert(rv) for rv in raw_values] if raw_values else []

    return result
