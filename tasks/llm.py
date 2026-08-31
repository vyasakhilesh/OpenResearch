from prefect import task, get_run_logger
import requests
import os
from typing import Optional, Tuple, List, Dict, Any
import re
import json
from tasks.utils import _find_code_fence_content, _extract_balanced_braces, _clean_json_like
import ast

OPENROUTER_URL = "https://openrouter.ai/api/v1/responses"
PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "INFO")  # Set default logging level to INFO if not specified

# oldmodel = openai/gpt-5.4-mini
# newmodel =anthropic/claude-sonnet-5
@task(retries=1, retry_delay_seconds=5)
def call_openrouter_task(prompt: str, api_key: str, model: str = "openai/gpt-5.4-mini", temperature: float = 0.1, max_output_tokens: int = 2000) -> Optional[dict]:
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    if not api_key:
        logger.error("LLM API key not provided")
        raise RuntimeError("LLM API key not provided")
    payload = {
        "model": model,
        "input": prompt,
        "temperature": temperature,
        "max_output_tokens": max_output_tokens,
        "tools":[{ "type": "openrouter:web_search", "search_context_size": "medium"}],
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=60)
    logger.debug("OpenRouter response status: %s, content: %s", resp.status_code, resp.text)
    logger.debug("OpenRouter response JSON: %s", resp.json())
    resp.raise_for_status()
    return resp.json()

@task
def extract_text_from_response(resp_json: dict) -> Optional[str]:
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
    choices = resp_json.get("choices")
    if isinstance(choices, list) and choices:
        choice = choices[0]
        if isinstance(choice, dict):
            msg = choice.get("message") or choice.get("delta") or {}
            if isinstance(msg, dict):
                content = msg.get("content") or msg.get("text")
                if isinstance(content, str):
                    return content.strip()
    for key in ("text", "response", "result"):
        val = resp_json.get(key)
        if isinstance(val, str):
            return val.strip()
    return None

@task
def split_template_and_sources(text: str) -> Tuple[Optional[str], List[str]]:
    template_match = re.search(r"(\{Event[\s\S]*?\})", text)
    if not template_match:
        return None, []
    template = template_match.group(1).strip()
    remainder = text[template_match.end():].strip()
    sources = []
    if remainder:
        m = re.search(r"(?i)^(Sources|References)\s*:\s*\n?(.*)$", remainder, re.S)
        if m:
            body = m.group(2).strip()
        else:
            body = remainder
        urls = re.findall(r"https?://[^\s\]\)]+", body)
        seen = set()
        for u in urls:
            if u not in seen:
                seen.add(u)
                sources.append(u)
    return template, sources

@task
def get_event_template(llm_api_key: str, prompt: str) -> Optional[str]:
    resp_json = call_openrouter_task(prompt, llm_api_key)
    if not resp_json:
        return None
    text = extract_text_from_response(resp_json)
    if not text:
        return None
    template, sources = split_template_and_sources(text)
    if template:
        if sources:
            return template + "\n\n" + "\n=== Sources ===\n" + "\n\n".join(sources)
        else:
            remainder = text.split(template, 1)[1].strip()
            if remainder:
                return template + "\n\n" + "\n=== Sources ===\n" + remainder
            else:
                return template
    return None

@task
def get_event_fields(llm_api_key: str, prompt:str) -> Optional[str]:
    resp_json = call_openrouter_task(prompt, llm_api_key)
    if not resp_json:
        return None
    text = extract_text_from_response(resp_json)
    if not text:
        return None
    parsed_response = extract_json_object_from_llm(text)
    return parsed_response

@task
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


def fix_and_validate_event_template(text: str) -> Tuple[Optional[str], Optional[str]]:
    t = text or ""
    m = re.search(r"\{\{Event\b(.*?)\}\}", t, flags=re.S)
    if not m:
        m = re.search(r"\{Event\b(.*?)\}", t, flags=re.S)
    if not m:
        return None, "No Event template found"
    body = m.group(1)
    start, end = m.span()
    body = body.strip("\n\r")
    fixed_template = "{{Event\n" + body + "\n}}"
    required = ['|Acronym=', '|Title=', '|Start date=', '|End date=', '|City=', '|Country=']
    missing = [f for f in required if f not in fixed_template]
    if missing:
        return None, f"Missing required fields: {missing}"
    if fixed_template.count("{{") != fixed_template.count("}}"):
        return None, "Unbalanced double braces after fix"
    fixed_text = t[:start] + fixed_template + t[end:]
    return fixed_text, None

def fix_event_template(text: str) -> Tuple[Optional[str], Optional[str]]:
    t = text or ""
    m = re.search(r"\{\{Event\b(.*?)\}\}", t, flags=re.S)
    if not m:
        m = re.search(r"\{Event\b(.*?)\}", t, flags=re.S)
    if not m:
        return None, "No Event template found"
    body = m.group(1)
    start, end = m.span()
    body = body.strip("\n\r")
    fixed_template = "{{Event\n" + body + "\n}}"
    required = ['|Acronym=', '|Title=']
    missing = [f for f in required if f not in fixed_template]
    if missing:
        return None, f"Missing required fields: {missing}"
    if fixed_template.count("{{") != fixed_template.count("}}"):
        return None, "Unbalanced double braces after fix"
    fixed_text = t[:start] + fixed_template + t[end:]
    return fixed_text, None