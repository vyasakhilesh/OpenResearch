from prefect import task, get_run_logger
import requests
from typing import Optional, Tuple, List, Dict
import re
import json

OPENROUTER_URL = "https://openrouter.ai/api/v1/responses"

@task(retries=1, retry_delay_seconds=5)
def call_openrouter_task(prompt: str, api_key: str, model: str = "openai/gpt-5.4-mini", temperature: float = 0.0, max_output_tokens: int = 2000) -> Optional[dict]:
    logger = get_run_logger()
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
def get_event_template(title: str, series_name: str, target_year: int, llm_api_key: str, prompt_template: str) -> Optional[str]:
    prompt = prompt_template.format(TITLE=title, SERIES_NAME=series_name, TARGET_YEAR=target_year)
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