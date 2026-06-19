from prefect import task, get_run_logger
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Tuple

DEFAULT_USER_AGENT = "openresearch-core-ranker/1.0 (contact: you@example.org)"

def _make_session(user_agent: str = DEFAULT_USER_AGENT) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent})
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429,500,502,503,504], allowed_methods=["POST","GET"])
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

@task(retries=2, retry_delay_seconds=10)
def login_and_get_csrf(api_url: str, username: str, password: str) -> Tuple[str, dict]:
    logger = get_run_logger()
    s = _make_session()
    r = s.get(api_url, params={"action":"query","meta":"tokens","type":"login","format":"json"}, timeout=30)
    r.raise_for_status()
    token = r.json()["query"]["tokens"]["logintoken"]
    payload = {"action":"login","lgname":username,"lgpassword":password,"lgtoken":token,"format":"json"}
    r = s.post(api_url, data=payload, timeout=30)
    r.raise_for_status()
    result = r.json().get("login", {}).get("result")
    if result not in ("Success", "NeedToken"):
        logger.error("Login failed: %s", r.text)
        raise RuntimeError(f"Login failed: {r.json()}")
    r = s.get(api_url, params={"action":"query","meta":"tokens","format":"json"}, timeout=30)
    r.raise_for_status()
    csrf = r.json()["query"]["tokens"]["csrftoken"]
    session_info = {"User-Agent": s.headers.get("User-Agent")}
    return csrf, session_info