import re
from html import unescape
from typing import Iterable
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from prefect import task, get_run_logger
import os

PREFECT_LOGGING_LEVEL = os.environ.get("PREFECT_LOGGING_LEVEL", "DEBUG")

RETRY_STRATEGY = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET"]),
    raise_on_status=False,
    respect_retry_after_header=True,
)

SESSION = requests.Session()
HTTP_ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY, pool_connections=20, pool_maxsize=20)
SESSION.mount("http://", HTTP_ADAPTER)
SESSION.mount("https://", HTTP_ADAPTER)
SESSION.headers.update({"User-Agent": "OpenResearchBot/1.0"})

def extract_core_conference_details(url: str, source:str="Source: ICORE2026") -> dict[str, str]:
    ROW_PATTERN = re.compile(r'<div class="row [^"]+">(.*?)</div>', re.DOTALL | re.IGNORECASE)
    TITLE_PATTERN = re.compile(r'<h2>(.*?)</h2>', re.DOTALL | re.IGNORECASE)
    TAG_PATTERN = re.compile(r'<[^>]+>')
    
    def _clean_html_text(value: str) -> str:
        text = TAG_PATTERN.sub(" ", value)
        text = unescape(text)
        return re.sub(r"\s+", " ", text).strip()


    def _extract_rows(html: str) -> list[str]:
        return [_clean_html_text(match) for match in ROW_PATTERN.findall(html)]


    def _value_after_prefix(rows: Iterable[str], prefix: str) -> str:
        for row in rows:
            if row.startswith(prefix):
                return row.removeprefix(prefix).strip()
        raise ValueError(f"Could not find row starting with {prefix!r}.")

    response = SESSION.get(url, timeout=(10, 30))
    response.raise_for_status()
    html = response.text

    titles = [_clean_html_text(match) for match in TITLE_PATTERN.findall(html)]
    if len(titles) < 2:
        raise ValueError("Could not find the conference title in the page heading.")

    rows = _extract_rows(html)
    acronym = _value_after_prefix(rows, "Acronym:")
    dblp_source = _value_after_prefix(rows, "DBLP Source:")\
                  .replace("https://dblp.uni-trier.de/db/conf/", '')\
                  .replace("https://dblp.org/db/conf/", '')\
                  .replace("http://dblp.uni-trier.de/db/conf/", '')\
                  .replace("http://dblp.org/db/conf/", '')\
                  .replace("https://dblp.uni-trier.de/db/journals/", '')\
                  .replace("https://dblp.org/db/journals/", '')\
                  .replace("http://dblp.uni-trier.de/db/journals/", '')\
                  .replace("http://dblp.org/db/journals/", '')\
                  .replace("/index.html", '')
                

    icore_rank = ""
    icore_field_1 = ""
    icore_field_2 = ""
    icore_field_3 = ""
    for index, row in enumerate(rows):
        if row == source:
            for next_row in rows[index + 1 :]:
                if next_row.startswith("Source:"):
                    break
                if not icore_rank and next_row.startswith("Rank:"):
                    icore_rank = next_row.removeprefix("Rank:").strip()
                if not icore_field_1 and next_row.startswith("Field Of Research:"):
                    icore_field_1 = re.sub(r'^\s*\d+\s*-\s*', '', next_row.removeprefix("Field Of Research:").strip()\
                                  .replace(" ( h-index )", '')\
                                  .replace(" ( citation )", '')).strip()
                elif not icore_field_2 and next_row.startswith("Field Of Research:"):
                    icore_field_2 = re.sub(r'^\s*\d+\s*-\s*', '', next_row.removeprefix("Field Of Research:").strip()\
                                  .replace(" ( h-index )", '')\
                                  .replace(" ( citation )", '')).strip()
                elif not icore_field_3 and next_row.startswith("Field Of Research:"):
                    icore_field_3 = re.sub(r'^\s*\d+\s*-\s*', '', next_row.removeprefix("Field Of Research:").strip()\
                                  .replace(" ( h-index )", '')\
                                  .replace(" ( citation )", '')).strip()
            break


    return {
        "Title":titles[1],
        "Acronym":acronym,
        "DblpSeries":dblp_source,
        "Rank":icore_rank,
        "Field":f"{icore_field_1}, {icore_field_2}, {icore_field_3}",
        "field_of_research_1":icore_field_1,
        "field_of_research_2":icore_field_2,
        "field_of_research_3":icore_field_3
    }

@task(name="create-icore-conference-details", description="create a dict with conference details from ICORE2026 page and return it")
def create_icore_conference_details_data(core_path: str, source: str = "Source: ICORE2026") -> pd.DataFrame:
    """
    Create a dataframe with conference details from ICORE2026 page and return it
    """
    logger = get_run_logger()
    logger.setLevel(PREFECT_LOGGING_LEVEL)
    details = []
    failed_urls = []
    df_core = pd.read_csv(core_path, header=None)
    urls = df_core[0].apply(lambda url_value : "https://portal.core.edu.au/conf-ranks/"+ str(url_value)).tolist()  # Assuming the first column contains the URLs
    logger.info(f"Extracting conference details from {len(urls)}, {urls[0:5]} URLs in {core_path}")

    for url in urls:
        try:
            detail = extract_core_conference_details(url, source)
            details.append(detail)
            logger.debug(f"Detail: {detail}")
        except Exception as e:
            failed_urls.append(url)
            logger.error(f"Error processing URL {url}: {e}")
    
    # Retry for the failed_urls
    if failed_urls:
        logger.warning(f"Failed to process {len(failed_urls)} URLs.")
        for url in failed_urls:
            try:
                detail = extract_core_conference_details(url, source)
                details.append(detail)
                logger.debug(f"Detail: {detail}")
                failed_urls.remove(url)  # Remove from failed_urls if successful
            except Exception as e:
                logger.error(f"Retry failed for URL {url}: {e}")
    logger.warning(f"Finally Failed to process {len(failed_urls)} URLs.")
    df = pd.DataFrame(details)
    if not df.empty and "Acronym" in df.columns:
        df.drop_duplicates(subset=["Acronym"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.to_csv(core_path.replace(".csv", "_details.csv"), index=False)
    logger.info(f"Created conference details dataframe with {len(df)} unique entries from {len(urls)} URLs.")
    logger.debug(f"Sample of the dataframe:\n{df.head()}")
    logger.info(f"Saved conference details to {core_path.replace('.csv', '_details.csv')}")
    return df