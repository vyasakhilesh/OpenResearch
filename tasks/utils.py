from typing import Dict, Optional, List
from prefect import task

@task
def ci_get(d: Dict[str, str], key: Optional[str]) -> Optional[str]:
    if key is None:
        return None
    k_norm = key.casefold()
    for dk, dv in d.items():
        if isinstance(dk, str) and dk.casefold() == k_norm:
            return dv
    return None

@task
def filter_rank_series(series_list: List[str], core_26_dict: Dict[str, str], core_23_dict: Dict[str, str], rank: List[str] = ['A*', 'A', 'B', 'C']) -> List[str]:
    candidates = []
    for series in series_list:
        rank_26 = ci_get.run(core_26_dict, series)
        rank_23 = ci_get.run(core_23_dict, series)
        if rank_26:
            if rank_26 in rank:
                candidates.append(series)
        elif rank_23:
            if rank_23 in rank:
                candidates.append(series)
    return candidates

@task
def extract_series_strict_two_tokens(acronym: Optional[str]) -> Optional[str]:
    if not acronym:
        return None
    _YEAR_RE = re.compile(r'^(?:19|20)\d{2}$')
    _ALLCAPS_TOKEN_RE = re.compile(r'^[A-Z0-9]+(?:(?:--|—|–|-|/|&)[A-Z0-9]+)*$')
    tokens = acronym.strip().split()
    if len(tokens) != 2:
        return None
    a, b = tokens[0].strip(), tokens[1].strip()
    if _ALLCAPS_TOKEN_RE.match(a) and _YEAR_RE.match(b):
        return a
    if _YEAR_RE.match(a) and _ALLCAPS_TOKEN_RE.match(b):
        return b
    return None