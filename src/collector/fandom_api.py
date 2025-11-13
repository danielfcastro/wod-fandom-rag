# src/collector/fandom_api.py
import os, time, requests
from typing import Dict, Iterator, Optional

API_BASE = os.getenv("FANDOM_API_BASE", "https://whitewolf.fandom.com/api.php")

def _throttle(delay: float = 0.35):
    time.sleep(delay)

def api_get(params: Dict):
    _throttle()
    params = dict(params)
    params.setdefault("format", "json")
    params.setdefault("formatversion", "2")
    r = requests.get(API_BASE, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def get_parse(title: str):
    return api_get({
        "action": "parse",
        "page": title,
        "prop": "wikitext|sections|links|categories",
        "redirects": 1,
    })

def iter_allpages(ap_namespace: int = 0, limit: Optional[int] = None) -> Iterator[str]:
    fetched = 0
    apcontinue = None
    while True:
        params = {
            "action": "query",
            "list": "allpages",
            "apnamespace": ap_namespace,
            "aplimit": "max",
            "apfilterredir": "nonredirects",
        }
        if apcontinue:
            params["apcontinue"] = apcontinue

        data = api_get(params)
        pages = data.get("query", {}).get("allpages", [])
        if not pages:
            break

        for p in pages:
            title = p.get("title")
            if not title:
                continue
            yield title
            fetched += 1
            if limit and fetched >= limit:
                return

        apcontinue = data.get("continue", {}).get("apcontinue")
        if not apcontinue:
            break
