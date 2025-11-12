# src/collector/fandom_api.py
import os
import time
import requests
from typing import Dict, Iterable, Optional

API_BASE = os.getenv("FANDOM_API_BASE", "https://whitewolf.fandom.com/api.php")


def _throttle(delay: float = 0.35) -> None:
    time.sleep(delay)


def api_get(params: Dict) -> Dict:
    _throttle()
    q = dict(params)
    q.setdefault("format", "json")
    q.setdefault("formatversion", "2")
    r = requests.get(API_BASE, params=q, timeout=30)
    r.raise_for_status()
    return r.json()


def get_parse(title: str) -> Dict:
    return api_get(
        {
            "action": "parse",
            "page": title,
            "prop": "wikitext|sections|links|categories",
            "redirects": 1,
        }
    )


def get_allpages(ap_namespace: int = 0, limit: Optional[int] = None) -> Iterable[str]:
    """
    Itera *todas* as páginas do namespace (sem redirects),
    usando aplimit=max e seguindo apcontinue até o fim.
    Se 'limit' for fornecido, corta após N títulos.
    """
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
