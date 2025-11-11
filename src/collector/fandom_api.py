
import os, time
from typing import Dict, Iterable, Optional
import requests
from tenacity import retry, wait_exponential, stop_after_attempt

FANDOM_API_BASE = os.getenv("FANDOM_API_BASE", "https://whitewolf.fandom.com/api.php")
HEADERS = {"User-Agent": "wod-fandom-rag/1.0 (collector)"}

def _throttle(delay: float = 0.35): time.sleep(delay)

@retry(wait=wait_exponential(multiplier=0.5, min=0.5, max=8), stop=stop_after_attempt(5))
def api_get(params: Dict) -> Dict:
    _throttle()
    params = dict(params); params["format"] = "json"
    r = requests.get(FANDOM_API_BASE, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def iter_allpages(namespace: int = 0, limit: Optional[int] = None) -> Iterable[Dict]:
    apcontinue = None; count = 0
    while True:
        params = {"action":"query","list":"allpages","apnamespace":namespace,"aplimit":"500"}
        if apcontinue: params["apcontinue"] = apcontinue
        data = api_get(params)
        for p in data.get("query", {}).get("allpages", []):
            yield {"title": p["title"], "pageid": p["pageid"]}
            count += 1
            if limit and count >= limit: return
        apcontinue = data.get("continue", {}).get("apcontinue")
        if not apcontinue: return

def get_parse(title: str) -> Dict:
    return api_get({"action":"parse","page":title,"prop":"wikitext|sections|links|categories","redirects":1})

def iter_recentchanges(hours: int = 24) -> Iterable[str]:
    import datetime as dt
    now = dt.datetime.utcnow(); rcend = now - dt.timedelta(hours=hours)
    params = {"action":"query","list":"recentchanges","rcprop":"title|ids|timestamp|type","rclimit":"500",
              "rcstart": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "rcend": rcend.strftime("%Y-%m-%dT%H:%M:%SZ")}
    rccontinue=None
    while True:
        p=dict(params); 
        if rccontinue: p["rccontinue"]=rccontinue
        data=api_get(p)
        for rc in data.get("query", {}).get("recentchanges", []):
            if rc.get("type") in ("edit","new") and rc.get("title"): yield rc["title"]
        rccontinue=data.get("continue", {}).get("rccontinue")
        if not rccontinue: return
