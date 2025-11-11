
import os, argparse, requests
from typing import List
from tqdm import tqdm
from .fandom_api import iter_allpages, get_parse, iter_recentchanges
from .parsers import parse_infobox, extract_sections, extract_relations
from .extract_graph import build_node
from .utils.text import yield_passages
from .indexers.opensearch_index import bulk_upsert as os_bulk_upsert
from .indexers.qdrant_index import upsert_vectors
from src.qa.embeddings import get_embeddings

BASE_URL = os.getenv("FANDOM_BASE_URL", "https://whitewolf.fandom.com")

def process_title(title: str):
    parsed = get_parse(title)
    url = f"{BASE_URL}/wiki/{title.replace(' ', '_')}"
    wikitext = parsed.get("parse", {}).get("wikitext", {}).get("*", "")
    cats = [c["*"] for c in parsed.get("parse", {}).get("categories", []) if not c.get("hidden")]
    infobox = parse_infobox(wikitext)
    sections = extract_sections(wikitext)

    node = build_node(title=title, cats=cats, url=url)
    edges = extract_relations(title, infobox, cats, sections)

    passages = list(yield_passages(title, url, sections))
    if passages:
        os_bulk_upsert(passages)
        try:
            vecs = get_embeddings([p['text'] for p in passages])
            upsert_vectors(vecs, passages)
        except Exception as e:
            print(f"[WARN] Qdrant upsert failed for {title}: {e}")

    from .graph.neo4j_store import upsert_nodes, upsert_edges
    upsert_nodes([node])
    if edges: upsert_edges(edges)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["allpages","localsitemap","recentchanges"], required=True)
    ap.add_argument("--ap-namespace", type=int, default=0)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--hours", type=int, default=24)
    args = ap.parse_args()

    titles: List[str] = []
    if args.mode == "allpages":
        for p in iter_allpages(namespace=args.ap_namespace, limit=args.limit):
            titles.append(p["title"])
    elif args.mode == "localsitemap":
        # fallback uses Special:AllPages
        from bs4 import BeautifulSoup
        count=0; next_url=f"{BASE_URL}/wiki/Special:AllPages"
        while next_url:
            html = requests.get(next_url, timeout=30).text
            soup = BeautifulSoup(html, "html5lib")
            for a in soup.select("ul.mw-allpages-chunk li a"):
                title = a.get_text(" ", strip=True)
                if ":" in title: continue
                titles.append(title); count+=1
                if args.limit and count>=args.limit: next_url=None; break
            nxt = soup.select_one("a.mw-nextlink"); next_url = BASE_URL + nxt.get("href") if nxt else None
    else:
        for title in iter_recentchanges(hours=args.hours):
            titles.append(title)

    titles = list(dict.fromkeys(titles))
    for t in tqdm(titles, desc=f"Ingesting {len(titles)} pages"):
        try: process_title(t)
        except Exception as e: print(f"[WARN] Failed to process {t}: {e}")

if __name__ == "__main__":
    main()
