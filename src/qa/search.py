import os
from typing import List, Dict
from opensearchpy import OpenSearch, RequestsHttpConnection

OPENSEARCH_URL = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
OPENSEARCH_INDEX = os.getenv("OPENSEARCH_INDEX", "passages-wod")

os_client = OpenSearch(
    hosts=[OPENSEARCH_URL],
    http_compress=True,
    use_ssl=False,
    verify_certs=False,
    connection_class=RequestsHttpConnection,
)

def lexical_search(query: str, k_lex: int = 20) -> List[Dict]:
    body = {
        "size": k_lex,
        "query": {
            "multi_match": {
                "query": query,
                "fields": [
                    "title^5",
                    "text^3",
                    "section",
                ],
                "type": "best_fields",
            }
        },
        "_source": ["title", "url", "text", "section"],
    }

    res = os_client.search(index=OPENSEARCH_INDEX, body=body)
    docs: List[Dict] = []

    for hit in res.get("hits", {}).get("hits", []):
        src = hit.get("_source", {})
        docs.append(
            {
                "title": src.get("title"),
                "url": src.get("url"),
                "text": src.get("text"),
                "section": src.get("section"),
                "score": float(hit.get("_score", 0.0)),
            }
        )

    return docs

def vector_search(query: str, k_vec: int = 20) -> List[Dict]:
    # Desabilitado por enquanto
    return []

def hybrid(query: str, k_lex: int = 20, k_vec: int = 20) -> List[Dict]:
    docs: List[Dict] = []

    if k_lex > 0:
        try:
            docs.extend(lexical_search(query, k_lex))
        except Exception as e:
            print(f"[ERRO] lexical_search falhou: {e}")

    if k_vec > 0:
        try:
            docs.extend(vector_search(query, k_vec))
        except Exception as e:
            print(f"[ERRO] vector_search falhou: {e}")

    # De-duplicação por (title, url, section)
    seen: Dict[tuple, Dict] = {}
    for d in docs:
        key = (d.get("title"), d.get("url"), d.get("section"))
        score = float(d.get("score", 0.0))
        if key not in seen or score > float(seen[key].get("score", 0.0)):
            seen[key] = d

    final = sorted(
        seen.values(),
        key=lambda d: float(d.get("score", 0.0)),
        reverse=True,
    )
    return final
