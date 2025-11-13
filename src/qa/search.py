# src/qa/search.py
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

_USE_VECTOR = True  # será desativado em runtime se der erro

def lexical_search(query: str, k_lex: int = 30) -> List[Dict]:
    body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["text^3", "title^2", "section"]
            }
        },
        "_source": ["title", "url", "text", "section"],
        "size": k_lex,
    }
    res = os_client.search(index=OPENSEARCH_INDEX, body=body)
    hits = res.get("hits", {}).get("hits", [])
    docs: List[Dict] = []
    for h in hits:
        src = h.get("_source", {})
        docs.append({
            "title": src.get("title"),
            "url": src.get("url"),
            "text": src.get("text"),
            "section": src.get("section"),
            "score": h.get("_score", 0.0),
            "source_type": "lexical",
        })
    return docs

def vector_search(query: str, k_vec: int = 30) -> List[Dict]:
    """
    Tentativa de KNN. Se falhar (campo 'vector' não é knn_vector, etc),
    desativa permanentemente a busca vetorial e passa a retornar [].
    """
    global _USE_VECTOR
    if not _USE_VECTOR or k_vec <= 0:
        return []

    try:
        # Aqui só é útil se o índice tiver 'vector' como knn_vector.
        # Como o teu índice hoje não está assim, isso vai falhar uma vez e desativar.
        body = {
            "size": k_vec,
            "query": {
                "neural": {
                    "vector": {
                        "query_text": query,
                    }
                }
            },
            "_source": ["title", "url", "text", "section"],
        }
        res = os_client.search(index=OPENSEARCH_INDEX, body=body)
        hits = res.get("hits", {}).get("hits", [])
        docs: List[Dict] = []
        for h in hits:
            src = h.get("_source", {})
            docs.append({
                "title": src.get("title"),
                "url": src.get("url"),
                "text": src.get("text"),
                "section": src.get("section"),
                "score": h.get("_score", 0.0),
                "source_type": "vector",
            })
        return docs
    except Exception as e:
        print(f"[WARN] vector_search desativado: {e}")
        _USE_VECTOR = False
        return []

def hybrid(query: str, k_lex: int = 30, k_vec: int = 30) -> List[Dict]:
    """
    Combina lexical + vetorial, de-duplica por (title, url, section) e ordena por score desc.
    """
    docs = []
    try:
        docs.extend(lexical_search(query, k_lex))
    except Exception as e:
        print(f"[ERRO] lexical_search falhou: {e}")

    try:
        docs.extend(vector_search(query, k_vec))
    except Exception as e:
        print(f"[ERRO] vector_search falhou: {e}")

    # De-duplicação
    seen = {}
    for d in docs:
        key = (d.get("title"), d.get("url"), d.get("section"))
        score = float(d.get("score", 0.0))
        if key not in seen or score > float(seen[key].get("score", 0.0)):
            seen[key] = d

    final = sorted(seen.values(), key=lambda d: float(d.get("score", 0.0)), reverse=True)
    return final
