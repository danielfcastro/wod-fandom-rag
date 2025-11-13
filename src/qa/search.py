# src/qa/search.py
import os
from typing import List, Dict
from .settings import OPENSEARCH_INDEX
from .embeddings import embed_query
from .reranker import maybe_rerank
from opensearchpy import OpenSearch

OS_URL = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
INDEX = OPENSEARCH_INDEX

os_client = OpenSearch(OS_URL, verify_certs=False, ssl_show_warn=False)

def lexical_search(query: str, k: int) -> List[Dict]:
    res = os_client.search(
        index=INDEX,
        body={
            "size": k,
            "query": {"match": {"text": {"query": query}}},
            "_source": ["title", "section", "url", "text", "offset"]
        },
    )
    out = []
    for hit in res.get("hits", {}).get("hits", []):
        src = hit.get("_source", {})
        src["_score"] = hit.get("_score", 0.0)
        out.append(src)
    return out

def vector_search(query: str, k: int) -> List[Dict]:
    # Só tenta se o índice tiver um campo 'vector' adequado
    try:
        vec = embed_query(query)
        body = {
            "size": k,
            "query": {
                "knn": {
                    "vector": {
                        "vector": vec,
                        "k": k
                    }
                }
            },
            "_source": ["title", "section", "url", "text", "offset"]
        }
        res = os_client.search(index=INDEX, body=body)
        out = []
        for hit in res.get("hits", {}).get("hits", []):
            src = hit.get("_source", {})
            src["_score"] = hit.get("_score", 0.0)
            out.append(src)
        return out
    except Exception as e:
        # Apenas alerta; segue sem KNN
        print(f"[WARN] vector_search desativado: {e}")
        return []

def hybrid(query: str, k_lex: int = 30, k_vec: int = 30) -> List[Dict]:
    lex = lexical_search(query, k_lex)
    vec = vector_search(query, k_vec)
    seen = set()
    out = []
    for item in lex + vec:
        key = (item.get("title"), item.get("offset"))
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return maybe_rerank(query, out, top_k=min(len(out), k_lex))

