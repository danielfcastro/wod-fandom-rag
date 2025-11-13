# src/qa/search.py
import os
from typing import List, Dict

from opensearchpy import OpenSearch
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer

OPENSEARCH_URL   = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
OPENSEARCH_INDEX = os.getenv("OPENSEARCH_INDEX", "passages-wod")

QDRANT_URL        = os.getenv("QDRANT_URL", "http://qdrant:6333")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "passages-wod")
EMBEDDING_MODEL   = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

os_client = OpenSearch(OPENSEARCH_URL)
qdrant    = QdrantClient(url=QDRANT_URL, prefer_grpc=False, check_compatibility=False)
_model    = SentenceTransformer(EMBEDDING_MODEL)

def lexical_search(query: str, k: int) -> List[Dict]:
    res = os_client.search(index=OPENSEARCH_INDEX, body={
        "size": k,
        "query": {"multi_match": {"query": query, "fields": ["title^2", "text"]}},
        "_source": ["title", "section", "url", "text", "offset"]
    })
    hits = res.get("hits", {}).get("hits", [])
    out = []
    for h in hits:
        s = h.get("_source", {})
        out.append({
            "title": s.get("title"),
            "section": s.get("section"),
            "url": s.get("url"),
            "text": s.get("text"),
            "offset": s.get("offset"),
            "score": h.get("_score", 0.0),
            "source": "lexical"
        })
    return out

def vector_search(query: str, k: int) -> List[Dict]:
    # Agora via Qdrant (nada de kNN no OpenSearch)
    vec = _model.encode([query], normalize_embeddings=True)[0].tolist()
    res = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=vec,
        limit=k,
        with_payload=True
    )
    out = []
    for p in res:
        payload = p.payload or {}
        out.append({
            "title": payload.get("title"),
            "section": payload.get("section"),
            "url": payload.get("url"),
            "text": None,  # opcional carregar texto completo
            "offset": payload.get("offset"),
            "score": float(p.score or 0.0),
            "source": "vector"
        })
    return out

def hybrid(query: str, k_lex: int, k_vec: int) -> List[Dict]:
    # concatena resultados; o reranker (se ativo) cuida da ordenação depois
    return lexical_search(query, k_lex) + vector_search(query, k_vec)
