
from typing import List, Dict
from opensearchpy import OpenSearch
from qdrant_client import QdrantClient
from .settings import OPENSEARCH_URL, OPENSEARCH_INDEX, QDRANT_URL, QDRANT_COLLECTION
from .embeddings import get_embeddings

def lexical_search(query: str, k: int = 25) -> List[Dict]:
    os_client = OpenSearch(OPENSEARCH_URL)
    res = os_client.search(index=OPENSEARCH_INDEX, body={
        "size": k, "query": {"match": {"text": query}}, "_source":["title","section","url","text","offset"]
    })
    return [h["_source"] for h in res["hits"]["hits"]]

def vector_search(query: str, k: int = 25) -> List[Dict]:
    q = QdrantClient(url=QDRANT_URL)
    vec = get_embeddings([query])[0]
    try:
        res = q.search(collection_name=QDRANT_COLLECTION, query_vector=vec, limit=k)
        return [r.payload for r in res]
    except Exception: return []

def hybrid(query: str, k_lex: int = 25, k_vec: int = 25) -> List[Dict]:
    seen=set(); merged=[]
    for doc in lexical_search(query,k_lex) + vector_search(query,k_vec):
        key=(doc.get("title"),doc.get("section"),doc.get("offset"))
        if key not in seen: merged.append(doc); seen.add(key)
    return merged
