
from typing import List, Dict, Tuple
from .settings import COHERE_API_KEY, RERANK_MODEL

def rerank(query: str, candidates: List[Dict], text_key: str = "text", top_k: int = 10) -> List[Tuple[int, float]]:
    if not candidates: return []
    texts = [c.get(text_key, "") for c in candidates]
    if COHERE_API_KEY:
        import cohere
        co = cohere.Client(api_key=COHERE_API_KEY)
        res = co.rerank(model="rerank-english-v3.0", query=query, documents=texts, top_n=top_k)
        return [(r.index, float(r.relevance_score)) for r in res.results]
    from sentence_transformers import CrossEncoder
    import torch
    ce = CrossEncoder(RERANK_MODEL)
    pairs = [[query, t] for t in texts]
    with torch.inference_mode(): scores = ce.predict(pairs)
    idx_scores = list(enumerate([float(s) for s in scores]))
    idx_scores.sort(key=lambda x:x[1], reverse=True)
    return idx_scores[:top_k]
