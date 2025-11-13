# src/qa/reranker.py
import os
from typing import List, Dict

COHERE_API_KEY = os.getenv("COHERE_API_KEY") or ""
RERANK_MODEL = os.getenv("RERANK_MODEL", "cross-encoder/ms-marco-MiniLM-L-6-v2")

# Versão simplificada: se não houver reranker configurado, apenas ordena pelos scores existentes

def rerank(query: str, docs: List[Dict], top_k: int | None = None) -> List[Dict]:
    """
    Recebe uma lista de docs do search.hybrid, já no formato:
        { "title": ..., "url": ..., "text": ..., "score": ... }

    Retorna a mesma lista, possivelmente reordenada, e opcionalmente truncada em top_k.
    """
    # Se quiser integrar com Cohere / outro reranker no futuro, pluga aqui.
    # Por enquanto: apenas ordena por 'score' desc.
    ordered = sorted(docs, key=lambda d: float(d.get("score", 0.0)), reverse=True)
    if top_k is not None:
        ordered = ordered[:top_k]
    return ordered
