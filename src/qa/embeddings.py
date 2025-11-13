import os
from typing import List

from sentence_transformers import SentenceTransformer

# Nome do modelo de embeddings (o mesmo que você já usou na ingestão)
EMBEDDING_MODEL = os.getenv(
    "EMBEDDING_MODEL",
    "sentence-transformers/all-MiniLM-L6-v2",
)

_model: SentenceTransformer | None = None


def _get_model() -> SentenceTransformer:
    """
    Lazy-load do modelo de embeddings para não inicializar
    antes da primeira chamada.
    """
    global _model
    if _model is None:
        _model = SentenceTransformer(EMBEDDING_MODEL)
    return _model


def embed_query(text: str) -> List[float]:
    """
    Gera o embedding de uma única string de consulta.
    Retorna uma lista de floats (compatível com Qdrant / OpenSearch).
    """
    if not text:
        return []
    model = _get_model()
    vec = model.encode(text)
    return vec.tolist()


def embed_passages(texts: List[str]) -> List[List[float]]:
    """
    Gera embeddings para uma lista de textos (passagens).
    """
    if not texts:
        return []
    model = _get_model()
    vecs = model.encode(texts)
    return [v.tolist() for v in vecs]

