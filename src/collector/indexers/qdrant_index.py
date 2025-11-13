# src/collector/indexers/qdrant_index.py
import os, hashlib
from typing import Dict, Iterable, List

from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer

URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "passages-wod")
MODEL_NAME = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

# Desliga checagem de compat de versão (vc está com server 1.11.x)
client = QdrantClient(
    url=URL, prefer_grpc=False, timeout=60, api_key=None, check_compatibility=False
)
_model = SentenceTransformer(MODEL_NAME)
DIM = _model.get_sentence_embedding_dimension()

def _ensure_collection():
    try:
        info = client.get_collection(COLLECTION)
        # se existir, OK
        _ = info.vectors_count  # só pra forçar acesso
    except Exception:
        client.recreate_collection(
            collection_name=COLLECTION,
            vectors_config=models.VectorParams(size=DIM, distance=models.Distance.COSINE),
        )

def _embed_texts(texts: List[str]):
    return _model.encode(texts, normalize_embeddings=True, show_progress_bar=False)

def _stable_id(d: Dict) -> str:
    """
    Gera um ID determinístico pro ponto a partir dos campos do payload.
    Evita duplicatas e satisfaz o Pydantic do PointStruct.
    """
    key = f"{d.get('title','')}|{d.get('url','')}|{d.get('section','')}|{d.get('offset',0)}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()

def bulk_upsert(docs: Iterable[Dict]):
    _ensure_collection()

    docs = list(docs) or []
    payloads, texts, ids = [], [], []

    for d in docs:
        txt = (d.get("text") or "").strip()
        if not txt:
            continue
        # payload limpo
        payload = {
            "title": d.get("title"),
            "section": d.get("section"),
            "url": d.get("url"),
            "offset": d.get("offset", 0),
        }
        payloads.append(payload)
        texts.append(txt)
        ids.append(_stable_id(payload))  # <- gera ID válido (string)

    if not texts:
        return

    vectors = _embed_texts(texts)

    # IMPORTANTE: não passe id=None; aqui TODO ponto tem id str estável
    points = [
        models.PointStruct(id=ids[i], vector=vectors[i].tolist(), payload=payloads[i])
        for i in range(len(texts))
    ]

    client.upsert(collection_name=COLLECTION, points=points)
