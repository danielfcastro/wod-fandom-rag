
import os
from typing import Dict, Iterable, List
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

COLLECTION = os.getenv("QDRANT_COLLECTION", "passages-wod")
URL = os.getenv("QDRANT_URL", "http://localhost:6333")
client = QdrantClient(url=URL)

def ensure_collection(dim: int = 384):
    if COLLECTION not in [c.name for c in client.get_collections().collections]:
        client.create_collection(collection_name=COLLECTION, vectors_config=VectorParams(size=dim, distance=Distance.COSINE))

def upsert_vectors(vectors: List[List[float]], payloads: List[Dict]):
    ensure_collection(dim=len(vectors[0]) if vectors else 384)
    points=[PointStruct(id=i, vector=v, payload=payloads[i]) for i,v in enumerate(vectors)]
    client.upsert(collection_name=COLLECTION, points=points)
