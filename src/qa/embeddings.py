
from typing import List
from .settings import OPENAI_API_KEY, EMBEDDING_MODEL

def get_embeddings(texts: List[str]) -> List[List[float]]:
    if OPENAI_API_KEY:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        res = client.embeddings.create(model="text-embedding-3-large", input=texts)
        return [d.embedding for d in res.data]
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(EMBEDDING_MODEL)
    return model.encode(texts, batch_size=32, normalize_embeddings=True).tolist()
