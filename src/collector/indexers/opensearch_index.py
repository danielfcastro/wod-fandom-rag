import os
from typing import Dict, Iterable
from opensearchpy import OpenSearch

INDEX = os.getenv("OPENSEARCH_INDEX", "passages-wod")
URL = os.getenv("OPENSEARCH_URL", "http://localhost:9200")
client = OpenSearch(URL)

MAPPING = {
    "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
    "mappings": {
        "properties": {
            "title":   {"type": "keyword"},
            "section": {"type": "keyword"},
            "url":     {"type": "keyword"},
            "text":    {"type": "text"},
            "offset":  {"type": "integer"},
        }
    },
}


def ensure_index():
    exists = client.indices.exists(index=INDEX)
    if not exists:
        client.indices.create(index=INDEX, body=MAPPING)


def bulk_upsert(passages: Iterable[Dict]):
    """
    Faz bulk upsert em batches.
    - Garante que o índice exista.
    - Remove campos inválidos (_id) do documento.
    - Loga erros retornados pelo OpenSearch.
    """
    from itertools import islice

    ensure_index()

    def chunks(iterable, n=500):
        it = iter(iterable)
        while True:
            batch = list(islice(it, n))
            if not batch:
                break
            yield batch

    ops = []

    for batch in chunks(passages, 500):
        ops.clear()
        for doc in batch:
            # Evita conflito com metadados do OpenSearch
            if "_id" in doc:
                doc = dict(doc)  # copia rasa pra não mutar o original em outros lugares
                doc.pop("_id", None)

            ops.append({"index": {"_index": INDEX}})
            ops.append(doc)

        resp = client.bulk(body=ops, refresh=True)

        if resp.get("errors"):
            print(f"[WARN] OpenSearch bulk errors em index={INDEX}:")
            for item in resp.get("items", []):
                err = item.get("index", {}).get("error")
                if err:
                    print("  -", err)
