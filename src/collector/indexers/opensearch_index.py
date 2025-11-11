import os
from typing import Dict, Iterable
from opensearchpy import OpenSearch

INDEX = os.getenv('OPENSEARCH_INDEX', 'passages-wod')
URL = os.getenv('OPENSEARCH_URL', 'http://localhost:9200')
client = OpenSearch(URL)

MAPPING = {
    'settings': {'index': {'number_of_shards': 1, 'number_of_replicas': 0}},
    'mappings': {
        'properties': {
            'title':   {'type': 'keyword'},
            'section': {'type': 'keyword'},
            'url':     {'type': 'keyword'},
            'text':    {'type': 'text'},
            'offset':  {'type': 'integer'},
        }
    }
}

def ensure_index():
    # opensearch-py 2.x exige 'index=' como keyword arg
    exists = client.indices.exists(index=INDEX)
    if not exists:
        client.indices.create(index=INDEX, body=MAPPING)

def bulk_upsert(passages: Iterable[Dict]):
    ensure_index()
    from itertools import islice
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
            ops.append({'index': {'_index': INDEX}})
            ops.append(doc)
        client.bulk(body=ops, refresh=True)
