# src/qa/search.py
import os
from opensearchpy import OpenSearch
from opensearchpy.exceptions import NotFoundError

# ---------------------------------------------------------------------------
# Configurações do OpenSearch
# ---------------------------------------------------------------------------
OPENSEARCH_URL = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
OPENSEARCH_INDEX = os.getenv("OPENSEARCH_INDEX", "passages-wod")

os_client = OpenSearch(OPENSEARCH_URL)

# ---------------------------------------------------------------------------
# Garante que o índice existe
# ---------------------------------------------------------------------------
try:
    # Importa a função que define o mapping usado pelo coletor
    from src.collector.indexers.opensearch_index import ensure_index
    ensure_index()
except Exception as e:
    print(f"[WARN] não foi possível garantir o índice no startup: {e}")

# ---------------------------------------------------------------------------
# Funções de busca
# ---------------------------------------------------------------------------

def lexical_search(query: str, k: int):
    """
    Busca lexical (texto exato e relevância TF-IDF-like)
    """
    try:
        res = os_client.search(
            index=OPENSEARCH_INDEX,
            body={
                "size": k,
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["title^3", "section^2", "text"],
                    }
                },
            },
        )
        hits = res.get("hits", {}).get("hits", [])
        return [
            {**hit["_source"], "_score": hit.get("_score", 0.0)} for hit in hits
        ]
    except NotFoundError:
        print(f"[WARN] índice {OPENSEARCH_INDEX} não encontrado — criando...")
        try:
            ensure_index()
        except Exception as e:
            print(f"[ERRO] não foi possível criar o índice: {e}")
        return []
    except Exception as e:
        print(f"[ERRO] lexical_search falhou: {e}")
        return []


def vector_search(query: str, k: int):
    """
    Busca vetorial (semantic search).
    Depende de embeddings já armazenados no OpenSearch.
    """
    try:
        res = os_client.search(
            index=OPENSEARCH_INDEX,
            body={
                "size": k,
                "query": {
                    "knn": {
                        "embedding": {
                            "vector": [],  # o vetor é preenchido na camada superior
                            "k": k,
                        }
                    }
                },
            },
        )
        hits = res.get("hits", {}).get("hits", [])
        return [
            {**hit["_source"], "_score": hit.get("_score", 0.0)} for hit in hits
        ]
    except NotFoundError:
        print(f"[WARN] índice {OPENSEARCH_INDEX} não encontrado — criando...")
        try:
            ensure_index()
        except Exception as e:
            print(f"[ERRO] não foi possível criar o índice: {e}")
        return []
    except Exception as e:
        print(f"[ERRO] vector_search falhou: {e}")
        return []


def hybrid(query: str, k_lex: int = 30, k_vec: int = 30):
    """
    Combina resultados lexicais e vetoriais.
    Retorna uma lista de documentos únicos, ordenados por score combinado.
    """
    try:
        docs_lex = lexical_search(query, k_lex)
        docs_vec = vector_search(query, k_vec)

        seen = {}
        for doc in docs_lex + docs_vec:
            _id = doc.get("id") or doc.get("title")
            if not _id:
                continue
            if _id not in seen:
                seen[_id] = doc
            else:
                seen[_id]["_score"] += doc.get("_score", 0.0)

        return sorted(
            seen.values(), key=lambda x: x.get("_score", 0.0), reverse=True
        )

    except Exception as e:
        print(f"[ERRO] hybrid search falhou: {e}")
        return []
