import os
import re
import hashlib
from typing import Dict, Iterable, Iterator, List, Tuple, Optional, Union, Any

# --- Fandom API (lista de páginas + parse) ---
from .fandom_api import iter_allpages, get_parse

# --- Indexadores ---
from .indexers.opensearch_index import (
    ensure_index as os_ensure_index,
    bulk_upsert as os_bulk_upsert,
)

# Qdrant é opcional: tenta importar, se falhar, segue sem vetor
_qdrant_upsert = None
try:
    from .indexers.qdrant_index import bulk_upsert as _qdrant_upsert
except Exception:
    try:
        from .indexers.qdrant_index import upsert_points as _qdrant_upsert
    except Exception:
        _qdrant_upsert = None  # vetorial opcional

# --- Grafo ---
from .extract_graph import extract as extract_graph
from .graph.neo4j_store import upsert_nodes, upsert_edges


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
OS_INDEX = os.getenv("OPENSEARCH_INDEX", "passages-wod")


# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _stable_id(*parts: str) -> str:
    base = "|".join(p if p is not None else "" for p in parts)
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def _page_url(title: str) -> str:
    base = os.getenv("FANDOM_BASE_URL", "https://whitewolf.fandom.com")
    return f"{base}/wiki/{title.replace(' ', '_')}"


# -----------------------------------------------------------------------------
# Extração de passagens (defensiva)
# -----------------------------------------------------------------------------
def extract_passages(title: str, parsed: Any) -> List[Dict]:
    """
    Transforma o resultado de get_parse(title) em passagens.

    - Se parsed vier como dict no formato do action=parse, usa:
        parsed["parse"]["wikitext"]["*"]  (ou equivalente)
    - Se vier como string ou qualquer outra coisa, converte para string.
    - NUNCA chama .get() em algo que não seja dict.
    - Para simplificar, gera por enquanto apenas uma passagem "Intro"
      com o texto inteiro.
    """
    passages: List[Dict] = []

    wikitext = ""

    # Caso 1: parsed é dict no formato padrão do MediaWiki
    if isinstance(parsed, dict):
        parse_block = parsed.get("parse") or {}
        wt = parse_block.get("wikitext")

        # wt pode ser dict {"*": "..."} ou string
        if isinstance(wt, dict):
            wikitext = wt.get("*", "") or ""
        elif isinstance(wt, str):
            wikitext = wt
        else:
            wikitext = ""
    else:
        # Caso 2: qualquer outra coisa (str, None, etc.)
        wikitext = str(parsed or "")

    # Se não vier nada de texto, não há passagens para indexar
    if not wikitext:
        return []

    body = _norm_space(wikitext)
    if not body:
        return []

    passages.append(
        {
            "_id": _stable_id(title, "Intro", "0"),
            "title": title,
            "section": "Intro",
            "url": _page_url(title),
            "text": body,
            "offset": 0,
        }
    )

    return passages


# -----------------------------------------------------------------------------
# Processamento de um título
# -----------------------------------------------------------------------------
def ingest_title(title: str):
    """
    Processa um título:
      - chama get_parse(title)
      - extrai passagens
      - upsert em OpenSearch (sempre)
      - upsert em Qdrant (se configurado)
      - atualiza grafo em Neo4j (se extract_graph retornar algo)

    Retorna tupla (os_docs, qdrant_pts, graph_edges).
    """
    # 1) parse
    parsed = get_parse(title)

    # 2) passagens
    passages = extract_passages(title, parsed)
    if not passages:
        return (0, 0, 0)

    # 3) upsert OpenSearch (sempre)
    os_bulk_upsert(passages)
    os_cnt = len(passages)

    # 4) upsert Qdrant (se disponível)
    qdr_cnt = 0
    if _qdrant_upsert is not None:
        try:
            _qdrant_upsert(passages)
            qdr_cnt = len(passages)
        except Exception as e:
            # não bloqueia ingestão se Qdrant falhar — apenas loga
            print(f"[WARN] Qdrant upsert falhou em '{title}': {e}")

    # 5) grafo (nodes, edges)
    g_edges = 0
    try:
        nodes, edges = extract_graph(title, parsed)
        if nodes:
            upsert_nodes(nodes)
        if edges:
            upsert_edges(edges)
        g_edges = len(edges or [])
    except Exception as e:
        print(f"[WARN] Grafo falhou em '{title}': {e}")

    return (os_cnt, qdr_cnt, g_edges)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def main():
    import argparse

    os_ensure_index()  # garante o índice lexical em OpenSearch

    ap = argparse.ArgumentParser("Ingestor Fandom -> OpenSearch/Qdrant/Neo4j")
    ap.add_argument("--mode", choices=["title", "allpages"], default="allpages")
    ap.add_argument("--title", type=str, help="Título único (mode=title)")
    ap.add_argument(
        "--ap-namespace", type=int, default=0, help="Namespace MediaWiki (0=artigos)"
    )
    ap.add_argument(
        "--limit", type=int, default=0, help="Limite de páginas (0 = sem limite)"
    )
    args = ap.parse_args()

    if args.mode == "title":
        if not args.title:
            raise SystemExit("--title é obrigatório com --mode title")
        os_n, qd_n, ge_n = ingest_title(args.title)
        print(f"[single] {args.title} -> OS={os_n} QD={qd_n} Gedges={ge_n}")
        return

    # mode=allpages
    total = 0
    for i, title in enumerate(
        iter_allpages(
            ap_namespace=args.ap_namespace, limit=(args.limit or None)
        ),
        start=1,
    ):
        try:
            os_n, qd_n, ge_n = ingest_title(title)
            print(f"[{i}] {title} -> OS={os_n} QD={qd_n} Gedges={ge_n}")
        except Exception as e:
            print(f"[WARN] ingest_title('{title}') falhou: {e}")
        total = i

    print(f"[done] processados: {total}")


if __name__ == "__main__":
    main()
