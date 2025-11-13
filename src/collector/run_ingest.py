import os
import re
import hashlib
from typing import Dict, Iterable, Iterator, List, Tuple, Optional, Union

# --- Fandom API (lista de páginas + parse) ---
from .fandom_api import iter_allpages, get_parse

# --- Indexadores ---
from .indexers.opensearch_index import (
    ensure_index as os_ensure_index,
    bulk_upsert as os_bulk_upsert,
)

# Qdrant pode ter nomes diferentes conforme sua versão do arquivo.
# Tentamos importar com fallback.
_qdrant_upsert = None
try:
    from .indexers.qdrant_index import bulk_upsert as _qdrant_upsert
except Exception:
    try:
        from .indexers.qdrant_index import upsert_points as _qdrant_upsert
    except Exception:
        _qdrant_upsert = None  # vetor opcional

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
    # MediaWiki titles → URL
    return f"{base}/wiki/{title.replace(' ', '_')}"


# -----------------------------------------------------------------------------
# Extração de passagens (bem simples, por seções do parse)
# -----------------------------------------------------------------------------
def extract_passages(title: str, parsed: Union[Dict, str]) -> List[Dict]:
    """
    Transforma o resultado do `action=parse` em passagens:
    - Uma passagem por seção com wikitext "linearizado".
    - Define um offset incremental para estabilidade.

    `parsed` pode ser:
      - o JSON completo retornado por `action=parse` (dict)
      - OU já um wikitext cru (str)
    """
    passages: List[Dict] = []

    # parsed pode vir como dict (resposta bruta do MediaWiki)
    # ou já como string (conteúdo plain/wikitext).
    if isinstance(parsed, str):
        wikitext = parsed
        sections = []
    else:
        wikitext = parsed.get("parse", {}).get("wikitext", {}).get("*", "")
        sections = parsed.get("parse", {}).get("sections", []) or []

    # Mapa index->nome de seção (por enquanto não usamos muito, mas mantemos)
    sec_names = {
        int(s.get("index", 0)): s.get("line", "")
        for s in sections
        if "index" in s
    }

    # Estratégia atual:
    # - Trabalhar com "chunks" de seções, mas como ainda não temos
    #   um parser de wikitext real plugado aqui, usamos o wikitext
    #   da página inteira em todas as seções, com uma "Intro".
    chunks: List[Tuple[str, Optional[str]]] = []

    if sections:
        # Cria uma entrada por seção, mas o texto vai ser o mesmo wikitext
        for s in sections:
            idx = int(s.get("index", 0))
            name = s.get("line", "") or f"Section {idx}"
            chunks.append((name, None))

        if not chunks:
            chunks = [("Intro", None)]
    else:
        chunks = [("Intro", None)]

    # Se houver mais de uma "seção", criamos:
    # - Intro com o wikitext inteiro
    # - Demais seções também com o mesmo texto (por enquanto)
    if chunks and len(chunks) > 1:
        out: List[Tuple[str, Optional[str]]] = [("Intro", wikitext)]
        out += [(name, wikitext) for (name, _) in chunks if name != "Intro"]
        chunks = out
    else:
        chunks = [("Intro", wikitext)]

    offset = 0
    for sec_name, text in chunks:
        body = _norm_space(text or "")
        if not body:
            continue
        passages.append(
            {
                "_id": _stable_id(title, sec_name or "Intro", str(offset)),
                "title": title,
                "section": sec_name or "Intro",
                "url": _page_url(title),
                "text": body,
                "offset": offset,
            }
        )
        offset += len(body)

    return passages


# -----------------------------------------------------------------------------
# Processamento de um título
# -----------------------------------------------------------------------------
def ingest_title(title: str):
    """
    Retorna tupla (os_docs, qdrant_pts, graph_edges)
    """
    # 1) parse
    parsed = get_parse(title)

    # 2) passagens
    passages = extract_passages(title, parsed)
    if not passages:
        return (0, 0, 0)

    # 3) upsert OpenSearch (sempre)
    os_bulk_upsert(passages)

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
    try:
        nodes, edges = extract_graph(title, parsed)
        if nodes:
            upsert_nodes(nodes)
        if edges:
            upsert_edges(edges)
        g_edges = len(edges or [])
    except Exception as e:
        print(f"[WARN] Grafo falhou em '{title}': {e}")
        g_edges = 0

    return (len(passages), qdr_cnt, g_edges)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def main():
    import argparse

    os_ensure_index()  # garante o índice lexical

    ap = argparse.ArgumentParser("Ingestor Fandom → OS/Qdrant/Neo4j")
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
