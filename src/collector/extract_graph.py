# src/collector/extract_graph.py

from typing import Any, Dict, List, Tuple

Node = Dict[str, Any]
Edge = Dict[str, Any]


def extract(title: str, parsed: Any) -> Tuple[List[Node], List[Edge]]:
    """
    Extrai nós e arestas de grafo a partir do parse de uma página.

    IMPLEMENTAÇÃO ATUAL (INTENCIONALMENTE SIMPLES E DEFENSIVA):

    - Se `parsed` for um dict no formato do MediaWiki (action=parse),
      você poderá, no futuro, inspecionar campos como:
        parsed["parse"]["links"], parsed["parse"]["categories"], etc.
    - Se `parsed` for string (wikitext/HTML) ou qualquer outra coisa,
      esta função apenas não cria nós/arestas.
    - NUNCA chama `.get()` em algo que não seja dict.

    Retorna:
      (nodes, edges)
    onde:
      nodes: lista de dicts representando nós do grafo
      edges: lista de dicts representando arestas
    """
    nodes: List[Node] = []
    edges: List[Edge] = []

    # Se não for dict, não tentamos interpretar como parse de MediaWiki
    if not isinstance(parsed, dict):
        return nodes, edges

    # A partir daqui, parsed é dict
    parse_block = parsed.get("parse") or {}
    if not isinstance(parse_block, dict):
        return nodes, edges

    # -------------------------------------------------------------------------
    # PONTO DE EXPANSÃO FUTURO:
    #
    # Exemplo: usar links internos pra construir um grafo de páginas.
    #
    # links = parse_block.get("links") or []
    # if isinstance(links, list):
    #     src_id = f"page:{title}"
    #     nodes.append({
    #         "id": src_id,
    #         "label": title,
    #         "type": "Page",
    #     })
    #
    #     for link in links:
    #         if not isinstance(link, dict):
    #             continue
    #         target = link.get("title")
    #         if not target:
    #             continue
    #
    #         dst_id = f"page:{target}"
    #         nodes.append({
    #             "id": dst_id,
    #             "label": target,
    #             "type": "Page",
    #         })
    #
    #         edges.append({
    #             "src": src_id,
    #             "dst": dst_id,
    #             "rel": "LINKS_TO",
    #         })
    #
    # Por enquanto, mantemos vazio para não poluir o grafo enquanto
    # a modelagem não está finalizada.
    # -------------------------------------------------------------------------

    return nodes, edges
