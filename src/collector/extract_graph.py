# src/collector/extract_graph.py
from typing import Any, Dict, List

def extract(*args: Any, **kwargs: Any) -> Dict[str, List[Dict[str, Any]]]:
    """
    Placeholder para extração de grafo a partir dos textos.

    Ele existe só para manter compatibilidade com run_ingest.py:
      from .extract_graph import extract as extract_graph

    Qualquer que seja a chamada (por exemplo:
        extract(passage)
        extract(title, full_text)
    )
    essa função:
      - não levanta erro
      - simplesmente diz "não achei nenhum nó/aresta novo".
    """
    return {"nodes": [], "edges": []}
