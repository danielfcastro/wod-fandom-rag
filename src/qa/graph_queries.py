# src/qa/graph_queries.py
import os
from typing import Any, Dict, List

from neo4j import GraphDatabase

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "please_change_me")

# Driver global simples. Se quiser, depois podemos adicionar close() etc.
_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def run_cypher(query: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    """
    Executa uma query Cypher read-only e retorna lista de dicts,
    no formato que o /graph e o /qa esperam.

    Exemplo:
        rows = run_cypher("RETURN 1 AS x")
        -> [ {"x": 1} ]
    """
    params = params or {}
    with _driver.session() as session:
        result = session.run(query, params)
        # result.data() já traz uma lista de dicts, mas vamos garantir:
        rows: List[Dict[str, Any]] = [dict(r) for r in result]
    return rows
