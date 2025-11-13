# src/qa/service.py
import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any

from .search import hybrid
from .reranker import rerank
from .graph_queries import run_cypher

QA_HOST = os.getenv("QA_HOST", "0.0.0.0")
QA_PORT = int(os.getenv("QA_PORT", "8000"))

app = FastAPI(title="WoD Fandom RAG")

# CORS liberado para dev (localhost:5173)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # em produção você pode restringir
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/qa")
def qa(
    query: str = Query(..., description="Pergunta do usuário"),
    top_k: int = Query(6, ge=1, le=50),
    use_graph: bool = Query(True),
):
    """
    Endpoint principal de QA.

    Retorna:
      {
        "query": "...",
        "passages": [ {title,url,text,score}, ... ],
        "graph": [ {col: val, ...}, ... ]
      }
    """
    # Busca híbrida (lexical + vetorial se estiver habilitado)
    candidates = hybrid(query, k_lex=max(top_k * 3, 30), k_vec=0)  # k_vec=0 por enquanto

    # Reordenar (no momento só ordena pelo score mesmo)
    ranked = rerank(query, candidates, top_k=max(top_k, 10))

    passages = [
        {
            "title": d.get("title"),
            "url": d.get("url"),
            "text": d.get("text"),
            "score": float(d.get("score", 0.0)),
            "section": d.get("section"),
        }
        for d in ranked[:top_k]
    ]

    graph_rows: List[Dict[str, Any]] = []
    if use_graph:
        # Aqui você pode sofisticar com matching de entidades etc.
        # Por enquanto: se a query mencionar "Ventrue", rodamos um exemplo simples.
        if "ventrue" in query.lower():
            cypher = """
            MATCH (c:Entity {type:"Clan"})-[:REL {rel:"HAS_DISCIPLINE"}]->(d:Entity {type:"Discipline"})
            WHERE c.id = "Ventrue"
            RETURN c.id AS clan, collect(d.id)[0..10] AS disciplines
            """
            graph_rows = run_cypher(cypher)

    return {
        "query": query,
        "passages": passages,
        "graph": graph_rows,
    }

@app.get("/graph")
def graph(query: str = Query(..., description="Cypher read-only")):
    """
    Endpoint genérico de Cypher (read-only).
    """
    rows = run_cypher(query)
    return {"query": query, "rows": rows}
