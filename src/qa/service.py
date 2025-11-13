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

@app.get("/graph")
def graph(query: str = Query(..., description="Cypher read-only")):
    """
    Endpoint genérico de Cypher (read-only).
    """
    rows = run_cypher(query)
    return {"query": query, "rows": rows}

@app.get("/qa")
def qa(
    query: str,
    top_k: int = 5,
    use_graph: bool = True,
):
    # --- BUSCA HÍBRIDA (já existia) ---
    passages = hybrid(query, k_lex=max(top_k, 10), k_vec=max(top_k, 10))

    # --- GRAFO (já existia) ---
    graph_rows: list[dict] = []
    if use_graph:
        # seja lá como você já está preenchendo graph_rows,
        # mantém essa parte exatamente igual:
        graph_rows = run_cypher(
            """
            MATCH (c:Entity {type:"Clan"})-[:REL {rel:"HAS_DISCIPLINE"}]->(d:Entity {type:"Discipline"})
            RETURN c.id AS clan, collect(d.id)[0..5] AS disciplines
            LIMIT 10
            """
        )

    # --------- NOVO: montar 'answer' pra UI ---------

    graph_answer = None
    if graph_rows:
        first = graph_rows[0]
        clan = first.get("clan") or first.get("id") or first.get("name")
        discs = first.get("disciplines") or first.get("sample")

        if isinstance(discs, list):
            discs_str = ", ".join(discs)
        else:
            discs_str = str(discs) if discs is not None else ""

        if clan and discs_str:
            graph_answer = f"As disciplinas de {clan} são: {discs_str}."

    passage_answer = None
    if passages:
        # melhor trecho textual, caso o grafo não resolva
        passage_answer = passages[0]["text"]

    answer = graph_answer or passage_answer or ""

    return {
        "query": query,
        "answer": answer,
        "passages": passages,
        "graph": graph_rows,
    }
