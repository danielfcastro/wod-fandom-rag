# src/qa/service.py
import os
from typing import List, Dict, Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

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
    Continua igual: você manda um Cypher e recebe rows de volta.
    """
    rows = run_cypher(query)
    return {"query": query, "rows": rows}


@app.get("/qa")
def qa(
    query: str,
    top_k: int = 5,
    use_graph: bool = True,
):
    """
    Endpoint principal de QA.

    - Usa busca híbrida (OpenSearch) + reranker.
    - 'answer' vem do melhor trecho recuperado.
    - O grafo é retornado apenas como contexto (por enquanto),
      não como resposta fixa.
    """

    # --- 1) Busca híbrida no OpenSearch/Qdrant ---
    # pegamos um pouco mais que top_k para o reranker poder escolher bem
    k_lex = max(top_k, 10)
    k_vec = max(top_k, 10)

    passages = hybrid(query, k_lex=k_lex, k_vec=k_vec)

    # --- 2) Rerank (hoje só ordena por score, mas já está plugado) ---
    passages = rerank(query, passages, top_k=top_k)

    # --- 3) GRAFO: por enquanto, só como dado bruto opcional ---
    graph_rows: List[Dict[str, Any]] = []
    if use_graph:
        # Ainda não temos NLU -> Cypher automático.
        # Em vez de rodar uma query fixa que ignora a pergunta
        # (e gera respostas erradas), deixamos o grafo vazio aqui.
        #
        # Quando você quiser, podemos:
        #  - detectar intents simples (ex: "disciplinas do clã X")
        #  - montar um Cypher parametrizado
        #  - e preencher graph_rows de forma alinhada à query.
        graph_rows = []

    # --- 4) Montar 'answer' a partir das passagens ---
    if passages:
        best = passages[0]
        raw_text = (best.get("text") or "").strip()

        # Opcional: limitar tamanho pra não jogar um testamento na tela.
        # Ajuste esse número conforme a UI (500, 1000, 1500, etc).
        max_chars = 1200
        if len(raw_text) > max_chars:
            raw_text = raw_text[:max_chars].rsplit(" ", 1)[0] + "..."

        answer = raw_text
    else:
        answer = (
            "Não encontrei nenhum trecho relevante no índice para essa pergunta. "
            "Talvez o artigo ainda não tenha sido ingerido ou o índice precise ser atualizado."
        )

    return {
        "query": query,
        "answer": answer,
        "passages": passages,
        "graph": graph_rows,
    }
