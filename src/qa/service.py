
from fastapi import FastAPI, Query, Header, HTTPException
from typing import List, Dict, Any
from .search import hybrid
from .reranker import rerank
from .graph_queries import related_disciplines, factions_of_clan, driver
from .settings import ADMIN_TOKEN

app = FastAPI(title="WoD Fandom RAG - QA Service", version="1.0")

@app.get("/health")
def health(): return {"ok": True}

@app.get("/qa")
def qa(query: str = Query(..., description="Pergunta do usuário"),
       top_k: int = 5, use_graph: bool = True) -> Dict[str, Any]:
    graph_additions: List[Dict] = []
    if use_graph:
        ql = query.lower()
        import re
        m = re.search(r"disciplines? (?:do|da|de) ([a-z0-9\-\s]+)", ql)
        if m: graph_additions = related_disciplines(m.group(1).strip())
        m2 = re.search(r"(?:fac[cç][aã]o|sect) (?:do|da|de) ([a-z0-9\-\s]+)", ql)
        if m2: graph_additions += factions_of_clan(m2.group(1).strip())
    candidates = hybrid(query, k_lex=30, k_vec=30)
    order = rerank(query, candidates, text_key="text", top_k=max(top_k,10))
    picked = []
    for idx, score in order[:top_k]:
        c = dict(candidates[idx]); c["score"]=score; picked.append(c)
    return {"query":query,"answers":[{"text":d.get("text",""),"title":d.get("title"),"section":d.get("section"),"url":d.get("url"),"score":d.get("score",0.0)} for d in picked],"graph":graph_additions}

@app.get("/graph")
def graph(query: str):
    forbidden=["create","merge","delete","set","call dbms","apoc.periodic.commit","load csv"]
    ql=query.strip().lower()
    if any(w in ql for w in forbidden): return {"error":"Mutating or unsafe queries are not allowed."}
    try:
        with driver.session() as s:
            rows=[r.data() for r in s.run(query)]
        return {"query":query,"rows":rows}
    except Exception as e:
        return {"error": str(e)}

def _check_admin(x_admin_token: str | None):
    if not ADMIN_TOKEN or not x_admin_token or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

from .admin import list_low_confidence, approve_edge, delete_edge, update_edge

@app.get("/admin/edges/low")
def admin_list_low(limit: int = 100, x_admin_token: str | None = Header(default=None)):
    _check_admin(x_admin_token); return {"items": list_low_confidence(limit=limit)}

@app.post("/admin/edges/approve")
def admin_approve(src: str, rel: str, dst: str, x_admin_token: str | None = Header(default=None)):
    _check_admin(x_admin_token); return approve_edge(src, rel, dst, user="admin")

@app.post("/admin/edges/delete")
def admin_delete(src: str, rel: str, dst: str, x_admin_token: str | None = Header(default=None)):
    _check_admin(x_admin_token); return delete_edge(src, rel, dst, user="admin")

@app.post("/admin/edges/update")
def admin_update(src: str, rel: str, dst: str, new_rel: str | None = None, new_dst: str | None = None, confidence: str | None = None, x_admin_token: str | None = Header(default=None)):
    _check_admin(x_admin_token); return update_edge(src, rel, dst, new_rel, new_dst, confidence, user="admin")
