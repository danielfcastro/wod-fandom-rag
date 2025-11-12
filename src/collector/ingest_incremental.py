# src/collector/ingest_incremental.py
import os, sys, time, signal, sqlite3, argparse, math, json
from datetime import datetime
from typing import Iterable, List, Dict, Tuple

from .fandom_api import get_allpages, get_parse  # usa o que você já tem
from .parsers import parse_page_to_passages_and_graph  # supõe função que retorna (passages, nodes, edges)
from .indexers.opensearch_index import bulk_upsert as os_upsert
from .indexers.qdrant_index import upsert_vectors as qd_upsert
from .graph.neo4j_store import upsert_nodes as g_upsert_nodes, upsert_edges as g_upsert_edges
from .embeddings import embed_passages  # já existe no seu QA (ou ajuste para seu caminho)
from .utils.text import strip_text  # util sua p/ limpar

DB_PATH = os.getenv("INGEST_DB_PATH", "checkpoints/ingest.db")
DEFAULT_BATCH = int(os.getenv("INGEST_BATCH_SIZE", "100"))
MAX_RETRIES = int(os.getenv("INGEST_MAX_RETRIES", "3"))

STOP = False
def _signal_handler(signum, frame):
    global STOP
    STOP = True
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

DDL_PAGES = """
CREATE TABLE IF NOT EXISTS pages(
  title TEXT PRIMARY KEY,
  status TEXT NOT NULL,              -- pending | ok | failed | skipped
  tries  INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  updated_at TEXT NOT NULL
);
"""
DDL_META = """
CREATE TABLE IF NOT EXISTS meta(
  key TEXT PRIMARY KEY,
  value TEXT
);
"""

def now_iso(): return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def open_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute(DDL_PAGES)
    con.execute(DDL_META)
    con.commit()
    return con

def meta_set(con, key, value):
    con.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    con.commit()

def meta_get(con, key, default=None):
    cur = con.execute("SELECT value FROM meta WHERE key=?", (key,))
    row = cur.fetchone()
    return row[0] if row else default

def page_set(con, title, status, err=None, inc_try=False):
    tries_inc = ", tries=tries+1" if inc_try else ""
    con.execute(
        f"INSERT INTO pages(title,status,tries,last_error,updated_at) VALUES(?,?,?,?,?) "
        f"ON CONFLICT(title) DO UPDATE SET status=excluded.status, last_error=excluded.last_error, updated_at=excluded.updated_at{tries_inc}",
        (title, status, 0, err, now_iso()),
    )

def seed_pending(con, titles: Iterable[str]):
    cur = con.executemany(
        "INSERT OR IGNORE INTO pages(title,status,tries,last_error,updated_at) VALUES(?,?,?,?,?)",
        [(t, "pending", 0, None, now_iso()) for t in titles]
    )
    con.commit()

def pending_titles(con, limit) -> List[str]:
    cur = con.execute(
        "SELECT title FROM pages WHERE status IN ('pending','failed') AND tries < ? ORDER BY updated_at LIMIT ?",
        (MAX_RETRIES, limit)
    )
    return [r[0] for r in cur.fetchall()]

def backoff(tries: int) -> float:
    return min(60.0, (2 ** (tries-1))) if tries > 0 else 0.0  # 1s,2s,4s,8s,... até 60s

def ingest_batch(titles: List[str]) -> Tuple[int,int,int]:
    """Processa um lote de títulos: coleta -> parse -> indexa"""
    passages_all: List[Dict] = []
    graph_nodes_all: List[Dict] = []
    graph_edges_all: List[Dict] = []

    for title in titles:
        if STOP: break
        try:
            parsed = get_parse(title)  # sections, links, categories, wikitext
            passages, nodes, edges = parse_page_to_passages_and_graph(title, parsed)
            # limpeza mínima
            for p in passages: p["text"] = strip_text(p.get("text",""))
            passages_all.extend(passages)
            graph_nodes_all.extend(nodes)
            graph_edges_all.extend(edges)
            yield ("ok", title, None)
        except Exception as e:
            yield ("error", title, repr(e))

    # indexação em lote (se houver algo)
    if passages_all:
        # embeddings + vetores + upserts
        vectors = embed_passages(passages_all)  # retorna List[List[float]] no mesmo ordenamento
        # OpenSearch: texto/keyword
        os_upsert(passages_all)
        # Qdrant: vetores + payloads
        payloads = [{"title": p["title"], "url": p["url"], "offset": p["offset"]} for p in passages_all]
        qd_upsert(vectors, payloads)

    if graph_nodes_all:
        g_upsert_nodes(graph_nodes_all)
    if graph_edges_all:
        g_upsert_edges(graph_edges_all)

def run(namespace: int, limit: int, batch_size: int, reset: bool, resume: bool):
    con = open_db()
    if reset:
        con.execute("DELETE FROM pages")
        con.execute("DELETE FROM meta")
        con.commit()

    meta_set(con, "namespace", str(namespace))
    meta_set(con, "started_at", meta_get(con, "started_at", now_iso()))

    # Seeding inicial (apenas se ainda não houver páginas)
    cur = con.execute("SELECT COUNT(*) FROM pages")
    count = cur.fetchone()[0]
    if count == 0:
        print(f"[seed] listando páginas via allpages (namespace={namespace}) ...", flush=True)
        titles = list(get_allpages(ap_namespace=namespace, limit=limit or None))
        if limit: titles = titles[:limit]
        seed_pending(con, titles)
        print(f"[seed] {len(titles)} títulos registrados como 'pending'")

    total = int(con.execute("SELECT COUNT(*) FROM pages").fetchone()[0])
    def remaining(): return int(con.execute("SELECT COUNT(*) FROM pages WHERE status IN ('pending','failed') AND tries < ?", (MAX_RETRIES,)).fetchone()[0])

    print(f"[stats] total no checkpoint: {total}, pendentes: {remaining()}", flush=True)

    processed = 0
    while not STOP:
        titles = pending_titles(con, batch_size)
        if not titles:
            print("[done] nada pendente dentro de max_retries — fim.", flush=True)
            break

        # marca tentativa (para backoff) ANTES de processar
        for t in titles:
            con.execute("UPDATE pages SET tries=tries+1, updated_at=? WHERE title=?", (now_iso(), t))
        con.commit()

        # backoff por título (pequeno sleep linear só pra espalhar chamadas)
        time.sleep(0.1)

        ok, err = 0, 0
        for status, title, maybe_err in ingest_batch(titles):
            if STOP: break
            if status == "ok":
                page_set(con, title, "ok")
                ok += 1
            else:
                # volta status='failed', mantém contador tries (já incrementado)
                page_set(con, title, "failed", err=maybe_err)
                # pequeno backoff local por título
                tries = con.execute("SELECT tries FROM pages WHERE title=?", (title,)).fetchone()[0]
                time.sleep(backoff(tries))
                err += 1

        con.commit()
        processed += len(titles)
        print(f"[batch] ok={ok} err={err} | progresso: {processed}/{total} | pendentes: {remaining()}", flush=True)

        if STOP:
            print("[stop] encerrado por sinal — checkpoints salvos.", flush=True)
            break

    print("[summary] status:", flush=True)
    ok_count   = con.execute("SELECT COUNT(*) FROM pages WHERE status='ok'").fetchone()[0]
    fail_count = con.execute("SELECT COUNT(*) FROM pages WHERE status='failed' AND tries>=?", (MAX_RETRIES,)).fetchone()[0]
    pend_count = con.execute("SELECT COUNT(*) FROM pages WHERE status IN ('pending','failed') AND tries<?", (MAX_RETRIES,)).fetchone()[0]
    print(f"  ok={ok_count}  failed(final)={fail_count}  pendentes(retry)={pend_count}")

def main():
    ap = argparse.ArgumentParser(description="Ingestão incremental e resumível (Fandom WoD).")
    ap.add_argument("--namespace", "--ap-namespace", type=int, default=0, help="MediaWiki namespace (0=artigos)")
    ap.add_argument("--limit", type=int, default=0, help="Máximo de páginas para seed inicial (0=sem limite)")
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH)
    ap.add_argument("--reset", action="store_true", help="Zera checkpoint (recomeça do zero)")
    ap.add_argument("--resume", action="store_true", help="Tenta retomar a partir do checkpoint (default)")
    ap.add_argument("--max-retries", type=int, default=MAX_RETRIES)
    args = ap.parse_args()

    global MAX_RETRIES
    MAX_RETRIES = args.max_retries

    run(namespace=args.namespace, limit=args.limit, batch_size=args.batch_size, reset=args.reset, resume=args.resume or True)

if __name__ == "__main__":
    main()
