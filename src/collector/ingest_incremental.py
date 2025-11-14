# src/collector/ingest_incremental.py
import os
import sys
import time
import signal
import sqlite3
import argparse
import subprocess
from datetime import datetime
from typing import Iterable, List

from .fandom_api import iter_allpages

# --- OpenSearch: checar existência por title (keyword) ---
from opensearchpy import OpenSearch

OS_URL = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
OS_INDEX = os.getenv("OPENSEARCH_INDEX", "passages-wod")
_os_client = OpenSearch(OS_URL)


def already_indexed_in_os(title: str) -> bool:
    """
    Retorna True se já existir qualquer doc com title == <title> no índice.
    (title é 'keyword' no mapping, então term query casa exato)
    """
    try:
        body = {"query": {"term": {"title": title}}, "size": 0}
        res = _os_client.search(index=OS_INDEX, body=body)
        return res.get("hits", {}).get("total", {}).get("value", 0) > 0
    except Exception:
        # se OS estiver indisponível, não bloqueia ingestão
        return False


# --- Checkpoint (SQLite) ---
DB_PATH = os.getenv("INGEST_DB_PATH", "checkpoints/ingest.db")
DEFAULT_BATCH = int(os.getenv("INGEST_BATCH_SIZE", "100"))
DEFAULT_MAX_RETRIES = int(os.getenv("INGEST_MAX_RETRIES", "3"))

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


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def open_db():
    # garante diretório
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute(DDL_PAGES)
    con.execute(DDL_META)
    con.commit()
    return con


def meta_set(con, key, value):
    con.execute(
        "INSERT INTO meta(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    con.commit()


def meta_get(con, key, default=None):
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def page_set(con, title, status, err=None, reset_tries=False):
    if reset_tries:
        con.execute(
            """
            INSERT INTO pages(title,status,tries,last_error,updated_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(title) DO UPDATE SET
              status=excluded.status,
              tries=0,
              last_error=excluded.last_error,
              updated_at=excluded.updated_at
            """,
            (title, status, 0, err, now_iso()),
        )
    else:
        con.execute(
            """
            INSERT INTO pages(title,status,tries,last_error,updated_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(title) DO UPDATE SET
              status=excluded.status,
              last_error=excluded.last_error,
              updated_at=excluded.updated_at
            """,
            (title, status, 0, err, now_iso()),
        )


def page_inc_try(con, title):
    con.execute(
        "UPDATE pages SET tries=tries+1, updated_at=? WHERE title=?",
        (now_iso(), title),
    )


def seed_pending(con, titles: Iterable[str]):
    con.executemany(
        "INSERT OR IGNORE INTO pages(title,status,tries,last_error,updated_at) "
        "VALUES(?,?,?,?,?)",
        [(t, "pending", 0, None, now_iso()) for t in titles],
    )
    con.commit()


def pending_titles(con, limit: int, max_retries: int) -> List[str]:
    cur = con.execute(
        """
        SELECT title
        FROM pages
        WHERE status IN ('pending','failed')
          AND tries < ?
        ORDER BY updated_at
        LIMIT ?
        """,
        (max_retries, limit),
    )
    return [r[0] for r in cur.fetchall()]


def process_title_via_cli(title: str):
    """
    Chama o mesmo fluxo que você já usa:

        python -m src.collector.run_ingest --mode title --title <title>

    Se der erro, levanta subprocess.CalledProcessError.
    """
    cmd = [
        sys.executable,
        "-m",
        "src.collector.run_ingest",
        "--mode",
        "title",
        "--title",
        title,
    ]
    subprocess.run(cmd, check=True)


def run(
    namespace: int,
    limit: int,
    batch_size: int,
    reset: bool,
    skip_existing_os: bool,
    max_retries: int,
):
    con = open_db()
    if reset:
        print("[reset] limpando checkpoint (tabelas pages/meta)...", flush=True)
        con.execute("DELETE FROM pages")
        con.execute("DELETE FROM meta")
        con.commit()

    meta_set(con, "namespace", str(namespace))
    meta_set(con, "started_at", meta_get(con, "started_at", now_iso()))

    # Seed inicial (só se vazio)
    count = con.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    if count == 0:
        print(
            f"[seed] listando allpages(ns={namespace}) aplimit=max + nonredirects ...",
            flush=True,
        )
        titles = list(iter_allpages(ap_namespace=namespace, limit=limit or None))
        if limit:
            titles = titles[:limit]
        seed_pending(con, titles)
        print(f"[seed] {len(titles)} títulos pendentes", flush=True)

    def remaining() -> int:
        return int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM pages
                WHERE status IN ('pending','failed')
                  AND tries < ?
                """,
                (max_retries,),
            ).fetchone()[0]
        )

    total = int(con.execute("SELECT COUNT(*) FROM pages").fetchone()[0])
    print(
        f"[stats] total no checkpoint: {total}, pendentes: {remaining()} (max_retries={max_retries})",
        flush=True,
    )

    processed = 0
    while not STOP:
        titles = pending_titles(con, batch_size, max_retries)
        if not titles:
            print(
                "[done] nada pendente dentro de max_retries — fim.",
                flush=True,
            )
            break

        ok = err = skipped = 0

        for title in titles:
            if STOP:
                break

            # marca a tentativa
            page_inc_try(con, title)
            con.commit()

            # pula se já existe no OpenSearch
            if skip_existing_os and already_indexed_in_os(title):
                page_set(con, title, "skipped", reset_tries=True)
                skipped += 1
                continue

            try:
                process_title_via_cli(title)
                page_set(con, title, "ok", reset_tries=True)
                ok += 1
            except Exception as e:
                print(f"[ERROR] falhou em '{title}': {repr(e)}", flush=True)
                page_set(con, title, "failed", err=repr(e))
                err += 1

        con.commit()
        processed += len(titles)
        print(
            f"[batch] ok={ok} skipped={skipped} err={err} | "
            f"progresso: {processed}/{total} | pendentes: {remaining()}",
            flush=True,
        )

        if STOP:
            print("[stop] encerrado por sinal — checkpoints salvos.", flush=True)
            break

    ok_count = con.execute(
        "SELECT COUNT(*) FROM pages WHERE status='ok'"
    ).fetchone()[0]
    skip_count = con.execute(
        "SELECT COUNT(*) FROM pages WHERE status='skipped'"
    ).fetchone()[0]
    fail_count = con.execute(
        "SELECT COUNT(*) FROM pages WHERE status='failed' AND tries>=?",
        (max_retries,),
    ).fetchone()[0]
    pend_count = con.execute(
        "SELECT COUNT(*) FROM pages WHERE status IN ('pending','failed') AND tries<?",
        (max_retries,),
    ).fetchone()[0]

    print(
        f"[summary] ok={ok_count} skipped={skip_count} "
        f"failed(final)={fail_count} pend(retry)={pend_count}",
        flush=True,
    )


def main():
    ap = argparse.ArgumentParser(
        description="Ingestão incremental e resumível (Fandom WoD)."
    )
    ap.add_argument(
        "--namespace",
        "--ap-namespace",
        type=int,
        default=0,
        help="MediaWiki namespace (0=artigos)",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Máximo de páginas para seed inicial (0=sem limite)",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH,
    )
    ap.add_argument(
        "--reset",
        action="store_true",
        help="Zera checkpoint (recomeça do zero)",
    )
    ap.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Máximo de tentativas por página (default={DEFAULT_MAX_RETRIES})",
    )
    ap.add_argument(
        "--skip-existing-os",
        action="store_true",
        default=True,
        help="Pula títulos que já existem no OpenSearch (default: True)",
    )
    args = ap.parse_args()

    run(
        namespace=args.namespace,
        limit=args.limit,
        batch_size=args.batch_size,
        reset=args.reset,
        skip_existing_os=args.skip_existing_os,
        max_retries=args.max_retries,
    )


if __name__ == "__main__":
    main()
