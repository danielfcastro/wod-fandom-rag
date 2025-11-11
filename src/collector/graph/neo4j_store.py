import os
from typing import Dict, Iterable, List, Any
from neo4j import GraphDatabase

URI = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')
USER = os.getenv('NEO4J_USER', 'neo4j')
PWD  = os.getenv('NEO4J_PASSWORD', 'please_change_me')
driver = GraphDatabase.driver(URI, auth=(USER, PWD))

# ---------- Sanitização ----------
def _to_primitive(x: Any) -> Any:
    try:
        if isinstance(x, dict):
            v = x.get('text') or x.get('value') or str(x)
            return str(v)[:600]
        if isinstance(x, (list, tuple)):
            out = []
            for item in x:
                p = _to_primitive(item)
                if isinstance(p, (str, int, float, bool)) or p is None:
                    out.append(p if not isinstance(p, str) else p[:600])
                else:
                    out.append(str(p)[:600])
            return out
        if isinstance(x, (str, int, float, bool)) or x is None:
            return x if not isinstance(x, str) else x[:600]
        return str(x)[:600]
    except Exception:
        return str(x)[:600]

def _ev_to_str(ev: Any) -> str:
    v = _to_primitive(ev)
    if isinstance(v, list):
        v = ' | '.join([str(i) for i in v if i is not None])
    return (v or '')[:600]

# ---------- Upserts ----------
def upsert_nodes(nodes: Iterable[Dict]):
    clean_rows: List[Dict] = []
    for n in nodes:
        clean_rows.append({
            'id':      _to_primitive(n.get('id')),
            'name':    _to_primitive(n.get('name')),
            'type':    _to_primitive(n.get('type')),
            'aliases': _to_primitive(n.get('aliases')),
            'line':    _to_primitive(n.get('line')),
            'edition': _to_primitive(n.get('edition')),
            'source':  _to_primitive(n.get('source')),
        })
    q = '''
    UNWIND $rows AS row
    MERGE (n:Entity {id: row.id})
    SET n += {
        name: row.name,
        type: row.type,
        aliases: row.aliases,
        line: row.line,
        edition: row.edition,
        source: row.source
    }
    '''
    with driver.session() as s:
        s.run(q, rows=clean_rows)

def upsert_edges(edges: Iterable[Dict]):
    rows: List[Dict] = []
    for e in edges:
        rows.append({
            'src': _to_primitive(e.get('src')),
            'dst': _to_primitive(e.get('dst')),
            'rel': _to_primitive(e.get('rel')),
            'confidence': _to_primitive(e.get('confidence') or 'low'),
            'evidence': _ev_to_str(e.get('evidence')),
        })
    q = '''
    UNWIND $rows AS row
    MERGE (a:Entity {id: row.src})
    MERGE (b:Entity {id: row.dst})
    MERGE (a)-[r:REL {rel: row.rel}]->(b)
    ON CREATE SET r.evidence = [row.evidence], r.confidence = row.confidence
    ON MATCH  SET r.evidence = coalesce(r.evidence, []) + [row.evidence],
                 r.confidence = row.confidence
    '''
    with driver.session() as s:
        s.run(q, rows=rows)
