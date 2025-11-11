
from typing import List, Dict, Optional
from datetime import datetime, timezone
from neo4j import GraphDatabase
from .settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def list_low_confidence(limit: int = 100) -> List[Dict]:
    q = """
    MATCH (a:Entity)-[r:REL]->(b:Entity)
    WHERE coalesce(r.confidence, 'low') = 'low'
    RETURN a.id AS src, r.rel AS rel, b.id AS dst, r.evidence AS evidence, r.confidence AS confidence
    LIMIT $limit
    """
    with driver.session() as s:
        return [r.data() for r in s.run(q, limit=limit)]

def approve_edge(src: str, rel: str, dst: str, user: str) -> Dict:
    q = """
    MATCH (a:Entity {id:$src})-[r:REL {rel:$rel}]->(b:Entity {id:$dst})
    SET r.confidence = 'high'
    WITH a,b,r
    CREATE (log:AuditLog {ts:$ts, actor:$user, action:'approve', src:$src, rel:$rel, dst:$dst})
    RETURN a.id AS src, r.rel AS rel, b.id AS dst, r.confidence AS confidence
    """
    ts = datetime.now(timezone.utc).isoformat()
    with driver.session() as s:
        return s.run(q, src=src, rel=rel, dst=dst, ts=ts, user=user).single().data()

def delete_edge(src: str, rel: str, dst: str, user: str) -> Dict:
    q = """
    MATCH (a:Entity {id:$src})-[r:REL {rel:$rel}]->(b:Entity {id:$dst})
    DELETE r
    WITH $src AS src, $rel AS rel, $dst AS dst
    CREATE (log:AuditLog {ts:$ts, actor:$user, action:'delete', src:src, rel:rel, dst:dst})
    RETURN src, rel, dst
    """
    ts = datetime.now(timezone.utc).isoformat()
    with driver.session() as s:
        return s.run(q, src=src, rel=rel, dst=dst, ts=ts, user=user).single().data()

def update_edge(src: str, rel: str, dst: str, new_rel: Optional[str], new_dst: Optional[str], confidence: Optional[str], user: str) -> Dict:
    q = """
    MATCH (a:Entity {id:$src})-[r:REL {rel:$rel}]->(b:Entity {id:$dst})
    WITH a,b,r
    CALL {
      WITH r, $new_rel AS nr, $new_dst AS nd
      WITH r, CASE WHEN nr IS NOT NULL AND nr <> '' THEN nr ELSE r.rel END AS rrel, nd
      SET r.rel = rrel
      RETURN r, nd
    }
    WITH a,b,r, nd
    CALL {
      WITH a,b,r, nd
      OPTIONAL MATCH (c:Entity {id:nd})
      WITH a,b,c,r, nd
      FOREACH(_ IN CASE WHEN nd IS NOT NULL AND c IS NOT NULL THEN [1] ELSE [] END |
        DELETE r
      )
      FOREACH(_ IN CASE WHEN nd IS NOT NULL AND c IS NOT NULL THEN [1] ELSE [] END |
        MERGE (a)-[r2:REL {rel:r.rel}]->(c)
        SET r = r2
      )
      RETURN r
    }
    WITH a,r, coalesce($confidence, r.confidence) AS conf, $src AS old_src, $rel AS old_rel, $dst AS old_dst
    SET r.confidence = conf
    WITH r, old_src, old_rel, old_dst
    CREATE (log:AuditLog {ts:$ts, actor:$user, action:'update', src:old_src, rel:old_rel, dst:old_dst, new_rel:r.rel})
    RETURN r.rel AS rel, r.confidence AS confidence
    """
    ts = datetime.now(timezone.utc).isoformat()
    with driver.session() as s:
        return s.run(q, src=src, rel=rel, dst=dst, new_rel=new_rel, new_dst=new_dst, confidence=confidence, ts=ts, user=user).single().data()
