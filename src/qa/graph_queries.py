
from typing import List, Dict
from neo4j import GraphDatabase
from .settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def related_disciplines(clan: str) -> List[Dict]:
    q = """
    MATCH (c:Entity {id: $cid, type:'Clan'})-[:REL {rel:'HAS_DISCIPLINE'}]->(d:Entity {type:'Discipline'})
    RETURN d.name AS name
    """
    with driver.session() as s:
        cid = "-".join(clan.lower().split())
        res = s.run(q, cid=cid)
        return [{"discipline": r["name"]} for r in res]

def factions_of_clan(clan: str) -> List[Dict]:
    q = """
    MATCH (c:Entity {id: $cid, type:'Clan'})-[:REL {rel:'MEMBER_OF'}]->(s:Entity {type:'Sect'})
    RETURN s.name AS name
    """
    with driver.session() as s:
        cid = "-".join(clan.lower().split())
        res = s.run(q, cid=cid)
        return [{"sect": r["name"]} for r in res]
