
from typing import Dict, List
from .parsers import guess_entity_type
import re

def to_id(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")

def build_node(title: str, cats: List[str], aliases: List[str] = None, line: str = None, editions: List[str] = None, url: str = None) -> Dict:
    return {
        "id": to_id(title),
        "type": guess_entity_type(title, cats),
        "name": title,
        "aliases": aliases or [],
        "line": line,
        "edition": editions or [],
        "source": url,
    }
