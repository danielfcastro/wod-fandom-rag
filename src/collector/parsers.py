
from typing import Dict, List, Tuple
import re, mwparserfromhell

INFOBOX_KEYS = {
    "clan": ["clan", "Clan"],
    "sect": ["sect", "Sect", "Affiliation", "Affiliations"],
    "disciplines": ["disciplines", "Disciplines"],
    "bloodline_of": ["bloodline of", "Bloodline of", "Parent clan"],
    "appears_in": ["first appearance", "appears in", "Appearances"],
    "aliases": ["aka", "aliases", "Alias", "Nicknames"],
}

def clean_text(s: str) -> str: return re.sub(r"\s+", " ", s or "").strip()

def parse_infobox(wikitext: str) -> Dict:
    out: Dict[str, str] = {}
    code = mwparserfromhell.parse(wikitext or "")
    for tmpl in code.filter_templates():
        name = str(tmpl.name).strip().lower()
        if "infobox" in name:
            for k_norm, variants in INFOBOX_KEYS.items():
                for v in variants:
                    if tmpl.has(v):
                        val = tmpl.get(v).value.strip_code().strip()
                        if val: out[k_norm] = clean_text(str(val)); break
            break
    return out

def extract_sections(wikitext: str) -> List[Tuple[str, str]]:
    code = mwparserfromhell.parse(wikitext or ""); text = code.strip_code()
    sections = re.split(r"\n={2,}\s*(.+?)\s*={2,}\n", text)
    out=[]; 
    if sections:
        lead = sections[0].strip()
        if lead: out.append(("Lead", lead))
        for i in range(1, len(sections), 2):
            title = sections[i].strip()
            body = (sections[i+1] if i+1 < len(sections) else "").strip()
            if body: out.append((title, body))
    else:
        out.append(("Lead", text.strip()))
    return out

def guess_entity_type(title: str, cats: List[str]) -> str:
    t=title.lower(); cats_l=[c.lower() for c in cats]
    if "clan" in t or any("clans" in c for c in cats_l): return "Clan"
    if "discipline" in t or any("disciplines" in c for c in cats_l): return "Discipline"
    if "sect" in t or any("sects" in c for c in cats_l) or any(x in c for c in cats_l for x in ["camarilla","sabbat","anarchs"]): return "Sect"
    if "bloodline" in t or any("bloodlines" in c for c in cats_l): return "Bloodline"
    if any("vampire" in c for c in cats_l) or "npc" in t: return "NPC"
    if any("locations" in c for c in cats_l): return "Location"
    if any("books" in c for c in cats_l) or "book" in t: return "Book"
    if any(x in c for c in cats_l for x in ["v5","v20","edition"]): return "Edition"
    return "Entity"

def extract_relations(title: str, infobox: Dict, cats: List[str], sections: List[Tuple[str,str]]) -> List[Dict]:
    rels=[]
    def add(src, rel, dst, evidence, confidence="high"):
        rels.append({"src": src, "rel": rel, "dst": dst, "evidence": evidence, "confidence": confidence})
    def to_id(name: str) -> str:
        import re; return re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")
    title_id = to_id(title)
    sect_val = infobox.get("sect") or ""
    if sect_val:
        import re
        for s in re.split(r"[;,/]| and ", sect_val, flags=re.I):
            s=s.strip(); 
            if s: add(title_id,"MEMBER_OF",to_id(s),{"type":"infobox","text":sect_val})
    discs = infobox.get("disciplines") or ""
    if discs:
        import re
        for d in re.split(r"[;,/]| and ", discs, flags=re.I):
            d=d.strip()
            if d: add(title_id,"HAS_DISCIPLINE",to_id(d),{"type":"infobox","text":discs})
    blof = infobox.get("bloodline_of") or ""
    if blof: add(title_id,"DERIVES_FROM",to_id(blof.strip()),{"type":"infobox","text":blof})
    app = infobox.get("appears_in") or ""
    if app:
        import re
        for b in re.split(r"[;,/]| and ", app, flags=re.I):
            b=b.strip()
            if b: add(title_id,"APPEARS_IN",to_id(b),{"type":"infobox","text":app})
    if sections:
        lead = sections[0][1][:400]
        import re
        m = re.search(r"\b(Camarilla|Sabbat|Anarchs?)\b", lead, flags=re.I)
        if m: add(title_id,"MEMBER_OF",to_id(m.group(1)),{"type":"text","text":lead},"low")
    return rels
