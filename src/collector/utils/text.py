
import re
from typing import Iterable

def yield_passages(title: str, url: str, sections: Iterable):
    for sec_title, body in sections:
        paras=[p.strip() for p in re.split(r"\n{2,}", body) if p.strip()]
        offset=0
        for p in paras:
            yield {"title":title,"section":sec_title,"url":url,"text":p,"offset":offset}
            offset += len(p) + 2
