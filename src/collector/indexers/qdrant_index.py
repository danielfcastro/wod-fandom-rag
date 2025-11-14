# src/collector/indexers/qdrant_index.py
"""
Stub de integração com Qdrant.

Motivo:
- Evitar download de modelos SentenceTransformer via HuggingFace
  durante o ingest.
- Neste momento só estamos usando OpenSearch para RAG.

Se no futuro você quiser reativar Qdrant, basta restaurar a
implementação original e configurar o serviço.
"""

from typing import Dict, Iterable


def bulk_upsert(passages: Iterable[Dict]):
    """
    Stub: simplesmente consome o iterável (para manter o fluxo igual)
    e não envia nada para lugar nenhum.
    """
    count = 0
    for _ in passages:
        count += 1

    if count:
        print(f"[INFO] Qdrant bulk_upsert desativado — ignorando {count} vetores.")
