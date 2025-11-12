mkdir -p tools
cat > tools/ingest_robusto.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail

NS="${1:-0}"           # namespace (0=artigos)
LIMIT="${2:-0}"        # 0 = sem limite na semente inicial
BATCH="${3:-100}"      # tamanho do lote
RETRIES="${4:-3}"      # tentativas por página

echo "[run] ns=$NS limit=$LIMIT batch=$BATCH retries=$RETRIES"
docker compose exec qa bash -lc "python -m src.collector.ingest_incremental --namespace $NS --limit $LIMIT --batch-size $BATCH --max-retries $RETRIES --resume"
SH
chmod +x tools/ingest_robusto.sh
