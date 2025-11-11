mkdir -p tools
cat > tools/sync_from_container.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
declare -a FILES=(
  "src/collector/graph/neo4j_store.py"
  "src/collector/indexers/opensearch_index.py"
  "src/collector/indexers/qdrant_index.py"
)
for f in "${FILES[@]}"; do
  echo "-> syncing $f"
  docker compose cp qa:/app/$f $f
done
echo "done."
SH
chmod +x tools/sync_from_container.sh
