#!/usr/bin/env bash
set -euo pipefail

# Caminho do DB: pode passar como argumento, senão usa o default
DB_PATH="${1:-checkpoints/ingest.db}"
MAX_RETRIES="${2:-3}"

if [ ! -f "$DB_PATH" ]; then
  echo "ERRO: banco não encontrado em '$DB_PATH'"
  echo "Use: $0 /caminho/para/ingest.db [max_retries]"
  exit 1
fi

echo "Usando DB: $DB_PATH (max_retries = $MAX_RETRIES)"
echo

# 1) Resumo geral por status
echo "== Contagem por status =="
sqlite3 "$DB_PATH" <<'SQL'
.headers on
.mode column
SELECT status, COUNT(*) AS total
FROM pages
GROUP BY status
ORDER BY status;
SQL

echo
echo "== Pendentes dentro de max_retries (pending/failed com tries < max_retries) =="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
SELECT COUNT(*) AS pendentes
FROM pages
WHERE status IN ('pending','failed')
  AND tries < $MAX_RETRIES;
SQL

echo
echo "== Falhas definitivas (status='failed' com tries >= max_retries) =="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
SELECT COUNT(*) AS falhas_definitivas
FROM pages
WHERE status='failed'
  AND tries >= $MAX_RETRIES;
SQL

echo
echo "== OK e SKIPPED =="
sqlite3 "$DB_PATH" <<'SQL'
.headers on
.mode column
SELECT
  (SELECT COUNT(*) FROM pages WHERE status='ok')      AS ok,
  (SELECT COUNT(*) FROM pages WHERE status='skipped') AS skipped;
SQL

echo
echo "== Top 20 com erro (status='failed') =="
sqlite3 "$DB_PATH" <<SQL
.headers on
.mode column
SELECT title, tries, status, substr(last_error,1,120) AS last_error_preview
FROM pages
WHERE status='failed'
ORDER BY updated_at DESC
LIMIT 20;
SQL
