#!/usr/bin/env bash

set -e

CONTAINER=qa
CONTAINER_DIR=/app
HOST_DIR=/mnt/data/projetos/wod-fandom-rag

echo "[INFO] Copiando arquivos do container '$CONTAINER' para '$HOST_DIR'..."
echo

# cria pasta se não existir
mkdir -p "$HOST_DIR"

# copia tudo do /app para o host
docker compose cp $CONTAINER:$CONTAINER_DIR/. "$HOST_DIR"

echo
echo "[INFO] Sincronização concluída!"
echo "Agora você pode rodar: git add . && git commit -m 'Sync from container'"
