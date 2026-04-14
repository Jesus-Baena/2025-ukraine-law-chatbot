#!/bin/bash

set -euo pipefail

# SSH tunnel to the Portainer-managed Swarm Postgres service.
#
# This script starts a temporary socat container on the remote Docker host,
# attaches it to the overlay network, and forwards local localhost:5432 to the
# Swarm service DNS name used by this project.

REMOTE_USER="${REMOTE_USER:-sysop}"
REMOTE_HOST="${REMOTE_HOST:-100.86.206.113}"
NETWORK="${NETWORK:-storage-internal}"
SERVICE_DNS="${SERVICE_DNS:-storage_postgres}"
PROXY_CONTAINER="${PROXY_CONTAINER:-rada-db-tunnel-proxy}"
REMOTE_PORT="${REMOTE_PORT:-5433}"
LOCAL_PORT="${LOCAL_PORT:-5432}"

echo "Preparing remote proxy on ${REMOTE_HOST}..."

if command -v ss >/dev/null 2>&1 && ss -ltn "( sport = :${LOCAL_PORT} )" | grep -q LISTEN; then
  echo "Local port ${LOCAL_PORT} is already in use. Stop the conflicting service or set LOCAL_PORT."
  exit 1
fi

ssh "${REMOTE_USER}@${REMOTE_HOST}" "sudo docker rm -f ${PROXY_CONTAINER} 2>/dev/null || true; \
  sudo docker run -d --name ${PROXY_CONTAINER} --rm \
  --network ${NETWORK} \
  -p 127.0.0.1:${REMOTE_PORT}:5432 \
  alpine/socat TCP-LISTEN:5432,fork TCP:${SERVICE_DNS}:5432"

echo "Remote proxy started."
echo "Establishing SSH tunnel: localhost:${LOCAL_PORT} -> ${REMOTE_HOST}:127.0.0.1:${REMOTE_PORT} -> ${SERVICE_DNS}:5432"
echo "Keep this terminal open while using the database."

cleanup() {
  echo
  echo "Stopping remote proxy..."
  ssh "${REMOTE_USER}@${REMOTE_HOST}" "sudo docker stop ${PROXY_CONTAINER}" >/dev/null 2>&1 || true
}

trap cleanup EXIT INT TERM

ssh -N -L "${LOCAL_PORT}:127.0.0.1:${REMOTE_PORT}" "${REMOTE_USER}@${REMOTE_HOST}"