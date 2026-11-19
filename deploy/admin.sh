#!/usr/bin/env bash
# Run an admin API call from the server (the admin API is not reachable from the internet).
#   deploy/admin.sh GET  /admin/routes
#   deploy/admin.sh POST /admin/tenants '{"name":"acme"}'
set -euo pipefail
cd "$(dirname "$0")/.."
METHOD="${1:?usage: admin.sh METHOD PATH [json]}"; PATH_="${2:?}"; DATA="${3:-}"
ENV_FILE="${ENV_FILE:-.env.prod}"
COMPOSE=(docker compose -f deploy/docker-compose.prod.yml --env-file "$ENV_FILE")
# $ADMIN_TOKEN is expanded INSIDE the container, so the secret never appears in `ps` on the host.
"${COMPOSE[@]}" exec -T api sh -c \
  'curl -fsS -X "$0" -H "Authorization: Bearer $ADMIN_TOKEN" -H "content-type: application/json" ${2:+-d "$2"} "http://localhost:8000$1"' \
  "$METHOD" "$PATH_" "$DATA"
echo
