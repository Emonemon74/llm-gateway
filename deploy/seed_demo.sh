#!/usr/bin/env bash
# Create a demo tenant + API key + a model alias pointing at the built-in mock upstream.
# Prints the API key ONCE; store it. Re-running creates another key (tenant name must be unique).
set -euo pipefail
cd "$(dirname "$0")"
NAME="${1:-demo}"
./admin.sh POST /admin/routes '{"alias":"demo","provider":"mock","upstream_model":"demo","priority":0}' >/dev/null
T=$(./admin.sh POST /admin/tenants "{\"name\":\"$NAME\",\"monthly_token_budget\":2000000}")
TID=$(echo "$T" | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
./admin.sh POST "/admin/tenants/$TID/keys" '{"rpm_limit":120,"tpm_limit":200000}'
