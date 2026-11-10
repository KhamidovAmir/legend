#!/usr/bin/env bash
# Registers (or updates) the Debezium outbox connector of resolution-service in Kafka Connect.
# PUT /connectors/{name}/config is idempotent: the same call creates and updates.
# On stands this step is a pipeline job of the platform team; the config file lives in the service repository.
set -euo pipefail
CONNECT_URL=${CONNECT_URL:-http://localhost:8088}
FILE=${1:-services/resolution-service/deploy/debezium/resolution-outbox-connector.json}
DB_HOST=${DB_HOST:-postgres} DB_PORT=${DB_PORT:-5432}
DB_CDC_USER=${DB_CDC_USER:-resolution} DB_CDC_PASSWORD=${DB_CDC_PASSWORD:-resolution}

name=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['name'])" "$FILE")
# only OUR placeholders are substituted; ${routedByValue} belongs to Debezium and must stay as is
config=$(python3 - "$FILE" <<PY
import json, sys
cfg = json.load(open(sys.argv[1]))["config"]
subst = {"\${DB_HOST}": "$DB_HOST", "\${DB_PORT}": "$DB_PORT", "\${DB_CDC_USER}": "$DB_CDC_USER", "\${DB_CDC_PASSWORD}": "$DB_CDC_PASSWORD"}
print(json.dumps({k: subst.get(v, v) for k, v in cfg.items()}))
PY
)
curl -fsS -X PUT -H "Content-Type: application/json" --data "$config" "$CONNECT_URL/connectors/$name/config" > /dev/null
echo "connector '$name' registered; status:"
sleep 3
curl -fsS "$CONNECT_URL/connectors/$name/status"; echo
