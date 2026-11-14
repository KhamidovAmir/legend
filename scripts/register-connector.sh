#!/usr/bin/env bash
# Registers (or updates) the Debezium outbox connector of resolution-service in Kafka Connect.
# PUT /connectors/{name}/config is idempotent: the same call creates and updates.
# Normally started in a container ("make connector"), so the host needs no bash/curl; it also runs on a Linux/macOS
# host as is. On stands this step is a pipeline job; the config file lives in the service repository.
set -euo pipefail
CONNECT_URL=${CONNECT_URL:-http://localhost:8088}
FILE=${CONNECTOR_FILE:-services/resolution-service/deploy/debezium/resolution-outbox-connector.json}
NAME=${CONNECTOR_NAME:-resolution-outbox}
DB_HOST=${DB_HOST:-postgres} DB_PORT=${DB_PORT:-5432}
DB_CDC_USER=${DB_CDC_USER:-resolution} DB_CDC_PASSWORD=${DB_CDC_PASSWORD:-resolution}

# Only OUR placeholders are substituted; ${routedByValue} belongs to Debezium and must stay as is.
config=$(tr -d '\r' < "$FILE" | sed \
  -e "s|\${DB_HOST}|$DB_HOST|" -e "s|\${DB_PORT}|$DB_PORT|" \
  -e "s|\${DB_CDC_USER}|$DB_CDC_USER|" -e "s|\${DB_CDC_PASSWORD}|$DB_CDC_PASSWORD|")

# Connect may still be starting, and the outbox table appears only after resolution-service ran its migrations.
for attempt in $(seq 1 40); do
  code=$(curl -s -o /tmp/connector-response -w '%{http_code}' -X PUT -H 'Content-Type: application/json' \
    --data "$config" "$CONNECT_URL/connectors/$NAME/config" || true)
  if [ "$code" = 200 ] || [ "$code" = 201 ]; then break; fi
  echo "attempt $attempt: Kafka Connect answered ${code:-nothing}, retrying in 3s"
  sleep 3
done
if [ "$code" != 200 ] && [ "$code" != 201 ]; then
  echo "connector was not registered:"; cat /tmp/connector-response 2>/dev/null; echo; exit 1
fi

# "RUNNING" for the connector is not enough: the task is what actually reads the WAL.
for attempt in $(seq 1 20); do
  status=$(curl -s "$CONNECT_URL/connectors/$NAME/status" || true)
  case "$status" in
    *'"tasks":[{"id":0,"state":"RUNNING"'*) echo "connector '$NAME' is running"; exit 0 ;;
    *'"state":"FAILED"'*) echo "connector task FAILED (is resolution-service started, does table outbox exist?):"; echo "$status"; exit 1 ;;
  esac
  sleep 2
done
echo "connector '$NAME' registered, but its task is not RUNNING yet:"; echo "$status"; exit 1
